from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeGuard, TypeVar
from urllib.parse import parse_qs, urlsplit

from playwright.async_api import Locator, Page, Request, Response

from .config import AppConfig, AudienceConfig
from .guard import CREATE_MARKERS

if TYPE_CHECKING:
    from .inventory import SeatSelection


SUBMIT_LABEL = "去支付"
AUDIENCE_ALREADY_PURCHASED_CODE = "27902319"
SEAT_LOST_CODE = "22035010"
POLL_INTERVAL_MS = 250
T = TypeVar("T")


def is_success_payload(payload: object) -> TypeGuard[dict[str, Any]]:
    if not isinstance(payload, dict):
        return False
    status = payload.get("statusCode")
    return status is None or str(status) == "200"


class PiaoxingqiuPage:
    def __init__(self, page: Page, config: AppConfig) -> None:
        self.page = page
        self.config = config
        self._pick_seat_url: str | None = None
        self.prefilled_plan_id: str | None = None
        booking = urlsplit(config.project.booking_url)
        self._origin = f"{booking.scheme}://{booking.netloc}"
        query = parse_qs(booking.query)
        self._show_id = (
            query.get("showId") or [booking.path.rstrip("/").rsplit("/", 1)[-1]]
        )[0]
        self._session_id = (query.get("saleShowSessionId") or [""])[0]
        if not re.fullmatch(r"[0-9a-fA-F]{24}", self._show_id) or (
            self._session_id and not re.fullmatch(r"[0-9a-fA-F]{24}", self._session_id)
        ):
            raise RuntimeError("booking_url 缺少有效 showId 或 saleShowSessionId")

    @property
    def show_id(self) -> str:
        return self._show_id

    @property
    def origin(self) -> str:
        return self._origin

    async def resolve_booking_ids(self) -> tuple[str, str]:
        if self._session_id:
            return self._show_id, self._session_id
        endpoint = (
            f"{self._origin}/cyy_gatewayapi/show/pub/v3/show/"
            f"{self._show_id}/sessions_static_data"
        )
        response = await self.page.context.request.get(endpoint)
        if not response.ok:
            raise RuntimeError(f"场次接口返回 HTTP {response.status}")
        payload = await response.json()
        if not is_success_payload(payload):
            raise RuntimeError("场次接口返回异常业务状态")
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        matches = [
            str(item.get("bizShowSessionId") or "")
            for item in data.get("sessionVOs", [])
            if isinstance(item, dict)
            and str(item.get("sessionName") or "") == self.config.purchase.session
        ]
        matches = list(
            dict.fromkeys(
                item for item in matches if re.fullmatch(r"[0-9a-fA-F]{24}", item)
            )
        )
        if len(matches) != 1:
            raise RuntimeError("公开场次接口未唯一匹配配置的完整场次名")
        self._session_id = matches[0]
        return self._show_id, self._session_id

    async def open_purchase(self) -> None:
        await self._goto(self.config.project.booking_url)

    async def reopen_seat_map(self) -> None:
        if self._pick_seat_url is None:
            raise RuntimeError("尚未记录可复用的选座地址")
        await self._goto(self._pick_seat_url)
        if urlsplit(self.page.url).path != "/pick-seat":
            raise RuntimeError("原选座地址已失效")

    async def _goto(self, url: str) -> None:
        await self.page.goto(url, wait_until="domcontentloaded")

        async def body_ready() -> bool | None:
            ready = await self.page.evaluate(
                "() => Boolean(document.body && document.body.innerText.trim())"
            )
            return True if ready else None

        await self._poll(body_ready)

    async def prepare_order(
        self,
        selection: SeatSelection,
        audiences: tuple[AudienceConfig, ...] | None = None,
        *,
        open_map: bool = True,
    ) -> Locator:
        from .seat_map import select_seats

        selected_audiences = (
            self.config.purchase.audiences if audiences is None else audiences
        )
        if len(selected_audiences) != len(selection.candidates):
            raise RuntimeError("观演人数与目标座位数不一致")
        confirm = await select_seats(self, selection, open_map=open_map)
        await self._confirm_selected_seat(confirm)
        await self._select_audiences(selected_audiences)

        async def find_submit() -> Locator | None:
            return await self._find_exact(SUBMIT_LABEL)

        submit = await self._poll(find_submit)
        if submit is None:
            body = _redact_preview(
                await self.page.locator("body").inner_text(),
                limit=500,
            )
            raise RuntimeError(
                f"确认选座后未到达提交订单页：{self.page.url}；页面：{body}"
            )
        return submit

    async def _select_audiences(self, audiences: tuple[AudienceConfig, ...]) -> None:
        async def find_audience(name: str, masked_id: str) -> Locator | None:
            for audience in await self._exact_candidates(name):
                if await audience.evaluate(
                    r"""(element, expected) => {
                        for (let node = element, depth = 0;
                             node && depth < 5;
                             node = node.parentElement, depth++) {
                            const text = String(node.innerText || '').replace(/\s+/g, '');
                            if (text.includes(expected)) return true;
                        }
                        return false;
                    }""",
                    masked_id,
                ):
                    return audience
            return None

        targets = {(item.name, item.masked_id) for item in audiences}
        located: dict[tuple[str, str], Locator] = {}
        for configured in self.config.purchase.audiences:

            async def find_configured() -> Locator | None:
                return await find_audience(configured.name, configured.masked_id)

            audience = await self._poll(find_configured)
            if audience is None:
                raise RuntimeError(f"订单页等待观演人“{configured.name}”超时")
            key = (configured.name, configured.masked_id)
            located[key] = audience
            if await _is_audience_selected(audience) and key not in targets:
                await self._click_action(audience)

        for configured in audiences:
            audience = located[(configured.name, configured.masked_id)]
            if not await _is_audience_selected(audience):
                await self._click_action(audience)

        async def audience_selected() -> bool | None:
            for key, audience in located.items():
                if await _is_audience_selected(audience) != (key in targets):
                    return None
            body = _normalize(await self.page.locator("body").inner_text())
            quantity = len(audiences)
            return True if f"已选{quantity}/{quantity}" in body else None

        if await self._poll(audience_selected) is None:
            raise RuntimeError("订单页实际选中的观演人与配置不完全一致")

    async def find_created_order_id(self) -> str | None:
        async def find() -> str | None:
            body = await self.page.locator("body").inner_text()
            matched = re.search(r"订单编号\s*[:：]\s*([A-Za-z0-9-]+)", body)
            return matched.group(1) if matched else None

        return await self._poll(find)

    async def _open_seat_map(self, plan_id: str, plan_name: str) -> None:
        session = await self._poll(
            lambda: self._find_exact(self.config.purchase.session)
        )
        if session is None:
            raise RuntimeError("booking 页等待配置场次超时")
        async with self._expect_seat_plans_request() as request_info:
            await self._click_action(session)
        await self._check_finished_request(await request_info.value)
        await self.page.evaluate("() => new Promise(requestAnimationFrame)")

        selected_plan = await self._selected_configured_plan()
        wrong_prefill = (
            self.prefilled_plan_id is not None and self.prefilled_plan_id != plan_id
        )
        if wrong_prefill or (selected_plan is not None and selected_plan != plan_name):
            target_plan = await self._poll(lambda: self._find_exact(plan_name))
            if target_plan is None or await _is_disabled(target_plan):
                raise RuntimeError(f"booking 页无法切换到目标票档“{plan_name}”")
            await self._click_action(target_plan)

        async def find_go_pick() -> Locator | None:
            candidate = await self._find_exact("去选座")
            return (
                candidate
                if candidate is not None and not await _is_disabled(candidate)
                else None
            )

        go_pick = await self._poll(find_go_pick)
        if go_pick is None:
            raise RuntimeError("booking 页等待可用的“去选座”按钮超时")
        pages_before = set(self.page.context.pages)
        url_before = self.page.url
        action = self._action_target(go_pick)
        target = action if await action.count() else go_pick
        async with self._expect_dynamic_request(plan_id) as request_info:
            await target.evaluate("element => element.click()")
            if not await self._wait_for_transition(pages_before, url_before):
                raise RuntimeError("点击“去选座”后等待页面变化超时")
        await self._check_finished_request(await request_info.value)
        if urlsplit(self.page.url).path != "/pick-seat":
            raise RuntimeError("进入选座流程后未到达官方选座页")
        self._pick_seat_url = self.page.url

    async def _selected_configured_plan(self) -> str | None:
        for name in self.config.purchase.plans:
            candidate = await self._find_exact(name)
            if candidate is not None and await _is_selected(candidate):
                return name
        return None

    def _expect_dynamic_request(self, plan_id: str):
        show_id, session_id = self._booking_ids()
        expected_path = (
            f"/cyy_gatewayapi/show/buyer/v5/show/{show_id}/session/"
            f"{session_id}/seating/dynamic"
        )

        def matches(request: Request) -> bool:
            parsed = urlsplit(request.url)
            query = parse_qs(parsed.query)
            plans = {
                item
                for value in query.get("bizSeatPlanIds", ())
                for item in value.split(",")
            }
            return (
                request.method.upper() == "GET"
                and parsed.path == expected_path
                and plan_id in plans
            )

        return self.page.context.expect_event(
            "requestfinished",
            predicate=matches,
            timeout=self.config.browser.timeout_ms,
        )

    def _expect_seat_plans_request(self):
        show_id, session_id = self._booking_ids()

        def matches(request: Request) -> bool:
            parsed = urlsplit(request.url)
            query = parse_qs(parsed.query)
            return (
                request.method.upper() == "GET"
                and parsed.path
                == "/cyy_gatewayapi/show/buyer/v5/show/session/seat_plans/dynamic"
                and (query.get("showId") or [""])[0] == show_id
                and (query.get("sessionId") or [""])[0] == session_id
            )

        return self.page.context.expect_event(
            "requestfinished",
            predicate=matches,
            timeout=self.config.browser.timeout_ms,
        )

    def _booking_ids(self) -> tuple[str, str]:
        if not self._session_id:
            raise RuntimeError("尚未解析目标场次 ID")
        return self._show_id, self._session_id

    async def _check_finished_request(self, request: Request) -> None:
        response = await request.response()
        if response is None:
            raise RuntimeError("动态座位请求未返回响应")
        if not response.ok:
            raise RuntimeError(f"动态座位接口返回 HTTP {response.status}")

    async def _confirm_selected_seat(self, confirm: Locator) -> None:
        pages_before = set(self.page.context.pages)
        url_before = self.page.url
        body_before = _normalize(await self.page.locator("body").inner_text())
        await self._click_action(confirm)
        if not await self._wait_for_transition(
            pages_before,
            url_before,
            body_before=body_before,
        ):
            raise RuntimeError("点击“确认选座”后等待页面变化超时")

    async def _poll(
        self,
        finder: Callable[[], Awaitable[T | None]],
    ) -> T | None:
        deadline = (
            asyncio.get_running_loop().time() + self.config.browser.timeout_ms / 1000
        )
        while asyncio.get_running_loop().time() < deadline:
            result = await finder()
            if result is not None:
                return result
            await self.page.wait_for_timeout(POLL_INTERVAL_MS)
        return None

    async def _wait_for_transition(
        self,
        pages_before: set[Page],
        url_before: str,
        *,
        body_before: str | None = None,
    ) -> bool:
        deadline = (
            asyncio.get_running_loop().time() + self.config.browser.timeout_ms / 1000
        )
        while asyncio.get_running_loop().time() < deadline:
            new_pages = [
                page for page in self.page.context.pages if page not in pages_before
            ]
            if new_pages:
                self.page = new_pages[-1]
                await self.page.wait_for_load_state("domcontentloaded")
                return True
            if self.page.url != url_before:
                return True
            if body_before is not None:
                body = _normalize(await self.page.locator("body").inner_text())
                if body != body_before:
                    return True
            await self.page.wait_for_timeout(POLL_INTERVAL_MS)
        return False

    async def _click_action(self, locator: Locator) -> None:
        action = self._action_target(locator)
        target = action if await action.count() else locator
        await target.click()

    def _action_target(self, locator: Locator) -> Locator:
        return locator.locator(
            "xpath=ancestor-or-self::*[self::button or self::a or "
            "self::uni-button or @role='button' or contains(@class, 'button') or "
            "contains(@class, 'btn') or contains(@class, 'buy') or "
            "contains(@class, 'item')][1]"
        )

    async def _find_exact(self, text: str) -> Locator | None:
        candidates = await self._exact_candidates(text)
        return candidates[0] if candidates else None

    async def _exact_candidates(self, text: str) -> list[Locator]:
        locator = self.page.get_by_text(text, exact=True)
        candidates: list[Locator] = []
        for index in range(await locator.count()):
            candidate = locator.nth(index)
            if await candidate.is_visible():
                candidates.append(candidate)
        return candidates


@dataclass(frozen=True)
class CreateResult:
    success: bool
    order_id: str | None
    http_status: int
    code: str | None
    sub_code: str | None
    message: str | None


class CreateResponseWatcher:
    def __init__(self) -> None:
        self._responses: asyncio.Queue[Response] = asyncio.Queue()

    async def handle(self, response: Response) -> None:
        if response.request.method.upper() != "POST":
            return
        response_url = response.url.lower()
        if not any(marker in response_url for marker in CREATE_MARKERS):
            return
        self._responses.put_nowait(response)

    async def wait(self, timeout_seconds: float) -> CreateResult:
        response = await asyncio.wait_for(
            self._responses.get(), timeout=timeout_seconds
        )
        try:
            payload = await response.json()
        except Exception:
            payload = None
        order_id = _find_order_id(payload)
        code = _find_scalar(payload, ("code", "statusCode", "errorCode"))
        message = (
            _redact_preview(
                _find_scalar(
                    payload,
                    ("message", "msg", "errorMessage", "errorMsg", "comments", "desc"),
                )
                or "",
                limit=300,
            )
            or None
        )
        return CreateResult(
            success=response.ok
            and (
                _response_success(payload, order_id)
                or (code == "200" and message == "成功")
            ),
            order_id=order_id,
            http_status=response.status,
            code=code,
            sub_code=_find_scalar(payload, ("subCode", "sub_code", "bizCode")),
            message=message,
        )


async def _is_disabled(locator: Locator) -> bool:
    return bool(
        await locator.evaluate(
            """element => {
                for (let node = element, depth = 0;
                     node && depth < 6; node = node.parentElement, depth++) {
                    const cls = String(node.className || '').toLowerCase();
                    if (node.disabled || node.getAttribute('aria-disabled') === 'true' ||
                        /(disabled|inactive)/.test(cls)) return true;
                }
                return false;
            }"""
        )
    )


async def _is_selected(locator: Locator) -> bool:
    return bool(
        await locator.evaluate(
            r"""element => {
                for (let node = element, depth = 0;
                     node && depth < 6; node = node.parentElement, depth++) {
                    if (node.getAttribute('aria-checked') === 'true') return true;
                    if (node.matches('input:checked') || node.querySelector('input:checked')) {
                        return true;
                    }
                    const cls = String(node.className || '').toLowerCase();
                    if (/(^|\s)(selected|checked|active)(\s|$)/.test(cls)) return true;
                }
                return false;
            }"""
        )
    )


async def _is_audience_selected(locator: Locator) -> bool:
    return bool(
        await locator.evaluate(
            r"""element => {
                for (let node = element, depth = 0;
                     node && depth < 6; node = node.parentElement, depth++) {
                    if (String(node.className || '').includes('audiences-label')) {
                        return Boolean(node.querySelector('.icon-xuanzhong'));
                    }
                }
                return false;
            }"""
        )
    )


def _normalize(value: str) -> str:
    return re.sub(r"\s+", "", value or "")


def _redact_preview(value: str, limit: int = 1200) -> str:
    preview = re.sub(r"\s+", " ", value).strip()[:limit]
    preview = re.sub(r"(?<!\d)1\d{10}(?!\d)", "1**********", preview)
    return re.sub(
        r"(?<![0-9Xx])\d{6}(?:19|20)\d{2}\d{2}\d{2}\d{3}[0-9Xx](?![0-9Xx])",
        "******************",
        preview,
    )


def find_already_purchased_ids(value: str) -> tuple[str, ...]:
    normalized = _normalize(value)
    if "已购买过" not in normalized or "请更换其他实名信息" not in normalized:
        return ()
    return tuple(
        dict.fromkeys(re.findall(r"(?<!\d)\d{3,6}\*+[0-9Xx]{4}(?![0-9Xx])", normalized))
    )


def is_audience_already_purchased(result: CreateResult) -> bool:
    return result.http_status == 200 and result.code == AUDIENCE_ALREADY_PURCHASED_CODE


def is_seat_lost(result: CreateResult) -> bool:
    return (
        result.http_status == 200
        and result.code == SEAT_LOST_CODE
        and result.message == "手慢了一步，票被抢走啦"
    )


def match_configured_ids(
    reported_ids: tuple[str, ...], configured_ids: tuple[str, ...]
) -> tuple[str, ...]:
    matched = []
    for configured in configured_ids:
        prefix = configured[:3]
        suffix = configured[-4:]
        if any(
            item.startswith(prefix) and item.endswith(suffix) for item in reported_ids
        ):
            matched.append(configured)
    return tuple(matched)


def _response_success(payload: object, order_id: str | None) -> bool:
    if not isinstance(payload, dict):
        return False
    code = payload.get("code", payload.get("statusCode"))
    if code is not None and str(code) not in {"0", "200", "200000"}:
        return False
    return payload.get("success") is True or order_id is not None


def _find_scalar(payload: object, keys: tuple[str, ...]) -> str | None:
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, (str, int, float, bool)):
                return str(value)
        for value in payload.values():
            found = _find_scalar(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _find_scalar(value, keys)
            if found is not None:
                return found
    return None


def _find_order_id(payload: object) -> str | None:
    if isinstance(payload, dict):
        for key in (
            "orderId",
            "order_id",
            "tradeOrderId",
            "orderNo",
            "orderNumber",
            "tradeOrderNo",
        ):
            value = payload.get(key)
            if isinstance(value, (str, int)) and str(value):
                return str(value)
        for key in ("data", "result"):
            found = _find_order_id(payload.get(key))
            if found:
                return found
    return None
