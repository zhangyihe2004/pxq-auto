from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlencode

from playwright.async_api import APIResponse

from .auth import AuthGuard, request_context
from .service import ON_SALE_STATUSES, PREWARM_SECONDS, TERMINAL_STATUSES
from .site import PiaoxingqiuPage, is_success_payload


INTENSIVE_SECONDS = 3
STATUS_POLL_SECONDS = 0.25
WARM_POLL_SECONDS = 1.0
FAR_POLL_SECONDS = 30.0


@dataclass(frozen=True)
class SaleState:
    show_status: str
    session_status: str
    sale_time_ms: int | None
    server_time_ms: int

    @property
    def on_sale(self) -> bool:
        return self.session_status in ON_SALE_STATUSES

    @property
    def remaining_seconds(self) -> float | None:
        if self.sale_time_ms is None:
            return None
        return (self.sale_time_ms - self.server_time_ms) / 1000


class SaleGate:
    def __init__(self, site: PiaoxingqiuPage) -> None:
        self.site = site
        self.show_id = site.show_id
        _, self.session_id = site.booking_ids
        root = f"{site.origin}/cyy_gatewayapi/show/pub"
        common = urlencode(request_context())
        self.show_url = f"{root}/v5/show/{self.show_id}/dynamic?{common}"
        self.session_url = (
            f"{root}/v5/show/{self.show_id}/sessions?"
            f"source=FROM_QUICK_ORDER&src=WEB&{common}"
        )

    async def fetch(self) -> SaleState:
        payloads, now_ms = await self._fetch_payloads((self.show_url, self.session_url))
        dictionaries = [item for payload in payloads for item in _walk_dicts(payload)]
        session_items = self._session_items(dictionaries)
        session_status = _first_text(
            session_items,
            "sessionStatus",
            "bizSessionStatus",
        )
        if session_status == "UNKNOWN":
            raise RuntimeError("开售状态接口未找到 booking_url 对应的目标场次")
        session_sale_time = _first_millis(
            session_items,
            "sessionSaleTime",
            "saleTime",
            "saleStartTime",
            "startSaleTime",
        )
        return SaleState(
            show_status=_first_text(dictionaries, "showDetailStatus"),
            session_status=session_status,
            sale_time_ms=session_sale_time
            or _unique_millis(
                dictionaries,
                "sessionSaleTime",
                "saleTime",
                "saleStartTime",
                "startSaleTime",
            ),
            server_time_ms=now_ms,
        )

    async def _refresh_session(self, previous: SaleState) -> SaleState:
        payloads, now_ms = await self._fetch_payloads((self.session_url,))
        session_status = _first_text(
            self._session_items(list(_walk_dicts(payloads[0]))),
            "sessionStatus",
            "bizSessionStatus",
        )
        if session_status == "UNKNOWN":
            raise RuntimeError("场次状态轮询未找到目标场次")
        return SaleState(
            show_status=previous.show_status,
            session_status=session_status,
            sale_time_ms=previous.sale_time_ms,
            server_time_ms=now_ms,
        )

    async def _fetch_payloads(
        self, urls: tuple[str, ...]
    ) -> tuple[list[dict[str, Any]], int]:
        responses = await asyncio.gather(
            *(self.site.page.context.request.get(url) for url in urls)
        )
        payloads = []
        server_times = []
        for response in responses:
            self._check_response(response)
            payload = await response.json()
            if not is_success_payload(payload):
                raise RuntimeError("开售状态接口返回异常业务状态")
            payloads.append(payload)
            if server_time := _server_time_ms(response):
                server_times.append(server_time)
        return payloads, max(server_times, default=int(time.time() * 1000))

    def _session_items(
        self, dictionaries: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return [
            item
            for item in dictionaries
            if str(
                item.get("bizShowSessionId")
                or item.get("showSessionId")
                or item.get("sessionId")
                or ""
            )
            == self.session_id
        ]

    async def wait_until_prewarm(self, state: SaleState, auth: AuthGuard) -> SaleState:
        if state.on_sale:
            return state
        self._check_terminal(state)
        remaining = state.remaining_seconds
        if remaining is None:
            raise RuntimeError("官方接口未返回开售时间，无法进入预抢票模式")
        loop = asyncio.get_running_loop()
        next_auth = loop.time() + auth.interval(remaining)
        while remaining > PREWARM_SECONDS:
            await asyncio.sleep(
                min(
                    FAR_POLL_SECONDS,
                    remaining - PREWARM_SECONDS,
                    max(0, next_auth - loop.time()),
                )
            )
            state = await self._refresh_session(state)
            if state.on_sale:
                await auth.require_valid(allow_refresh=True)
                return state
            self._check_terminal(state)
            remaining = state.remaining_seconds
            if remaining is None:
                raise RuntimeError("等待期间官方接口不再返回开售时间")
            if loop.time() >= next_auth:
                await auth.ensure()
                next_auth = loop.time() + auth.interval(remaining)
        return state

    async def wait_until_sale(self, state: SaleState, auth: AuthGuard) -> SaleState:
        loop = asyncio.get_running_loop()
        next_auth = loop.time() + 10
        final_auth = False
        while True:
            state = await self._refresh_session(state)
            remaining = state.remaining_seconds
            if state.on_sale:
                if not final_auth:
                    await auth.require_valid(
                        allow_refresh=remaining is None or remaining > INTENSIVE_SECONDS
                    )
                return state
            self._check_terminal(state)
            now = loop.time()
            if remaining is not None and remaining <= INTENSIVE_SECONDS:
                if not final_auth:
                    await auth.require_valid(allow_refresh=False)
                    final_auth = True
            elif not final_auth and now >= next_auth:
                await auth.require_valid(allow_refresh=True)
                next_auth = now + 10
            delay = (
                STATUS_POLL_SECONDS
                if remaining is None or remaining <= INTENSIVE_SECONDS
                else min(WARM_POLL_SECONDS, remaining - INTENSIVE_SECONDS)
            )
            await asyncio.sleep(max(STATUS_POLL_SECONDS, delay))

    @staticmethod
    def _check_response(response: APIResponse) -> None:
        if response.status in {401, 429, 469}:
            raise RuntimeError(
                f"开售状态接口触发限制（HTTP {response.status}），已停止"
            )
        if not response.ok:
            raise RuntimeError(f"开售状态接口返回 HTTP {response.status}")

    @staticmethod
    def _check_terminal(state: SaleState) -> None:
        if (
            state.show_status in TERMINAL_STATUSES
            or state.session_status in TERMINAL_STATUSES
        ):
            status = (
                state.session_status
                if state.session_status in TERMINAL_STATUSES
                else state.show_status
            )
            raise RuntimeError(f"场次已不可售（{status}）")


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk_dicts(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_dicts(item)


def _first_text(items: list[dict[str, Any]], *keys: str) -> str:
    for item in items:
        for key in keys:
            value = item.get(key)
            if isinstance(value, str) and value:
                return value.upper()
    return "UNKNOWN"


def _first_millis(items: list[dict[str, Any]], *keys: str) -> int | None:
    for item in items:
        for key in keys:
            value = item.get(key)
            if isinstance(value, (int, float)) and value > 1_000_000_000_000:
                return int(value)
    return None


def _unique_millis(items: list[dict[str, Any]], *keys: str) -> int | None:
    values = {
        int(value)
        for item in items
        for key in keys
        if isinstance((value := item.get(key)), (int, float))
        and value > 1_000_000_000_000
    }
    return values.pop() if len(values) == 1 else None


def _server_time_ms(response: APIResponse) -> int | None:
    value = response.headers.get("date")
    if not value:
        return None
    try:
        return int(parsedate_to_datetime(value).timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None
