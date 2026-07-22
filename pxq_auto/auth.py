from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlencode

from playwright.async_api import Request

from .site import PiaoxingqiuPage


class AuthenticationError(RuntimeError):
    pass


class AuthenticationRequired(AuthenticationError):
    pass


class AuthGuard:
    def __init__(
        self,
        site: PiaoxingqiuPage,
    ) -> None:
        self.site = site
        self.headers: dict[str, str] = {}
        self.show_user_data: dict[str, Any] = {}
        self._verified_at = 0.0
        root = f"{site.origin}/cyy_gatewayapi/show"
        self.endpoint = f"{root}/buyer/v5/show/{site.show_id}/show_user"

    async def ensure(self) -> None:
        if self.headers and await self.check():
            return
        await self.refresh()

    async def refresh(self) -> None:
        try:
            headers = await capture_authenticated_headers(self.site)
        except AuthenticationRequired:
            raise AuthenticationRequired("登录状态无效，请通过飞书重新登录") from None
        self.headers = headers
        if not await self.check():
            raise AuthenticationRequired("登录状态无效，请通过飞书重新登录")

    async def require_valid(self, *, allow_refresh: bool) -> None:
        if await self.check():
            return
        if allow_refresh:
            await self.refresh()
            return
        raise AuthenticationRequired("登录状态失效，请通过飞书重新登录")

    async def require_recent(self, max_age: float = 5.0) -> None:
        if asyncio.get_running_loop().time() - self._verified_at <= max_age:
            return
        await self.require_valid(allow_refresh=False)

    async def check(self) -> bool:
        if not self.headers:
            return False
        query = urlencode(request_context(self.headers))
        response = await self.site.page.context.request.get(
            f"{self.endpoint}?{query}", headers=self.headers
        )
        if response.status in {401, 403}:
            self.show_user_data = {}
            self._verified_at = 0.0
            return False
        if response.status in {429, 469}:
            raise AuthenticationError(
                f"登录检查触发限制（HTTP {response.status}），已停止"
            )
        if not response.ok:
            raise AuthenticationError(f"登录检查返回 HTTP {response.status}")
        payload = await response.json()
        if not isinstance(payload, dict):
            raise AuthenticationError("登录检查响应不是 JSON 对象")
        if str(payload.get("statusCode")) != "200":
            self.show_user_data = {}
            self._verified_at = 0.0
            return False
        data = payload.get("data")
        self.show_user_data = data if isinstance(data, dict) else {}
        self._verified_at = asyncio.get_running_loop().time()
        return True

    @staticmethod
    def interval(remaining_seconds: float) -> float:
        if remaining_seconds > 3600:
            return 900
        if remaining_seconds > 600:
            return 300
        if remaining_seconds > 120:
            return 60
        return 15


def request_context(headers: dict[str, str] | None = None) -> dict[str, str]:
    headers = headers or {}
    return {
        "lang": "zh",
        "utcOffset": headers.get("utc-offset", "480"),
        "terminalSrc": headers.get("terminal-src", "H5"),
        "ver": headers.get("ver", "4.63.3"),
        "currency": "CNY",
    }


async def capture_authenticated_headers(site: PiaoxingqiuPage) -> dict[str, str]:
    loop = asyncio.get_running_loop()
    future: asyncio.Future[Request] = loop.create_future()

    def capture(request: Request) -> None:
        if (
            not future.done()
            and request.method.upper() == "GET"
            and "/buyer/" in request.url
            and request.headers.get("access-token")
        ):
            future.set_result(request)

    site.page.context.on("request", capture)
    try:
        await site.open_purchase()
        try:
            request = await asyncio.wait_for(
                future,
                timeout=site.config.browser.timeout_ms / 1000,
            )
        except TimeoutError as exc:
            raise AuthenticationRequired("booking 未产生有效的认证请求") from exc
    finally:
        site.page.context.remove_listener("request", capture)
    return {
        name: value
        for name, value in request.headers.items()
        if not name.startswith(":") and name.lower() not in {"host", "content-length"}
    }
