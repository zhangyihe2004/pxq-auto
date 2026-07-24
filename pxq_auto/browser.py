from __future__ import annotations

import os
import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from pathlib import Path

from playwright.async_api import BrowserContext, Error, Playwright, async_playwright

from .config import BrowserConfig


@asynccontextmanager
async def persistent_browser(
    config: BrowserConfig,
) -> AsyncIterator[BrowserContext]:
    config.profile_dir.mkdir(parents=True, exist_ok=True)
    playwright: Playwright = await async_playwright().start()
    context: BrowserContext | None = None
    try:
        context = await playwright.chromium.launch_persistent_context(
            user_data_dir=str(config.profile_dir),
            headless=config.headless,
            viewport={"width": 430, "height": 900},
            screen={"width": 430, "height": 900},
            device_scale_factor=2,
            has_touch=True,
            is_mobile=True,
            user_agent=(
                "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/149.0.0.0 Mobile Safari/537.36"
            ),
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            env={**os.environ, "CHROME_LOG_FILE": os.devnull},
        )
        context.set_default_timeout(config.timeout_ms)
        yield context
    except Exception as exc:
        message = str(exc)
        if (
            "ProcessSingleton" in message
            or "user data directory is already in use" in message
        ):
            raise RuntimeError(
                "浏览器资料目录正在使用，请先关闭该账号的其他浏览器实例"
            ) from exc
        raise
    finally:
        if context is not None:
            with suppress(Error):
                await context.close()
        with suppress(Error):
            await playwright.stop()


class AccountBrowserPool:
    """每个账号保留一个浏览器；同一账号的页面操作严格串行。"""

    def __init__(self) -> None:
        self._locks: dict[int, asyncio.Lock] = {}
        self._managers: dict[int, AbstractAsyncContextManager[BrowserContext]] = {}
        self._contexts: dict[int, BrowserContext] = {}

    @asynccontextmanager
    async def use(
        self, account_id: int, config: BrowserConfig
    ) -> AsyncIterator[BrowserContext]:
        lock = self._locks.setdefault(account_id, asyncio.Lock())
        async with lock:
            context = self._contexts.get(account_id)
            if context is None:
                manager = persistent_browser(config)
                context = await manager.__aenter__()
                self._managers[account_id] = manager
                self._contexts[account_id] = context
            try:
                yield context
            except Error as exc:
                if "closed" in str(exc).lower():
                    closed_manager = self._managers.pop(account_id, None)
                    self._contexts.pop(account_id, None)
                    if closed_manager is not None:
                        await closed_manager.__aexit__(None, None, None)
                raise

    async def close(self, account_id: int) -> None:
        lock = self._locks.setdefault(account_id, asyncio.Lock())
        async with lock:
            manager = self._managers.pop(account_id, None)
            self._contexts.pop(account_id, None)
            if manager is not None:
                await manager.__aexit__(None, None, None)

    async def close_all(self) -> None:
        for account_id in tuple(self._managers):
            await self.close(account_id)


async def save_screenshot(page, directory: Path, name: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.png"
    await page.screenshot(path=str(path), full_page=True)
    return path
