from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from playwright.async_api import BrowserContext, Playwright, async_playwright

from .config import BrowserConfig


@asynccontextmanager
async def persistent_browser(config: BrowserConfig):
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
            await context.close()
        await playwright.stop()


async def save_screenshot(page, directory: Path, name: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.png"
    await page.screenshot(path=str(path), full_page=True)
    return path
