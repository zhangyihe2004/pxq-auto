from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from playwright.async_api import Locator, Page, Response

from .auth import AuthGuard, AuthenticationRequired
from .browser import persistent_browser
from .config import (
    SystemConfig,
    build_login_config,
    remove_account_home,
    validate_phone,
)
from .db import Database
from .feishu import FeishuGateway, IncomingCommand
from .messages import next_step
from .site import PiaoxingqiuPage


SEND_CODE_PATH = "/pub/v5/send_verify_code"
LOGIN_PATH = "/pub/v3/login_or_register"
IMAGE_CODE_REQUIRED = {"15012012", "15012018"}
SESSION_TTL = 300


@dataclass(frozen=True)
class APIResult:
    code: str
    message: str

    @property
    def success(self) -> bool:
        return self.code == "200"


@dataclass
class LoginSession:
    owner: str
    task_id: int
    phase: str = "PHONE"
    account_id: int | None = None
    context_manager: Any = None
    page: Page | None = None
    login: Locator | None = None
    last_message_id: str = ""
    touched_at: float = 0.0
    release_on_failure: bool = False
    success_status: str = "STOPPED"


class FeishuLoginManager:
    """每位管理员一个短登录会话；消息 worker 始终保持非阻塞。"""

    def __init__(
        self,
        db: Database,
        feishu: FeishuGateway,
        system: SystemConfig,
        cancel_account: Callable[[int], Awaitable[None]],
    ) -> None:
        self.db = db
        self.feishu = feishu
        self.system = system
        self.cancel_account_worker = cancel_account
        self.reply: Callable[[str, str], Awaitable[object]] = feishu.reply_text
        self.sessions: dict[str, LoginSession] = {}

    def start(self, command: IncomingCommand, task_id: int) -> str:
        if not self.db.get_task(task_id):
            return f"抢票任务 #{task_id} 不存在。"
        if command.sender_open_id in self.sessions:
            return "你已有登录流程进行中；发送“取消”后才能重新开始。"
        self.sessions[command.sender_open_id] = LoginSession(
            command.sender_open_id,
            task_id,
            last_message_id=command.message_id,
            touched_at=time.monotonic(),
        )
        return (
            f"登录抢票任务 #{task_id} 的账号。\n"
            "请发送 11 位手机号；系统会先检查全局唯一性，再发起登录请求。\n"
            "退出：取消"
        )

    async def consume(self, command: IncomingCommand) -> str | None:
        session = self.sessions.get(command.sender_open_id)
        if session is None:
            return None
        session.last_message_id = command.message_id
        session.touched_at = time.monotonic()
        if command.text.strip() == "取消":
            account_id = session.account_id
            release = session.release_on_failure
            await self._drop(session, release=session.release_on_failure)
            if account_id and not release:
                status = self._failure_status(session)
                self.db.set_account_status(account_id, status)
                detail = (
                    "账号资料和订单保护状态已保留。"
                    if status in {"CREATED", "UNKNOWN", "COMPLETE"}
                    else "账号资料已保留，状态为登录失效。"
                )
                return f"登录已取消；{detail}"
            if release:
                return "登录已取消；新账号占用已释放。"
            return "登录已取消。"
        try:
            if session.phase == "PHONE":
                return await self._phone(session, command)
            if session.phase == "IMAGE":
                return await self._image(session, command)
            if session.phase == "SMS":
                return await self._sms(session, command)
            raise RuntimeError("登录会话状态异常")
        except ValueError as exc:
            return f"{exc}\n退出：取消"
        except Exception as exc:
            await self._drop(session, release=session.release_on_failure)
            if session.account_id and not session.release_on_failure:
                self.db.set_account_status(
                    session.account_id, self._failure_status(session), error=str(exc)
                )
            suffix = (
                "手机号占用已释放"
                if session.release_on_failure
                else f"账号仍保留，可再次发送“登录 {session.task_id}”"
            )
            return f"登录失败：{exc}\n{suffix}。"

    async def run_forever(self) -> None:
        while True:
            await asyncio.sleep(30)
            now = time.monotonic()
            expired = [
                session
                for session in self.sessions.values()
                if now - session.touched_at >= SESSION_TTL
            ]
            for session in expired:
                await self._drop(session, release=session.release_on_failure)
                if session.account_id and not session.release_on_failure:
                    self.db.set_account_status(
                        session.account_id, self._failure_status(session)
                    )
                if session.last_message_id:
                    detail = (
                        "新手机号占用已释放。"
                        if session.release_on_failure
                        else f"账号资料已保留；可再次发送：登录 {session.task_id}"
                    )
                    await self.reply(
                        session.last_message_id,
                        f"登录流程 5 分钟未操作，已取消。{detail}",
                    )

    async def close(self) -> None:
        for session in list(self.sessions.values()):
            await self._drop(session, release=session.release_on_failure)

    @staticmethod
    def _failure_status(session: LoginSession) -> str:
        return (
            session.success_status
            if session.success_status in {"CREATED", "UNKNOWN", "COMPLETE"}
            else "NEEDS_LOGIN"
        )

    async def _phone(self, session: LoginSession, command: IncomingCommand) -> str:
        phone = validate_phone(command.text)
        account = self.db.get_account_by_phone(phone)
        if account:
            if account["task_id"] != session.task_id:
                raise ValueError(
                    f"该手机号已绑定抢票任务 #{account['task_id']}，一个账号只能绑定一个场次"
                )
            session.account_id = int(account["id"])
            session.success_status = (
                account["status"]
                if account["status"] in {"CREATED", "UNKNOWN", "COMPLETE"}
                else "STOPPED"
            )
            self.db.deactivate_account(account["id"])
            await self.cancel_account_worker(account["id"])
        else:
            # 唯一性必须先在 BEGIN IMMEDIATE 中确定；此行之前没有包含手机号的网络请求。
            account = self.db.reserve_account(session.task_id, phone)
            session.account_id = int(account["id"])
            session.release_on_failure = True
        return await self._begin_login(session, command, account, phone)

    async def _begin_login(
        self,
        session: LoginSession,
        command: IncomingCommand,
        account,
        phone: str,
    ) -> str:
        task = self.db.get_task(session.task_id)
        assert task is not None
        config = build_login_config(task, account, self.system)
        manager = persistent_browser(config.browser)
        context = await manager.__aenter__()
        session.context_manager = manager
        page = context.pages[0] if context.pages else await context.new_page()
        session.page = page
        site = PiaoxingqiuPage(page, config)
        await site.open_purchase()
        popup = page.locator(".global-login-popup:visible")
        login = await _wait_optional_unique(popup)
        if login is None:
            try:
                await AuthGuard(site).ensure()
            except AuthenticationRequired:
                login = await _wait_unique(popup, "登录弹层")
            else:
                assert session.account_id is not None
                account_id = session.account_id
                self.db.set_account_status(account_id, session.success_status)
                await self._drop(session, release=False)
                return (
                    f"账号 #{account_id} 当前登录状态仍然有效，无需重新验证。"
                    f"{self._configuration_prompt(account_id)}"
                )
        session.login = login
        step = await _wait_unique(
            login.locator(".login-step:visible"), "手机号登录步骤"
        )
        phone_input = await _wait_unique(
            step.locator('input[type="number"][maxlength="11"]:visible'),
            "手机号输入框",
        )
        await phone_input.fill(phone)
        agreement = await _wait_unique(step.locator(".agreement:visible"), "用户协议")
        if not await agreement.locator(".icon-xuanzhong").count():
            await agreement.click()
        send = await _wait_unique(step.locator(".code-btn:visible"), "获取验证码按钮")
        await _require_text(send, "获取验证码登录", "获取验证码按钮")
        await _wait_enabled(send, "获取验证码按钮")
        result = await _click_for_result(page, send, SEND_CODE_PATH)
        if result.success:
            session.phase = "SMS"
            await _wait_unique(login.locator(".code-step:visible"), "短信验证码步骤")
            return "短信验证码已发送，请直接回复验证码。\n退出：取消"
        if result.code not in IMAGE_CODE_REQUIRED:
            raise RuntimeError(_api_error("发送短信验证码失败", result))
        session.phase = "IMAGE"
        await self._send_captcha(session, command.message_id)
        return "请查看上一条验证码图片，直接回复 4 位图形验证码。\n退出：取消"

    async def cancel_account(self, account_id: int) -> None:
        for session in list(self.sessions.values()):
            if session.account_id == account_id:
                await self._drop(session, release=False)

    async def _image(self, session: LoginSession, command: IncomingCommand) -> str:
        code = command.text.strip()
        if not code.isdigit() or len(code) != 4:
            return "请回复 4 位数字图形验证码。\n退出：取消"
        assert session.page is not None
        dialog = await _wait_unique(
            session.page.locator(".alertDialog:visible"), "图形验证码弹层"
        )
        code_input = await _wait_unique(
            dialog.locator('.mask-code input[type="number"][maxlength="4"]:visible'),
            "图形验证码输入框",
        )
        await code_input.fill(code)
        confirm = await _wait_unique(
            dialog.locator(".btn-view .agree:visible"), "图形验证码确认按钮"
        )
        await _require_text(confirm, "确认", "图形验证码确认按钮")
        result = await _click_for_result(session.page, confirm, SEND_CODE_PATH)
        if result.success:
            assert session.login is not None
            await _wait_unique(
                session.login.locator(".code-step:visible"), "短信验证码步骤"
            )
            session.phase = "SMS"
            return (
                "图形验证码已通过，短信验证码已发送，请直接回复短信验证码。\n退出：取消"
            )
        if result.code in IMAGE_CODE_REQUIRED:
            await self._send_captcha(session, command.message_id)
            return (
                f"图形验证码未通过：{result.message}\n已刷新图片，请重试。\n退出：取消"
            )
        raise RuntimeError(_api_error("图形验证码校验失败", result))

    async def _sms(self, session: LoginSession, command: IncomingCommand) -> str:
        assert session.page is not None and session.login is not None
        step = await _wait_unique(
            session.login.locator(".code-step:visible"), "短信验证码步骤"
        )
        sms_input = await _wait_unique(
            step.locator(".code-box input:visible"), "短信验证码输入框"
        )
        length = int(await sms_input.get_attribute("maxlength") or "4")
        code = command.text.strip()
        if not code.isdigit() or len(code) != length:
            return f"请回复 {length} 位短信验证码。\n退出：取消"
        await sms_input.fill("")
        async with session.page.expect_response(
            lambda response: _matches(response, LOGIN_PATH), timeout=10_000
        ) as info:
            await sms_input.fill(code)
        result = await _read_result(await info.value)
        if not result.success:
            return f"{_api_error('短信验证码登录失败', result)}\n请重试。\n退出：取消"
        await session.login.wait_for(state="hidden", timeout=10_000)
        assert session.account_id is not None
        account_id = session.account_id
        self.db.set_account_status(account_id, session.success_status)
        await self._drop(session, release=False)
        return f"账号 #{account_id} 登录成功。{self._configuration_prompt(account_id)}"

    def _configuration_prompt(self, account_id: int) -> str:
        step = next_step(self.db, account_id)
        return f"\n\n{step}" if step else ""

    async def _send_captcha(self, session: LoginSession, message_id: str) -> None:
        assert session.page is not None
        dialog = await _wait_unique(
            session.page.locator(".alertDialog:visible"), "图形验证码弹层"
        )
        image = await _wait_unique(
            dialog.locator("img.YZM-image:visible, .YZM-image img:visible"),
            "图形验证码图片",
        )
        content = await image.screenshot()
        if not await self.feishu.reply_image(message_id, content):
            raise RuntimeError("验证码图片发送到飞书失败")

    async def _drop(self, session: LoginSession, *, release: bool) -> None:
        self.sessions.pop(session.owner, None)
        manager = session.context_manager
        if manager is not None:
            with suppress(Exception):
                await manager.__aexit__(None, None, None)
        if release and session.account_id is not None:
            key = self.db.delete_account(session.account_id)
            if key:
                remove_account_home(key)


async def _wait_unique(locator: Locator, label: str) -> Locator:
    for _ in range(40):
        count = await locator.count()
        if count == 1:
            return locator.first
        if count > 1:
            break
        await asyncio.sleep(0.25)
    raise RuntimeError(f"{label}应唯一可见，实际找到 {count} 个")


async def _wait_optional_unique(locator: Locator) -> Locator | None:
    for _ in range(10):
        count = await locator.count()
        if count == 1:
            return locator.first
        if count > 1:
            raise RuntimeError(f"登录弹层应唯一可见，实际找到 {count} 个")
        await asyncio.sleep(0.25)
    return None


async def _wait_enabled(locator: Locator, label: str) -> None:
    for _ in range(40):
        if (
            "disabled-code-btn"
            not in (await locator.get_attribute("class") or "").split()
        ):
            return
        await asyncio.sleep(0.25)
    raise RuntimeError(f"{label}在 10 秒内未启用")


async def _require_text(locator: Locator, expected: str, label: str) -> None:
    actual = (await locator.inner_text()).strip()
    if actual != expected:
        raise RuntimeError(f"{label}文字应为“{expected}”，实际为“{actual}”")


async def _click_for_result(page: Page, target: Locator, path: str) -> APIResult:
    async with page.expect_response(
        lambda response: _matches(response, path), timeout=10_000
    ) as info:
        await target.click()
    return await _read_result(await info.value)


def _matches(response: Response, path: str) -> bool:
    return response.request.method.upper() == "POST" and urlsplit(
        response.url
    ).path.endswith(path)


async def _read_result(response: Response) -> APIResult:
    if not response.ok:
        return APIResult(str(response.status), f"HTTP {response.status}")
    payload = await response.json()
    if not isinstance(payload, dict):
        return APIResult("INVALID_PAYLOAD", "接口响应格式错误")
    return APIResult(
        str(payload.get("statusCode", payload.get("code", ""))),
        str(
            payload.get("comments")
            or payload.get("message")
            or payload.get("msg")
            or "未提供错误信息"
        ),
    )


def _api_error(action: str, result: APIResult) -> str:
    return f"{action}：code={result.code}，{result.message}"
