"""账号票档、数量和观演人的分步配置流程。"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .config import mask_id, required_audience_count
from .db import Database
from .feishu import IncomingCommand
from .task_service import parse_numbers, real_name_label


SESSION_TTL = 1800


@dataclass
class AccountSetupSession:
    owner: str
    account_id: int
    plan_ids: list[str]
    quantity: int
    people: list[tuple[str, str]]
    last_message_id: str
    phase: str = "PLANS"
    touched_at: float = 0.0


class AccountSetupFlow:
    """在内存中编辑配置；只有“完成”才一次性写入数据库。"""

    def __init__(
        self,
        db: Database,
        cancel_account: Callable[[int], Awaitable[None]],
        reply: Callable[[str, str], Awaitable[None]],
    ) -> None:
        self.db = db
        self.cancel_account = cancel_account
        self.reply = reply
        self.sessions: dict[str, AccountSetupSession] = {}
        self.account_owners: dict[int, str] = {}

    async def start(self, command: IncomingCommand, account_id: int) -> str:
        await self.expire()
        owner = command.sender_open_id
        if owner in self.sessions:
            return "你已有配置流程进行中；发送“取消”后再配置其他账号。"
        account = self.db.get_account(account_id)
        if not account:
            return f"账号 #{account_id} 不存在。"
        if account_id in self.account_owners:
            return f"账号 #{account_id} 正由另一处配置，请稍后再试。"
        if account["status"] == "RUNNING":
            return "账号正在创建订单，当前不能修改配置，请等待本次结果。"
        if account["status"] in {"CREATED", "UNKNOWN"}:
            return (
                "账号存在待支付订单或未知订单保护。\n"
                f"下一步：人工核对；确认无订单后发送：重置 {account_id}"
            )
        if account["status"] in {"RESERVED", "NEEDS_LOGIN"}:
            return f"账号尚未登录。\n下一步：登录 {account['task_id']}"

        if not self.db.begin_account_configuration(account_id):
            return "账号刚刚进入抢票流程，请等待本次结果后再配置。"
        await self.cancel_account(account_id)
        session = AccountSetupSession(
            owner,
            account_id,
            [row["seat_plan_id"] for row in self.db.get_account_plans(account_id)],
            int(account["quantity"]),
            [
                (row["name"], row["masked_id"])
                for row in self.db.get_audiences(account_id)
            ],
            command.message_id,
            touched_at=time.monotonic(),
        )
        self.sessions[owner] = session
        self.account_owners[account_id] = owner
        return self._plans_prompt(session)

    async def consume(self, command: IncomingCommand) -> str | None:
        owner = command.sender_open_id
        session = self.sessions.get(owner)
        if session and time.monotonic() - session.touched_at >= SESSION_TTL:
            self._drop(session)
            return "配置流程 30 分钟未操作，已取消；账号保持停止。"
        await self.expire()
        session = self.sessions.get(owner)
        if session is None:
            return None
        session.last_message_id = command.message_id
        session.touched_at = time.monotonic()
        text = command.text.strip()
        if text == "取消":
            self._drop(session)
            return (
                f"已取消账号 #{session.account_id} 的配置修改；正式配置未改变，"
                "账号保持停止。"
            )
        if session.phase == "PLANS":
            return self._consume_plans(session, text)
        if session.phase == "QUANTITY":
            return self._consume_quantity(session, text)
        if session.phase == "CONFIRM":
            if text == "完成":
                return self._finish(session)
            return self._confirm_prompt(session, "发送“完成”保存，或发送“取消”退出。")
        return self._consume_people(session, text)

    def cancel_account_session(self, account_id: int) -> None:
        owner = self.account_owners.get(account_id)
        if owner and (session := self.sessions.get(owner)):
            self._drop(session)

    def is_configuring(self, account_id: int) -> bool:
        return account_id in self.account_owners

    def cancel_task_sessions(self, task_id: int) -> None:
        account_ids = {int(account["id"]) for account in self.db.list_accounts(task_id)}
        for account_id in account_ids:
            self.cancel_account_session(account_id)

    def _consume_plans(self, session: AccountSetupSession, text: str) -> str:
        plans = self._task_plans(session)
        if text == "保留":
            if not session.plan_ids:
                return self._plans_prompt(session, "当前没有已选票档。")
        else:
            try:
                numbers = parse_numbers(text, len(plans))
            except ValueError as exc:
                return self._plans_prompt(session, str(exc))
            session.plan_ids = [plans[number - 1]["seat_plan_id"] for number in numbers]
        session.phase = "QUANTITY"
        return self._quantity_prompt(session)

    def _consume_quantity(self, session: AccountSetupSession, text: str) -> str:
        task = self._task(session)
        maximum = max(1, int(task["session_limitation"]))
        if not text.isdigit() or not 1 <= int(text) <= maximum:
            return self._quantity_prompt(
                session, f"购票数量必须是 1~{maximum} 的整数。"
            )
        session.quantity = int(text)
        if task["real_name_mode"] == "NONE":
            session.people.clear()
            session.phase = "CONFIRM"
            return self._confirm_prompt(session)
        session.phase = "PEOPLE"
        return self._people_prompt(session)

    def _consume_people(self, session: AccountSetupSession, text: str) -> str:
        if text == "清空":
            session.people.clear()
            return self._people_prompt(session, "观演人已清空。")
        if text.startswith("删除 "):
            if not session.people:
                return self._people_prompt(session, "当前没有观演人可删除。")
            value = text[3:].strip()
            if not value.isdigit() or not 1 <= int(value) <= len(session.people):
                return self._people_prompt(
                    session,
                    f"删除序号必须在 1~{len(session.people)} 之间。",
                )
            removed = session.people.pop(int(value) - 1)
            return self._people_prompt(session, f"已删除：{removed[0]}。")
        if text == "完成":
            required = self._required_people(session)
            if len(session.people) != required:
                return self._people_prompt(
                    session,
                    f"当前需要 {required} 位观演人，已添加 {len(session.people)} 位。",
                )
            return self._finish(session)

        parts = text.split()
        if len(parts) < 3:
            return self._people_prompt(
                session,
                "格式不正确。请发送：姓名 身份证前3位 后4位",
            )
        name = " ".join(parts[:-2]).strip()
        try:
            masked_id = mask_id(" ".join(parts[-2:]))
        except ValueError as exc:
            return self._people_prompt(session, str(exc))
        if not name:
            return self._people_prompt(session, "姓名不能为空。")
        if any(existing_id == masked_id for _, existing_id in session.people):
            return self._people_prompt(session, "该观演人已经添加，不需要重复发送。")
        required = self._required_people(session)
        if len(session.people) >= required:
            return self._people_prompt(
                session, f"已经添加所需的 {required} 位观演人。"
            )
        session.people.append((name, masked_id))
        return self._people_prompt(session, f"已添加：{name}。")

    def _finish(self, session: AccountSetupSession) -> str:
        try:
            self.db.save_account_config(
                session.account_id,
                session.plan_ids,
                session.quantity,
                session.people,
            )
        except ValueError as exc:
            prompt = (
                self._confirm_prompt
                if session.phase == "CONFIRM"
                else self._people_prompt
            )
            return prompt(session, f"无法保存：{exc}")
        summary = self._summary(session)
        self._drop(session)
        return (
            f"账号 #{session.account_id} 配置已保存。\n\n{summary}\n\n"
            f"状态：已停止\n下一步：启动 {session.account_id}"
        )

    def _plans_prompt(self, session: AccountSetupSession, notice: str = "") -> str:
        plans = self._task_plans(session)
        selected = {plan_id: index for index, plan_id in enumerate(session.plan_ids, 1)}
        lines = [
            f"配置账号 #{session.account_id}（1/3）：选择票档",
            "账号已停止；完成或取消后均不会自动启动。",
        ]
        if notice:
            lines.extend(("", notice))
        for number, plan in enumerate(plans, 1):
            priority = selected.get(plan["seat_plan_id"])
            mark = f"✓ 优先级 {priority}" if priority else ""
            lines.append(
                f"{number}. {plan['plan_name']}｜¥{plan['price']:g}"
                f"{'｜支持套票优惠' if plan['has_combo'] else ''}"
                f"{f'｜{mark}' if mark else ''}"
            )
        lines.extend(
            (
                "",
                "选择：1,3,2（按优先级）",
            )
        )
        if session.plan_ids:
            lines.append("保留原票档：保留")
        lines.append("退出：取消")
        return "\n".join(lines)

    def _quantity_prompt(self, session: AccountSetupSession, notice: str = "") -> str:
        task = self._task(session)
        maximum = max(1, int(task["session_limitation"]))
        lines = [f"配置账号 #{session.account_id}（2/3）：设置数量"]
        if notice:
            lines.extend(("", notice))
        lines.extend(
            (
                "",
                f"场次限购：最多 {maximum} 张",
                f"实名规则：{real_name_label(task['real_name_mode'])}",
                f"当前数量：{session.quantity}",
                "",
                f"发送数量：1~{maximum}",
                "退出：取消",
            )
        )
        return "\n".join(lines)

    def _people_prompt(self, session: AccountSetupSession, notice: str = "") -> str:
        required = self._required_people(session)
        lines = [f"配置账号 #{session.account_id}（3/3）：添加观演人"]
        if notice:
            lines.extend(("", notice))
        lines.extend(("", f"需要 {required} 位｜已暂存 {len(session.people)} 位："))
        lines.extend(
            f"{index}. {name}｜{masked_id}"
            for index, (name, masked_id) in enumerate(session.people, 1)
        )
        if not session.people:
            lines.append("尚未添加")
        lines.extend(
            (
                "",
                "添加：姓名 身份证前3位 后4位",
                "例如：金阳 110 2321",
                "保存：完成",
                "其他：删除 <序号> / 清空 / 取消",
            )
        )
        return "\n".join(lines)

    def _confirm_prompt(self, session: AccountSetupSession, notice: str = "") -> str:
        lines = [f"配置账号 #{session.account_id}（3/3）：确认配置"]
        if notice:
            lines.extend(("", notice))
        lines.extend(("", self._summary(session), "", "保存：完成", "退出：取消"))
        return "\n".join(lines)

    def _summary(self, session: AccountSetupSession) -> str:
        plans = {
            row["seat_plan_id"]: row["plan_name"] for row in self._task_plans(session)
        }
        task = self._task(session)
        lines = [f"票档：{' → '.join(plans[item] for item in session.plan_ids)}"]
        lines.extend(
            (
                f"数量：{session.quantity} 张",
                f"实名：{real_name_label(task['real_name_mode'])}",
            )
        )
        if session.people:
            lines.append(f"观演人（{len(session.people)}）：")
            lines.extend(
                f"{index}. {name}｜{masked_id}"
                for index, (name, masked_id) in enumerate(session.people, 1)
            )
        return "\n".join(lines)

    def _task_plans(self, session: AccountSetupSession):
        account = self.db.get_account(session.account_id)
        if not account:
            raise ValueError(f"账号 #{session.account_id} 已被删除")
        return self.db.get_task_plans(account["task_id"])

    def _task(self, session: AccountSetupSession):
        account = self.db.get_account(session.account_id)
        task = self.db.get_task(account["task_id"]) if account else None
        if not task:
            raise ValueError(f"账号 #{session.account_id} 已被删除")
        return task

    def _required_people(self, session: AccountSetupSession) -> int:
        task = self._task(session)
        return required_audience_count(task["real_name_mode"], session.quantity)

    def _drop(self, session: AccountSetupSession) -> None:
        self.sessions.pop(session.owner, None)
        self.account_owners.pop(session.account_id, None)

    async def expire(self) -> None:
        now = time.monotonic()
        for session in list(self.sessions.values()):
            if now - session.touched_at >= SESSION_TTL:
                self._drop(session)
                await self.reply(
                    session.last_message_id,
                    "配置流程 30 分钟未操作，已取消；账号保持停止。",
                )
