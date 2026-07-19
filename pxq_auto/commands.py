from __future__ import annotations

import asyncio
import shlex
import time

from .config import mask_phone, remove_account_home, validate_masked_id
from .db import Database
from .engine import AutoEngine
from .feishu import FeishuGateway, IncomingCommand
from .guard import PersistentOrderGuard
from .login import FeishuLoginManager
from .messages import audience_prompt, next_step, plan_prompt
from .service import MIN_INTERVAL, TaskService, extract_show_id, parse_numbers


SEARCH_TTL = 900
HELP = """票星球自动抢票

【任务】
搜索 <关键词>
抢票 <搜索序号|showId|链接> [场次 <序号>] [间隔 <秒>]
列表
暂停 <任务ID> / 恢复 <任务ID> / 删除 <任务ID>
间隔 <任务ID> [秒]

【账号】
登录 <任务ID>
票档 <账号ID> [编号列表|全部]
观演人 <账号ID> [姓名|打码证件号[,姓名|打码证件号]]
账号 [任务ID]
解绑 <账号ID>
重置 <账号ID>  （确认无待支付订单后使用）

机器人只创建待支付订单，不执行支付。群聊使用时请先 @机器人。"""


class CommandWorker:
    def __init__(
        self,
        queue: asyncio.Queue[IncomingCommand],
        db: Database,
        service: TaskService,
        engine: AutoEngine,
        login: FeishuLoginManager,
        feishu: FeishuGateway,
    ) -> None:
        self.queue = queue
        self.db = db
        self.service = service
        self.engine = engine
        self.login = login
        self.feishu = feishu
        self.searches: dict[tuple[str, str], tuple[float, list[dict]]] = {}

    async def run_forever(self) -> None:
        while True:
            command = await self.queue.get()
            try:
                response = await self.execute(command)
            except Exception as exc:
                response = f"执行失败：{exc}"
            try:
                await self.feishu.reply_text(command.message_id, response)
            finally:
                self.queue.task_done()

    async def execute(self, command: IncomingCommand) -> str:
        response = await self.login.consume(command)
        if response is not None:
            return response
        try:
            parts = shlex.split(command.text)
        except ValueError as exc:
            return f"指令格式错误：{exc}"
        if not parts or parts[0] in {"帮助", "help", "?"}:
            return HELP
        name = parts[0]
        if name not in {"搜索", "列表", "账号"} and not command.is_admin:
            return "无操作权限。"
        if name == "搜索":
            return await self._search(command, parts)
        if name == "抢票":
            return await self._create(command, parts)
        if name == "列表":
            return self._tasks()
        if name in {"暂停", "恢复", "删除"}:
            return await self._task_action(name, parts)
        if name == "票档":
            return self._plans(parts)
        if name == "间隔":
            return self._interval(parts)
        if name == "登录":
            return await self._login(command, parts)
        if name == "观演人":
            return self._audiences(command, parts)
        if name == "账号":
            return self._accounts(parts)
        if name == "解绑":
            return await self._detach(command, parts)
        if name == "重置":
            return self._reset(command, parts)
        return f"无法识别指令：{name}\n\n{HELP}"

    @staticmethod
    def _key(command: IncomingCommand) -> tuple[str, str]:
        return command.chat_id, command.sender_open_id

    async def _search(self, command: IncomingCommand, parts: list[str]) -> str:
        if len(parts) < 2:
            return "发送：搜索 <关键词>"
        shows = await self.service.search(" ".join(parts[1:]))
        self.searches[self._key(command)] = (time.monotonic(), shows)
        if not shows:
            return "搜索结果（0）\n请缩短关键词或改用演出名称中的连续文字重试。"
        lines = [f"搜索结果（{len(shows)}）"]
        for index, show in enumerate(shows, 1):
            lines.extend(
                (
                    "",
                    f"{index}. {show.get('showName', '')}",
                    f"时间：{show.get('showDate') or '未知'}",
                    f"地点：{show.get('cityName', '')} {show.get('venueName', '')}".strip(),
                    f"showId：{show.get('showId', '')}",
                )
            )
        lines.extend(("", "发送：抢票 <搜索序号>"))
        return "\n".join(lines)

    def _target(self, command: IncomingCommand, value: str) -> str | None:
        if value.isdigit() and len(value) <= 3:
            cached = self.searches.get(self._key(command))
            if not cached or time.monotonic() - cached[0] >= SEARCH_TTL:
                return None
            index = int(value)
            if 1 <= index <= len(cached[1]):
                return extract_show_id(str(cached[1][index - 1].get("showId") or ""))
            return None
        return extract_show_id(value)

    async def _create(self, command: IncomingCommand, parts: list[str]) -> str:
        usage = "发送：抢票 <序号|showId|链接> [场次 <序号>] [间隔 <秒>]"
        if len(parts) < 2:
            return usage
        show_id = self._target(command, parts[1])
        if not show_id:
            return "无法解析演出，请先重新搜索。"
        options = {"场次": "", "间隔": "60"}
        index = 2
        while index < len(parts):
            if parts[index] not in options or index + 1 >= len(parts):
                return usage
            options[parts[index]] = parts[index + 1]
            index += 2
        show_name, sessions = await self.service.show_sessions(show_id)
        if not sessions:
            return "该演出当前没有场次。"
        if len(sessions) > 1 and not options["场次"]:
            lines = [f"演出：{show_name}", f"场次（{len(sessions)}）："]
            lines.extend(
                f"{number}. {session.get('sessionName', '')}"
                for number, session in enumerate(sessions, 1)
            )
            lines.extend(("", f"发送：抢票 {parts[1]} 场次 <序号>"))
            return "\n".join(lines)
        try:
            session_number = int(options["场次"] or "1")
            interval = max(MIN_INTERVAL, int(options["间隔"]))
        except ValueError:
            return "场次和间隔必须是数字。"
        if not 1 <= session_number <= len(sessions):
            return f"场次编号必须在 1~{len(sessions)} 之间。"
        session = sessions[session_number - 1]
        plans = await self.service.plans(show_id, session["bizShowSessionId"])
        task_id, created, ordered = self.service.create_task(
            show_id=show_id,
            show_name=show_name,
            session=session,
            plans=plans,
            interval=interval,
        )
        if not created:
            accounts = self.db.list_accounts(task_id)
            guide = (
                f"发送：登录 {task_id} 添加账号。"
                if not accounts
                else "发送：列表 查看现有账号和下一步。"
            )
            return f"该场次已存在：抢票任务 #{task_id}。\n{guide}"
        lines = [
            f"已创建抢票任务 #{task_id}\n"
            f"演出：{show_name}\n场次：{session.get('sessionName', '')}\n"
            f"间隔：{interval} 秒",
            f"票档目录（{len(ordered)}）：",
        ]
        lines.extend(
            f"{number}. {plan['plan_name']}｜¥{plan['price']:g}"
            for number, plan in enumerate(ordered, 1)
        )
        lines.extend(("", f"下一步发送：登录 {task_id}"))
        return "\n".join(lines)

    def _tasks(self) -> str:
        tasks = self.db.list_tasks()
        if not tasks:
            return "抢票任务（0）"
        blocks = []
        for task in tasks:
            plans = self.db.get_task_plans(task["id"])
            accounts = self.db.list_accounts(task["id"])
            mode = _mode(task, plans)
            lines = [
                f"任务 #{task['id']}",
                f"演出：{task['show_name']}",
                f"场次：{task['session_name']}",
                f"状态：{task['status']}｜{mode}",
                f"账号：{len(accounts)}",
                f"票档目录（{len(plans)}）：",
            ]
            lines.extend(
                f"✓ {index}. {plan['plan_name']}｜¥{plan['price']:g}"
                for index, plan in enumerate(plans, 1)
            )
            lines.append("账号明细：")
            for account in accounts:
                account_plans = self.db.get_account_plans(account["id"])
                plan_names = " → ".join(plan["plan_name"] for plan in account_plans)
                lines.append(
                    f"✓ #{account['id']} {mask_phone(account['phone'])}｜{account['status']}｜"
                    f"观演人 {len(self.db.get_audiences(account['id']))}｜"
                    f"票档 {plan_names or '未配置'}"
                )
                lines.append(f"  {next_step(self.db, account['id'])}")
            if not accounts:
                lines.append(f"下一步：发送“登录 {task['id']}”添加账号。")
            blocks.append("\n".join(lines))
        return f"抢票任务（{len(tasks)}）\n\n" + "\n\n".join(blocks)

    async def _task_action(self, name: str, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return f"发送：{name} <任务ID>"
        task_id = int(parts[1])
        if name == "删除":
            if not self.db.get_task(task_id):
                return f"抢票任务 #{task_id} 不存在。"
            await self.engine.cancel_task(task_id)
            for account in self.db.list_accounts(task_id):
                await self.login.cancel_account(account["id"])
            keys = self.db.delete_task(task_id)
            for key in keys:
                remove_account_home(key)
            return f"已删除抢票任务 #{task_id} 及其全部账号资料。"
        status = "paused" if name == "暂停" else "active"
        if not self.db.set_task_status(task_id, status):
            return f"抢票任务 #{task_id} 不存在。"
        if status == "paused":
            await self.engine.cancel_task(task_id)
        return f"已{name}抢票任务 #{task_id}。"

    def _plans(self, parts: list[str]) -> str:
        if len(parts) not in {2, 3} or not parts[1].isdigit():
            return "先发送：票档 <账号ID>\n查看目录后再选择。"
        account_id = int(parts[1])
        account = self.db.get_account(account_id)
        if not account:
            return f"账号 #{account_id} 不存在。"
        if len(parts) == 2:
            return plan_prompt(self.db, account_id)
        if account["status"] == "RUNNING":
            return "账号正在抢票，请先暂停所属任务。"
        plans = self.db.get_task_plans(account["task_id"])
        try:
            numbers = (
                list(range(1, len(plans) + 1))
                if parts[2].lower() in {"all", "全部"}
                else parse_numbers(parts[2], len(plans))
            )
        except ValueError as exc:
            return f"{exc}\n\n{plan_prompt(self.db, account_id)}"
        ids = [plans[number - 1]["seat_plan_id"] for number in numbers]
        self.db.replace_account_plans(account_id, ids)
        self._refresh_account_status(account_id)
        names = " → ".join(plans[number - 1]["plan_name"] for number in numbers)
        return (
            f"账号 #{account_id} 已更新票档优先级：{names}\n\n"
            f"{plan_prompt(self.db, account_id)}\n\n{next_step(self.db, account_id)}"
        )

    def _interval(self, parts: list[str]) -> str:
        if len(parts) not in {2, 3} or not parts[1].isdigit():
            return "先发送：间隔 <任务ID>\n查看当前值后再修改。"
        task_id = int(parts[1])
        task = self.db.get_task(task_id)
        if not task:
            return f"抢票任务 #{task_id} 不存在。"
        if len(parts) == 2:
            return (
                f"任务 #{task_id}\n当前间隔：{task['interval_sec']} 秒\n\n"
                f"发送：间隔 {task_id} <秒>（最低 {MIN_INTERVAL} 秒）"
            )
        if not parts[2].isdigit():
            return "间隔必须是整数秒。"
        requested = int(parts[2])
        interval = max(MIN_INTERVAL, requested)
        if not self.db.set_task_interval(task_id, interval):
            return f"抢票任务 #{task_id} 不存在。"
        self.engine.next_poll[task_id] = 0
        adjusted = (
            f"（输入 {requested} 秒，已按最低值调整）"
            if requested < MIN_INTERVAL
            else ""
        )
        return f"已更新任务 #{task_id}\n间隔：{interval} 秒{adjusted}"

    async def _login(self, command: IncomingCommand, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：登录 <任务ID>"
        return await self.login.start(command, int(parts[1]))

    def _audiences(self, command: IncomingCommand, parts: list[str]) -> str:
        if len(parts) < 2 or not parts[1].isdigit():
            return "先发送：观演人 <账号ID>\n查看当前配置和填写格式。"
        account_id = int(parts[1])
        account = self.db.get_account(account_id)
        if not account:
            return f"账号 #{account_id} 不存在。"
        if len(parts) == 2:
            return audience_prompt(self.db, account_id)
        if account["status"] == "RUNNING":
            return "账号正在抢票，请先暂停所属任务。"
        people = []
        for raw in " ".join(parts[2:]).replace("，", ",").split(","):
            pair = [item.strip() for item in raw.split("|", 1)]
            if len(pair) != 2 or not pair[0]:
                return "每位观演人格式必须是：姓名|打码证件号"
            try:
                people.append((pair[0], validate_masked_id(pair[1])))
            except ValueError as exc:
                return str(exc)
        if len(people) != len(set(people)):
            return "观演人不能重复。"
        self.db.replace_audiences(account_id, people)
        self._refresh_account_status(account_id)
        return (
            f"账号 #{account_id} 已配置 {len(people)} 位观演人。\n\n"
            f"{audience_prompt(self.db, account_id)}\n\n{next_step(self.db, account_id)}"
        )

    def _accounts(self, parts: list[str]) -> str:
        if len(parts) > 2 or (len(parts) == 2 and not parts[1].isdigit()):
            return "发送：账号 [任务ID]"
        task_id = int(parts[1]) if len(parts) == 2 else None
        accounts = self.db.list_accounts(task_id)
        if not accounts:
            if task_id is not None and self.db.get_task(task_id):
                return f"任务 #{task_id} 暂无账号。\n下一步：发送“登录 {task_id}”。"
            return "账号（0）\n请先用“搜索”找到演出并创建抢票任务。"
        lines = [f"账号（{len(accounts)}）"]
        for account in accounts:
            lines.extend(
                (
                    f"#{account['id']}｜{mask_phone(account['phone'])}｜任务 #{account['task_id']}｜"
                    f"{account['status']}｜观演人 {len(self.db.get_audiences(account['id']))}｜"
                    f"票档 {len(self.db.get_account_plans(account['id']))}",
                    next_step(self.db, account["id"]),
                    "",
                )
            )
        if lines[-1] == "":
            lines.pop()
        return "\n".join(lines)

    async def _detach(self, command: IncomingCommand, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：解绑 <账号ID>"
        account_id = int(parts[1])
        await self.engine.cancel_account(account_id)
        await self.login.cancel_account(account_id)
        key = self.db.delete_account(account_id)
        if not key:
            return f"账号 #{account_id} 不存在。"
        remove_account_home(key)
        return f"账号 #{account_id} 已解绑；登录资料、观演人和订单保护状态均已删除。"

    def _reset(self, command: IncomingCommand, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：重置 <账号ID>"
        account_id = int(parts[1])
        account = self.db.get_account(account_id)
        if not account:
            return f"账号 #{account_id} 不存在。"
        task = self.db.get_task(account["task_id"])
        plans = self.db.get_account_plans(account_id)
        people = self.db.get_audiences(account_id)
        if not task or not people or not plans:
            return next_step(self.db, account_id) or "账号配置不完整。"
        from .config import build_order_config

        config = build_order_config(task, plans, people, account, self.engine.system)
        PersistentOrderGuard(config.state_path, config.plan_key).ready()
        self.db.set_account_status(account_id, "READY")
        return f"账号 #{account_id} 已恢复 READY。\n{next_step(self.db, account_id)}"

    def _refresh_account_status(self, account_id: int) -> None:
        account = self.db.get_account(account_id)
        if account and account["status"] not in {"CREATED", "UNKNOWN", "COMPLETE"}:
            self.db.set_account_status(
                account_id, self.db.configuration_status(account_id)
            )


def _mode(task, plans) -> str:
    if task["status"] != "active":
        return "已暂停"
    if any(plan["sale_started"] and plan["can_buy_count"] > 0 for plan in plans):
        return "有票，等待/正在抢票"
    if task["session_status"].upper() in {"ONSALE", "ON_SALE", "LACK_OF_TICKET"} or any(
        plan["sale_started"] for plan in plans
    ):
        return "回流票等待"
    return "预抢票等待"
