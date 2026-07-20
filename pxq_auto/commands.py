from __future__ import annotations

import asyncio
import shlex
import time

from .config import build_order_config, mask_phone, remove_account_home
from .configuration import AccountConfigurator
from .db import Database
from .engine import AutoEngine
from .feishu import FeishuGateway, IncomingCommand
from .guard import PersistentOrderGuard
from .login import FeishuLoginManager
from .messages import next_step, status_label
from .service import MIN_INTERVAL, TaskService, extract_show_id, sale_phase


SEARCH_TTL = 900
REPLY_RETRY_SECONDS = 10
MAX_PENDING_REPLIES = 100
HELP = """票星球自动抢票

【创建】
搜索 <关键词>
抢票 <搜索序号|showId|链接> [场次 <序号>] [间隔 <秒>]

【查看】
列表
详情 <任务ID>

【账号】
登录 <任务ID>
配置 <账号ID>
启动 <账号ID>
停止 <账号ID>
解绑 <账号ID>
重置 <账号ID>（确认无待支付订单后使用）

【任务】
间隔 <任务ID> [秒]
暂停 <任务ID> / 恢复 <任务ID> / 删除 <任务ID>

配置必须发送“完成”后才保存，保存后还需明确“启动”；程序只创建待支付订单，永不支付。
登录和配置的任意步骤均可发送“取消”，退出且不保存当前步骤。
群聊中非管理员仅可使用搜索、列表和详情。
群聊使用时每条消息都要先 @机器人。"""


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
        self.pending_replies: dict[str, str] = {}
        self.configurator = AccountConfigurator(
            db,
            engine.cancel_account,
            self._reply,
        )
        self.searches: dict[tuple[str, str], tuple[float, list[dict]]] = {}

    async def run_forever(self) -> None:
        maintenance = asyncio.create_task(self._maintain())
        try:
            while True:
                command = await self.queue.get()
                try:
                    response = await self.execute(command)
                except Exception as exc:
                    response = f"执行失败：{exc}"
                try:
                    await self._reply(command.message_id, response)
                finally:
                    self.queue.task_done()
        finally:
            maintenance.cancel()
            await asyncio.gather(maintenance, return_exceptions=True)

    async def _reply(self, message_id: str, text: str) -> None:
        if self.pending_replies:
            self._queue_reply(message_id, text)
            return
        if await self.feishu.reply_text(message_id, text):
            return
        self._queue_reply(message_id, text)

    def _queue_reply(self, message_id: str, text: str) -> None:
        if len(self.pending_replies) >= MAX_PENDING_REPLIES:
            self.pending_replies.pop(next(iter(self.pending_replies)))
        self.pending_replies[message_id] = text

    async def _maintain(self) -> None:
        while True:
            await asyncio.sleep(REPLY_RETRY_SECONDS)
            await self.configurator.expire()
            await self._retry_replies()

    async def _retry_replies(self) -> None:
        for message_id, text in list(self.pending_replies.items()):
            if not await self.feishu.reply_text(message_id, text):
                break
            self.pending_replies.pop(message_id, None)

    async def execute(self, command: IncomingCommand) -> str:
        response = await self.login.consume(command)
        if response is not None:
            return response
        response = await self.configurator.consume(command)
        if response is not None:
            return response
        try:
            parts = shlex.split(command.text)
        except ValueError as exc:
            return f"指令格式错误：{exc}"
        if not parts or parts[0] == "帮助":
            return HELP
        name = parts[0]
        if name not in {"搜索", "列表", "详情"} and not command.is_admin:
            return "无操作权限。"
        if name == "搜索":
            return await self._search(command, parts)
        if name == "抢票":
            return await self._create(command, parts)
        if name == "列表":
            return self._tasks()
        if name == "详情":
            return self._detail(parts)
        if name in {"暂停", "恢复", "删除"}:
            return await self._task_action(name, parts)
        if name == "间隔":
            return self._interval(parts)
        if name == "登录":
            return await self._login(command, parts)
        if name == "配置":
            return await self._configure(command, parts)
        if name == "启动":
            return self._start_account(parts)
        if name == "停止":
            return await self._stop_account(parts)
        if name == "解绑":
            return await self._detach(parts)
        if name == "重置":
            return self._reset(parts)
        return f"无法识别指令：{name}\n发送：帮助"

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
                )
            )
        lines.extend(("", "发送：抢票 <搜索序号>"))
        return "\n".join(lines)

    def _target(self, command: IncomingCommand, value: str) -> str | None:
        if value.isdigit() and len(value) <= 3:
            cached = self.searches.get(self._key(command))
            if not cached:
                return None
            if time.monotonic() - cached[0] >= SEARCH_TTL:
                self.searches.pop(self._key(command), None)
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
            lines.extend(
                (
                    "",
                    f"发送：抢票 {parts[1]} 场次 <序号> 间隔 {options['间隔']}",
                )
            )
            return "\n".join(lines)
        try:
            session_number = int(options["场次"] or "1")
            requested_interval = int(options["间隔"])
        except ValueError:
            return "场次和间隔必须是数字。"
        if not 1 <= session_number <= len(sessions):
            return f"场次编号必须在 1~{len(sessions)} 之间。"
        interval = max(MIN_INTERVAL, requested_interval)
        session = sessions[session_number - 1]
        plans = await self.service.plans(show_id, session["bizShowSessionId"])
        task_id, created = self.service.create_task(
            show_id=show_id,
            show_name=show_name,
            session=session,
            plans=plans,
            interval=interval,
        )
        if not created:
            return f"该场次已经是任务 #{task_id}，不会重复创建。\n发送：详情 {task_id}"
        adjusted = (
            f"（输入 {requested_interval} 秒，已按最低值调整）"
            if requested_interval < MIN_INTERVAL
            else ""
        )
        return (
            f"已创建任务 #{task_id}\n\n"
            f"演出：{show_name}\n"
            f"场次：{session.get('sessionName', '')}\n"
            f"方式：{'选座' if session.get('supportSeatPicking') else '不选座'}\n"
            f"间隔：{interval} 秒{adjusted}\n"
            "账号：0\n状态：等待添加账号\n\n"
            f"下一步：登录 {task_id}"
        )

    def _tasks(self) -> str:
        tasks = self.db.list_tasks()
        if not tasks:
            return "抢票任务（0）\n下一步：搜索 <关键词>"
        blocks = []
        for task in tasks:
            accounts = self.db.list_accounts(task["id"])
            enabled = sum(bool(account["enabled"]) for account in accounts)
            block = (
                f"任务 #{task['id']}｜{'运行中' if task['status'] == 'active' else '已暂停'}\n"
                f"演出：{task['show_name']}\n"
                f"场次：{task['session_name']}\n"
                f"账号：{len(accounts)}（已启动 {enabled}）\n"
                f"间隔：{task['interval_sec']} 秒"
            )
            if not accounts:
                block += f"\n下一步：登录 {task['id']}"
            blocks.append(block)
        return f"抢票任务（{len(tasks)}）\n\n" + "\n\n".join(blocks)

    def _detail(self, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：详情 <任务ID>"
        task_id = int(parts[1])
        task = self.db.get_task(task_id)
        if not task:
            return f"任务 #{task_id} 不存在。"
        plans = self.db.get_task_plans(task_id)
        accounts = self.db.list_accounts(task_id)
        phase = sale_phase(task, plans)
        lines = [
            f"任务 #{task_id}",
            f"演出：{task['show_name']}",
            f"场次：{task['session_name']}",
            f"方式：{'选座' if task['support_seat_picking'] else '不选座'}",
            f"状态：{_mode(task, plans)}",
            f"间隔：{task['interval_sec']} 秒",
            "",
            f"票档目录（{len(plans)}）：",
        ]
        for number, plan in enumerate(plans, 1):
            stock = (
                "未开售"
                if phase in {"PRESALE", "PREWARM", "WAITING"}
                else f"有票 {plan['can_buy_count']} 张"
                if plan["sale_started"] and plan["can_buy_count"] > 0
                else "可售但无票"
                if plan["sale_started"]
                else "未开售"
            )
            lines.append(f"{number}. {plan['plan_name']}｜¥{plan['price']:g}｜{stock}")
        lines.extend(("", f"账号（{len(accounts)}）："))
        for account in accounts:
            account_plans = self.db.get_account_plans(account["id"])
            people = self.db.get_audiences(account["id"])
            lines.extend(
                (
                    f"#{account['id']}｜{mask_phone(account['phone'])}｜{status_label(account['status'])}",
                    "票档："
                    + (
                        " → ".join(plan["plan_name"] for plan in account_plans)
                        if account_plans
                        else "未配置"
                    ),
                    f"观演人（{len(people)}）：",
                )
            )
            lines.extend(
                f"  {index}. {person['name']}｜{person['masked_id']}"
                for index, person in enumerate(people, 1)
            )
            if step := next_step(self.db, account["id"]):
                lines.append(step)
            lines.append("")
        if not accounts:
            lines.append(f"下一步：登录 {task_id}")
        elif lines[-1] == "":
            lines.pop()
        return "\n".join(lines)

    async def _task_action(self, name: str, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return f"发送：{name} <任务ID>"
        task_id = int(parts[1])
        if name == "删除":
            if not self.db.get_task(task_id):
                return f"任务 #{task_id} 不存在。"
            self.configurator.cancel_task_sessions(task_id)
            await self.engine.cancel_task(task_id)
            for account in self.db.list_accounts(task_id):
                await self.login.cancel_account(account["id"])
            keys = self.db.delete_task(task_id)
            for key in keys:
                remove_account_home(key)
            return f"已删除任务 #{task_id} 及其全部账号资料。"
        status = "paused" if name == "暂停" else "active"
        if not self.db.set_task_status(task_id, status):
            return f"任务 #{task_id} 不存在。"
        if status == "paused":
            await self.engine.cancel_task(task_id)
        else:
            self.engine.next_poll[task_id] = 0
        return f"已{name}任务 #{task_id}。"

    def _interval(self, parts: list[str]) -> str:
        if len(parts) not in {2, 3} or not parts[1].isdigit():
            return "发送：间隔 <任务ID> [秒]"
        task_id = int(parts[1])
        task = self.db.get_task(task_id)
        if not task:
            return f"任务 #{task_id} 不存在。"
        if len(parts) == 2:
            return (
                f"任务 #{task_id} 当前间隔：{task['interval_sec']} 秒\n"
                f"修改：间隔 {task_id} <秒>（最低 {MIN_INTERVAL} 秒）"
            )
        if not parts[2].isdigit():
            return "间隔必须是整数秒。"
        requested = int(parts[2])
        interval = max(MIN_INTERVAL, requested)
        self.db.set_task_interval(task_id, interval)
        self.engine.next_poll[task_id] = 0
        adjusted = "（已按最低值调整）" if requested < MIN_INTERVAL else ""
        return f"任务 #{task_id} 间隔已改为 {interval} 秒{adjusted}。"

    async def _login(self, command: IncomingCommand, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：登录 <任务ID>"
        return await self.login.start(command, int(parts[1]))

    async def _configure(self, command: IncomingCommand, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：配置 <账号ID>"
        return await self.configurator.start(command, int(parts[1]))

    def _start_account(self, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：启动 <账号ID>"
        account_id = int(parts[1])
        account = self.db.get_account(account_id)
        if not account:
            return f"账号 #{account_id} 不存在。"
        if self.configurator.is_configuring(account_id):
            return "账号正在配置中；请先在原会话发送“完成”或“取消”。"
        if account["status"] in {"READY", "RUNNING"} and account["enabled"]:
            step = next_step(self.db, account_id)
            return (
                f"账号 #{account_id} 已经启动。\n{step}"
                if step
                else f"账号 #{account_id} 已经启动。"
            )
        try:
            self.db.activate_account(account_id)
        except ValueError as exc:
            step = next_step(self.db, account_id)
            return f"{exc}。\n{step}" if step else f"{exc}。"
        self.engine.next_poll[account["task_id"]] = 0
        task = self.db.get_task(account["task_id"])
        assert task is not None
        if not self.engine.system.create_order_enabled:
            result = "系统当前禁止创建订单，只会继续监控。"
        elif task["status"] == "paused":
            result = f"账号已启用，但任务已暂停。发送：恢复 {task['id']}"
        else:
            result = "正在等待开售或回流；发现目标库存后自动抢票。"
        return f"账号 #{account_id} 已启动。\n{result}"

    async def _stop_account(self, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：停止 <账号ID>"
        account_id = int(parts[1])
        account = self.db.get_account(account_id)
        if not account:
            return f"账号 #{account_id} 不存在。"
        self.configurator.cancel_account_session(account_id)
        await self.engine.cancel_account(account_id)
        self.db.deactivate_account(account_id)
        account = self.db.get_account(account_id)
        assert account is not None
        suffix = (
            next_step(self.db, account_id)
            if account["status"] in {"CREATED", "UNKNOWN"}
            else "配置和登录资料均已保留。"
        )
        return f"账号 #{account_id} 已停止。\n{suffix}"

    async def _detach(self, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：解绑 <账号ID>"
        account_id = int(parts[1])
        self.configurator.cancel_account_session(account_id)
        await self.engine.cancel_account(account_id)
        await self.login.cancel_account(account_id)
        key = self.db.delete_account(account_id)
        if not key:
            return f"账号 #{account_id} 不存在。"
        remove_account_home(key)
        return f"账号 #{account_id} 已解绑，全部本地资料和手机号占用均已删除。"

    def _reset(self, parts: list[str]) -> str:
        if len(parts) != 2 or not parts[1].isdigit():
            return "发送：重置 <账号ID>"
        account_id = int(parts[1])
        account = self.db.get_account(account_id)
        if not account:
            return f"账号 #{account_id} 不存在。"
        task = self.db.get_task(account["task_id"])
        plans = self.db.get_account_plans(account_id)
        people = self.db.get_audiences(account_id)
        if not task or not plans:
            return f"账号配置不完整。发送：配置 {account_id}"
        if people:
            config = build_order_config(
                task, plans, people, account, self.engine.system
            )
            PersistentOrderGuard(config.state_path, config.plan_key).ready()
        self.db.set_account_status(account_id, "STOPPED")
        self.db.deactivate_account(account_id)
        next_action = (
            f"启动 {account_id}"
            if people
            else f"配置 {account_id}（观演人已全部移出待抢名单）"
        )
        return (
            f"账号 #{account_id} 的订单保护已重置。\n"
            f"状态：已停止\n下一步：{next_action}"
        )


def _mode(task, plans) -> str:
    if task["status"] != "active":
        return "已暂停"
    return {
        "PRESALE": "等待开售",
        "PREWARM": "预抢票准备",
        "AVAILABLE": "当前有票",
        "ONSALE": "等待回流",
        "WAITING": "等待开售",
    }[sale_phase(task, plans)]
