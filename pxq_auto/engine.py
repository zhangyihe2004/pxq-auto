from __future__ import annotations

import asyncio
import logging
import time

from .browser import persistent_browser
from .config import SystemConfig, build_order_config
from .db import Database
from .feishu import FeishuGateway
from .runner import RunResult, check_login, run_account
from .service import TaskService


log = logging.getLogger("pxq.auto")
PREWARM_SECONDS = 60
ON_SALE = {"ONSALE", "ON_SALE", "LACK_OF_TICKET"}
TERMINAL = {"SALE_END", "ENDED", "CANCELLED", "CANCELED", "OFF_SHELF"}


class AutoEngine:
    def __init__(
        self,
        db: Database,
        service: TaskService,
        feishu: FeishuGateway,
        system: SystemConfig,
    ) -> None:
        self.db = db
        self.service = service
        self.feishu = feishu
        self.system = system
        self.semaphore = asyncio.Semaphore(system.max_concurrent_accounts)
        self.jobs: dict[int, asyncio.Task] = {}
        self.checks: dict[int, asyncio.Task] = {}
        self.next_poll: dict[int, float] = {}
        self.next_auth: dict[int, float] = {}

    async def run_forever(self) -> None:
        log.info("自动抢票引擎启动")
        while True:
            await self.tick()
            await asyncio.sleep(1)

    async def tick(self) -> None:
        now = time.monotonic()
        tasks = self.db.list_tasks()
        active_ids = {task["id"] for task in tasks if task["status"] == "active"}
        for task_id in set(self.next_poll) - active_ids:
            self.next_poll.pop(task_id, None)
        for task in tasks:
            if task["status"] != "active" or now < self.next_poll.get(task["id"], 0):
                continue
            try:
                await self._poll(task)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("抢票任务 #%s 轮询失败", task["id"])
                self.next_poll[task["id"]] = now + task["interval_sec"]

    async def _poll(self, task) -> None:
        task_id = int(task["id"])
        status, snapshot = await self.service.refresh_task(task)
        if not self.db.update_task_snapshot(task_id, status, snapshot):
            return
        task = self.db.get_task(task_id)
        assert task is not None
        plans = self.db.get_task_plans(task_id)
        now_ms = int(time.time() * 1000)
        remaining = (
            (task["sale_time_ms"] - now_ms) / 1000
            if task["sale_time_ms"] is not None
            else None
        )
        if status.upper() in TERMINAL:
            self.db.set_task_status(task_id, "paused")
            await self.feishu.send_card(
                "抢票任务已暂停",
                f"任务 #{task_id}\n**{task['show_name']}**\n场次状态：{status}",
                "orange",
            )
            await self.cancel_task(task_id)
            return

        started = status.upper() in ON_SALE or any(
            plan["sale_started"] for plan in plans
        )
        presale = not started and remaining is not None and remaining <= PREWARM_SECONDS
        if self.system.create_order_enabled:
            for account in self.db.list_accounts(task_id):
                account_plans = self.db.get_account_plans(account["id"])
                available = any(
                    plan["sale_started"] and plan["can_buy_count"] > 0
                    for plan in account_plans
                )
                if account["status"] == "READY" and (presale or available):
                    self._start_account(account["id"], presale=presale)

        await self._schedule_login_checks(task, remaining)
        delay = task["interval_sec"]
        if not started and remaining is not None and remaining > PREWARM_SECONDS:
            delay = min(delay, max(1.0, remaining - PREWARM_SECONDS))
        self.next_poll[task_id] = time.monotonic() + delay

    def _start_account(self, account_id: int, *, presale: bool) -> None:
        if account_id in self.jobs:
            return
        job = asyncio.create_task(
            self._run_after_check(account_id, presale, self.checks.get(account_id))
        )
        self.jobs[account_id] = job

        def forget_job(_job: asyncio.Task) -> None:
            if self.jobs.get(account_id) is _job:
                self.jobs.pop(account_id, None)

        job.add_done_callback(forget_job)

    async def _run_after_check(
        self,
        account_id: int,
        presale: bool,
        check: asyncio.Task | None,
    ) -> None:
        if check:
            check.cancel()
            await asyncio.gather(check, return_exceptions=True)
        await self._run_account(account_id, presale)

    async def _run_account(self, account_id: int, presale: bool) -> None:
        async with self.semaphore:
            account = self.db.get_account(account_id)
            if not account or account["status"] != "READY":
                return
            task = self.db.get_task(account["task_id"])
            if not task or task["status"] != "active":
                return
            plans = self.db.get_account_plans(account_id)
            people = self.db.get_audiences(account_id)
            config = build_order_config(task, plans, people, account, self.system)
            self.db.set_account_status(account_id, "RUNNING")
            try:
                async with persistent_browser(config.browser) as context:
                    result = await run_account(config, context, presale=presale)
            except asyncio.CancelledError:
                if self.db.get_account(account_id):
                    self.db.set_account_status(account_id, "READY")
                raise
            except Exception as exc:
                log.exception("账号 #%s 抢票执行失败", account_id)
                if self.db.get_account(account_id):
                    self.db.set_account_status(account_id, "READY", error=str(exc))
                await self._notify_result(
                    account_id, task, RunResult("FAILED", str(exc))
                )
                return
            self._apply_removed_audiences(account_id, result.removed_audiences)
            if not self.db.get_account(account_id):
                return
            next_status = {
                "CREATED": "CREATED",
                "UNKNOWN": "UNKNOWN",
                "NEEDS_LOGIN": "NEEDS_LOGIN",
                "COMPLETE": "COMPLETE",
            }.get(result.status, "READY")
            self.db.set_account_status(
                account_id,
                next_status,
                order_id=result.order_id,
                error=""
                if result.status in {"CREATED", "COMPLETE"}
                else result.message,
            )
            if result.status not in {"NO_STOCK"}:
                await self._notify_result(account_id, task, result)

    async def _schedule_login_checks(self, task, remaining: float | None) -> None:
        interval = 900 if remaining is None or remaining > 3600 else 300
        if remaining is not None and remaining <= 600:
            interval = 60
        if remaining is not None and remaining <= 120:
            interval = 15
        now = time.monotonic()
        for account in self.db.list_accounts(task["id"]):
            account_id = int(account["id"])
            if (
                account["status"] != "READY"
                or account_id in self.jobs
                or account_id in self.checks
                or now < self.next_auth.get(account_id, 0)
            ):
                continue
            self.next_auth[account_id] = now + interval
            check = asyncio.create_task(self._check_account_login(account_id, task))
            self.checks[account_id] = check

            def forget_check(_check: asyncio.Task, aid: int = account_id) -> None:
                if self.checks.get(aid) is _check:
                    self.checks.pop(aid, None)

            check.add_done_callback(forget_check)

    async def _check_account_login(self, account_id: int, task) -> None:
        async with self.semaphore:
            account = self.db.get_account(account_id)
            if not account or account["status"] != "READY" or account_id in self.jobs:
                return
            plans = self.db.get_account_plans(account_id)
            people = self.db.get_audiences(account_id)
            if not people:
                return
            config = build_order_config(task, plans, people, account, self.system)
            try:
                async with persistent_browser(config.browser) as context:
                    valid = await check_login(config, context)
            except Exception as exc:
                log.warning("账号 #%s 登录检查失败：%s", account_id, exc)
                return
            if not valid and self.db.get_account(account_id):
                self.db.set_account_status(
                    account_id, "NEEDS_LOGIN", error="登录已失效"
                )
                await self.feishu.send_card(
                    "账号登录已失效",
                    f"任务 #{task['id']}｜账号 #{account_id}\n发送：登录 {task['id']}",
                    "orange",
                )

    def _apply_removed_audiences(
        self, account_id: int, removed: tuple[str, ...]
    ) -> None:
        if not removed:
            return
        people = [
            (person["name"], person["masked_id"])
            for person in self.db.get_audiences(account_id)
            if person["masked_id"] not in removed
        ]
        self.db.replace_audiences(account_id, people)

    async def _notify_result(self, account_id: int, task, result: RunResult) -> None:
        title = {
            "CREATED": "订单已创建",
            "UNKNOWN": "订单结果未知",
            "NEEDS_LOGIN": "账号需要重新登录",
            "COMPLETE": "观演人均已购",
        }.get(result.status, "抢票执行异常")
        color = "green" if result.status in {"CREATED", "COMPLETE"} else "orange"
        body = (
            f"任务 #{task['id']}｜账号 #{account_id}\n"
            f"**{task['show_name']}**\n{result.message}\n操作：未支付"
        )
        await self.feishu.send_card(title, body, color)

    async def cancel_account(self, account_id: int) -> None:
        activities = [
            activity
            for activity in (self.jobs.get(account_id), self.checks.get(account_id))
            if activity
        ]
        for activity in activities:
            activity.cancel()
        if activities:
            await asyncio.gather(*activities, return_exceptions=True)

    async def cancel_task(self, task_id: int) -> None:
        for account in self.db.list_accounts(task_id):
            await self.cancel_account(account["id"])

    async def close(self) -> None:
        activities = [*self.jobs.values(), *self.checks.values()]
        for activity in activities:
            activity.cancel()
        if activities:
            await asyncio.gather(*activities, return_exceptions=True)
