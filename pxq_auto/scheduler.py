"""任务轮询、账号调度和结果通知。"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from .public_api import PxqError
from .auth import AuthGuard
from .browser import persistent_browser
from .config import AccountRunConfig, SystemConfig, build_order_config
from .db import Database
from .feishu import FeishuGateway
from .order_guard import PersistentOrderGuard
from .checkout import RunResult, check_login, run_account
from .sale_state import (
    ACTIVE_SESSION_STATUSES,
    PREWARM_SECONDS,
    sale_phase,
)
from .task_service import (
    SessionUnavailable,
    TaskService,
)


log = logging.getLogger("pxq.auto")
ERROR_ALERT_COOLDOWN = 3600.0
MAX_BACKOFF_FACTOR = 16
MAX_CONCURRENT_POLLS = 3
NOTICE_RETRY_SECONDS = 10.0


@dataclass(frozen=True)
class PendingNotice:
    key: str
    title: str
    body: str
    template: str
    required_task_status: str = "active"
    available_after: frozenset[str] | None = None
    error_alert: bool = False


class TaskScheduler:
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
        self.failures: dict[int, int] = {}
        self.last_error_alert: dict[int, float] = {}
        self.pending_notices: dict[int, dict[str, PendingNotice]] = {}
        self.next_notice_retry = 0.0
        self.available_plans: dict[int, set[str]] = {}
        self.phases: dict[int, str] = {}

    async def run_forever(self) -> None:
        log.info("自动抢票引擎启动")
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("抢票调度循环异常，将在下一轮重试")
            await asyncio.sleep(1)

    async def tick(self) -> None:
        tasks = self.db.list_tasks()
        active_ids = {task["id"] for task in tasks if task["status"] == "active"}
        self._prune_runtime_state(active_ids)
        now = time.monotonic()
        due = [
            task
            for task in tasks
            if task["status"] == "active" and now >= self.next_poll.get(task["id"], 0)
        ]
        if due:
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_POLLS)
            session_tasks: dict[str, asyncio.Task[list[dict]]] = {}

            async def poll_due(task) -> None:
                async with semaphore:
                    show_id = str(task["show_id"])
                    sessions_task = session_tasks.get(show_id)
                    if sessions_task is None:
                        sessions_task = asyncio.create_task(
                            self.service.client.quick_order_sessions(show_id)
                        )
                        session_tasks[show_id] = sessions_task
                    await self._poll_safely(task, sessions_task)

            await asyncio.gather(*(poll_due(task) for task in due))
        await self._retry_notices()

    async def _poll_safely(
        self,
        task,
        sessions_task: asyncio.Task[list[dict]],
    ) -> None:
        task_id = int(task["id"])
        try:
            await self._poll(task, sessions_task)
            if self.failures.get(task_id):
                log.info("抢票任务 #%s 轮询恢复正常", task_id)
                self.pending_notices.get(task_id, {}).pop("poll-error", None)
            self.failures[task_id] = 0
        except SessionUnavailable:
            self.failures[task_id] = 0
            await self._pause_task(
                task,
                "session-gone",
                "目标场次未出现在官方场次列表",
            )
        except PxqError as exc:
            if exc.status_code == 22024033:
                self.failures[task_id] = 0
                await self._pause_task(
                    task,
                    "show-unavailable",
                    f"票星球返回 22024033：{exc.comments or '节目暂不可售'}",
                )
            else:
                self._handle_poll_error(task, exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._handle_poll_error(task, exc)
        finally:
            self._schedule_next(task_id)

    async def _poll(
        self,
        task,
        sessions_task: asyncio.Task[list[dict]],
    ) -> None:
        task_id = int(task["id"])
        if task_id not in self.available_plans:
            previous = self.db.get_task_plans(task_id)
            self.available_plans[task_id] = self._available_plan_ids(
                task_id,
                previous,
                sale_phase(task, previous),
            )
        status, sale_time_ms, snapshot = await self.service.refresh_task(
            task, sessions_task
        )
        if not self.db.update_task_snapshot(task_id, status, sale_time_ms, snapshot):
            return
        task = self.db.get_task(task_id)
        assert task is not None
        plans = self.db.get_task_plans(task_id)
        now_ms = int(time.time() * 1000)
        remaining = (
            (task["sale_time_ms"] - now_ms) / 1000
            if status == "PENDING" and task["sale_time_ms"] is not None
            else None
        )
        if status == "DELAY":
            await self._pause_task(task, "session-delay", "目标场次延期（DELAY）")
            return
        if status not in ACTIVE_SESSION_STATUSES:
            await self._pause_task(
                task,
                "unknown-status",
                f"官方返回未知场次状态：{status}",
            )
            return

        phase = sale_phase(task, plans, now_ms)
        previous_phase = self.phases.get(task_id)
        if phase != previous_phase:
            log.info(
                "抢票任务 #%s 阶段切换：%s -> %s（距开售 %s）",
                task_id,
                previous_phase or "初始",
                phase,
                "未知" if remaining is None else f"{remaining:.1f} 秒",
            )
            self.phases[task_id] = phase
        prewarm = phase == "PREWARM"
        if self.system.create_order_enabled:
            for account in self.db.list_accounts(task_id):
                account_plans = self.db.get_account_plans(account["id"])
                available = any(
                    plan["sale_started"] and plan["can_buy_count"] > 0
                    for plan in account_plans
                )
                if (
                    account["enabled"]
                    and account["status"] == "READY"
                    and (prewarm or (phase == "AVAILABLE" and available))
                ):
                    self._start_account(account["id"], prewarm=prewarm)

        self._update_stock_notice(task, plans, phase)
        self._schedule_login_checks(task, remaining)

    async def _pause_task(self, task, key: str, reason: str) -> None:
        task_id = int(task["id"])
        if not self.db.set_task_status(task_id, "paused", current_status="active"):
            return
        await self.cancel_task(task_id, reason=reason)
        self._clear_runtime_state(task_id)
        self._send_notice(
            task_id,
            PendingNotice(
                key,
                "抢票任务已暂停",
                (
                    f"任务 #{task_id}\n**{task['show_name']}**\n"
                    f"场次：{task['session_name']}\n{reason}\n"
                    f"核对后发送：恢复 {task_id}"
                ),
                "orange",
                required_task_status="paused",
            ),
        )

    def _handle_poll_error(self, task, exc: Exception) -> None:
        task_id = int(task["id"])
        failures = self.failures.get(task_id, 0) + 1
        self.failures[task_id] = failures
        log.warning(
            "抢票任务 #%s 轮询失败（连续 %s 次）：%s",
            task_id,
            failures,
            exc,
        )
        if failures >= 5:
            self._alert_poll_error(task, failures, exc)

    def _update_stock_notice(self, task, plans, phase: str) -> None:
        task_id = int(task["id"])
        current = self._available_plan_ids(task_id, plans, phase)
        added = current - self.available_plans[task_id]
        if not added:
            self.available_plans[task_id] = current
            return
        available = [plan for plan in plans if str(plan["seat_plan_id"]) in added]
        self._send_notice(
            task_id,
            PendingNotice(
                "stock",
                "余票提醒",
                "\n".join(
                    (
                        f"任务 #{task_id}\n**{task['show_name']}**",
                        f"场次：{task['session_name']}",
                        *(
                            f"· {plan['plan_name']}：最多可买 "
                            f"{plan['can_buy_count'] * plan['unit_qty']} 张"
                            for plan in available
                        ),
                    )
                ),
                "red",
                available_after=frozenset(current),
            ),
        )

    def _available_plan_ids(self, task_id: int, plans, phase: str) -> set[str]:
        watched = {
            str(plan["seat_plan_id"])
            for account in self.db.list_accounts(task_id)
            if account["enabled"]
            for plan in self.db.get_account_plans(account["id"])
        }
        return {
            str(plan["seat_plan_id"])
            for plan in plans
            if phase == "AVAILABLE"
            and str(plan["seat_plan_id"]) in watched
            and plan["sale_started"]
            and plan["can_buy_count"] > 0
        }

    def _alert_poll_error(
        self,
        task,
        failures: int,
        exc: Exception,
    ) -> None:
        task_id = int(task["id"])
        last = self.last_error_alert.get(task_id)
        if last is not None and time.monotonic() - last < ERROR_ALERT_COOLDOWN:
            return
        self._send_notice(
            task_id,
            PendingNotice(
                "poll-error",
                "抢票监控异常",
                (
                    f"任务 #{task_id}\n**{task['show_name']}**\n"
                    f"连续 {failures} 次抓取失败，已自动放慢频率。\n最近错误：{exc}"
                ),
                "orange",
                error_alert=True,
            ),
        )

    def _send_notice(self, task_id: int, notice: PendingNotice) -> None:
        pending = self.pending_notices.setdefault(task_id, {})
        if notice.key in pending:
            return
        pending[notice.key] = notice

    async def _retry_notices(self) -> None:
        if not self.pending_notices or time.monotonic() < self.next_notice_retry:
            return
        failed = False
        for task_id, pending in list(self.pending_notices.items()):
            task = self.db.get_task(task_id)
            if not task:
                self.pending_notices.pop(task_id, None)
                continue
            for notice in list(pending.values()):
                if task["status"] != notice.required_task_status:
                    pending.pop(notice.key, None)
                    continue
                if not await self.feishu.send_card(
                    notice.title,
                    notice.body,
                    notice.template,
                ):
                    failed = True
                    continue
                pending.pop(notice.key, None)
                self._complete_notice(task_id, notice)
            if not pending:
                self.pending_notices.pop(task_id, None)
        self.next_notice_retry = (
            time.monotonic() + NOTICE_RETRY_SECONDS if failed else 0.0
        )

    def _complete_notice(self, task_id: int, notice: PendingNotice) -> None:
        if notice.available_after is not None:
            self.available_plans[task_id] = set(notice.available_after)
        if notice.error_alert:
            self.last_error_alert[task_id] = time.monotonic()

    def _schedule_next(self, task_id: int) -> None:
        task = self.db.get_task(task_id)
        if not task or task["status"] != "active":
            self._clear_runtime_state(task_id)
            return
        delay = float(task["interval_sec"])
        delay *= min(2 ** self.failures.get(task_id, 0), MAX_BACKOFF_FACTOR)
        if task["session_status"] == "PENDING" and task["sale_time_ms"] is not None:
            remaining = (task["sale_time_ms"] - int(time.time() * 1000)) / 1000
            if remaining > PREWARM_SECONDS:
                delay = min(delay, max(1.0, remaining - PREWARM_SECONDS))
        self.next_poll[task_id] = time.monotonic() + delay

    def _prune_runtime_state(self, active_ids: set[int]) -> None:
        for mapping in (
            self.next_poll,
            self.failures,
            self.last_error_alert,
            self.available_plans,
            self.phases,
        ):
            for task_id in set(mapping) - active_ids:
                mapping.pop(task_id, None)

    def _clear_runtime_state(self, task_id: int) -> None:
        for mapping in (
            self.next_poll,
            self.failures,
            self.last_error_alert,
            self.available_plans,
            self.phases,
        ):
            mapping.pop(task_id, None)

    def _start_account(self, account_id: int, *, prewarm: bool) -> None:
        if account_id in self.jobs:
            log.debug("账号 #%s 已有执行任务，忽略重复启动", account_id)
            return
        account = self.db.get_account(account_id)
        task_id = int(account["task_id"]) if account else None
        mode = "开售预热" if prewarm else "回流抢票"
        log.info("任务 #%s 账号 #%s 已加入执行队列（%s）", task_id, account_id, mode)
        job = asyncio.create_task(
            self._run_after_check(account_id, prewarm, self.checks.get(account_id)),
            name=f"pxq-account-{account_id}-{'prewarm' if prewarm else 'available'}",
        )
        self.jobs[account_id] = job

        def forget_job(_job: asyncio.Task) -> None:
            if self.jobs.get(account_id) is _job:
                self.jobs.pop(account_id, None)
            try:
                outcome = _job.result()
            except asyncio.CancelledError:
                log.warning(
                    "任务 #%s 账号 #%s 执行任务被取消（%s）",
                    task_id,
                    account_id,
                    mode,
                )
            except Exception as exc:
                log.error(
                    "任务 #%s 账号 #%s 执行任务异常退出（%s）：%s",
                    task_id,
                    account_id,
                    mode,
                    exc,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
            else:
                log.info(
                    "任务 #%s 账号 #%s 执行任务结束（%s）：%s",
                    task_id,
                    account_id,
                    mode,
                    outcome,
                )

        job.add_done_callback(forget_job)

    async def _run_after_check(
        self,
        account_id: int,
        prewarm: bool,
        check: asyncio.Task | None,
    ) -> str:
        if check:
            log.info("账号 #%s 启动抢票前取消正在进行的登录检查", account_id)
            check.cancel()
            await asyncio.gather(check, return_exceptions=True)
        return await self._run_account(account_id, prewarm)

    async def _run_account(self, account_id: int, prewarm: bool) -> str:
        async with self.semaphore:
            log.info(
                "账号 #%s 获得执行权（%s）",
                account_id,
                "开售预热" if prewarm else "回流抢票",
            )
            account = self.db.get_account(account_id)
            if not account:
                return "账号已不存在"
            if not account["enabled"]:
                return "账号未启动"
            if account["status"] != "READY":
                return f"账号状态为 {account['status']}"
            task = self.db.get_task(account["task_id"])
            if not task:
                return "任务已不存在"
            if task["status"] != "active":
                return f"任务状态为 {task['status']}"
            plans = self.db.get_account_plans(account_id)
            if prewarm:
                phase = sale_phase(task, plans)
                if phase == "AVAILABLE":
                    prewarm = False
                elif phase != "PREWARM":
                    return f"预热阶段已结束（{phase}）"
            people = self.db.get_audiences(account_id)
            config = build_order_config(task, plans, people, account, self.system)
            if not self.db.claim_account(account_id):
                return "账号状态竞争失败"
            try:
                async with persistent_browser(config.browser) as context:
                    result = await run_account(
                        config,
                        context,
                        prewarm=prewarm,
                        trace_label=f"任务 #{task['id']}｜账号 #{account_id}",
                    )
            except asyncio.CancelledError:
                current = self.db.get_account(account_id)
                if current:
                    protected = _protected_order_state(config)
                    if protected:
                        self.db.set_account_status(
                            account_id,
                            protected[0],
                            order_id=protected[1],
                            error="执行中断，请人工核对待支付订单",
                        )
                    else:
                        self.db.set_account_status(
                            account_id,
                            "READY" if current["enabled"] else "STOPPED",
                        )
                raise
            except Exception as exc:
                log.exception("账号 #%s 抢票执行失败", account_id)
                current = self.db.get_account(account_id)
                result = RunResult("FAILED", str(exc))
                if current:
                    protected = _protected_order_state(config)
                    if protected:
                        status, order_id = protected
                        self.db.set_account_status(
                            account_id,
                            status,
                            order_id=order_id,
                            error=str(exc),
                        )
                        result = RunResult(status, str(exc), order_id=order_id)
                    else:
                        self.db.set_account_status(
                            account_id,
                            "READY" if current["enabled"] else "STOPPED",
                            error=str(exc),
                        )
                self._notify_result(account_id, task, result)
                return result.status
            self.db.apply_fulfillment(
                account_id,
                result.fulfilled_quantity,
                result.removed_audiences,
            )
            current = self.db.get_account(account_id)
            if current is None:
                return "执行完成后账号已不存在"
            next_status = {
                "CREATED": "CREATED",
                "UNKNOWN": "UNKNOWN",
                "NEEDS_LOGIN": "NEEDS_LOGIN",
                "COMPLETE": "COMPLETE",
            }.get(result.status, "READY" if current["enabled"] else "STOPPED")
            self.db.set_account_status(
                account_id,
                next_status,
                order_id=result.order_id,
                error=""
                if result.status in {"CREATED", "COMPLETE"}
                else result.message,
            )
            if result.status != "RESTOCK":
                self._notify_result(account_id, task, result)
            return result.status

    def _schedule_login_checks(self, task, remaining: float | None) -> None:
        interval = AuthGuard.interval(
            remaining if remaining is not None else float("inf")
        )
        now = time.monotonic()
        for account in self.db.list_accounts(task["id"]):
            account_id = int(account["id"])
            if (
                not account["enabled"]
                or account["status"] != "READY"
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
            if (
                not account
                or not account["enabled"]
                or account["status"] != "READY"
                or account_id in self.jobs
            ):
                return
            plans = self.db.get_account_plans(account_id)
            people = self.db.get_audiences(account_id)
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
                self._send_notice(
                    int(task["id"]),
                    PendingNotice(
                        f"login:{account_id}",
                        "账号登录已失效",
                        f"任务 #{task['id']}｜账号 #{account_id}\n发送：登录 {task['id']}",
                        "orange",
                    ),
                )

    def _notify_result(self, account_id: int, task, result: RunResult) -> None:
        title = {
            "CREATED": "订单已创建",
            "UNKNOWN": "订单结果未知",
            "NEEDS_LOGIN": "账号需要重新登录",
            "COMPLETE": "目标数量已完成",
        }.get(result.status, "抢票执行异常")
        color = "green" if result.status in {"CREATED", "COMPLETE"} else "orange"
        action = {
            "CREATED": (
                "下一步：在票星球处理待支付订单；"
                f"如已取消并需继续，发送：重置 {account_id}"
            ),
            "UNKNOWN": (
                f"下一步：检查票星球待支付订单；确认无订单后发送：重置 {account_id}"
            ),
            "NEEDS_LOGIN": f"下一步：登录 {task['id']}",
            "COMPLETE": f"下一步：配置 {account_id}（设置新的目标数量）",
        }.get(result.status, "状态：账号保持启动，将自动继续等待。")
        body = (
            f"任务 #{task['id']}｜账号 #{account_id}\n"
            f"**{task['show_name']}**\n{result.message}\n{action}\n操作：未支付"
        )
        self._send_notice(
            int(task["id"]),
            PendingNotice(
                f"result:{account_id}:{result.status}",
                title,
                body,
                color,
            ),
        )

    async def cancel_account(
        self,
        account_id: int,
        *,
        reason: str = "账号操作",
    ) -> None:
        account = self.db.get_account(account_id)
        if account:
            pending = self.pending_notices.get(int(account["task_id"]))
            if pending:
                for key in tuple(pending):
                    if key == f"login:{account_id}" or key.startswith(
                        f"result:{account_id}:"
                    ):
                        pending.pop(key, None)
        self.next_auth.pop(account_id, None)
        activities = [
            activity
            for activity in (self.jobs.get(account_id), self.checks.get(account_id))
            if activity
        ]
        if activities:
            log.warning("取消账号 #%s 后台任务：%s", account_id, reason)
        for activity in activities:
            activity.cancel()
        if activities:
            await asyncio.gather(*activities, return_exceptions=True)

    async def cancel_task(self, task_id: int, *, reason: str = "任务操作") -> None:
        self.pending_notices.pop(task_id, None)
        await asyncio.gather(
            *(
                self.cancel_account(
                    account["id"],
                    reason=f"{reason}（任务 #{task_id}）",
                )
                for account in self.db.list_accounts(task_id)
            )
        )

    async def close(self) -> None:
        activities = [*self.jobs.values(), *self.checks.values()]
        for activity in activities:
            activity.cancel()
        if activities:
            await asyncio.gather(*activities, return_exceptions=True)


def _protected_order_state(
    config: AccountRunConfig,
) -> tuple[str, str | None] | None:
    try:
        state = PersistentOrderGuard(config.state_path, config.plan_key).current()
    except RuntimeError:
        return "UNKNOWN", None
    if state.status == "SUBMITTING":
        return "UNKNOWN", state.order_id
    if state.status in {"CREATED", "UNKNOWN"}:
        return state.status, state.order_id
    return None
