from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass

from .auth import AuthGuard, AuthenticationRequired
from .browser import save_screenshot
from .config import AppConfig
from .guard import OrderFirewall, PersistentOrderGuard
from .inventory import (
    GeneralAdmissionInventory,
    Inventory,
    InventoryBootstrap,
    InventoryUnavailable,
    StaticInventoryUnavailable,
)
from .presale import SaleGate, SaleUnavailable
from .site import (
    CreateResponseWatcher,
    PiaoxingqiuPage,
    find_already_purchased_ids,
    is_audience_already_purchased,
    is_seat_lost,
    match_configured_ids,
)


@dataclass(frozen=True)
class RunResult:
    status: str
    message: str
    order_id: str | None = None
    removed_audiences: tuple[str, ...] = ()


async def run_account(config: AppConfig, context, *, prewarm: bool) -> RunResult:
    """执行一个账号的一次自动抢票生命周期，最多成功创建一个订单。"""
    if not config.create_order:
        return RunResult("DISABLED", "全局 create_order_enabled 尚未开启")
    page = context.pages[0] if context.pages else await context.new_page()
    site = PiaoxingqiuPage(page, config)
    firewall = OrderFirewall()
    await context.route("**/*", firewall.route)
    watcher = CreateResponseWatcher()
    context.on("response", watcher.handle)
    guard = PersistentOrderGuard(config.state_path, config.plan_key)
    guard.require_ready()
    auth = AuthGuard(site)
    try:
        await auth.ensure()
    except AuthenticationRequired:
        return RunResult("NEEDS_LOGIN", "登录状态已失效")

    seat_source: Inventory | None = None
    general_source: GeneralAdmissionInventory | None = None
    bootstrap: InventoryBootstrap | None = None
    people = config.purchase.audiences
    removed: list[str] = []
    try:
        if not config.project.support_seat_picking:
            if prewarm:
                gate = SaleGate(site)
                sale = await gate.fetch()
                if not sale.on_sale:
                    sale = await gate.wait_until_prewarm(sale, auth)
                    if not sale.on_sale:
                        await gate.wait_until_sale(sale, auth)
            general_source = GeneralAdmissionInventory.open(site, auth)
            if prewarm:
                general_selection = await _parallel_result(
                    general_source.wait_available(len(people)),
                    site.open_purchase(),
                )
            else:
                general_selection = await general_source.refresh(len(people))
            selected_people = people[: general_selection.quantity]
            prepared = await site.prepare_general_order(
                general_selection, selected_people
            )
        elif prewarm:
            gate = SaleGate(site)
            sale = await gate.fetch()
            if not sale.on_sale:
                sale = await gate.wait_until_prewarm(sale, auth)
                bootstrap = InventoryBootstrap.open(site, auth)
                with suppress(StaticInventoryUnavailable):
                    seat_source = await bootstrap.activate()
                if not sale.on_sale:
                    await gate.wait_until_sale(sale, auth)
            if bootstrap is None:
                bootstrap = InventoryBootstrap.open(site, auth)

            async def wait_selection():
                current = seat_source or await bootstrap.wait_static()
                return current, await current.wait_available(len(people))

            seat_source, seat_selection = await _parallel_result(
                wait_selection(),
                site.open_purchase(),
            )
            selected_people = people[: len(seat_selection.candidates)]
            prepared = await site.prepare_order(seat_selection, selected_people)
        else:
            seat_source = await Inventory.open(site, auth)
            seat_selection = await seat_source.refresh(len(people))
            selected_people = people[: len(seat_selection.candidates)]
            prepared = await site.prepare_order(seat_selection, selected_people)
    except AuthenticationRequired:
        return RunResult("NEEDS_LOGIN", "登录状态已失效")
    except (InventoryUnavailable, SaleUnavailable):
        return RunResult("RESTOCK", "配置票档当前没有可售库存")
    except StaticInventoryUnavailable:
        return RunResult("RESTOCK", "静态座位资源尚未下发")
    except Exception:
        await _save_failure(site, config, "prepare-failed")
        raise

    if firewall.blocked_requests:
        raise RuntimeError("准备阶段出现意外创建请求，已拦截并停止")

    attempt = 0
    while True:
        attempt += 1
        try:
            await auth.require_recent()
        except AuthenticationRequired:
            return RunResult("NEEDS_LOGIN", "提交前登录状态已失效")
        firewall.arm_once()
        guard.submitting()
        try:
            await site._click_action(prepared)
        except Exception as exc:
            firewall.disarm()
            if firewall.attempt_allowed:
                guard.unknown()
                return RunResult(
                    "UNKNOWN",
                    f"创建请求可能已经发出，但点击流程异常：{exc}",
                    removed_audiences=tuple(removed),
                )
            guard.ready()
            raise
        try:
            result = await watcher.wait(config.browser.timeout_ms / 1000)
        except TimeoutError:
            firewall.disarm()
            blocked = _unexpected_posts(firewall)
            if firewall.attempt_allowed:
                guard.unknown()
                return RunResult(
                    "UNKNOWN",
                    "创建请求已经发出，但没有观察到确定结果" + blocked,
                )
            guard.ready()
            return RunResult("FAILED", "没有创建请求被放行" + blocked)
        firewall.disarm()

        if result.success:
            order_id = result.order_id
            guard.created(order_id)
            fulfilled = tuple(person.masked_id for person in selected_people)
            removed.extend(item for item in fulfilled if item not in removed)
            remaining = len(people) - len(selected_people)
            message = f"订单已创建（尝试 {attempt} 次）"
            if remaining:
                message += f"；本单 {len(selected_people)} 人，剩余 {remaining} 人将在本单处理并重置后继续等待"
            return RunResult(
                "CREATED",
                message,
                order_id,
                tuple(removed),
            )

        reported = find_already_purchased_ids(result.message or "")
        purchased = match_configured_ids(
            reported, tuple(person.masked_id for person in selected_people)
        )
        if is_seat_lost(result):
            guard.ready()
        elif is_audience_already_purchased(result) and purchased:
            guard.ready()
            removed.extend(item for item in purchased if item not in removed)
            people = tuple(
                person for person in people if person.masked_id not in purchased
            )
            if not people:
                return RunResult(
                    "COMPLETE",
                    "配置的观演人均已购买，本次不再创建订单",
                    removed_audiences=tuple(removed),
                )
        else:
            guard.unknown()
            await _save_failure(site, config, "create-failed")
            return RunResult(
                "UNKNOWN",
                (
                    f"创建结果未知：code={result.code or '无'}"
                    f" subCode={result.sub_code or '无'} {result.message or ''}"
                    f"{_unexpected_posts(firewall)}"
                ),
                removed_audiences=tuple(removed),
            )

        try:
            if config.project.support_seat_picking:
                if seat_source is None:
                    raise RuntimeError("选座库存状态无效")
                seat_selection = await _parallel_result(
                    seat_source.refresh(len(people)),
                    site.reopen_seat_map(),
                )
                selected_people = people[: len(seat_selection.candidates)]
                prepared = await site.prepare_order(
                    seat_selection, selected_people, open_map=False
                )
            else:
                if general_source is None:
                    raise RuntimeError("票档库存状态无效")
                general_selection = await _parallel_result(
                    general_source.refresh(len(people)),
                    site.open_purchase(),
                )
                selected_people = people[: general_selection.quantity]
                prepared = await site.prepare_general_order(
                    general_selection, selected_people
                )
        except AuthenticationRequired:
            return RunResult(
                "NEEDS_LOGIN",
                "冲突恢复时登录状态已失效",
                removed_audiences=tuple(removed),
            )
        except InventoryUnavailable:
            return RunResult(
                "RESTOCK",
                "冲突后刷新实时库存，当前已无可售票",
                removed_audiences=tuple(removed),
            )
        except Exception:
            await _save_failure(site, config, "recover-failed")
            raise


async def _parallel_result(result_coro, side_coro):
    result = asyncio.create_task(result_coro)
    side = asyncio.create_task(side_coro)
    try:
        value, _ = await asyncio.gather(result, side)
        return value
    except BaseException:
        result.cancel()
        side.cancel()
        await asyncio.gather(result, side, return_exceptions=True)
        raise


async def check_login(config: AppConfig, context) -> bool:
    page = context.pages[0] if context.pages else await context.new_page()
    site = PiaoxingqiuPage(page, config)
    try:
        await AuthGuard(site).ensure()
        return True
    except AuthenticationRequired:
        return False


async def _save_failure(site: PiaoxingqiuPage, config: AppConfig, name: str) -> None:
    with suppress(Exception):
        directory = config.browser.profile_dir.parent / "artifacts"
        await save_screenshot(site.page, directory, name)


def _unexpected_posts(firewall: OrderFirewall) -> str:
    if not firewall.unexpected_posts:
        return ""
    return "；已拦截未识别 POST：" + "、".join(sorted(firewall.unexpected_posts))
