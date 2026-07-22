from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable
from typing import Any

from .api import PxqClient
from .db import Database


MIN_INTERVAL = 10
PREWARM_SECONDS = 60
ON_SALE_STATUSES = {"ONSALE", "ON_SALE", "LACK_OF_TICKET"}
TERMINAL_STATUSES = {"SALE_END", "ENDED", "CANCELLED", "CANCELED", "OFF_SHELF"}


class SessionUnavailable(RuntimeError):
    pass


def parse_numbers(value: str, upper: int) -> list[int]:
    try:
        numbers = list(
            dict.fromkeys(
                int(item.strip())
                for item in value.replace("，", ",").split(",")
                if item.strip()
            )
        )
    except ValueError as exc:
        raise ValueError("票档编号必须用逗号分隔，如 1,3") from exc
    if not numbers or any(number < 1 or number > upper for number in numbers):
        raise ValueError(f"票档编号必须在 1~{upper} 之间")
    return numbers


def sale_phase(task, plans, now_ms: int | None = None) -> str:
    """Return PRESALE, PREWARM, AVAILABLE, ONSALE or WAITING."""
    sale_time_ms = task["sale_time_ms"]
    if sale_time_ms is not None:
        current_ms = int(time.time() * 1000) if now_ms is None else now_ms
        remaining = (sale_time_ms - current_ms) / 1000
        if remaining > 0:
            return "PREWARM" if remaining <= PREWARM_SECONDS else "PRESALE"
    if task["session_status"].upper() in ON_SALE_STATUSES:
        return (
            "AVAILABLE"
            if any(plan["sale_started"] and plan["can_buy_count"] > 0 for plan in plans)
            else "ONSALE"
        )
    return "WAITING"


class TaskService:
    def __init__(self, db: Database, client: PxqClient):
        self.db = db
        self.client = client

    async def search(self, keyword: str) -> list[dict]:
        return await self.client.search_shows(keyword)

    async def show_sessions(self, show_id: str) -> tuple[str, list[dict]]:
        sessions, dynamic = await asyncio.gather(
            self.client.quick_order_sessions(show_id),
            self.client.show_dynamic(show_id),
        )
        for session in sessions:
            if sale_time := _session_sale_time(
                dynamic, str(session.get("bizShowSessionId") or "")
            ) or _sale_time(session):
                session["_sale_time_ms"] = sale_time
        show_name = str(sessions[0].get("showName") or show_id) if sessions else show_id
        return show_name, sessions

    async def plans(self, show_id: str, session_id: str) -> list[dict]:
        payload = await self.client.quick_order_plans(show_id, session_id)
        return [
            {
                "pid": item["seatPlanId"],
                "plan_name": str(item.get("seatPlanName") or item["seatPlanId"]),
                "price": float(item.get("originalPrice") or 0),
                "can_buy_count": int(item.get("canBuyCount") or 0),
                "limitation": int(item.get("limitation") or 0),
                "sale_started": bool(item.get("saleStarted")),
            }
            for item in payload["seatPlans"]
        ]

    def create_task(
        self,
        *,
        show_id: str,
        show_name: str,
        session: dict,
        plans: list[dict],
        interval: int,
    ) -> tuple[int, bool]:
        session_id = str(session["bizShowSessionId"])
        if not plans:
            raise ValueError("该场次当前没有票档")
        task_id, created = self.db.create_task(
            show_id=show_id,
            show_name=show_name,
            session_id=session_id,
            session_name=str(session.get("sessionName") or session_id),
            support_seat_picking=bool(session.get("supportSeatPicking")),
            interval_sec=max(MIN_INTERVAL, interval),
            sale_time_ms=session.get("_sale_time_ms") or _sale_time(session),
            plans=plans,
        )
        return task_id, created

    async def refresh_task(
        self,
        task,
        sessions_task: Awaitable[list[dict]] | None = None,
    ) -> tuple[str, int | None, list[tuple[str, int, bool]]]:
        show_id, session_id = task["show_id"], task["session_id"]
        plans_task = asyncio.create_task(
            self.client.quick_order_plans(show_id, session_id)
        )
        try:
            sessions = await (
                sessions_task or self.client.quick_order_sessions(show_id)
            )
            session = next(
                (
                    item
                    for item in sessions
                    if item.get("bizShowSessionId") == session_id
                ),
                None,
            )
            if session is None:
                raise SessionUnavailable("目标场次已从公开接口移除")
            plans = await plans_task
        finally:
            if not plans_task.done():
                plans_task.cancel()
            await asyncio.gather(plans_task, return_exceptions=True)
        snapshot = [
            (
                str(item["seatPlanId"]),
                int(item.get("canBuyCount") or 0),
                bool(item.get("saleStarted")),
            )
            for item in plans["seatPlans"]
        ]
        return (
            str(session.get("sessionStatus") or session.get("bizSessionStatus") or ""),
            _sale_time(session),
            snapshot,
        )


def _sale_time(value: Any) -> int | None:
    candidates = [
        int(item)
        for item in _walk(value)
        if isinstance(item, (int, float)) and item > 1_000_000_000_000
    ]
    return min(candidates) if candidates else None


def _session_sale_time(value: Any, session_id: str) -> int | None:
    matches = [
        item
        for item in _walk_dicts(value)
        if str(
            item.get("bizShowSessionId")
            or item.get("showSessionId")
            or item.get("sessionId")
            or ""
        )
        == session_id
    ]
    return _sale_time(matches)


def _walk(value: Any):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {
                "sessionSaleTime",
                "saleTime",
                "saleStartTime",
                "startSaleTime",
            }:
                yield item
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk_dicts(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_dicts(item)
