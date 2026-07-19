from __future__ import annotations

import re
from typing import Any

from .api import PxqClient
from .db import Database


SHOW_ID_RE = re.compile(r"(?<![0-9a-f])([0-9a-f]{24})(?![0-9a-f])", re.I)
MIN_INTERVAL = 10


def extract_show_id(value: str) -> str | None:
    match = SHOW_ID_RE.search(value)
    return match.group(1).lower() if match else None


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


class TaskService:
    def __init__(self, db: Database, client: PxqClient):
        self.db = db
        self.client = client

    async def search(self, keyword: str) -> list[dict]:
        return await self.client.search_shows(keyword)

    async def show_sessions(self, show_id: str) -> tuple[str, list[dict]]:
        import asyncio

        static, dynamic = await asyncio.gather(
            self.client.sessions_static(show_id), self.client.show_dynamic(show_id)
        )
        sessions = [dict(session) for session in static["sessionVOs"]]
        for session in sessions:
            if sale_time := _session_sale_time(
                dynamic, str(session.get("bizShowSessionId") or "")
            ):
                session["_sale_time_ms"] = sale_time
        return str(static.get("showName") or show_id), sessions

    async def plans(self, show_id: str, session_id: str) -> list[dict]:
        static, dynamic = await self._plan_payloads(show_id, session_id)
        live = {item["seatPlanId"]: item for item in dynamic["seatPlans"]}
        return [
            {
                "pid": item["seatPlanId"],
                "plan_name": str(item.get("seatPlanName") or item["seatPlanId"]),
                "price": float(item.get("originalPrice") or 0),
                "can_buy_count": int(
                    live.get(item["seatPlanId"], {}).get("canBuyCount") or 0
                ),
                "sale_started": bool(
                    live.get(item["seatPlanId"], {}).get("saleStarted")
                ),
            }
            for item in static["seatPlans"]
        ]

    def create_task(
        self,
        *,
        show_id: str,
        show_name: str,
        session: dict,
        plans: list[dict],
        interval: int,
    ) -> tuple[int, bool, list[dict]]:
        session_id = str(session["bizShowSessionId"])
        if not plans:
            raise ValueError("该场次当前没有票档")
        task_id, created = self.db.create_task(
            show_id=show_id,
            show_name=show_name,
            session_id=session_id,
            session_name=str(session.get("sessionName") or session_id),
            interval_sec=max(MIN_INTERVAL, interval),
            sale_time_ms=session.get("_sale_time_ms") or _sale_time(session),
            plans=plans,
        )
        return task_id, created, plans

    async def refresh_task(self, task) -> tuple[str, list[tuple[str, int, bool]]]:
        show_id, session_id = task["show_id"], task["session_id"]
        sessions, plans = await self._dynamic_payloads(show_id, session_id)
        session = next(
            (
                item
                for item in sessions["sessionVOs"]
                if item.get("bizShowSessionId") == session_id
            ),
            None,
        )
        if session is None:
            raise RuntimeError("目标场次已从公开接口移除")
        snapshot = [
            (
                str(item["seatPlanId"]),
                int(item.get("canBuyCount") or 0),
                bool(item.get("saleStarted")),
            )
            for item in plans["seatPlans"]
        ]
        return str(session.get("sessionStatus") or ""), snapshot

    async def _plan_payloads(self, show_id: str, session_id: str):
        import asyncio

        return await asyncio.gather(
            self.client.seat_plans_static(show_id, session_id),
            self.client.seat_plans_dynamic(show_id, session_id),
        )

    async def _dynamic_payloads(self, show_id: str, session_id: str):
        import asyncio

        return await asyncio.gather(
            self.client.sessions_dynamic(show_id),
            self.client.seat_plans_dynamic(show_id, session_id),
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
            if key in {"saleTime", "saleStartTime", "startSaleTime"}:
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
