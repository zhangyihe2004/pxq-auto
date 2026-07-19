from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from playwright.async_api import Request, Route


GuardStatus = Literal["READY", "SUBMITTING", "CREATED", "UNKNOWN"]
CREATE_MARKERS = (
    "/trade/buyer/v1/items/orders/submit",
    "/create_order",
    "/createorder",
)


@dataclass
class OrderState:
    plan_key: str
    status: GuardStatus
    updated_at: str
    order_id: str | None = None


class PersistentOrderGuard:
    def __init__(self, path: Path, plan_key: str) -> None:
        self.path = path
        self.plan_key = plan_key

    def current(self) -> OrderState:
        if not self.path.exists():
            return self._new("READY")
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if raw.get("plan_key") != self.plan_key:
                return self._new("READY")
            return OrderState(
                plan_key=self.plan_key,
                status=raw["status"],
                updated_at=raw["updated_at"],
                order_id=raw.get("order_id"),
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise RuntimeError(f"下单状态文件损坏，请人工检查：{self.path}") from exc

    def require_ready(self) -> None:
        state = self.current()
        if state.status != "READY":
            raise RuntimeError(
                f"当前计划状态为 {state.status}，为避免重复订单已禁止再次提交。"
                f"请先在票星球“待支付”订单中人工核对。状态文件：{self.path}"
            )

    def submitting(self) -> None:
        self._write(self._new("SUBMITTING"))

    def ready(self) -> None:
        self._write(self._new("READY"))

    def created(self, order_id: str | None = None) -> None:
        self._write(self._new("CREATED", order_id=order_id))

    def unknown(self) -> None:
        self._write(self._new("UNKNOWN"))

    def _new(
        self,
        status: GuardStatus,
        *,
        order_id: str | None = None,
    ) -> OrderState:
        return OrderState(
            plan_key=self.plan_key,
            status=status,
            updated_at=datetime.now(timezone.utc).isoformat(),
            order_id=order_id,
        )

    def _write(self, state: OrderState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(asdict(state), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, self.path)


class OrderFirewall:
    """创建订单请求默认拒绝；武装后仅放行一个请求。"""

    def __init__(self) -> None:
        self.armed = False
        self.attempt_allowed = False
        self.blocked_requests = 0

    async def route(self, route: Route, request: Request) -> None:
        if request.method.upper() != "POST":
            await route.continue_()
            return

        request_url = request.url.lower()
        is_create = any(marker in request_url for marker in CREATE_MARKERS)
        if is_create:
            if self.armed and not self.attempt_allowed:
                self.attempt_allowed = True
                await route.continue_()
                return
            self.blocked_requests += 1
            await route.abort("blockedbyclient")
            return

        if self.armed:
            self.blocked_requests += 1
            await route.abort("blockedbyclient")
            return
        await route.continue_()

    def arm_once(self) -> None:
        if self.armed:
            raise RuntimeError("创建请求防火墙已处于武装状态")
        self.armed = True
        self.attempt_allowed = False

    def disarm(self) -> None:
        self.armed = False
