from __future__ import annotations

import asyncio
import base64
import heapq
import math
import re
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar
from urllib.parse import urlencode

import geobuf
from .auth import AuthGuard, request_context
from .site import PiaoxingqiuPage, is_success_payload


BATCH_SIZE = 25
DYNAMIC_CONCURRENCY = 4
DOWNLOAD_CONCURRENCY = 5
FAST_STOCK_POLL_SECONDS = 0.25
FAST_STOCK_WINDOW_SECONDS = 5.0
STOCK_POLL_SECONDS = 1.0
STOCK_WAIT_SECONDS = 60.0
STATIC_UNAVAILABLE_CODE = "22024036"
T = TypeVar("T")


class InventoryUnavailable(RuntimeError):
    pass


class StaticInventoryUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class Seat:
    zone_id: str
    zone_name: str
    seat_id: str
    row: str
    seat_no: int
    x: float
    y: float


@dataclass(frozen=True)
class ZoneGeometry:
    center: tuple[float, float]


@dataclass(frozen=True)
class VenueGeometry:
    center: tuple[float, float]
    zones: dict[str, ZoneGeometry]


@dataclass(frozen=True)
class Candidate:
    seat: Seat
    plan: str
    plan_id: str
    plan_rank: int
    row_index: int


@dataclass(frozen=True)
class SeatGroup:
    cohesion: int
    candidates: tuple[Candidate, ...]

    @property
    def score(self) -> tuple:
        seats = self.candidates
        plan_priority = tuple(sorted(item.plan_rank for item in seats))
        if self.cohesion == 0:
            return plan_priority
        compactness = min(
            (max(distances), sum(distances))
            for anchor in seats
            if (distances := tuple(_distance(anchor.seat, item.seat) for item in seats))
        )
        if self.cohesion == 1:
            return plan_priority + compactness
        return compactness + plan_priority


@dataclass(frozen=True)
class SeatSelection:
    candidates: tuple[Candidate, ...]


@dataclass(frozen=True)
class GeneralAdmissionSelection:
    plan: str
    quantity: int


@dataclass
class GeneralAdmissionInventory:
    site: PiaoxingqiuPage
    endpoint: str
    common: dict[str, str]
    headers: dict[str, str]
    plan_names: tuple[str, ...]
    plan_ids: tuple[str, ...]

    @classmethod
    def open(
        cls,
        site: PiaoxingqiuPage,
        auth: AuthGuard,
    ) -> GeneralAdmissionInventory:
        show_id, session_id = site.booking_ids
        origin = site.origin
        if not auth.headers:
            raise RuntimeError("库存查询缺少已验证的登录状态")
        return cls(
            site=site,
            endpoint=(
                f"{origin}/cyy_gatewayapi/show/pub/v5/show/{show_id}/session/"
                f"{session_id}/seat_plans"
            ),
            common=request_context(auth.headers),
            headers=auth.headers,
            plan_names=site.config.purchase.plans,
            plan_ids=site.config.purchase.plan_ids,
        )

    async def refresh(self, quantity: int) -> GeneralAdmissionSelection:
        query = dict(self.common, source="FROM_QUICK_ORDER", src="WEB")
        response = await self.site.page.context.request.get(
            _url(self.endpoint, query), headers=self.headers
        )
        if not response.ok:
            raise RuntimeError(f"票档库存接口返回 HTTP {response.status}")
        data = _response_data(await response.json(), "票档库存接口")
        if not isinstance(data, dict) or not isinstance(data.get("seatPlans"), list):
            raise RuntimeError("票档库存接口缺少 seatPlans 数组")
        live = {
            str(item.get("seatPlanId") or ""): item
            for item in data["seatPlans"]
            if isinstance(item, dict)
        }
        options: list[GeneralAdmissionSelection] = []
        for name, plan_id in zip(self.plan_names, self.plan_ids):
            item = live.get(plan_id)
            if not item or not item.get("saleStarted"):
                continue
            can_buy = int(item.get("canBuyCount") or 0)
            limitation = int(item.get("limitation") or 0)
            available = min(can_buy, limitation) if limitation > 0 else can_buy
            if available > 0:
                options.append(
                    GeneralAdmissionSelection(
                        plan=name,
                        quantity=min(quantity, available),
                    )
                )
        if not options:
            raise InventoryUnavailable("配置票档当前均没有可售票")
        full = next((option for option in options if option.quantity == quantity), None)
        return full or max(options, key=lambda option: option.quantity)

    async def wait_available(self, quantity: int) -> GeneralAdmissionSelection:
        return await _wait_inventory(lambda: self.refresh(quantity))


@dataclass
class InventoryBootstrap:
    site: PiaoxingqiuPage
    endpoint: str
    static_url: str
    common: dict[str, str]
    headers: dict[str, str]
    plan_names: tuple[str, ...]
    plan_ids: tuple[str, ...]

    @classmethod
    def open(
        cls,
        site: PiaoxingqiuPage,
        auth: AuthGuard,
    ) -> InventoryBootstrap:
        show_id, session_id = site.booking_ids
        origin = site.origin
        headers = auth.headers
        if not headers:
            raise RuntimeError("库存预热缺少已验证的登录状态")
        common = request_context(headers)
        root = f"{origin}/cyy_gatewayapi/show"
        plan_names = site.config.purchase.plans
        plan_ids = site.config.purchase.plan_ids
        static_url = _url(
            f"{root}/pub/v5/show/{show_id}/session/{session_id}/seating/static",
            common,
        )
        site.prefilled_plan_id = _prefilled_plan_id(auth.show_user_data, session_id)
        return cls(
            site=site,
            endpoint=f"{root}/buyer/v5/show/{show_id}/session/{session_id}/seating/dynamic",
            static_url=static_url,
            common=common,
            headers=headers,
            plan_names=plan_names,
            plan_ids=plan_ids,
        )

    async def activate(self) -> Inventory:
        response = await self.site.page.context.request.get(self.static_url)
        if response.status in {401, 429, 469}:
            raise RuntimeError(
                f"静态座位接口触发限制（HTTP {response.status}），已停止"
            )
        if not response.ok:
            raise RuntimeError(f"静态座位接口返回 HTTP {response.status}")
        static_data = _static_data(await response.json())
        resources = {
            str(item["zoneConcreteId"]): str(item["url"])
            for item in static_data.get("staticResList", [])
            if isinstance(item, dict)
            and item.get("dataType") == "ZONE_SEAT_DATA"
            and item.get("zoneConcreteId")
            and item.get("url")
        }
        venue_url = next(
            (
                str(item["url"])
                for item in static_data.get("staticResList", [])
                if isinstance(item, dict)
                and item.get("dataType") == "VENUE_DATA"
                and item.get("url")
            ),
            None,
        )
        if not resources:
            raise StaticInventoryUnavailable("静态座位资源尚未下发")
        return Inventory(
            site=self.site,
            endpoint=self.endpoint,
            common=self.common,
            headers=self.headers,
            plan_names=self.plan_names,
            plan_ids=self.plan_ids,
            resources=resources,
            venue_url=venue_url,
            venue=None,
            zones={},
        )

    async def wait_static(self) -> Inventory:
        return await _wait_inventory(self.activate)


@dataclass
class Inventory:
    site: PiaoxingqiuPage
    endpoint: str
    common: dict[str, str]
    headers: dict[str, str]
    plan_names: tuple[str, ...]
    plan_ids: tuple[str, ...]
    resources: dict[str, str]
    venue_url: str | None
    venue: VenueGeometry | None
    zones: dict[str, tuple[Seat, ...]]

    @classmethod
    async def open(cls, site: PiaoxingqiuPage, auth: AuthGuard) -> Inventory:
        return await InventoryBootstrap.open(site, auth).activate()

    async def refresh(
        self,
        quantity: int,
    ) -> SeatSelection:
        records = await _fetch_all_dynamic(
            self.site,
            self.endpoint,
            self.common,
            self.headers,
            tuple(self.resources),
            self.plan_ids,
        )
        inventories = {
            (rank, plan_name, plan_id): {
                str(record["zoneConcreteId"]): bits
                for record in records
                if (bits := _plan_bits(record, plan_id)) and any(bits)
            }
            for rank, (plan_name, plan_id) in enumerate(
                zip(self.plan_names, self.plan_ids)
            )
        }
        zone_plans: dict[str, list[str]] = {}
        for (_, plan_name, _), bitsets in inventories.items():
            for zone_id in bitsets:
                zone_plans.setdefault(zone_id, []).append(plan_name)
        conflicts = {
            zone_id: names for zone_id, names in zone_plans.items() if len(names) > 1
        }
        if conflicts:
            details = "；".join(
                f"{zone_id}：{'、'.join(names)}" for zone_id, names in conflicts.items()
            )
            raise RuntimeError(f"同一看台出现多个可售票档，无法确定唯一价格：{details}")

        live_zone_ids = set(zone_plans)
        if not live_zone_ids:
            raise InventoryUnavailable("配置票档当前均没有可售座位")
        missing = live_zone_ids - self.zones.keys()
        if missing:
            self.zones.update(await _decode_zones(self.site, self.resources, missing))

        available_zones = [
            (
                rank,
                plan_name,
                plan_id,
                self.zones[zone_id],
                tuple(
                    seat
                    for seat in self.zones[zone_id]
                    if _bit_is_set(bits, seat.seat_no)
                ),
            )
            for (rank, plan_name, plan_id), bitsets in inventories.items()
            for zone_id, bits in bitsets.items()
        ]
        mapped_count = sum(len(item[4]) for item in available_zones)
        if not mapped_count:
            raise RuntimeError("动态库存存在，但未能映射到静态座位")
        selected_quantity = min(quantity, mapped_count)
        if selected_quantity > 1 and self.venue is None:
            if self.venue_url is None:
                raise RuntimeError("多人选座缺少全场几何资源")
            self.venue = await _decode_venue_geometry(self.site, self.venue_url)
        available = tuple(
            candidate
            for rank, plan_name, plan_id, seats, live_seats in available_zones
            for candidate in _rank_zone(
                seats,
                live_seats,
                self.venue,
                plan_name,
                plan_id,
                rank,
            )
        )
        selected = next(
            (
                group
                for current_quantity in range(selected_quantity, 0, -1)
                if (group := _select_group(available, current_quantity)) is not None
            ),
            None,
        )
        if selected is None:
            raise RuntimeError("可售座位无法组成有效选择")
        selected_candidates = tuple(
            sorted(
                selected.candidates,
                key=lambda item: (item.seat.zone_id, item.seat.seat_no),
            )
        )
        return SeatSelection(candidates=selected_candidates)

    async def wait_available(self, quantity: int) -> SeatSelection:
        return await _wait_inventory(lambda: self.refresh(quantity))


async def _wait_inventory(load: Callable[[], Awaitable[T]]) -> T:
    started = asyncio.get_running_loop().time()
    while True:
        try:
            return await load()
        except (InventoryUnavailable, StaticInventoryUnavailable):
            elapsed = asyncio.get_running_loop().time() - started
            if elapsed >= STOCK_WAIT_SECONDS:
                raise
            await asyncio.sleep(_stock_poll_delay(elapsed))


def _stock_poll_delay(elapsed: float) -> float:
    interval = (
        FAST_STOCK_POLL_SECONDS
        if elapsed < FAST_STOCK_WINDOW_SECONDS
        else STOCK_POLL_SECONDS
    )
    return min(interval, STOCK_WAIT_SECONDS - elapsed)


def _prefilled_plan_id(data: dict[str, Any], session_id: str) -> str | None:
    records = [
        item
        for key in ("preFilledList", "sessionPreFilledList")
        for item in data.get(key, [])
        if isinstance(item, dict)
        and str(item.get("bizShowSessionId") or "") == session_id
        and not item.get("existOrder")
    ]
    if not records:
        return None
    latest = max(records, key=lambda item: int(item.get("updateTime") or 0))
    plan_id = str(latest.get("bizSeatPlanId") or "")
    return plan_id if re.fullmatch(r"[0-9a-fA-F]{24}", plan_id) else None


async def _fetch_all_dynamic(
    site: PiaoxingqiuPage,
    endpoint: str,
    common: dict[str, str],
    headers: dict[str, str],
    zone_ids: tuple[str, ...],
    plan_ids: tuple[str, ...],
) -> list[dict[str, Any]]:
    semaphore = asyncio.Semaphore(DYNAMIC_CONCURRENCY)

    async def fetch(batch: tuple[str, ...]) -> list[dict[str, Any]]:
        query = dict(common)
        query.update(
            zoneConcreteIds=",".join(batch),
            bizSeatPlanIds=",".join(plan_ids),
        )
        async with semaphore:
            response = await site.page.context.request.get(
                _url(endpoint, query), headers=headers
            )
        if not response.ok:
            raise RuntimeError(f"批量动态座位接口返回 HTTP {response.status}")
        data = _response_data(await response.json(), "批量动态座位接口")
        if not isinstance(data, list):
            raise RuntimeError("批量动态座位接口缺少 data 数组")
        return [item for item in data if isinstance(item, dict)]

    batches = await asyncio.gather(
        *(
            fetch(zone_ids[start : start + BATCH_SIZE])
            for start in range(0, len(zone_ids), BATCH_SIZE)
        )
    )
    return [record for batch in batches for record in batch]


def _plan_bits(record: dict[str, Any], plan_id: str) -> bytes:
    for item in record.get("seatPlanSeatBits", []):
        if isinstance(item, dict) and str(item.get("bizSeatPlanId")) == plan_id:
            value = str(item.get("bitstr") or "")
            return base64.b64decode(value + "=" * (-len(value) % 4))
    return b""


def _response_data(payload: Any, label: str) -> Any:
    if not is_success_payload(payload):
        status = payload.get("statusCode") if isinstance(payload, dict) else None
        raise RuntimeError(f"{label}返回异常业务状态（{status or '无状态码'}）")
    if "data" not in payload:
        raise RuntimeError(f"{label}缺少 data")
    return payload["data"]


def _static_data(payload: Any) -> dict[str, Any]:
    if (
        isinstance(payload, dict)
        and str(payload.get("statusCode")) == STATIC_UNAVAILABLE_CODE
    ):
        raise StaticInventoryUnavailable("静态座位资源尚未下发")
    data = _response_data(payload, "静态座位接口")
    if not isinstance(data, dict):
        raise RuntimeError("静态座位接口缺少 data 对象")
    return data


async def _decode_zones(
    site: PiaoxingqiuPage,
    resources: dict[str, str],
    zone_ids: set[str],
) -> dict[str, tuple[Seat, ...]]:
    semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)

    async def decode(zone_id: str) -> tuple[str, tuple[Seat, ...]]:
        async with semaphore:
            response = await site.page.context.request.get(resources[zone_id])
        if not response.ok:
            raise RuntimeError(f"看台布局接口返回 HTTP {response.status}")
        features = geobuf.decode(await response.body()).get("features", [])
        seats = tuple(filter(None, (_seat(feature, zone_id) for feature in features)))
        return zone_id, seats

    return dict(await asyncio.gather(*(decode(zone_id) for zone_id in zone_ids)))


async def _decode_venue_geometry(
    site: PiaoxingqiuPage,
    venue_url: str,
) -> VenueGeometry:
    response = await site.page.context.request.get(venue_url)
    if not response.ok:
        raise RuntimeError(f"全场几何接口返回 HTTP {response.status}")
    features = geobuf.decode(await response.body()).get("features", [])
    zones: dict[str, ZoneGeometry] = {}
    centers: list[tuple[float, float]] = []
    for feature in features:
        properties = feature.get("properties", {}) if isinstance(feature, dict) else {}
        geometry = feature.get("geometry", {}) if isinstance(feature, dict) else {}
        if properties.get("level") != "zone" or geometry.get("type") != "Polygon":
            continue
        coordinates = geometry.get("coordinates", [])
        if coordinates and isinstance(coordinates[0], list):
            points = _points(coordinates[0])
            center = _polygon_centroid(points)
            if center is None:
                continue
            zone = ZoneGeometry(center=center)
            centers.append(center)
            for key in ("zoneConcreteId", "id", "code", "text", "standText"):
                if value := str(properties.get(key) or ""):
                    zones[value] = zone
    if not centers or not zones:
        raise RuntimeError("全场几何资源中没有可用的看台多边形")
    center = (
        sum(point[0] for point in centers) / len(centers),
        sum(point[1] for point in centers) / len(centers),
    )
    return VenueGeometry(center=center, zones=zones)


def _points(ring: list[Any]) -> tuple[tuple[float, float], ...]:
    return tuple(
        (float(point[0]), float(point[1]))
        for point in ring
        if isinstance(point, list)
        and len(point) >= 2
        and isinstance(point[0], (int, float))
        and isinstance(point[1], (int, float))
    )


def _polygon_centroid(
    points: tuple[tuple[float, float], ...],
) -> tuple[float, float] | None:
    if len(points) < 3:
        return None
    area = sum(
        left[0] * right[1] - right[0] * left[1]
        for left, right in zip(points, points[1:] + points[:1])
    )
    if abs(area) < 1e-15:
        return (
            sum(point[0] for point in points) / len(points),
            sum(point[1] for point in points) / len(points),
        )
    x = sum(
        (left[0] + right[0]) * (left[0] * right[1] - right[0] * left[1])
        for left, right in zip(points, points[1:] + points[:1])
    )
    y = sum(
        (left[1] + right[1]) * (left[0] * right[1] - right[0] * left[1])
        for left, right in zip(points, points[1:] + points[:1])
    )
    return x / (3 * area), y / (3 * area)


def _seat(feature: Any, zone_id: str) -> Seat | None:
    if not isinstance(feature, dict):
        return None
    geometry = feature.get("geometry", {})
    properties = feature.get("properties", {})
    coordinates = geometry.get("coordinates") if isinstance(geometry, dict) else None
    if (
        not isinstance(properties, dict)
        or not isinstance(coordinates, list)
        or len(coordinates) < 2
    ):
        return None
    seat_id = str(properties.get("seatConcreteId") or "")
    if not seat_id:
        return None
    try:
        return Seat(
            zone_id=zone_id,
            zone_name=str(properties.get("zoneName") or zone_id),
            seat_id=seat_id,
            row=str(properties.get("row") or ""),
            seat_no=int(properties["seatNo"]),
            x=float(coordinates[0]),
            y=float(coordinates[1]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _bit_is_set(bits: bytes, seat_no: int) -> bool:
    byte_index, bit_index = divmod(seat_no, 8)
    return byte_index < len(bits) and bool(bits[byte_index] & (128 >> bit_index))


def _rank_zone(
    seats: tuple[Seat, ...],
    available: tuple[Seat, ...],
    venue: VenueGeometry | None,
    plan: str,
    plan_id: str,
    plan_rank: int,
) -> tuple[Candidate, ...]:
    if not seats or not available:
        return ()
    if venue is None:
        return tuple(
            Candidate(seat, plan, plan_id, plan_rank, seat.seat_no)
            for seat in available
        )
    zone = _zone_geometry(venue, seats[0])
    radial_x = zone.center[0] - venue.center[0]
    radial_y = zone.center[1] - venue.center[1]
    length = math.hypot(radial_x, radial_y)
    if length == 0:
        raise RuntimeError(f"看台“{seats[0].zone_name}”缺少有效的场馆相对位置")
    tangent = (-radial_y / length, radial_x / length)
    rows: dict[str, list[Seat]] = {}
    for seat in seats:
        rows.setdefault(seat.row, []).append(seat)
    row_indexes: dict[str, int] = {}
    for row_seats in rows.values():
        ordered = sorted(
            row_seats,
            key=lambda seat: _dot((seat.x, seat.y), tangent),
        )
        row_indexes.update({seat.seat_id: index for index, seat in enumerate(ordered)})
    return tuple(
        Candidate(
            seat=seat,
            plan=plan,
            plan_id=plan_id,
            plan_rank=plan_rank,
            row_index=row_indexes[seat.seat_id],
        )
        for seat in available
    )


def _select_group(
    candidates: tuple[Candidate, ...],
    quantity: int,
) -> SeatGroup | None:
    continuous = _continuous_groups(candidates, quantity)
    if continuous:
        return _random_best(continuous)

    by_zone: dict[str, list[Candidate]] = {}
    for candidate in candidates:
        by_zone.setdefault(candidate.seat.zone_id, []).append(candidate)
    same_zone = [
        group
        for zone_candidates in by_zone.values()
        if len(zone_candidates) >= quantity
        if (group := _compact_group(tuple(zone_candidates), quantity, cohesion=1))
    ]
    if same_zone:
        return _random_best(same_zone)
    return _compact_group(candidates, quantity, cohesion=2)


def _continuous_groups(
    candidates: tuple[Candidate, ...],
    quantity: int,
) -> list[SeatGroup]:
    rows: dict[tuple[str, str], list[Candidate]] = {}
    for candidate in candidates:
        rows.setdefault((candidate.seat.zone_id, candidate.seat.row), []).append(
            candidate
        )
    groups: list[SeatGroup] = []
    for row in rows.values():
        ordered = sorted(row, key=lambda candidate: candidate.row_index)
        run: list[Candidate] = []
        for candidate in ordered:
            if run and candidate.row_index != run[-1].row_index + 1:
                _append_windows(groups, run, quantity)
                run = []
            run.append(candidate)
        _append_windows(groups, run, quantity)
    return groups


def _append_windows(
    groups: list[SeatGroup],
    run: list[Candidate],
    quantity: int,
) -> None:
    groups.extend(
        SeatGroup(cohesion=0, candidates=tuple(run[start : start + quantity]))
        for start in range(len(run) - quantity + 1)
    )


def _compact_group(
    candidates: tuple[Candidate, ...],
    quantity: int,
    *,
    cohesion: int,
) -> SeatGroup | None:
    if len(candidates) < quantity:
        return None
    groups: dict[tuple[str, ...], SeatGroup] = {}
    for anchor in candidates:
        nearest = tuple(
            heapq.nsmallest(
                quantity,
                candidates,
                key=lambda candidate: _distance(anchor.seat, candidate.seat),
            )
        )
        key = tuple(sorted(candidate.seat.seat_id for candidate in nearest))
        groups[key] = SeatGroup(cohesion=cohesion, candidates=nearest)
    return _random_best(list(groups.values()))


def _random_best(groups: list[SeatGroup]) -> SeatGroup:
    best_score = min(group.score for group in groups)
    return secrets.choice([group for group in groups if group.score == best_score])


def _distance(left: Seat, right: Seat) -> float:
    return math.hypot(left.x - right.x, left.y - right.y)


def _zone_geometry(venue: VenueGeometry, seat: Seat) -> ZoneGeometry:
    zone = venue.zones.get(seat.zone_id) or venue.zones.get(seat.zone_name)
    if zone is None:
        raise RuntimeError(f"全场几何资源缺少看台“{seat.zone_name}”")
    return zone


def _dot(left: tuple[float, float], right: tuple[float, float]) -> float:
    return left[0] * right[0] + left[1] * right[1]


def _url(endpoint: str, query: dict[str, str]) -> str:
    return f"{endpoint}?{urlencode(query)}"
