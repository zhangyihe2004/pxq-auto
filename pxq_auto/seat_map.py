from __future__ import annotations

import asyncio
from itertools import groupby

from playwright.async_api import Locator

from .inventory import Candidate, SeatSelection
from .site import PiaoxingqiuPage


FIND_VENUE_MAP_JS = """() => {
    const container = document.getElementById('vr-container');
    const cached = window.__pxqAutoVenueMap;
    if (cached?.venueBoxSelf?.mapbox?.getContainer?.() === container) return cached;
    const queue = [document.getElementById('app')?._vnode];
    const seen = new WeakSet();
    while (queue.length) {
        const value = queue.shift();
        if (!value || (typeof value !== 'object' && typeof value !== 'function') ||
            seen.has(value)) continue;
        seen.add(value);
        try {
            if (typeof value.spliceSelectedZoneIdsByDisableZone === 'function' &&
                typeof value.venueBoxSelf?.loadSeatsInZoneCodes === 'function' &&
                value.venueBoxSelf?.mapbox?.getContainer?.() === container) {
                window.__pxqAutoVenueMap = value;
                return value;
            }
        } catch {}
        if (value instanceof Node) continue;
        let keys = [];
        try { keys = Object.keys(value); } catch {}
        for (const key of keys) {
            let child;
            try { child = value[key]; } catch { continue; }
            if (child && (typeof child === 'object' || typeof child === 'function')) {
                queue.push(child);
            }
        }
    }
    return null;
}"""


LOAD_ZONE_JS = (
    "async target => { const findVenueMap = "
    + FIND_VENUE_MAP_JS
    + ";"
    + """
    const deadline = performance.now() + target.timeoutMs;
    const frame = label => new Promise((resolve, reject) => {
        const remaining = deadline - performance.now();
        if (remaining <= 0) return reject(new Error(label));
        const timer = setTimeout(() => reject(new Error(label)), remaining);
        requestAnimationFrame(() => { clearTimeout(timer); resolve(); });
    });
    const bounded = (promise, label) => new Promise((resolve, reject) => {
        const remaining = deadline - performance.now();
        if (remaining <= 0) return reject(new Error(label));
        const timer = setTimeout(() => reject(new Error(label)), remaining);
        Promise.resolve(promise).then(
            value => { clearTimeout(timer); resolve(value); },
            error => { clearTimeout(timer); reject(error); },
        );
    });
    let venueMap;
    do {
        venueMap = findVenueMap();
        if (venueMap?.venueBoxSelf?.mapbox?.isStyleLoaded?.() === true) break;
        await frame('等待票星球场馆对象超时');
    } while (true);

    const {enabledZoneCodes = []} =
        venueMap.spliceSelectedZoneIdsByDisableZone([target.zoneId]);
    if (!enabledZoneCodes.length) throw new Error('目标看台当前不可售');

    const box = venueMap.venueBoxSelf;
    const map = box.mapbox;
    const enabled = properties => properties.enable === true ||
        properties.enable === 'true' || properties.enable === 1 ||
        properties.enable === '1';
    const view = {
        center: [target.x, target.y],
        zoom: Math.min(
            box.strategy.maxZoom,
            box.strategy.getSeatMinZoom() + 0.01,
        ),
    };
    for (const code of enabledZoneCodes) box._cachedZoneCodes?.delete(code);
    if (box.seatData?.features) {
        box.seatData.features = box.seatData.features.filter(
            feature => feature.properties?.zoneConcreteId !== target.zoneId
        );
    }
    venueMap.isRefreshing = true;
    await bounded(new Promise((resolve, reject) => {
        Promise.resolve(box.loadSeatsInZoneCodes(enabledZoneCodes, resolve)).catch(reject);
    }), '加载目标看台数据超时');
    const globals = document.getElementById('app')?._vnode?.appContext?.config
        ?.globalProperties;
    globals?.$loading?.().hide();
    let targetFeatures;
    do {
        targetFeatures = (box.seatData?.features || []).filter(
            feature => feature.properties?.zoneConcreteId === target.zoneId
        );
        if (targetFeatures.some(feature => {
            const properties = feature.properties || {};
            return enabled(properties) && properties.seatConcreteId === target.seatId &&
                properties.seatPlanId === target.planId;
        })) break;
        await frame('等待目标座位数据超时');
    } while (true);
    await frame('等待目标座位绘制超时');
    const source = map.getSource('venueMapRowSeatDataSource');
    if (typeof source?.setData !== 'function') {
        throw new Error('未找到票星球座位数据源');
    }
    const seats = target.seats || [];
    const selectedBefore = document.querySelectorAll('.seat-item').length;
    for (const [index, seat] of seats.entries()) {
        let point;
        do {
            if (index === 0) {
                map.stop();
                map.jumpTo(view);
                source.setData({type: 'FeatureCollection', features: targetFeatures});
                map.triggerRepaint();
            }
            await frame('等待目标座位渲染超时');
            point = map.project([seat.x, seat.y]);
            const feature = map.queryRenderedFeatures([point.x, point.y]).find(item => {
                const properties = item.properties || {};
                return enabled(properties) && properties.seatConcreteId === seat.seatId &&
                    properties.zoneConcreteId === seat.zoneId &&
                    properties.seatPlanId === seat.planId;
            });
            if (feature) break;
        } while (true);

        const rect = map.getCanvas().getBoundingClientRect();
        map.fire('click', {
            point,
            lngLat: map.unproject([point.x, point.y]),
            originalEvent: new MouseEvent('click', {
                bubbles: true,
                cancelable: true,
                clientX: rect.left + point.x,
                clientY: rect.top + point.y,
            }),
        });
        while (document.querySelectorAll('.seat-item').length <
               selectedBefore + index + 1) {
            await frame('等待座位选中状态超时');
        }
    }
    return targetFeatures.length;
}"""
)

RESET_SELECTION_JS = (
    "async timeoutMs => { const findVenueMap = "
    + FIND_VENUE_MAP_JS
    + ";"
    + """
    const deadline = performance.now() + timeoutMs;
    const frame = label => new Promise((resolve, reject) => {
        const remaining = deadline - performance.now();
        if (remaining <= 0) return reject(new Error(label));
        const timer = setTimeout(() => reject(new Error(label)), remaining);
        requestAnimationFrame(() => { clearTimeout(timer); resolve(); });
    });
    let venueMap;
    do {
        venueMap = findVenueMap();
        if (venueMap?.venueBoxSelf?.mapbox?.isStyleLoaded?.() === true) break;
        await frame('等待票星球场馆对象超时');
    } while (true);

    venueMap.cancelAllSeat();
    do {
        await frame('等待清空选座状态超时');
        const confirm = [...document.querySelectorAll('button')].find(
            element => element.innerText?.trim() === '确认选座'
        );
        const disabled = confirm && (
            confirm.disabled || confirm.getAttribute('aria-disabled') === 'true' ||
            /disabled|inactive/.test(String(confirm.className).toLowerCase())
        );
        if (!document.querySelector('.seat-item') && disabled) return true;
    } while (true);
}"""
)


async def select_seats(
    site: PiaoxingqiuPage,
    selection: SeatSelection,
    *,
    open_map: bool = True,
) -> Locator:
    if open_map:
        candidate = selection.candidates[0]
        await site._open_seat_map(candidate.plan_id, candidate.plan)
    else:
        await _reset_selection(site)
    for _, grouped in groupby(
        selection.candidates,
        key=lambda candidate: candidate.seat.zone_id,
    ):
        candidates = tuple(grouped)
        await _load_zone(site, candidates)

    confirm = await site._poll(lambda: site._enabled_exact("确认选座"))
    if confirm is None:
        raise RuntimeError(
            f"点击 {len(selection.candidates)} 个目标座位后等待“确认选座”启用超时"
        )
    return confirm


async def _reset_selection(site: PiaoxingqiuPage) -> None:
    try:
        await asyncio.wait_for(
            site.page.evaluate(
                RESET_SELECTION_JS,
                max(250, site.config.browser.timeout_ms - 250),
            ),
            timeout=site.config.browser.timeout_ms / 1000,
        )
    except TimeoutError as exc:
        raise RuntimeError("等待清空原选座状态超时") from exc


async def _load_zone(
    site: PiaoxingqiuPage,
    candidates: tuple[Candidate, ...],
) -> None:
    candidate = candidates[0]
    try:
        count = await asyncio.wait_for(
            site.page.evaluate(
                LOAD_ZONE_JS,
                {
                    "zoneId": candidate.seat.zone_id,
                    "seatId": candidate.seat.seat_id,
                    "planId": candidate.plan_id,
                    "x": candidate.seat.x,
                    "y": candidate.seat.y,
                    "timeoutMs": max(250, site.config.browser.timeout_ms - 250),
                    "seats": [_seat_target(item) for item in candidates],
                },
            ),
            timeout=site.config.browser.timeout_ms / 1000,
        )
    except TimeoutError as exc:
        raise RuntimeError(
            f"等待加载目标看台“{candidate.seat.zone_name}”的座位数据超时"
        ) from exc
    if not count:
        raise RuntimeError(f"目标看台“{candidate.seat.zone_name}”没有返回座位数据")


def _seat_target(candidate: Candidate) -> dict[str, str | float]:
    return {
        "x": candidate.seat.x,
        "y": candidate.seat.y,
        "zoneId": candidate.seat.zone_id,
        "seatId": candidate.seat.seat_id,
        "planId": candidate.plan_id,
    }
