"""票星球公开接口客户端。"""

from __future__ import annotations

from typing import Any

import httpx

BASE_URL = "https://m.piaoxingqiu.com/cyy_gatewayapi"
USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
)


class PxqError(RuntimeError):
    """票星球接口返回了非成功状态。"""


class PxqClient:
    def __init__(self, timeout: float = 15.0):
        self._http = httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "PxqClient":
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        await self.aclose()

    async def _get(self, path: str, params: dict | None = None) -> Any:
        resp = await self._http.get(BASE_URL + path, params=params)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            raise PxqError(f"{path} -> 响应不是 JSON 对象")
        if payload.get("statusCode") != 200:
            raise PxqError(
                f"{path} -> statusCode={payload.get('statusCode')} {payload.get('comments')}"
            )
        return payload.get("data")

    async def _get_object(self, path: str, params: dict | None = None) -> dict:
        data = await self._get(path, params)
        if not isinstance(data, dict):
            raise PxqError(f"{path} -> 响应缺少 data 对象")
        return data

    @staticmethod
    def _require_list(data: dict, key: str, path: str) -> list[dict]:
        value = data.get(key)
        if not isinstance(value, list):
            raise PxqError(f"{path} -> 响应缺少 {key} 数组")
        if not all(isinstance(item, dict) for item in value):
            raise PxqError(f"{path} -> {key} 包含无效数据")
        return value

    async def search_shows(
        self, keyword: str, page: int = 1, length: int = 10
    ) -> list[dict]:
        path = "/home/pub/v3/show_list/search"
        data = await self._get_object(
            path,
            params={"keyword": keyword, "pageNum": page, "length": length},
        )
        return [
            show
            for show in self._require_list(data, "searchData", path)
            if show.get("searchType") == "SHOW"
        ]

    async def quick_order_sessions(self, show_id: str) -> list[dict]:
        """快速购票场次：场次、开售状态及是否支持选座。"""
        path = f"/show/pub/v5/show/{show_id}/sessions"
        data = await self._get(path, {"source": "FROM_QUICK_ORDER", "src": "WEB"})
        if not isinstance(data, list) or not all(
            isinstance(item, dict) for item in data
        ):
            raise PxqError(f"{path} -> 响应缺少 data 数组")
        return data

    async def show_dynamic(self, show_id: str) -> dict:
        """演出动态信息：包含官方开售时间与演出状态。"""
        return await self._get_object(f"/show/pub/v5/show/{show_id}/dynamic")

    async def quick_order_plans(self, show_id: str, session_id: str) -> dict:
        """快速购票票档：名称、价格、实时可买数和限购数。"""
        path = f"/show/pub/v5/show/{show_id}/session/{session_id}/seat_plans"
        data = await self._get_object(
            path, {"source": "FROM_QUICK_ORDER", "src": "WEB"}
        )
        self._require_list(data, "seatPlans", path)
        return data
