"""票星球公开接口客户端。"""

from __future__ import annotations

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

    async def _get(self, path: str, params: dict | None = None) -> dict:
        resp = await self._http.get(BASE_URL + path, params=params)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            raise PxqError(f"{path} -> 响应不是 JSON 对象")
        if payload.get("statusCode") != 200:
            raise PxqError(
                f"{path} -> statusCode={payload.get('statusCode')} {payload.get('comments')}"
            )
        data = payload.get("data")
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
        data = await self._get(
            path,
            params={"keyword": keyword, "pageNum": page, "length": length},
        )
        return [
            show
            for show in self._require_list(data, "searchData", path)
            if show.get("searchType") == "SHOW"
        ]

    async def sessions_static(self, show_id: str) -> dict:
        """场次静态信息：showName、sessionName、开赛时间。"""
        path = f"/show/pub/v3/show/{show_id}/sessions_static_data"
        data = await self._get(path)
        self._require_list(data, "sessionVOs", path)
        return data

    async def show_dynamic(self, show_id: str) -> dict:
        """演出动态信息：包含官方开售时间与演出状态。"""
        return await self._get(f"/show/pub/v5/show/{show_id}/dynamic")

    async def sessions_dynamic(self, show_id: str) -> dict:
        """场次动态状态：sessionStatus（ONSALE / LACK_OF_TICKET / ...）。"""
        path = f"/show/pub/v3/show/{show_id}/sessions_dynamic_data"
        data = await self._get(path)
        self._require_list(data, "sessionVOs", path)
        return data

    async def seat_plans_static(self, show_id: str, session_id: str) -> dict:
        """票档静态信息：seatPlanName、originalPrice。"""
        path = (
            f"/show/pub/v3/show/{show_id}/show_session/{session_id}/"
            "seat_plans_static_data"
        )
        data = await self._get(path)
        self._require_list(data, "seatPlans", path)
        return data

    async def seat_plans_dynamic(self, show_id: str, session_id: str) -> dict:
        """票档余票：每个 seatPlanId 的 canBuyCount、saleStarted。"""
        path = (
            f"/show/pub/v3/show/{show_id}/show_session/{session_id}/"
            "seat_plans_dynamic_data"
        )
        data = await self._get(path)
        self._require_list(data, "seatPlans", path)
        return data
