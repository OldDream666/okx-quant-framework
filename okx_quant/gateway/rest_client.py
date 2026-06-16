"""OKX V5 API 异步 REST 客户端。

使用 ``httpx.AsyncClient`` 进行连接池管理和 keep-alive。
内置速率限制、429/错误时的指数退避重试，
以及自动将响应解析为统一数据模型。

用法::

    from okx_quant.config import OKXConfig, OKXAuth, load_config
    from okx_quant.gateway.rest_client import RESTClient

    cfg = load_config()
    auth = OKXAuth(cfg.okx)
    async with RESTClient(cfg.okx, auth) as client:
        tick = await client.get_ticker("BTC-USDT")
        bars = await client.get_candles("BTC-USDT", "1H")
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from okx_quant.config.auth import OKXAuth
from okx_quant.config.settings import OKXConfig
from okx_quant.models.market import (
    AccountData,
    BarData,
    InstrumentData,
    OKXAPIError,
    PositionData,
    TickData,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _okx_bar(timeframe: str) -> str:
    """将用户友好的 timeframe 转换为 OKX bar 格式。

    分钟: 小写（``1m``、``5m``、``15m``）。
    小时及以上: 大写（``1H``、``4H``、``1D``、``1W``、``1M``）。
    """
    tf = timeframe.strip()
    if tf.endswith("m"):
        return tf
    return tf[:-1] + tf[-1].upper()


# ---------------------------------------------------------------------------
# REST Client
# ---------------------------------------------------------------------------


class RESTClient:
    """带速率限制和重试的异步 OKX V5 REST 客户端。

    参数:
        config: OKX 连接设置。
        auth:   私有端点的签名器。
    """

    def __init__(self, config: OKXConfig, auth: OKXAuth) -> None:
        self._config = config
        self._auth = auth
        self._base_url = config.base_url.rstrip("/")
        self._client: httpx.AsyncClient | None = None
        self._semaphore = asyncio.Semaphore(10)  # max concurrent requests
        self._max_retries = 3
        self._base_delay = 1.0  # seconds, doubles each retry
        self._inst_specs: dict[str, dict[str, float]] = {}  # symbol → {lotSz, minSz, tickSz}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """创建底层 ``httpx.AsyncClient``。"""
        if self._client is not None:
            return
        default_headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._config.is_demo:
            default_headers["x-simulated-trading"] = "1"
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            headers=default_headers,
        )

    async def close(self) -> None:
        """关闭 HTTP 客户端并释放资源。"""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> RESTClient:
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Public Market Endpoints
    # ------------------------------------------------------------------

    async def get_ticker(self, symbol: str) -> TickData:
        """获取 *symbol* 的最新行情（例如 ``BTC-USDT``）。"""
        data = await self._request("GET", "/api/v5/market/ticker",
                                   params={"instId": symbol})
        return TickData.from_okx(data[0])

    async def get_candles(
        self, symbol: str, bar: str = "1H", limit: int = 300
    ) -> list[BarData]:
        """获取最近的 K 线数据（升序）。

        参数:
            symbol: 交易品种 ID（``BTC-USDT``）。
            bar:    时间周期（``1m``、``5m``、``15m``、``1H``、``4H``、``1D`` ...）。
            limit:  最大 K 线数量（OKX 该端点上限为 300）。
        """
        data = await self._request(
            "GET", "/api/v5/market/candles",
            params={"instId": symbol, "bar": _okx_bar(bar), "limit": str(limit)},
        )
        return [BarData.from_okx(c, symbol) for c in data]

    async def get_history_candles(
        self,
        symbol: str,
        bar: str = "1H",
        after: int | None = None,
        limit: int = 300,
    ) -> list[BarData]:
        """获取历史 K 线数据（降序——最新的在前）。

        参数:
            after: 返回比此时间戳（毫秒）**更早** 的 K 线。
                   传入 *None* 从最近的开始。
        """
        params: dict[str, str] = {
            "instId": symbol,
            "bar": _okx_bar(bar),
            "limit": str(limit),
        }
        if after is not None:
            params["after"] = str(after)
        data = await self._request("GET", "/api/v5/market/history-candles",
                                   params=params)
        # OKX returns descending — reverse for chronological order
        return [BarData.from_okx(c, symbol) for c in reversed(data)]

    async def get_instruments(
        self, inst_type: str = "SPOT"
    ) -> list[InstrumentData]:
        """获取 *inst_type*（SPOT、SWAP、FUTURES ...）的品种规格。"""
        data = await self._request(
            "GET", "/api/v5/public/instruments",
            params={"instType": inst_type},
            signed=False,
        )
        return [InstrumentData.from_okx(d) for d in data]

    # ------------------------------------------------------------------
    # Private Account Endpoints
    # ------------------------------------------------------------------

    async def get_balance(self) -> AccountData:
        """获取账户余额（需要认证）。"""
        data = await self._request("GET", "/api/v5/account/balance", signed=True)
        return AccountData.from_okx_balance(data[0])

    async def get_positions(self) -> list[PositionData]:
        """获取所有持仓（需要认证）。"""
        data = await self._request("GET", "/api/v5/account/positions", signed=True)
        return [PositionData.from_okx(d) for d in data]

    async def set_leverage(
        self, symbol: str, leverage: int, mgn_mode: str = "cross",
    ) -> dict[str, Any]:
        """设置品种的杠杆倍数。

        参数:
            symbol:   交易品种 ID（``ETH-USDT-SWAP``）。
            leverage: 杠杆倍数（1-125）。
            mgn_mode: ``cross`` 或 ``isolated``。
        """
        body = {
            "instId": symbol,
            "lever": str(leverage),
            "mgnMode": mgn_mode,
        }
        data = await self._request("POST", "/api/v5/account/set-leverage",
                                   body=body, signed=True)
        return data[0] if data else {}

    # ------------------------------------------------------------------
    # Order Endpoints
    # ------------------------------------------------------------------

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        size: str,
        price: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """下单。

        自动将 ``size`` 和 ``price`` 四舍五入到品种的
        lot_size / tick_size 精度。首次调用时获取品种规格并缓存。

        参数:
            symbol:     交易品种 ID（``BTC-USDT-SWAP``）。
            side:       ``buy`` 或 ``sell``。
            order_type: ``market``、``limit``、``post_only``、``fok``、``ioc``。
            size:       订单数量，字符串格式（自动四舍五入）。
            price:      限价，字符串格式（自动四舍五入）。

        返回:
            下单的原始 OKX 响应数据。
        """
        # Auto-fetch instrument specs if not cached
        spec = await self._get_instrument_spec(symbol)
        if spec:
            size = self._round_to_lot(float(size), spec["lotSz"], spec["minSz"])
            if price is not None:
                price = self._round_to_tick(float(price), spec["tickSz"])

        body: dict[str, Any] = {
            "instId": symbol,
            "tdMode": kwargs.pop("tdMode", "cross"),
            "side": side,
            "ordType": order_type,
            "sz": size,
        }
        if price is not None:
            body["px"] = price
        body.update(kwargs)
        data = await self._request("POST", "/api/v5/trade/order",
                                   body=body, signed=True)
        return data[0]

    async def cancel_order(self, symbol: str, order_id: str) -> dict[str, Any]:
        """取消未成交订单。"""
        body = {"instId": symbol, "ordId": order_id}
        data = await self._request("POST", "/api/v5/trade/cancel-order",
                                   body=body, signed=True)
        return data[0]

    async def get_open_orders(
        self, symbol: str | None = None
    ) -> list[dict[str, Any]]:
        """获取所有待处理订单，可按品种筛选。"""
        params: dict[str, str] = {}
        if symbol:
            params["instId"] = symbol
        return await self._request("GET", "/api/v5/trade/orders-pending",
                                   params=params, signed=True)

    # ------------------------------------------------------------------
    # Internal: request + retry
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> list[dict[str, Any]]:
        """执行带速率限制和重试的 HTTP 请求。

        返回:
            OKX 响应中的 ``data`` 列表。

        异常:
            OKXAPIError: OKX 返回非零 ``code`` 时抛出。
            httpx.HTTPStatusError: 不可重试的 HTTP 错误时抛出。
        """
        if self._client is None:
            raise RuntimeError("RESTClient not connected — call connect() first")

        return await self._retry_with_backoff(
            lambda: self._do_request(method, path, params=params,
                                     body=body, signed=signed)
        )

    async def _do_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None,
        body: dict[str, Any] | None,
        signed: bool,
    ) -> list[dict[str, Any]]:
        """单次 HTTP 请求尝试。"""
        assert self._client is not None

        headers: dict[str, str] = {}
        json_body = ""
        if body is not None:
            import json
            json_body = json.dumps(body)

        if signed:
            headers.update(self._auth.sign(method, path, json_body))

        async with self._semaphore:
            if method == "GET":
                resp = await self._client.get(
                    path, params=params, headers=headers
                )
            else:
                resp = await self._client.post(
                    path, content=json_body, headers=headers
                )

        resp.raise_for_status()
        result = resp.json()

        code = result.get("code", "0")
        if code != "0":
            raise OKXAPIError(
                code=code,
                message=result.get("msg", ""),
                data=result.get("data"),
            )

        return result.get("data", [])

    async def _retry_with_backoff(self, coro_factory: Any) -> Any:
        """遇到临时错误时进行指数退避重试。

        以下情况重试：
        - ``httpx.HTTPStatusError`` 状态码为 429（请求过多）
        - ``httpx.ConnectTimeout``、``httpx.ReadTimeout``
        - ``httpx.RemoteProtocolError``（连接断开）

        以下情况立即抛出：
        - ``OKXAPIError``（业务逻辑错误——不重试）
        - 其他 ``httpx.HTTPStatusError``（非 429 的 4xx/5xx）
        """
        last_exc: Exception | None = None
        delay = self._base_delay

        for attempt in range(self._max_retries + 1):
            try:
                return await coro_factory()
            except (httpx.ConnectTimeout, httpx.ReadTimeout,
                    httpx.RemoteProtocolError) as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    logger.warning(
                        "Request failed (attempt %d/%d): %s — retrying in %.1fs",
                        attempt + 1, self._max_retries + 1, exc, delay,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30.0)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    last_exc = exc
                    if attempt < self._max_retries:
                        retry_after = float(
                            exc.response.headers.get("Retry-After", delay)
                        )
                        logger.warning(
                            "Rate limited (429) — retrying in %.1fs", retry_after
                        )
                        await asyncio.sleep(retry_after)
                        delay = min(delay * 2, 30.0)
                        continue
                raise  # non-429 HTTP errors are not retried
            except OKXAPIError:
                raise  # business errors are not retried

        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Instrument specs & precision rounding
    # ------------------------------------------------------------------

    async def _get_instrument_spec(self, symbol: str) -> dict[str, float] | None:
        """获取并缓存品种规格（lotSz、minSz、tickSz）。

        使用公共 ``/api/v5/public/instruments`` 端点。
        缓存结果——对同一品种的后续调用为 O(1)。
        """
        if symbol in self._inst_specs:
            return self._inst_specs[symbol]

        try:
            # Determine instType from symbol suffix
            inst_type = "SWAP" if symbol.endswith("-SWAP") else "SPOT"
            data = await self._request(
                "GET", "/api/v5/public/instruments",
                params={"instType": inst_type},
                signed=False,
            )
            for inst in data:
                if inst.get("instId") == symbol:
                    spec = {
                        "lotSz": float(inst.get("lotSz", "1")),
                        "minSz": float(inst.get("minSz", "1")),
                        "tickSz": float(inst.get("tickSz", "0.01")),
                    }
                    self._inst_specs[symbol] = spec
                    return spec
            # Symbol not found — cache empty to avoid repeated lookups
            self._inst_specs[symbol] = {}
            return None
        except Exception as exc:
            logger.warning("获取合约规格失败 %s: %s", symbol, exc)
            return None

    @staticmethod
    def _round_to_lot(value: float, lot_size: float, min_size: float) -> str:
        """将数量四舍五入到品种的 lot_size 精度。

        如果四舍五入后的值低于 min_size，返回 min_size 字符串。
        """
        from decimal import Decimal, ROUND_DOWN
        if lot_size <= 0:
            return str(value)
        rounded = float(Decimal(str(value)).quantize(
            Decimal(str(lot_size)), rounding=ROUND_DOWN
        ))
        if rounded < min_size:
            rounded = min_size
        # Format to match lot_size decimal places
        lot_str = str(lot_size)
        if "." in lot_str:
            decimals = len(lot_str.split(".")[1].rstrip("0"))
            return f"{rounded:.{decimals}f}"
        return str(int(rounded))

    @staticmethod
    def _round_to_tick(price: float, tick_size: float) -> str:
        """将价格四舍五入到品种的 tick_size 精度。"""
        from decimal import Decimal, ROUND_HALF_UP
        if tick_size <= 0:
            return str(price)
        rounded = float(Decimal(str(price)).quantize(
            Decimal(str(tick_size)), rounding=ROUND_HALF_UP
        ))
        tick_str = str(tick_size)
        if "." in tick_str:
            decimals = len(tick_str.split(".")[1].rstrip("0"))
            return f"{rounded:.{decimals}f}"
        return str(int(rounded))
