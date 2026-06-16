"""OKX V5 API 异步 WebSocket 客户端。

功能特性:
- 指数退避自动重连（1→2→4→8→16→30 秒）。
- 每 25 秒发送心跳 ping；pong 超时 10 秒。
- 基于回调的按 channel 消息分发。
- 重连后自动重新订阅。

用法::

    from okx_quant.gateway.ws_client import WebSocketClient

    client = WebSocketClient(ws_url)
    client.subscribe_ticker(["BTC-USDT", "ETH-USDT"], on_tick)
    client.subscribe_candles("BTC-USDT", "1H", on_bar)
    await client.connect()
    ...
    await client.disconnect()
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Coroutine

import websockets
import websockets.exceptions

from okx_quant.config.auth import OKXAuth
from okx_quant.models.market import (
    BarData,
    OKXConnectionError,
    OrderData,
    TickData,
)

logger = logging.getLogger(__name__)

# Type alias for async callbacks
AsyncCallback = Callable[..., Coroutine[Any, Any, None]]


# ---------------------------------------------------------------------------
# WebSocket Client
# ---------------------------------------------------------------------------


class WebSocketClient:
    """带自动重连和心跳的异步 OKX V5 WebSocket 客户端。

    参数:
        url:  WebSocket 端点（公共或私有）。
        auth: 私有 channel 登录的可选签名器。
    """

    # Reconnect backoff limits
    _RECONNECT_BASE = 1.0
    _RECONNECT_MAX = 30.0

    # Heartbeat intervals (seconds)
    _PING_INTERVAL = 25.0
    _PONG_TIMEOUT = 10.0

    def __init__(self, url: str, auth: OKXAuth | None = None) -> None:
        self._url = url
        self._auth = auth

        # Connection state
        self._ws: Any = None  # websockets client protocol
        self._connected = asyncio.Event()
        self._running = False
        self._reconnect_lock: asyncio.Lock | None = None  # lazily created
        self._pending_tasks: set[asyncio.Task[None]] = set()  # 防止 GC 回收

        # Background tasks
        self._recv_task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None

        # Subscriptions: channel → list of (args_dict, callback)
        self._subscriptions: dict[str, list[tuple[dict[str, str], AsyncCallback]]] = {}

        # Track last pong time for heartbeat monitoring
        self._last_pong: float = 0.0

    # ------------------------------------------------------------------
    # Public: subscribe
    # ------------------------------------------------------------------

    def subscribe_ticker(
        self,
        symbols: list[str],
        callback: AsyncCallback,
    ) -> None:
        """订阅多个品种的行情更新。"""
        for sym in symbols:
            self._add_subscription("tickers", {"instId": sym}, callback)

    def subscribe_candles(
        self,
        symbol: str,
        bar: str,
        callback: AsyncCallback,
    ) -> None:
        """订阅 K 线更新。"""
        from okx_quant.gateway.rest_client import _okx_bar
        self._add_subscription(
            "candle" + _okx_bar(bar), {"instId": symbol}, callback
        )

    def subscribe_orderbook(
        self,
        symbol: str,
        callback: AsyncCallback,
        depth: str = "books5",
    ) -> None:
        """订阅订单簿更新（``books5`` 或 ``books``）。"""
        self._add_subscription(depth, {"instId": symbol}, callback)

    def subscribe_orders(
        self,
        callback: AsyncCallback,
        inst_type: str = "SWAP",
    ) -> None:
        """订阅订单更新（私有 channel，需要认证）。"""
        self._add_subscription("orders", {"instType": inst_type}, callback)

    # ------------------------------------------------------------------
    # Public: lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """打开 WebSocket 连接并启动后台任务。"""
        if self._running:
            return
        self._running = True
        try:
            await self._do_connect()
        except Exception:
            self._running = False
            raise
        self._recv_task = asyncio.create_task(self._receive_loop())
        self._ping_task = asyncio.create_task(self._heartbeat_loop())

    async def disconnect(self) -> None:
        """优雅地关闭连接并取消后台任务。"""
        self._running = False
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
            self._ws = None
        self._connected.clear()

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    # ------------------------------------------------------------------
    # Internal: connection management
    # ------------------------------------------------------------------

    async def _do_connect(self) -> None:
        """单次连接尝试。"""
        logger.info("正在连接 %s", self._url)
        try:
            self._ws = await websockets.connect(
                self._url,
                ping_interval=None,   # we manage pings ourselves
                ping_timeout=None,
                close_timeout=5,
                max_size=2**20,       # 1 MB
            )
            self._last_pong = time.monotonic()
            self._connected.set()
            logger.info("已连接 %s", self._url)

            # Login for private channels
            if self._auth is not None:
                await self._login()

            # Re-subscribe all channels
            await self._resubscribe()

        except (websockets.exceptions.InvalidStatusCode,
                websockets.exceptions.ConnectionClosed,
                OSError) as exc:
            logger.error("连接失败: %s", exc)
            self._connected.clear()
            raise
        except Exception as exc:
            logger.error("连接未知错误: %s", exc, exc_info=True)
            self._connected.clear()
            raise

    async def _reconnect(self) -> None:
        """指数退避重连。"""
        # 防止并发重连：同一时间只有一个协程执行重连
        if self._reconnect_lock is None:
            self._reconnect_lock = asyncio.Lock()
        async with self._reconnect_lock:
            if self._connected.is_set():
                return  # 另一个协程已经重连成功
            self._connected.clear()
            delay = self._RECONNECT_BASE

            while self._running:
                logger.info("%.1f 秒后重连...", delay)
                await asyncio.sleep(delay)
                try:
                    await self._do_connect()
                    return
                except Exception as exc:
                    logger.warning("重连失败: %s", exc)
                    delay = min(delay * 2, self._RECONNECT_MAX)

    async def _login(self) -> None:
        """在私有 WebSocket channel 上进行认证。

        OKX WebSocket 登录使用 **Unix 时间戳（秒）**（与 REST 使用的
        ISO 8601 不同）。签名使用相同的 ``_compute_signature`` 方法
        计算，但使用 Unix 时间戳字符串。
        """
        import time as _time

        assert self._auth is not None
        # WS login requires Unix timestamp in seconds, NOT ISO 8601
        ts = str(int(_time.time()))
        sig = self._auth._compute_signature(ts, "GET", "/users/self/verify", "")
        login_msg = {
            "op": "login",
            "args": [{
                "apiKey": self._auth._key,
                "passphrase": self._auth._passphrase,
                "timestamp": ts,
                "sign": sig,
            }],
        }
        await self._ws.send(json.dumps(login_msg))
        # Wait for login response
        resp_raw = await asyncio.wait_for(self._ws.recv(), timeout=10)
        resp = json.loads(resp_raw)
        if resp.get("event") == "login":
            logger.info("WebSocket 登录成功")
        else:
            logger.error("WebSocket 登录失败: %s", resp)
            raise OKXConnectionError(f"Login failed: {resp}")

    async def _resubscribe(self) -> None:
        """重新发送所有已注册 channel 的订阅消息。"""
        if not self._subscriptions:
            return
        args: list[dict[str, str]] = []
        for channel, entries in self._subscriptions.items():
            for arg_dict, _ in entries:
                args.append({"channel": channel, **arg_dict})
        if args:
            msg = json.dumps({"op": "subscribe", "args": args})
            await self._ws.send(msg)
            logger.info("已重新订阅 %d 个频道", len(args))

    # ------------------------------------------------------------------
    # Internal: receive loop
    # ------------------------------------------------------------------

    async def _receive_loop(self) -> None:
        """主消息接收循环——运行直到断开连接。"""
        while self._running:
            try:
                await self._connected.wait()
                assert self._ws is not None
                raw = await self._ws.recv()

                # Handle raw text messages (pong, ping) before JSON parsing
                if isinstance(raw, str) and raw.strip() in ("pong", "ping"):
                    self._last_pong = time.monotonic()
                    continue

                self._dispatch(json.loads(raw))
            except websockets.exceptions.ConnectionClosed as exc:
                logger.warning("WebSocket 已关闭: %s", exc)
                self._connected.clear()
                if self._running:
                    await self._reconnect()
            except asyncio.CancelledError:
                return
            except json.JSONDecodeError:
                # Non-JSON text message (e.g. raw pong from OKX)
                self._last_pong = time.monotonic()
                continue
            except Exception as exc:
                logger.error("接收错误: %s", exc)
                if self._running:
                    await asyncio.sleep(1)

    def _dispatch(self, msg: dict[str, Any]) -> None:
        """将原始 OKX WebSocket 消息路由到对应的回调。

        OKX 推送格式:
            {"arg": {"channel": "tickers", "instId": "BTC-USDT"}, "data": [{...}]}
        事件格式: {"event": "subscribe", ...}
        Pong: "pong"（原始字符串）或 {"op": "pong"}
        """
        # Handle pong
        if isinstance(msg, str) and msg == "pong":
            self._last_pong = time.monotonic()
            return
        if msg.get("event") == "pong" or msg.get("op") == "pong":
            self._last_pong = time.monotonic()
            return

        # Handle subscription events
        event = msg.get("event")
        if event in ("subscribe", "unsubscribe", "error"):
            logger.debug("WS event: %s", msg)
            return

        # Data push
        arg = msg.get("arg")
        data_list = msg.get("data")
        if arg is None or data_list is None:
            return

        channel = arg.get("channel", "")
        inst_id = arg.get("instId", "")

        for arg_dict, callback in self._subscriptions.get(channel, []):
            # Check instId filter (if subscription has one)
            if arg_dict.get("instId") and arg_dict["instId"] != inst_id:
                continue
            for item in data_list:
                task = asyncio.create_task(self._safe_call(callback, channel, item, inst_id))
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)

    async def _safe_call(
        self, callback: AsyncCallback, channel: str, data: Any, symbol: str = "",
    ) -> None:
        """调用回调，捕获并记录异常。"""
        try:
            # Parse based on channel type
            parsed = self._parse_push(channel, data, symbol)
            await callback(parsed)
        except Exception as exc:
            logger.error("频道 %s 回调错误: %s", channel, exc)

    @staticmethod
    def _parse_push(channel: str, data: Any, symbol: str = "") -> Any:
        """将原始 OKX 推送数据解析为类型化的模型。

        返回解析后的 dataclass，对于未知 channel 返回原始 dict。

        对于 K 线 channel，OKX 发送的数据是数组列表：
        ``[["ts","o","h","l","c","vol",...]]``——不是 dict。
        ``symbol`` 取自订阅参数。
        """
        if channel == "tickers":
            return TickData.from_okx(data)
        if channel.startswith("candle"):
            return BarData.from_okx(data, symbol)
        if channel == "orders":
            return OrderData.from_okx(data)
        # Fallback: return raw dict for unrecognized channels
        return data

    # ------------------------------------------------------------------
    # Internal: heartbeat
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """每 25 秒发送 ping；如果 10 秒内未收到 pong 则触发重连。"""
        while self._running:
            await asyncio.sleep(self._PING_INTERVAL)
            if not self._connected.is_set():
                continue
            try:
                assert self._ws is not None
                await self._ws.ping()
                # Also send OKX-style text ping
                await self._ws.send("ping")

                # Check pong freshness
                elapsed = time.monotonic() - self._last_pong
                if elapsed > self._PING_INTERVAL + self._PONG_TIMEOUT:
                    logger.warning(
                        "%.1f 秒未收到 pong — 强制重连", elapsed
                    )
                    await self._ws.close()
                    # _receive_loop will detect the close and reconnect

            except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                logger.warning("心跳失败: %s", exc)
                if self._running:
                    self._connected.clear()
                    await self._reconnect()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("心跳未知错误: %s", exc, exc_info=True)
                if self._running:
                    self._connected.clear()
                    await self._reconnect()

    # ------------------------------------------------------------------
    # Internal: subscription bookkeeping
    # ------------------------------------------------------------------

    def _add_subscription(
        self,
        channel: str,
        arg_dict: dict[str, str],
        callback: AsyncCallback,
    ) -> None:
        """注册订阅条目。"""
        if channel not in self._subscriptions:
            self._subscriptions[channel] = []
        self._subscriptions[channel].append((arg_dict, callback))

        # If already connected, send subscribe message immediately
        if self._connected.is_set() and self._ws:
            msg = json.dumps({
                "op": "subscribe",
                "args": [{"channel": channel, **arg_dict}],
            })
            task = asyncio.create_task(self._ws.send(msg))
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
