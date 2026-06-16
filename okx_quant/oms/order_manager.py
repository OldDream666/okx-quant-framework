"""订单管理系统（OMS）。

维护一个完全由 WebSocket 推送更新驱动的本地订单簿（无 REST 轮询）。
策略通过 OMS 提交、撤销和查询订单；OMS 负责状态跟踪、部分成交记账和事件传播。

用法::

    oms = OrderManager(rest_client, ws_client)
    await oms.start()

    order = await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")
    print(oms.get_active_orders("BTC-USDT"))

    await oms.stop()
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from typing import Any, Callable, Coroutine

from okx_quant.gateway.rest_client import RESTClient
from okx_quant.gateway.ws_client import WebSocketClient
from okx_quant.models.market import OKXAPIError, OrderData, OrderStatus

logger = logging.getLogger(__name__)

# Async callback signature: receives an OrderData
OrderCallback = Callable[[OrderData], Coroutine[Any, Any, None]]

# Terminal states — orders in these states are moved to history
_TERMINAL_STATES = frozenset({
    OrderStatus.FILLED,
    OrderStatus.CANCELLED_DONE,
    OrderStatus.FAILED,
})


class OrderManager:
    """基于 WebSocket 的订单管理系统。

    Parameters:
        rest: 用于下单/撤单的 REST client。
        ws:   用于实时订单更新的 WebSocket client。
    """

    def __init__(self, rest: RESTClient, ws: WebSocketClient) -> None:
        self._rest = rest
        self._ws = ws

        # Order storage
        self._active: dict[str, OrderData] = {}     # ordId → OrderData
        self._history: dict[str, OrderData] = {}     # ordId → OrderData (terminal)

        # Client-order-id → ordId mapping (for correlating REST submit with WS push)
        self._clord_map: dict[str, str] = {}

        # Event handlers
        self._handlers: list[OrderCallback] = []

        # Protect concurrent writes
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self, inst_type: str = "SPOT") -> None:
        """订阅 OKX 订单通道并开始跟踪。"""
        self._ws.subscribe_orders(self._on_ws_order_update, inst_type=inst_type)
        logger.info("订单管理器已启动 — 订阅订单频道 (%s)", inst_type)

    async def stop(self) -> None:
        """清除所有跟踪的订单和处理器。"""
        async with self._lock:
            self._active.clear()
            self._history.clear()
            self._clord_map.clear()
        logger.info("订单管理器已停止")

    # ------------------------------------------------------------------
    # Order operations
    # ------------------------------------------------------------------

    async def submit_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        size: str,
        price: str | None = None,
        client_order_id: str | None = None,
        pos_side: str | None = None,
        **kwargs: Any,
    ) -> OrderData:
        """通过 REST 下单并开始跟踪。

        返回的 :class:`OrderData` 是一个**初始快照**，从 REST response 中填充。
        后续状态变化（成交、撤销）通过 WebSocket 推送并自动更新 :attr:`active_orders`。

        Parameters:
            symbol:          交易对 ID（``BTC-USDT``）。
            side:            ``buy`` 或 ``sell``。
            order_type:      ``market``、``limit``、``post_only``、``fok``、``ioc``。
            size:            委托数量（字符串）。
            price:           限价（非市价单时必填）。
            client_order_id: 可选的客户端订单 ID，用于关联。
            **kwargs:        传递给 REST 的额外参数（如 ``tdMode``）。

        Returns:
            初始 :class:`OrderData` 快照。

        Raises:
            OKXAPIError: 交易所拒绝订单时抛出。
        """
        # Inject clOrdId if provided
        if client_order_id:
            kwargs["clOrdId"] = client_order_id

        result = await self._rest.place_order(
            symbol, side, order_type, size, price=price,
            pos_side=pos_side, **kwargs,
        )

        # Build initial OrderData from REST response
        ord_id = result.get("ordId", "")
        s_code = result.get("sCode", "0")

        if s_code != "0":
            raise OKXAPIError(
                code=s_code,
                message=result.get("sMsg", "Order rejected"),
                data=result,
            )

        # Create initial snapshot
        order = OrderData(
            order_id=ord_id,
            client_order_id=client_order_id or result.get("clOrdId", ""),
            symbol=symbol,
            side=_parse_side(side),
            pos_side=pos_side or "net",
            order_type=_parse_order_type(order_type),
            price=float(price) if price else 0.0,
            quantity=float(size),
            filled_qty=0.0,
            avg_price=0.0,
            status=OrderStatus.LIVE,
            fee=0.0,
            fee_currency="",
            timestamp=0,  # will be updated by WS push
            update_time=0,
        )

        async with self._lock:
            self._active[ord_id] = order
            if client_order_id:
                self._clord_map[client_order_id] = ord_id

        logger.info(
            "Order submitted: %s %s %s %s @ %s (ordId=%s)",
            symbol, side, order_type, size, price or "market", ord_id,
        )
        return order

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """撤销活跃订单。

        Returns:
            如果 OKX 接受了撤销请求，返回 ``True``。
        """
        try:
            await self._rest.cancel_order(symbol, order_id)
            logger.info("撤单请求: %s %s", symbol, order_id)
            return True
        except OKXAPIError as exc:
            logger.warning("撤单失败 %s: %s", order_id, exc)
            return False

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_active_orders(self, symbol: str | None = None) -> list[OrderData]:
        """返回所有活跃（非终结）订单，可按交易对过滤。"""
        if symbol is None:
            return list(self._active.values())
        return [o for o in self._active.values() if o.symbol == symbol]

    def get_order(self, order_id: str) -> OrderData | None:
        """按 ID 查询订单（先查活跃订单，再查历史订单）。"""
        return self._active.get(order_id) or self._history.get(order_id)

    @property
    def active_count(self) -> int:
        """活跃订单数量。"""
        return len(self._active)

    @property
    def history(self) -> dict[str, OrderData]:
        """所有终结（已成交/已撤销/已失败）订单。"""
        return dict(self._history)

    # ------------------------------------------------------------------
    # Event registration
    # ------------------------------------------------------------------

    def on_order_update(self, handler: OrderCallback) -> None:
        """注册订单状态变化的异步回调。

        回调接收一个 :class:`OrderData` 参数。
        按注册顺序调用处理器；异常会被记录但不会传播。
        """
        self._handlers.append(handler)

    # ------------------------------------------------------------------
    # Internal: WebSocket callback
    # ------------------------------------------------------------------

    async def _on_ws_order_update(self, order: OrderData) -> None:
        """处理从 WebSocket 推送的订单更新。"""
        to_emit: OrderData | None = None

        async with self._lock:
            existing = self._active.get(order.order_id)

            if existing is None:
                # New order we haven't seen via submit_order (e.g. placed via web UI)
                if order.status in _TERMINAL_STATES:
                    self._history[order.order_id] = order
                else:
                    self._active[order.order_id] = order
                to_emit = order
            else:
                # Merge: take the newer state
                merged = self._merge_order(existing, order)
                self._active[order.order_id] = merged

                # Transition to history if terminal
                if merged.status in _TERMINAL_STATES:
                    del self._active[order.order_id]
                    self._history[order.order_id] = merged
                    logger.info(
                        "Order terminal: %s → %s (filled=%.6f/%.6f)",
                        order.order_id, merged.status.value,
                        merged.filled_qty, merged.quantity,
                    )

                to_emit = merged

        # Emit outside the lock so handlers can safely call OMS methods
        if to_emit is not None:
            await self._emit_event(to_emit)

    @staticmethod
    def _merge_order(old: OrderData, new: OrderData) -> OrderData:
        """合并两个订单快照，优先使用较新的数据。

        规则：
        - 如果 ``new.update_time >= old.update_time``，使用 ``new`` 作为基准。
        - 始终取**较大**的 ``filled_qty``（不允许减少）。
        - 始终取**较新**的 ``avg_price``（来自成交更多的推送）。
        """
        if new.update_time >= old.update_time:
            # New is newer — but guard filled_qty
            if new.filled_qty < old.filled_qty:
                # Defensive: OKX should never decrease this, but be safe
                return replace(new, filled_qty=old.filled_qty)
            return new
        else:
            # Old is newer (stale push) — keep old, but log
            logger.debug(
                "Stale order update ignored: %s (old_ts=%d > new_ts=%d)",
                old.order_id, old.update_time, new.update_time,
            )
            return old

    async def _emit_event(self, order: OrderData) -> None:
        """通知所有已注册的处理器订单状态变化。"""
        for handler in self._handlers:
            try:
                await handler(order)
            except Exception as exc:
                logger.error(
                    "Order handler error for %s: %s", order.order_id, exc
                )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_side(s: str) -> "OrderSide":
    from okx_quant.models.market import OrderSide
    return OrderSide(s.lower())


def _parse_order_type(s: str) -> "OrderType":
    from okx_quant.models.market import OrderType
    return OrderType(s.lower())
