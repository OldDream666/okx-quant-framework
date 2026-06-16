"""Unit tests for Module 3: Order Management System (OMS).

Covers:
  - submit_order: REST call → active_orders entry
  - WS push: state transition (LIVE → PARTIALLY_FILLED → FILLED)
  - Terminal states: FILLED/CANCELLED → moved to history
  - cancel_order: REST call, success/failure handling
  - Partial fill: filled_qty cumulative update
  - Stale push: old update_time ignored
  - Event handlers: triggered on state change
  - Multi-symbol filtering
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from okx_quant.gateway.rest_client import RESTClient
from okx_quant.gateway.ws_client import WebSocketClient
from okx_quant.models.market import (
    OKXAPIError,
    OrderData,
    OrderSide,
    OrderStatus,
    OrderType,
)
from okx_quant.oms.order_manager import OrderManager


# ======================================================================
# Helpers
# ======================================================================


def _make_order(
    order_id: str = "100",
    symbol: str = "BTC-USDT",
    side: str = "buy",
    status: OrderStatus = OrderStatus.LIVE,
    filled_qty: float = 0.0,
    avg_price: float = 0.0,
    quantity: float = 0.001,
    price: float = 63000.0,
    update_time: int = 1000,
    timestamp: int = 1000,
) -> OrderData:
    return OrderData(
        order_id=order_id,
        client_order_id=f"cl_{order_id}",
        symbol=symbol,
        side=OrderSide(side),
        order_type=OrderType.LIMIT,
        price=price,
        quantity=quantity,
        filled_qty=filled_qty,
        avg_price=avg_price,
        status=status,
        fee=0.0,
        fee_currency="USDT",
        timestamp=timestamp,
        update_time=update_time,
    )


def _rest_submit_response(order_id: str = "100", s_code: str = "0") -> dict:
    return {"ordId": order_id, "sCode": s_code, "sMsg": "", "clOrdId": f"cl_{order_id}"}


# ======================================================================
# OrderManager tests
# ======================================================================


class TestOrderManager:

    @pytest.fixture
    def mock_rest(self) -> MagicMock:
        rest = MagicMock(spec=RESTClient)
        rest.place_order = AsyncMock()
        rest.cancel_order = AsyncMock()
        return rest

    @pytest.fixture
    def mock_ws(self) -> MagicMock:
        ws = MagicMock(spec=WebSocketClient)
        ws.subscribe_orders = MagicMock()
        return ws

    @pytest.fixture
    def oms(self, mock_rest: MagicMock, mock_ws: MagicMock) -> OrderManager:
        return OrderManager(mock_rest, mock_ws)

    # --- Lifecycle ---

    @pytest.mark.asyncio
    async def test_start_subscribes_orders(self, oms: OrderManager, mock_ws: MagicMock):
        await oms.start(inst_type="SPOT")
        mock_ws.subscribe_orders.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_clears_state(self, oms: OrderManager):
        oms._active["100"] = _make_order()
        oms._history["99"] = _make_order(order_id="99", status=OrderStatus.FILLED)
        await oms.stop()
        assert len(oms._active) == 0
        assert len(oms._history) == 0

    # --- Submit Order ---

    @pytest.mark.asyncio
    async def test_submit_order_enters_active(self, oms: OrderManager, mock_rest: MagicMock):
        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        order = await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")

        assert order.order_id == "100"
        assert order.symbol == "BTC-USDT"
        assert order.side == OrderSide.BUY
        assert order.status == OrderStatus.LIVE
        assert "100" in oms._active

    @pytest.mark.asyncio
    async def test_submit_order_rejected_raises(self, oms: OrderManager, mock_rest: MagicMock):
        mock_rest.place_order = AsyncMock(
            return_value=_rest_submit_response("100", s_code="51000")
        )
        with pytest.raises(OKXAPIError) as exc_info:
            await oms.submit_order("BTC-USDT", "buy", "limit", "0.001")
        assert exc_info.value.code == "51000"

    @pytest.mark.asyncio
    async def test_submit_with_client_order_id(self, oms: OrderManager, mock_rest: MagicMock):
        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        order = await oms.submit_order(
            "BTC-USDT", "buy", "limit", "0.001",
            price="63000", client_order_id="my_cl_1",
        )
        assert order.client_order_id == "my_cl_1"
        assert oms._clord_map["my_cl_1"] == "100"

    # --- WS Push Updates ---

    @pytest.mark.asyncio
    async def test_ws_push_updates_active_order(self, oms: OrderManager, mock_rest: MagicMock):
        """WS push with partially filled updates the active order."""
        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")

        # Simulate WS push: partially filled
        ws_order = _make_order(
            order_id="100",
            status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=0.0005,
            avg_price=63000.0,
            update_time=2000,
        )
        await oms._on_ws_order_update(ws_order)

        active = oms.get_active_orders()
        assert len(active) == 1
        assert active[0].filled_qty == 0.0005
        assert active[0].status == OrderStatus.PARTIALLY_FILLED

    @pytest.mark.asyncio
    async def test_ws_push_filled_moves_to_history(self, oms: OrderManager, mock_rest: MagicMock):
        """Fully filled order moves from active to history."""
        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")

        # WS push: filled
        ws_order = _make_order(
            order_id="100",
            status=OrderStatus.FILLED,
            filled_qty=0.001,
            avg_price=63000.0,
            update_time=3000,
        )
        await oms._on_ws_order_update(ws_order)

        assert len(oms.get_active_orders()) == 0
        assert "100" in oms.history
        assert oms.history["100"].status == OrderStatus.FILLED

    @pytest.mark.asyncio
    async def test_ws_push_cancelled_moves_to_history(self, oms: OrderManager, mock_rest: MagicMock):
        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")

        ws_order = _make_order(order_id="100", status=OrderStatus.CANCELLED_DONE, update_time=2000)
        await oms._on_ws_order_update(ws_order)

        assert len(oms.get_active_orders()) == 0
        assert oms.get_order("100").status == OrderStatus.CANCELLED_DONE

    # --- Partial Fill Accumulation ---

    @pytest.mark.asyncio
    async def test_partial_fill_accumulates(self, oms: OrderManager, mock_rest: MagicMock):
        """Multiple partial fills accumulate filled_qty."""
        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")

        # First partial fill
        await oms._on_ws_order_update(_make_order(
            order_id="100", status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=0.0003, avg_price=63000.0, update_time=2000,
        ))
        # Second partial fill
        await oms._on_ws_order_update(_make_order(
            order_id="100", status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=0.0007, avg_price=63001.0, update_time=3000,
        ))

        active = oms.get_active_orders()
        assert len(active) == 1
        assert active[0].filled_qty == 0.0007
        assert active[0].avg_price == 63001.0

    # --- Stale Push Rejection ---

    @pytest.mark.asyncio
    async def test_stale_push_ignored(self, oms: OrderManager, mock_rest: MagicMock):
        """Older update_time push should not overwrite newer state."""
        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")

        # Newer update first
        await oms._on_ws_order_update(_make_order(
            order_id="100", status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=0.0007, update_time=3000,
        ))
        # Stale push (older update_time) — should be ignored
        await oms._on_ws_order_update(_make_order(
            order_id="100", status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=0.0003, update_time=2000,
        ))

        active = oms.get_active_orders()
        assert active[0].filled_qty == 0.0007  # kept the newer value

    # --- Cancel Order ---

    @pytest.mark.asyncio
    async def test_cancel_order_success(self, oms: OrderManager, mock_rest: MagicMock):
        mock_rest.cancel_order = AsyncMock(return_value={})
        result = await oms.cancel_order("BTC-USDT", "100")
        assert result is True

    @pytest.mark.asyncio
    async def test_cancel_order_api_error(self, oms: OrderManager, mock_rest: MagicMock):
        mock_rest.cancel_order = AsyncMock(
            side_effect=OKXAPIError("51000", "Order not found")
        )
        result = await oms.cancel_order("BTC-USDT", "100")
        assert result is False

    # --- Event Handlers ---

    @pytest.mark.asyncio
    async def test_event_handler_triggered_on_update(self, oms: OrderManager, mock_rest: MagicMock):
        events: list[OrderData] = []

        async def handler(order: OrderData):
            events.append(order)

        oms.on_order_update(handler)

        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")

        # WS push
        await oms._on_ws_order_update(_make_order(
            order_id="100", status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=0.0005, update_time=2000,
        ))

        assert len(events) == 1
        assert events[0].filled_qty == 0.0005

    @pytest.mark.asyncio
    async def test_event_handler_error_does_not_propagate(self, oms: OrderManager, mock_rest: MagicMock):
        async def bad_handler(order: OrderData):
            raise ValueError("boom")

        oms.on_order_update(bad_handler)

        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")

        # Should not raise despite bad handler
        await oms._on_ws_order_update(_make_order(
            order_id="100", status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=0.0005, update_time=2000,
        ))

    # --- Multi-Symbol Filtering ---

    @pytest.mark.asyncio
    async def test_get_active_orders_by_symbol(self, oms: OrderManager, mock_rest: MagicMock):
        mock_rest.place_order = AsyncMock(side_effect=[
            _rest_submit_response("100"),
            _rest_submit_response("101"),
        ])
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")
        await oms.submit_order("ETH-USDT", "buy", "limit", "0.1", price="3000")

        btc_orders = oms.get_active_orders("BTC-USDT")
        eth_orders = oms.get_active_orders("ETH-USDT")
        all_orders = oms.get_active_orders()

        assert len(btc_orders) == 1
        assert len(eth_orders) == 1
        assert len(all_orders) == 2

    # --- Unknown WS Push ---

    @pytest.mark.asyncio
    async def test_ws_push_unknown_order_added(self, oms: OrderManager):
        """WS push for order not in active_orders should be added (e.g. web UI order)."""
        ws_order = _make_order(order_id="999", symbol="ETH-USDT", update_time=1000)
        await oms._on_ws_order_update(ws_order)
        assert "999" in oms._active

    # --- get_order ---

    @pytest.mark.asyncio
    async def test_get_order_returns_none_for_missing(self, oms: OrderManager):
        assert oms.get_order("nonexistent") is None

    @pytest.mark.asyncio
    async def test_get_order_from_history(self, oms: OrderManager):
        oms._history["99"] = _make_order(order_id="99", status=OrderStatus.FILLED)
        assert oms.get_order("99") is not None
        assert oms.get_order("99").status == OrderStatus.FILLED

    # --- active_count ---

    @pytest.mark.asyncio
    async def test_active_count(self, oms: OrderManager, mock_rest: MagicMock):
        assert oms.active_count == 0
        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001")
        assert oms.active_count == 1

    # --- filled_qty guard ---

    @pytest.mark.asyncio
    async def test_filled_qty_never_decreases(self, oms: OrderManager, mock_rest: MagicMock):
        """Even if a buggy push has lower filled_qty, OMS keeps the higher value."""
        mock_rest.place_order = AsyncMock(return_value=_rest_submit_response("100"))
        await oms.submit_order("BTC-USDT", "buy", "limit", "0.001", price="63000")

        # First: filled 0.0007
        await oms._on_ws_order_update(_make_order(
            order_id="100", status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=0.0007, update_time=3000,
        ))
        # Buggy push: filled drops to 0.0003 (newer timestamp)
        await oms._on_ws_order_update(_make_order(
            order_id="100", status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=0.0003, update_time=4000,
        ))

        # OMS should use the new push (newer timestamp) since the merge logic
        # takes the newer state and only guards against decrease.
        active = oms.get_active_orders()
        # The _merge_order takes new when new.update_time >= old.update_time,
        # but guards filled_qty — so filled_qty should stay at 0.0007
        assert active[0].filled_qty == 0.0007
