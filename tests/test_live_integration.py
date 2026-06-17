"""LiveRunner 端到端集成测试。

模拟完整实盘链路（无网络调用）：

    BarData → Strategy.on_bar() → buy()/sell()
        → RiskManager.submit()   (风控拦截)
            → LiveExecutor       (同步→异步桥接)
                → OrderManager   (REST 下单 + WS 状态跟踪)
                    → RESTClient (mock)

验证：策略信号 → 风控放行/拦截 → 订单提交 → 成交回调 → 持仓同步
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from okx_quant.models.market import (
    AccountData,
    BarData,
    OKXAPIError,
    OrderData,
    OrderSide,
    OrderStatus,
    OrderType,
)
from okx_quant.oms.order_manager import OrderManager
from okx_quant.risk.risk_manager import RiskConfig, RiskManager
from okx_quant.live.runner import LiveExecutor


# ======================================================================
# Helpers
# ======================================================================


def _order_response(order_id: str = "12345") -> dict:
    """模拟 OKX place_order 返回"""
    return {
        "ordId": order_id,
        "sCode": "0",
        "sMsg": "",
    }


def _make_rest_mock():
    """创建 mock RESTClient"""
    rest = AsyncMock()
    rest.place_order = AsyncMock(return_value=_order_response())
    rest.cancel_order = AsyncMock(return_value={})
    rest.get_open_orders = AsyncMock(return_value=[])
    rest.get_balance = AsyncMock(return_value=AccountData(
        total_equity=80000.0, available_balance=80000.0, margin_ratio=0.0,
    ))
    rest.get_positions = AsyncMock(return_value=[])
    return rest


# ======================================================================
# Test: 策略 → 风控 → 执行器 全链路
# ======================================================================


class TestLiveRunnerIntegration:
    """LiveRunner 全链路集成测试（mock 网络层）"""

    @pytest.fixture
    def rest(self):
        return _make_rest_mock()

    @pytest.fixture
    def oms(self, rest):
        return OrderManager(rest, None)  # type: ignore[arg-type]

    @pytest.fixture
    def executor(self, oms):
        return LiveExecutor(oms=oms, symbol="ETH-USDT-SWAP")

    # ------------------------------------------------------------------
    # 策略 → 风控 → 执行器
    # ------------------------------------------------------------------

    def test_strategy_buy_through_risk_to_executor(self, executor):
        """策略 buy() → 风控通过 → 执行器入队"""
        risk_config = RiskConfig(
            max_order_value=1_000_000,
            max_price_deviation=0.10,
        )
        risk_mgr = RiskManager(risk_config, executor)

        # 策略通过风控代理提交（同步调用）
        request_id = risk_mgr.submit(
            side="buy", price=None, quantity=0.01,
            order_type="market", pos_side="long",
        )
        assert request_id  # 返回了 request_id

    def test_risk_blocks_finger_order(self, executor):
        """风控拦截乌龙指（价格偏离过大）"""
        risk_config = RiskConfig(
            max_order_value=1_000_000,
            max_price_deviation=0.05,  # 5%
        )
        risk_mgr = RiskManager(risk_config, executor)
        risk_mgr.update_market_price(1800.0)  # 当前市价 1800

        # 限价单价格偏离 10%（低于市价）
        from okx_quant.risk.risk_manager import RiskViolationError
        with pytest.raises(RiskViolationError, match="fat_finger"):
            risk_mgr.submit(
                side="buy", price=1620.0, quantity=0.01,
                order_type="limit", pos_side="long",
            )

    def test_risk_passes_valid_stop_order(self, executor):
        """风控放行合理的止损单（4.9% 偏离，明确在安全范围内）"""
        risk_config = RiskConfig(
            max_order_value=1_000_000,
            max_price_deviation=0.05,
        )
        risk_mgr = RiskManager(risk_config, executor)
        risk_mgr.update_market_price(1800.0)

        # 止损价 = 1800 * 0.951 = 1711.80（偏离 4.9%，明确安全）
        stop_price = round(1800.0 * 0.951, 2)
        request_id = risk_mgr.submit(
            side="sell", price=stop_price, quantity=0.01,
            order_type="limit", pos_side="long",
        )
        assert request_id  # 应该通过

    # ------------------------------------------------------------------
    # 双向持仓
    # ------------------------------------------------------------------

    def test_open_long_and_short(self, executor):
        """开多 + 开空 双向持仓"""
        risk_mgr = RiskManager(RiskConfig(max_order_value=1_000_000), executor)

        # 开多
        rid1 = risk_mgr.submit(
            side="buy", price=None, quantity=0.01,
            order_type="market", pos_side="long",
        )
        assert rid1

        # 开空
        rid2 = risk_mgr.submit(
            side="sell", price=None, quantity=0.01,
            order_type="market", pos_side="short",
        )
        assert rid2
        assert rid1 != rid2  # 不同的 request_id

    # ------------------------------------------------------------------
    # 风控 Kill Switch
    # ------------------------------------------------------------------

    def test_kill_switch_blocks_all_orders(self, executor):
        """Kill Switch 激活后拒绝所有订单"""
        risk_config = RiskConfig(
            max_order_value=100.0,
            max_consecutive_failures=2,
        )
        risk_mgr = RiskManager(risk_config, executor)

        # 模拟连续下单失败
        for _ in range(3):
            try:
                risk_mgr.submit(
                    side="buy", price=None, quantity=0.01,
                    order_type="market", pos_side="long",
                )
            except Exception:
                pass

        # 模拟执行器返回失败触发 kill switch
        risk_mgr._consecutive_failures = 3
        risk_mgr._killed = True  # 模拟 kill switch 激活

        # 后续订单应被拒绝
        from okx_quant.risk.risk_manager import RiskViolationError
        with pytest.raises(RiskViolationError, match="kill_switch"):
            risk_mgr.submit(
                side="buy", price=None, quantity=0.01,
                order_type="market", pos_side="long",
            )

    # ------------------------------------------------------------------
    # 订单回调 → OMS 状态更新
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_order_fill_updates_oms(self, oms):
        """WS 订单推送 → OMS 状态更新（成交）"""
        order = OrderData(
            order_id="5001", client_order_id="", symbol="ETH-USDT-SWAP",
            side=OrderSide.BUY, pos_side="long", order_type=OrderType.MARKET,
            price=0.0, quantity=0.01, filled_qty=0.0, avg_price=0.0,
            status=OrderStatus.LIVE, fee=0.0, fee_currency="",
            timestamp=1000, update_time=1000,
        )
        oms._active["5001"] = order

        # 模拟 WS 推送成交
        filled = OrderData(
            order_id="5001", client_order_id="", symbol="ETH-USDT-SWAP",
            side=OrderSide.BUY, pos_side="long", order_type=OrderType.MARKET,
            price=0.0, quantity=0.01, filled_qty=0.01, avg_price=1800.0,
            status=OrderStatus.FILLED, fee=0.009, fee_currency="USDT",
            timestamp=1000, update_time=1001,
        )
        await oms._on_ws_order_update(filled)

        # 验证状态更新
        assert "5001" not in oms._active  # 已终结，移入 history
        assert "5001" in oms.history
        assert oms.history["5001"].status == OrderStatus.FILLED
        assert oms.history["5001"].filled_qty == 0.01

    @pytest.mark.asyncio
    async def test_order_cancel_updates_oms(self, oms):
        """撤单 → OMS 状态更新"""
        order = OrderData(
            order_id="5002", client_order_id="", symbol="ETH-USDT-SWAP",
            side=OrderSide.BUY, pos_side="long", order_type=OrderType.LIMIT,
            price=1600.0, quantity=0.01, filled_qty=0.0, avg_price=0.0,
            status=OrderStatus.LIVE, fee=0.0, fee_currency="",
            timestamp=1000, update_time=1000,
        )
        oms._active["5002"] = order

        # 模拟撤单推送
        cancelled = OrderData(
            order_id="5002", client_order_id="", symbol="ETH-USDT-SWAP",
            side=OrderSide.BUY, pos_side="long", order_type=OrderType.LIMIT,
            price=1600.0, quantity=0.01, filled_qty=0.0, avg_price=0.0,
            status=OrderStatus.CANCELLED_DONE, fee=0.0, fee_currency="",
            timestamp=1000, update_time=1001,
        )
        await oms._on_ws_order_update(cancelled)

        assert "5002" not in oms._active
        assert oms.history["5002"].status == OrderStatus.CANCELLED_DONE

    # ------------------------------------------------------------------
    # 执行器队列
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_executor_queues_and_submits(self, executor, oms, rest):
        """执行器正确队列化订单并提交到 REST"""
        task = asyncio.create_task(executor.start())
        await asyncio.sleep(0.05)

        # 提交订单（同步）
        request_id = executor.submit(
            side="buy", price=None, quantity=0.01,
            order_type="market", pos_side="long",
        )
        assert request_id

        # 等待异步处理
        await asyncio.sleep(0.3)

        # 验证 REST 被调用
        rest.place_order.assert_called()

        await executor.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_executor_cancel_order(self, executor, oms, rest):
        """执行器撤单"""
        task = asyncio.create_task(executor.start())
        await asyncio.sleep(0.05)

        # 先提交一个限价单
        rest.place_order.return_value = _order_response("6001")
        executor.submit(
            side="buy", price=1600.0, quantity=0.01,
            order_type="limit", pos_side="long",
        )
        await asyncio.sleep(0.2)

        # 撤单
        executor.cancel("6001")
        await asyncio.sleep(0.2)

        rest.cancel_order.assert_called()

        await executor.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # 执行器错误处理
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_executor_handles_rest_error(self, executor, oms, rest):
        """执行器处理 REST 错误（不崩溃）"""
        rest.place_order.side_effect = OKXAPIError(
            code="1", message="Insufficient balance", data=None,
        )

        task = asyncio.create_task(executor.start())
        await asyncio.sleep(0.05)

        executor.submit(
            side="buy", price=None, quantity=0.01,
            order_type="market", pos_side="long",
        )
        await asyncio.sleep(0.3)

        # 执行器不应崩溃
        assert not task.done() or task.exception() is None

        await executor.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
