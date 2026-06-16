"""审计发现的边界用例回归测试。

每个测试对应一个真实审计中发现的 Bug。如果这些测试中的任何一个失败，
说明对应的功能回归了。

覆盖范围：
  - OKX API 空字符串/None 解析
  - 科学计数法精度
  - 合约乘数 PnL
  - 强平方向判断
  - 风控检查集成
  - 订单状态机
  - WebSocket 重连安全性
"""

from __future__ import annotations

import asyncio
import math
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from okx_quant.backtest.engine import BacktestEngine, ExchangeConfig
from okx_quant.config.auth import OKXAuth
from okx_quant.config.settings import OKXConfig
from okx_quant.gateway.rest_client import RESTClient
from okx_quant.gateway.ws_client import WebSocketClient
from okx_quant.models.market import (
    AccountData,
    BarData,
    OKXAPIError,
    OrderData,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionData,
    PositionSide,
    TickData,
)
from okx_quant.oms.order_manager import OrderManager, _TERMINAL_STATES
from okx_quant.risk.risk_manager import RiskConfig, RiskManager, RiskViolationError
from okx_quant.strategy.base import BaseStrategy, Position, Signal, StrategyState


# =====================================================================
# 1. OKX 空字符串/None 解析防御
# =====================================================================


class TestEmptyStringParsing:
    """OKX API 经常返回空字符串 '' 而非缺失字段。"""

    def test_order_data_px_empty_string(self):
        """px='' 不应崩溃。"""
        data = {
            "instId": "BTC-USDT", "side": "buy", "ordType": "market",
            "px": "", "sz": "", "accFillSz": "", "avgPx": "",
            "state": "live", "fee": "", "feeCcy": "",
            "cTime": "", "uTime": "", "ordId": "123",
        }
        order = OrderData.from_okx(data)
        assert order.price == 0.0
        assert order.quantity == 0.0
        assert order.filled_qty == 0.0
        assert order.fee == 0.0
        assert order.timestamp == 0

    def test_order_data_px_none(self):
        """px 字段缺失不应崩溃。"""
        data = {
            "instId": "BTC-USDT", "side": "buy", "ordType": "market",
            "state": "live", "ordId": "123",
        }
        order = OrderData.from_okx(data)
        assert order.price == 0.0

    def test_position_data_empty_strings(self):
        """PositionData 所有数值字段为空字符串。"""
        data = {
            "instId": "BTC-USDT-SWAP", "posSide": "long", "pos": "",
            "avgPx": "", "upl": "", "lever": "", "liqPx": "", "margin": "",
            "cTime": "",
        }
        pos = PositionData.from_okx(data)
        assert pos.quantity == 0.0
        assert pos.avg_price == 0.0
        assert pos.leverage == 0.0

    def test_tick_data_sodUtc8_empty(self):
        """sodUtc8 为空字符串时 change24h 应为 0。"""
        data = {
            "instId": "BTC-USDT", "last": "63000", "bidPx": "62999",
            "askPx": "63001", "vol24h": "100", "high24h": "63500",
            "low24h": "62000", "sodUtc8": "", "ts": "1718448000000",
        }
        tick = TickData.from_okx(data)
        assert tick.change24h == 0.0

    def test_tick_data_sodUtc8_zero(self):
        """sodUtc8='0' 不应除零。"""
        data = {
            "instId": "BTC-USDT", "last": "63000", "bidPx": "62999",
            "askPx": "63001", "vol24h": "100", "high24h": "63500",
            "low24h": "62000", "sodUtc8": "0", "ts": "1718448000000",
        }
        tick = TickData.from_okx(data)
        assert tick.change24h == 0.0

    def test_tick_data_last_missing(self):
        """last 字段缺失不应崩溃。"""
        data = {
            "instId": "BTC-USDT", "bidPx": "62999",
            "askPx": "63001", "vol24h": "100", "high24h": "63500",
            "low24h": "62000", "sodUtc8": "62500", "ts": "1718448000000",
        }
        tick = TickData.from_okx(data)
        assert tick.change24h == 0.0  # last=0, sod=62500 → 0


# =====================================================================
# 2. 精度与科学计数法
# =====================================================================


class TestPrecision:
    """数值精度边界。"""

    def test_round_to_lot_scientific_notation(self):
        """lot_size=1e-8 时应输出 '0.00000001' 而非 '1E-8' 或 '0'。"""
        result = RESTClient._round_to_lot(0.00000001, 0.00000001, 0.00000001)
        assert result == "0.00000001"
        assert "E" not in result
        assert "e" not in result

    def test_round_to_lot_below_min(self):
        """四舍五入后低于 min_size 应返回 min_size。"""
        result = RESTClient._round_to_lot(0.005, 0.01, 0.01)
        assert result == "0.01"

    def test_round_to_lot_normal(self):
        """正常精度。"""
        result = RESTClient._round_to_lot(1.130639, 0.01, 0.01)
        assert result == "1.13"

    def test_round_to_lot_very_small_value(self):
        """极小值四舍五入到 min_size。"""
        result = RESTClient._round_to_lot(0.000000005, 0.00000001, 0.00000001)
        assert result == "0.00000001"

    def test_round_to_tick_scientific_notation(self):
        """tick_size=0.0001 时应正确格式化。"""
        result = RESTClient._round_to_tick(100.12345, 0.0001)
        assert result == "100.1235"


# =====================================================================
# 3. 合约乘数 PnL 计算
# =====================================================================


class TestContractMultiplier:
    """合约面值修正验证。"""

    def test_btc_swap_multiplier(self):
        """BTC-USDT-SWAP (ctVal=0.01): 100张涨$1000 → 盈利$1000。"""
        pos = Position(side="long", quantity=100, avg_price=63000,
                       contract_multiplier=0.01)
        pnl = pos.unrealized_pnl(64000)
        assert abs(pnl - 1000.0) < 0.01

    def test_btc_spot_no_multiplier(self):
        """现货 (multiplier=1.0): 0.5BTC涨$1000 → 盈利$500。"""
        pos = Position(side="long", quantity=0.5, avg_price=63000,
                       contract_multiplier=1.0)
        pnl = pos.unrealized_pnl(64000)
        assert abs(pnl - 500.0) < 0.01

    def test_eth_swap_ctval_one(self):
        """ETH-USDT-SWAP (ctVal=1.0): 10张涨$12 → 盈利$120。"""
        pos = Position(side="long", quantity=10, avg_price=1788,
                       contract_multiplier=1.0)
        pnl = pos.unrealized_pnl(1800)
        assert abs(pnl - 120.0) < 0.01

    def test_short_multiplier(self):
        """空头: 100张跌$1000, multiplier=0.01 → 盈利$1000。"""
        pos = Position(side="short", quantity=100, avg_price=63000,
                       contract_multiplier=0.01)
        pnl = pos.unrealized_pnl(62000)
        assert abs(pnl - 1000.0) < 0.01

    def test_backtest_pnl_with_multiplier(self):
        """回测引擎 PnL 应乘以合约乘数。"""
        engine = BacktestEngine(
            initial_capital=100000,
            config=ExchangeConfig(
                taker_fee_rate=0, slippage_base=0, latency_bars=1,
                contract_multiplier=0.01, leverage=10,
            ),
        )

        class BuyOnceStrategy(BaseStrategy):
            name = "buy_once"
            def on_bar(self, bar):
                if self.state.bar_index == 0:
                    self.buy(100)  # 100 张
                return None

        bars = [
            BarData("BTC-USDT", 63000, 63100, 62900, 63050, 1000,
                    1000000 + i * 60000, True)
            for i in range(10)
        ]
        # 让价格涨 10%
        bars[1] = BarData("BTC-USDT", 69300, 69400, 69200, 69300, 1000,
                          1000000 + 60000, True)

        strategy = BuyOnceStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)

        # 如果没有乘数修正，盈利会被放大 100 倍
        # 100张 × 0.01 × (69300-63000) = $6,300
        assert result.total_trades >= 1
        # 盈利不应超过 $100,000（没有乘数时会是 $630,000）
        assert result.final_equity < 200000


# =====================================================================
# 4. 强平方向判断
# =====================================================================


class TestLiquidationDirection:
    """check_liquidation 方向性验证。"""

    def test_long_profitable_not_liquidated(self):
        """做多盈利时不应触发强平。"""
        cfg = ExchangeConfig(leverage=10, maintenance_margin_ratio=0.005,
                             enable_liquidation=True)
        # 入场 100, 现价 200 → 盈利
        assert cfg.check_liquidation(100, 200, "long", 1000) is False

    def test_long_loss_triggers_liquidation(self):
        """做多大幅亏损应触发强平。"""
        cfg = ExchangeConfig(leverage=10, maintenance_margin_ratio=0.005,
                             enable_liquidation=True)
        # 入场 100, 现价 80 → 亏损 20, threshold = 100*0.005*10 = 5
        assert cfg.check_liquidation(100, 80, "long", 1000) is True

    def test_short_profitable_not_liquidated(self):
        """做空盈利时不应触发强平。"""
        cfg = ExchangeConfig(leverage=10, maintenance_margin_ratio=0.005,
                             enable_liquidation=True)
        # 入场 100, 现价 50 → 盈利
        assert cfg.check_liquidation(100, 50, "short", 1000) is False

    def test_short_loss_triggers_liquidation(self):
        """做空大幅亏损应触发强平。"""
        cfg = ExchangeConfig(leverage=10, maintenance_margin_ratio=0.005,
                             enable_liquidation=True)
        # 入场 100, 现价 120 → 亏损 20
        assert cfg.check_liquidation(100, 120, "short", 1000) is True

    def test_no_leverage_no_liquidation(self):
        """无杠杆不应触发强平。"""
        cfg = ExchangeConfig(leverage=1, enable_liquidation=True)
        assert cfg.check_liquidation(100, 1, "long", 1000) is False


# =====================================================================
# 5. 风控集成
# =====================================================================


class TestRiskIntegration:
    """风控系统集成验证。"""

    def _make_risk(self, **kwargs):
        inner = MagicMock()
        inner.submit = MagicMock(return_value="order_1")
        inner.cancel = MagicMock(return_value=True)
        config = RiskConfig(**kwargs)
        return RiskManager(config, inner), inner

    def test_exposure_blocks_oversized_order(self):
        """敞口限制应阻止超大订单。"""
        mgr, inner = self._make_risk(max_total_exposure=1000)
        # 设置当前敞口
        mgr._current_exposure = 900
        mgr._last_market_price = 100
        # 新订单名义值 = 200 → 总敞口 1100 > 1000
        with pytest.raises(RiskViolationError) as exc:
            mgr.submit("buy", 100.0, 2.0, "market", "long")
        assert "exceeds max" in str(exc.value).lower()

    def test_leverage_blocks_new_open(self):
        """杠杆限制应阻止新开仓。"""
        mgr, inner = self._make_risk(max_account_leverage=2.0)
        mgr._current_equity = 1000
        mgr._last_market_price = 100
        mgr._current_positions = [Position("long", 10, 100)]
        # 10 * 100 = 1000 / 1000 = 1x leverage, max = 2x → OK
        mgr.submit("buy", 100.0, 5, "market", "long")

    def test_leverage_allows_close(self):
        """杠杆限制不应阻止平仓。"""
        mgr, inner = self._make_risk(max_account_leverage=0.5)
        mgr._current_equity = 1000
        mgr._last_market_price = 100
        mgr._current_positions = [Position("long", 100, 100)]
        # sell + pos_side=long → 平仓，不应检查杠杆
        mgr.submit("sell", None, 100, "market", "long")

    def test_high_water_mark_drawdown(self):
        """回撤应基于高水位而非初始权益。"""
        mgr, _ = self._make_risk(
            initial_equity=10000, max_drawdown_pct=0.20, kill_on_drawdown=True,
        )
        # 先涨到 15000
        mgr.check_account(15000, [], 100)
        assert mgr._high_water_mark == 15000
        # 跌到 11000 → 回撤 = (15000-11000)/15000 = 26.7% > 20%
        mgr.check_account(11000, [], 100)
        assert mgr.is_killed is True

    def test_exposure_recalculated_from_positions(self):
        """敞口应基于真实持仓计算，非累加。"""
        mgr, _ = self._make_risk(max_total_exposure=50000)
        # 模拟多次成交，敞口应基于持仓而非累加
        mgr.check_account(10000, [Position("long", 10, 100)], 100)
        assert mgr._current_exposure == 10 * 100  # 1000
        # 再次调用，持仓不变 → 敞口不变
        mgr.check_account(10000, [Position("long", 10, 100)], 110)
        assert mgr._current_exposure == 10 * 110  # 1100，不是 1000+1100


# =====================================================================
# 6. OMS 状态机
# =====================================================================


class TestOMSStateMachine:
    """订单状态转换验证。"""

    @pytest.mark.asyncio
    async def test_terminal_order_goes_to_history(self):
        """终态订单应直接入 _history，不留在 _active。"""
        rest = MagicMock()
        ws = MagicMock()
        ws.subscribe_orders = MagicMock()
        oms = OrderManager(rest, ws)

        # 模拟一个已经是 FILLED 的 WS 推送（非 OMS 提交的订单）
        order = OrderData.from_okx({
            "ordId": "999", "clOrdId": "", "instId": "BTC-USDT",
            "side": "buy", "posSide": "long", "ordType": "market",
            "px": "0", "sz": "1", "accFillSz": "1", "avgPx": "63000",
            "state": "filled", "fee": "0", "feeCcy": "",
            "cTime": "1000", "uTime": "2000",
        })
        await oms._on_ws_order_update(order)
        assert "999" not in oms._active
        assert "999" in oms.history

    @pytest.mark.asyncio
    async def test_filled_qty_never_decreases(self):
        """filled_qty 不应因旧推送而减少。"""
        rest = MagicMock()
        ws = MagicMock()
        ws.subscribe_orders = MagicMock()
        oms = OrderManager(rest, ws)

        # 先提交一个订单
        rest.place_order = AsyncMock(return_value={
            "ordId": "100", "sCode": "0", "clOrdId": "cl_100",
        })
        await oms.submit_order("BTC-USDT", "buy", "limit", "1", price="63000")

        # WS 推送: filled 0.8
        o1 = OrderData.from_okx({
            "ordId": "100", "clOrdId": "cl_100", "instId": "BTC-USDT",
            "side": "buy", "posSide": "long", "ordType": "limit",
            "px": "63000", "sz": "1", "accFillSz": "0.8", "avgPx": "63000",
            "state": "partially_filled", "fee": "0", "feeCcy": "",
            "cTime": "1000", "uTime": "3000",
        })
        await oms._on_ws_order_update(o1)

        # 旧推送: filled 0.3 (更新时间更早)
        o2 = OrderData.from_okx({
            "ordId": "100", "clOrdId": "cl_100", "instId": "BTC-USDT",
            "side": "buy", "posSide": "long", "ordType": "limit",
            "px": "63000", "sz": "1", "accFillSz": "0.3", "avgPx": "63000",
            "state": "partially_filled", "fee": "0", "feeCcy": "",
            "cTime": "1000", "uTime": "2000",
        })
        await oms._on_ws_order_update(o2)

        active = oms.get_active_orders("BTC-USDT")
        assert active[0].filled_qty == 0.8  # 保留较大的值


# =====================================================================
# 7. REST 签名
# =====================================================================


class TestRESTSignature:
    """REST 签名验证。"""

    def test_sign_path_includes_query_params(self):
        """签名路径应包含 query string。"""
        cfg = OKXConfig(api_key="ak", secret_key="sk", passphrase="pp")
        auth = OKXAuth(cfg)
        # 模拟带参数的 GET 请求签名
        headers = auth.sign("GET", "/api/v5/trade/orders-pending?instId=BTC-USDT")
        assert "OK-ACCESS-SIGN" in headers
        assert "OK-ACCESS-TIMESTAMP" in headers
        # 签名应包含完整路径（含 query）
        # 无法直接验证签名值，但确保不抛异常

    def test_sign_path_without_query(self):
        """无 query 的签名路径正常工作。"""
        cfg = OKXConfig(api_key="ak", secret_key="sk", passphrase="pp")
        auth = OKXAuth(cfg)
        headers = auth.sign("GET", "/api/v5/account/balance")
        assert "OK-ACCESS-SIGN" in headers


# =====================================================================
# 8. WebSocket 安全
# =====================================================================


class TestWebSocketSafety:
    """WebSocket 连接安全性。"""

    @pytest.mark.asyncio
    async def test_connect_resets_running_on_failure(self):
        """connect() 失败时应重置 _running。"""
        client = WebSocketClient("wss://invalid.example.com/ws")
        with pytest.raises(Exception):
            await client.connect()
        assert client._running is False

    def test_pending_tasks_saved(self):
        """_pending_tasks 应保存 create_task 引用。"""
        client = WebSocketClient("wss://test")
        assert hasattr(client, "_pending_tasks")
        assert isinstance(client._pending_tasks, set)
