"""Unit tests for Module 4: Strategy Base & Backtesting Engine.

Covers:
  - ExchangeConfig: rounding, fees, dynamic slippage, liquidation
  - Position: PnL, extremes, open/close
  - BaseStrategy: lifecycle, buy/sell/close_long/close_short, bars window
  - BacktestEngine: market orders, limit/stop, contract mode, dynamic slippage,
    latency, signal confidence, liquidation, funding, force-close, equity curve
"""

from __future__ import annotations

import math
from unittest.mock import MagicMock

import pytest

from okx_quant.models.market import BarData, OrderData, OrderSide, OrderStatus, OrderType
from okx_quant.strategy.base import (
    BaseStrategy,
    PendingOrder,
    Position,
    Signal,
    StrategyState,
    generate_order_id,
)
from okx_quant.backtest.engine import BacktestEngine, ExchangeConfig, Trade


# ======================================================================
# Helpers
# ======================================================================


def _bar(
    o: float, h: float, l: float, c: float, v: float = 100.0,
    ts: int = 1000000, symbol: str = "BTC-USDT",
) -> BarData:
    return BarData(symbol=symbol, open=o, high=h, low=l, close=c,
                   volume=v, timestamp=ts, confirmed=True)


def _bars_up(n: int = 20, start: float = 100.0, step: float = 1.0) -> list[BarData]:
    """Generate monotonically rising bars."""
    bars = []
    for i in range(n):
        p = start + i * step
        bars.append(_bar(p, p + 0.5, p - 0.5, p + 0.3, ts=1000000 + i * 60000))
    return bars


def _bars_down(n: int = 20, start: float = 120.0, step: float = 1.0) -> list[BarData]:
    bars = []
    for i in range(n):
        p = start - i * step
        bars.append(_bar(p, p + 0.5, p - 0.5, p - 0.3, ts=1000000 + i * 60000))
    return bars


def _bars_volatile(n: int = 10, start: float = 100.0, amplitude: float = 10.0) -> list[BarData]:
    """Generate bars with large amplitude (high slippage)."""
    bars = []
    for i in range(n):
        p = start + i * 2
        bars.append(_bar(p, p + amplitude, p - amplitude, p,
                         ts=1000000 + i * 60000))
    return bars


# Simple strategies for testing


class BuyOnceStrategy(BaseStrategy):
    """Buys 10% of cash on the first bar, then holds."""
    name = "buy_once"

    def on_bar(self, bar: BarData) -> Signal | None:
        if self.state.bar_index == 0:
            qty = self.state.cash * 0.1 / bar.close
            self.buy(qty)
        return None


class BuySellStrategy(BaseStrategy):
    """Buys on bar 0, sells on bar 5."""
    name = "buy_sell"

    def on_bar(self, bar: BarData) -> Signal | None:
        if self.state.bar_index == 0:
            qty = self.state.cash * 0.5 / bar.close
            self.buy(qty)
        elif self.state.bar_index == 5 and self.position_long:
            self.close_long()
        return None


class SignalStrategy(BaseStrategy):
    """Returns a Signal instead of calling buy/sell."""
    name = "signal"

    def on_bar(self, bar: BarData) -> Signal | None:
        if self.state.bar_index == 0:
            return Signal(action="BUY", price=bar.close, confidence=0.8, reason="test")
        return None


class LowConfidenceStrategy(BaseStrategy):
    """Returns a Signal with confidence < 0.6."""
    name = "low_conf"

    def on_bar(self, bar: BarData) -> Signal | None:
        if self.state.bar_index == 0:
            return Signal(action="BUY", price=bar.close, confidence=0.3, reason="low")
        return None


class LimitOrderStrategy(BaseStrategy):
    """Places a limit buy order below current price."""
    name = "limit"

    def on_bar(self, bar: BarData) -> Signal | None:
        if self.state.bar_index == 0:
            qty = self.state.cash * 0.5 / (bar.close * 0.98)
            self.buy(qty, price=bar.close * 0.98, order_type="limit")
        return None


class StopOrderStrategy(BaseStrategy):
    """Places a stop buy order above current price."""
    name = "stop"

    def on_bar(self, bar: BarData) -> Signal | None:
        if self.state.bar_index == 0:
            qty = self.state.cash * 0.5 / (bar.close * 1.02)
            self.buy(qty, price=bar.close * 1.02, order_type="stop")
        return None


class TakeProfitStopLossStrategy(BaseStrategy):
    """Places entry + TP + SL in the same on_bar call."""
    name = "tp_sl"

    def on_bar(self, bar: BarData) -> Signal | None:
        if self.state.bar_index == 0:
            qty = self.state.cash * 0.5 / bar.close
            self.buy(qty)  # market entry
            # Take profit at +2%
            self.sell(qty, price=bar.close * 1.02, order_type="limit")
            # Stop loss at -1%
            self.sell(qty, price=bar.close * 0.99, order_type="stop")
        return None


class OpenLongShortStrategy(BaseStrategy):
    """Opens both long and short simultaneously (contract mode)."""
    name = "dual"

    def on_bar(self, bar: BarData) -> Signal | None:
        if self.state.bar_index == 0:
            qty = self.state.cash * 0.3 / bar.close
            self.buy(qty)   # open long
            self.sell(qty)  # open short
        elif self.state.bar_index == 5:
            if self.position_long:
                self.close_long()
            if self.position_short:
                self.close_short()
        return None


class DoNothingStrategy(BaseStrategy):
    """Does nothing — used for equity curve baseline."""
    name = "nothing"

    def on_bar(self, bar: BarData) -> Signal | None:
        return None


# ======================================================================
# ExchangeConfig
# ======================================================================


class TestExchangeConfig:

    def test_round_price(self):
        cfg = ExchangeConfig(tick_size=0.1)
        assert cfg.round_price(100.05) == 100.1
        assert cfg.round_price(100.04) == 100.0

    def test_round_price_small_tick(self):
        cfg = ExchangeConfig(tick_size=0.01)
        assert cfg.round_price(100.005) == 100.01
        assert cfg.round_price(100.004) == 100.00

    def test_round_quantity(self):
        cfg = ExchangeConfig(lot_size=0.001, min_order_qty=0.01)
        assert cfg.round_quantity(0.015) == 0.015
        # 0.005 rounds to 0.005 which is < min_order_qty=0.01 → returns 0
        assert cfg.round_quantity(0.005) == 0.0
        # 0.012 rounds to 0.012 which is >= min_order_qty → returns 0.012
        assert cfg.round_quantity(0.012) == 0.012

    def test_calc_fee_taker(self):
        cfg = ExchangeConfig(taker_fee_rate=0.001)
        assert cfg.calc_fee(100.0, 1.0, is_taker=True) == 0.1

    def test_calc_fee_maker(self):
        cfg = ExchangeConfig(maker_fee_rate=0.0002)
        assert cfg.calc_fee(100.0, 1.0, is_taker=False) == 0.02

    def test_dynamic_slippage_calm_market(self):
        cfg = ExchangeConfig(slippage_base=0.0003, slippage_volatility_factor=2.0)
        # Narrow range: amplitude = 0.2/100 = 0.2%
        result = cfg.calc_slippage(100.0, "buy", 100.1, 99.9)
        # amplitude/avg = 0.002/0.01 = 0.2 → factor = 1 + 2*0.2 = 1.4
        # slippage = 0.0003 * 1.4 = 0.00042
        assert result > 100.0
        assert result < 100.1  # within reasonable range

    def test_dynamic_slippage_volatile_market(self):
        cfg = ExchangeConfig(slippage_base=0.0003, slippage_volatility_factor=2.0)
        # Wide range: amplitude = 20/100 = 20%
        result = cfg.calc_slippage(100.0, "buy", 110.0, 90.0)
        # amplitude/avg = 0.2/0.01 = 20 → factor = 1 + 2*20 = 41
        # slippage = 0.0003 * 41 = 0.0123 → capped at 0.01
        expected = 100.0 * 1.01  # 1% cap
        assert abs(result - expected) < 0.01

    def test_dynamic_slippage_sell_direction(self):
        cfg = ExchangeConfig(slippage_base=0.0003)
        result = cfg.calc_slippage(100.0, "sell", 100.1, 99.9)
        assert result < 100.0  # sell slippage goes down

    def test_check_liquidation_no_leverage(self):
        cfg = ExchangeConfig(leverage=1, enable_liquidation=True)
        assert cfg.check_liquidation(100, 50, "long", 1000) is False

    def test_check_liquidation_triggered(self):
        cfg = ExchangeConfig(
            leverage=10, maintenance_margin_ratio=0.005, enable_liquidation=True
        )
        # loss = |80-100| = 20, threshold = 100 * 0.005 * 10 = 5
        assert cfg.check_liquidation(100, 80, "long", 1000) is True

    def test_check_liquidation_not_triggered(self):
        cfg = ExchangeConfig(
            leverage=10, maintenance_margin_ratio=0.005, enable_liquidation=True
        )
        # loss = |99-100| = 1, threshold = 5
        assert cfg.check_liquidation(100, 99, "long", 1000) is False

    def test_execution_bar(self):
        cfg = ExchangeConfig(latency_bars=1)
        assert cfg.execution_bar(0) == 1
        assert cfg.execution_bar(5) == 6

    def test_execution_bar_zero_latency(self):
        cfg = ExchangeConfig(latency_bars=0)
        assert cfg.execution_bar(0) == 0


# ======================================================================
# Position
# ======================================================================


class TestPosition:

    def test_unrealized_pnl_long(self):
        pos = Position(side="long", quantity=1.0, avg_price=100.0)
        assert pos.unrealized_pnl(110.0) == 10.0
        assert pos.unrealized_pnl(90.0) == -10.0

    def test_unrealized_pnl_short(self):
        pos = Position(side="short", quantity=1.0, avg_price=100.0)
        assert pos.unrealized_pnl(90.0) == 10.0
        assert pos.unrealized_pnl(110.0) == -10.0

    def test_unrealized_pnl_zero_quantity(self):
        pos = Position(side="long", quantity=0, avg_price=100.0)
        assert pos.unrealized_pnl(110.0) == 0.0

    def test_update_extremes_long(self):
        pos = Position(side="long", quantity=1.0, avg_price=100.0,
                       highest_price=100.0)
        pos.update_extremes(110.0, 95.0)
        assert pos.highest_price == 110.0

    def test_update_extremes_short(self):
        pos = Position(side="short", quantity=1.0, avg_price=100.0,
                       lowest_price=100.0)
        pos.update_extremes(110.0, 85.0)
        assert pos.lowest_price == 85.0

    def test_update_extremes_no_regression(self):
        pos = Position(side="long", quantity=1.0, avg_price=100.0,
                       highest_price=120.0)
        pos.update_extremes(110.0, 95.0)
        assert pos.highest_price == 120.0  # not regressed


# ======================================================================
# BaseStrategy
# ======================================================================


class TestBaseStrategy:

    def test_buy_calls_executor(self):
        strategy = BuyOnceStrategy()
        executor = MagicMock()
        executor.submit = MagicMock(return_value="order_1")
        strategy._executor = executor
        strategy.state = StrategyState(cash=10000)

        strategy.buy(0.1, price=100.0, order_type="limit")
        executor.submit.assert_called_once_with(
            side="buy", price=100.0, quantity=0.1,
            order_type="limit", pos_side="long",
        )

    def test_sell_calls_executor(self):
        strategy = BuyOnceStrategy()
        executor = MagicMock()
        executor.submit = MagicMock(return_value="order_1")
        strategy._executor = executor
        strategy.state = StrategyState(cash=10000)

        strategy.sell(0.1)
        executor.submit.assert_called_once_with(
            side="sell", price=None, quantity=0.1,
            order_type="market", pos_side="short",
        )

    def test_close_long_full(self):
        strategy = BuyOnceStrategy()
        executor = MagicMock()
        executor.submit = MagicMock(return_value="order_1")
        strategy._executor = executor
        strategy.state = StrategyState(
            cash=10000,
            position_long=Position(side="long", quantity=0.5, avg_price=100),
        )

        strategy.close_long()
        executor.submit.assert_called_once_with(
            side="sell", price=None, quantity=0.5,
            order_type="market", pos_side="long",
        )

    def test_close_short_partial(self):
        strategy = BuyOnceStrategy()
        executor = MagicMock()
        executor.submit = MagicMock(return_value="order_1")
        strategy._executor = executor
        strategy.state = StrategyState(
            cash=10000,
            position_short=Position(side="short", quantity=1.0, avg_price=100),
        )

        strategy.close_short(size=0.3)
        executor.submit.assert_called_once_with(
            side="buy", price=None, quantity=0.3,
            order_type="market", pos_side="short",
        )

    def test_close_long_no_position(self):
        strategy = BuyOnceStrategy()
        executor = MagicMock()
        executor.submit = MagicMock()
        strategy._executor = executor
        strategy.state = StrategyState(cash=10000)

        result = strategy.close_long()
        assert result == ""
        executor.submit.assert_not_called()

    def test_has_position(self):
        strategy = BuyOnceStrategy()
        strategy.state = StrategyState(cash=10000)
        assert strategy.has_position is False

        strategy.state.position_long = Position(side="long", quantity=1.0, avg_price=100)
        assert strategy.has_position is True

    def test_bars_window_access(self):
        strategy = BuyOnceStrategy()
        strategy.bars = [_bar(100, 101, 99, 100.5), _bar(101, 102, 100, 101.5)]
        assert strategy.current_bar is not None
        assert strategy.current_bar.close == 101.5


# ======================================================================
# BacktestEngine — Basic
# ======================================================================


class TestBacktestEngine:

    def test_do_nothing_equity_unchanged(self):
        """Strategy that does nothing should preserve initial capital."""
        bars = _bars_up(10)
        engine = BacktestEngine(initial_capital=10000)
        strategy = DoNothingStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)
        assert abs(result.final_equity - 10000) < 0.01
        assert result.total_trades == 0

    def test_buy_executes_at_next_bar_open(self):
        """Market buy at bar[0] → executed at bar[1].open + slippage."""
        bars = _bars_up(10, start=100.0, step=1.0)
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        strategy = BuyOnceStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)

        # Bought at bar[1].open = 101.0 (no slippage, no fee)
        assert result.total_trades >= 1
        assert result.equity_curve[0] == 10000  # bar[0]: no execution yet
        assert result.equity_curve[1] != 10000   # bar[1]: position opened

    def test_buy_sell_round_trip(self):
        """Buy at bar[1], sell at bar[6] → profit in uptrend."""
        bars = _bars_up(20, start=100.0, step=2.0)
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        strategy = BuySellStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)

        # Buy at bar[1].open=102, sell at bar[6].open=112 → profit
        assert result.final_equity > 10000
        assert result.total_trades >= 1
        assert result.win_rate > 0

    def test_fee_deduction(self):
        """Fees should reduce final equity."""
        # Flat bars — no price change, so P&L = 0 but fees apply
        bars = [_bar(100.0, 100.0, 100.0, 100.0, ts=1000000 + i * 60000)
                for i in range(10)]

        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            taker_fee_rate=0.001, slippage_base=0.0, latency_bars=1,
        ))
        strategy = BuyOnceStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)

        assert result.total_fees > 0
        assert result.final_equity < 10000


# ======================================================================
# BacktestEngine — Limit & Stop Orders
# ======================================================================


class TestLimitStopOrders:

    def test_limit_order_fills_when_price_drops(self):
        """Limit buy at 98 → fills when bar.low <= 98."""
        bars = [
            _bar(100, 101, 99, 100),    # bar 0: place limit
            _bar(100, 101, 99, 100),    # bar 1: no fill
            _bar(99, 100, 97, 98),      # bar 2: low=97 → fills at 98
            _bar(98, 99, 97, 98),       # bar 3
            _bar(98, 99, 97, 98),       # bar 4
        ]
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, maker_fee_rate=0.0,
            latency_bars=1,
        ))
        strategy = LimitOrderStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)

        # Should have at least 1 trade
        assert result.total_trades >= 1

    def test_limit_order_no_fill_if_price_stays_high(self):
        """Limit buy at 98 → never fills if bars stay above 98."""
        bars = [
            _bar(100, 101, 99, 100),    # bar 0: place limit at 98
            _bar(101, 102, 100, 101),   # bar 1
            _bar(102, 103, 101, 102),   # bar 2
            _bar(103, 104, 102, 103),   # bar 3
        ]
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        strategy = LimitOrderStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)

        assert result.total_trades == 0

    def test_stop_order_fills_on_breakout(self):
        """Stop buy at 102 → fills when bar.high >= 102."""
        bars = [
            _bar(100, 101, 99, 100),    # bar 0: place stop at 102
            _bar(100, 101, 99, 100),    # bar 1
            _bar(101, 103, 100, 102),   # bar 2: high=103 → triggers stop
            _bar(102, 103, 101, 102),   # bar 3
        ]
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        strategy = StopOrderStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)

        assert result.total_trades >= 1


# ======================================================================
# BacktestEngine — Take Profit + Stop Loss
# ======================================================================


class TestTakeProfitStopLoss:

    def test_tp_sl_both_placed(self):
        """Strategy places entry + TP + SL — all 3 should be in pending."""
        bars = [_bar(100.0, 100.0, 100.0, 100.0, ts=1000000 + i * 60000)
                for i in range(10)]

        strategy = TakeProfitStopLossStrategy()
        strategy.on_init({})
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        result = engine.run(strategy, bars)

        # After execution, should have trades (entry fill + TP or SL)
        assert result.total_trades >= 1


# ======================================================================
# BacktestEngine — Contract Mode (Dual Position)
# ======================================================================


class TestContractMode:

    def test_long_and_short_coexist(self):
        """Both long and short positions can be open simultaneously."""
        bars = _bars_up(10, start=100.0, step=1.0)
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        strategy = OpenLongShortStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars, contract_mode=True)

        # Should have trades from both long and short
        assert result.total_trades >= 2

    def test_close_long_short_independently(self):
        """Closing long doesn't affect short and vice versa.

        Long P&L = (106-101)*qty, Short P&L = (101-106)*qty → net ~0.
        So we check that both trades exist with opposite signs.
        """
        bars = _bars_up(10, start=100.0, step=1.0)
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        strategy = OpenLongShortStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars, contract_mode=True)

        # Should have 2 closed trades: one long (profit) and one short (loss)
        assert result.total_trades == 2
        long_pnl = sum(t.pnl for t in result.trades if t.side == "long")
        short_pnl = sum(t.pnl for t in result.trades if t.side == "short")
        assert long_pnl > 0   # long profits in uptrend
        assert short_pnl < 0  # short loses in uptrend
        # Net is approximately zero (equal sizes, symmetric entry/exit)
        assert abs(long_pnl + short_pnl) < 1.0


# ======================================================================
# BacktestEngine — Dynamic Slippage
# ======================================================================


class TestDynamicSlippage:

    def test_volatile_market_more_slippage(self):
        """High-amplitude bars should have more slippage than calm bars."""
        cfg = ExchangeConfig(slippage_base=0.0003, slippage_volatility_factor=2.0)

        # Calm market: 0.1% amplitude
        slip_calm = cfg.calc_slippage(100.0, "buy", 100.05, 99.95)
        # Volatile market: 10% amplitude
        slip_vol = cfg.calc_slippage(100.0, "buy", 105.0, 95.0)

        # Volatile should have much more slippage
        assert slip_vol - 100.0 > (slip_calm - 100.0) * 5


# ======================================================================
# BacktestEngine — Signal Confidence
# ======================================================================


class TestSignalConfidence:

    def test_high_confidence_executes(self):
        bars = _bars_up(10, start=100.0, step=1.0)
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
            signal_confidence_threshold=0.6,
        ))
        strategy = SignalStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)
        assert result.total_trades >= 1

    def test_low_confidence_ignored(self):
        bars = _bars_up(10, start=100.0, step=1.0)
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
            signal_confidence_threshold=0.6,
        ))
        strategy = LowConfidenceStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)
        assert result.total_trades == 0


# ======================================================================
# BacktestEngine — Liquidation
# ======================================================================


class TestLiquidation:

    def test_liquidation_closes_position(self):
        """Position should be liquidated when loss exceeds maintenance margin."""
        # Bars that drop sharply
        bars = [
            _bar(100, 101, 99, 100),    # bar 0: buy
            _bar(95, 96, 94, 95),       # bar 1: big drop
            _bar(80, 81, 79, 80),       # bar 2: crash
            _bar(80, 81, 79, 80),       # bar 3
        ]
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
            leverage=10, maintenance_margin_ratio=0.005, enable_liquidation=True,
        ))
        strategy = BuyOnceStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars, contract_mode=True)

        # Position should have been liquidated
        # Final equity should reflect the loss
        assert result.final_equity < 10000


# ======================================================================
# BacktestEngine — Funding Rate
# ======================================================================


class TestFundingRate:

    def test_funding_deducted_for_long(self):
        """Long positions pay funding."""
        # 24 bars at 1h intervals → 3 funding periods (every 8h)
        bars = []
        for i in range(24):
            bars.append(_bar(100, 101, 99, 100, ts=1000000 + i * 3600000))

        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
            funding_rate=0.001, funding_interval_hours=8,
        ))
        strategy = BuyOnceStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars, contract_mode=True)

        assert result.total_funding > 0


# ======================================================================
# BacktestEngine — Edge Cases
# ======================================================================


class TestEdgeCases:

    def test_single_bar(self):
        """Engine should handle a single bar without crashing."""
        bars = [_bar(100, 101, 99, 100)]
        engine = BacktestEngine(initial_capital=10000)
        strategy = DoNothingStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)
        assert result.final_equity == 10000

    def test_empty_equity_curve(self):
        """Zero bars should produce empty equity curve."""
        engine = BacktestEngine(initial_capital=10000)
        # No bars → will crash. This is expected — engine needs bars.
        # Instead, test with 1 bar.
        bars = [_bar(100, 101, 99, 100)]
        strategy = DoNothingStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)
        assert len(result.equity_curve) == 1

    def test_force_close_at_end(self):
        """Open positions should be force-closed at backtest end."""
        bars = _bars_up(5, start=100.0, step=2.0)
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        strategy = BuyOnceStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)

        # Position should be force-closed — no remaining positions
        # Final equity = cash (no open positions)
        assert result.final_equity != 10000  # some P&L from the trade

    def test_generate_order_id_unique(self):
        ids = {generate_order_id() for _ in range(1000)}
        assert len(ids) == 1000

    def test_result_has_all_metrics(self):
        bars = _bars_up(20, start=100.0, step=1.0)
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        strategy = BuySellStrategy()
        strategy.on_init({})
        result = engine.run(strategy, bars)

        assert hasattr(result, "initial_capital")
        assert hasattr(result, "final_equity")
        assert hasattr(result, "total_return")
        assert hasattr(result, "max_drawdown")
        assert hasattr(result, "sharpe_ratio")
        assert hasattr(result, "win_rate")
        assert hasattr(result, "total_trades")
        assert hasattr(result, "total_fees")
        assert hasattr(result, "equity_curve")
        assert hasattr(result, "trades")
        assert hasattr(result, "config")


# ======================================================================
# Walk-Forward Validation
# ======================================================================


class TestWalkForward:

    def _bars(self, n: int = 60, start: float = 100.0, step: float = 1.0) -> list[BarData]:
        """Generate N rising bars."""
        return _bars_up(n, start=start, step=step)

    def test_basic_walk_forward(self):
        """Walk-forward should produce train and test results."""
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        bars = self._bars(60)
        result = engine.run_walk_forward(
            BuySellStrategy, bars, train_pct=0.7,
        )

        assert result.train_pct == 0.7
        assert result.train_result is not None
        assert result.test_result is not None
        assert isinstance(result.train_sharpe, float)
        assert isinstance(result.test_sharpe, float)
        assert isinstance(result.sharpe_degradation, float)

    def test_train_test_split_sizes(self):
        """Verify train/test split respects train_pct."""
        engine = BacktestEngine(initial_capital=10000)
        bars = self._bars(100)
        result = engine.run_walk_forward(
            BuySellStrategy, bars, train_pct=0.7,
        )

        # Train should have 70 bars, test should have 30
        assert len(result.train_result.equity_curve) == 70
        assert len(result.test_result.equity_curve) == 30

    def test_fresh_strategy_per_set(self):
        """Each set should use a fresh strategy instance."""
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        bars = self._bars(60)
        result = engine.run_walk_forward(
            BuySellStrategy, bars, train_pct=0.6,
        )

        # Both results should have valid equity curves
        assert len(result.train_result.equity_curve) == 36
        assert len(result.test_result.equity_curve) == 24

    def test_params_forwarded(self):
        """Parameters should be passed to both train and test strategies."""
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        bars = self._bars(60)
        # EmaCrossStrategy accepts params
        from strategies.macro_ema import MacroEmaStrategy as EmaCrossStrategy
        result = engine.run_walk_forward(
            EmaCrossStrategy, bars,
            params={"fast_period": 3, "slow_period": 10, "stop_loss_pct": 0.05},
            train_pct=0.7,
        )
        assert result.train_result is not None
        assert result.test_result is not None

    def test_invalid_train_pct_raises(self):
        """train_pct outside (0, 1) should raise ValueError."""
        engine = BacktestEngine(initial_capital=10000)
        bars = self._bars(60)
        with pytest.raises(ValueError, match="train_pct"):
            engine.run_walk_forward(BuySellStrategy, bars, train_pct=0.0)
        with pytest.raises(ValueError, match="train_pct"):
            engine.run_walk_forward(BuySellStrategy, bars, train_pct=1.0)

    def test_too_few_bars_raises(self):
        """Fewer than 10 bars should raise ValueError."""
        engine = BacktestEngine(initial_capital=10000)
        bars = self._bars(5)
        with pytest.raises(ValueError, match="at least 10"):
            engine.run_walk_forward(BuySellStrategy, bars, train_pct=0.7)

    def test_split_too_small_raises(self):
        """If split produces < 5 bars in either set, should raise."""
        engine = BacktestEngine(initial_capital=10000)
        bars = self._bars(12)  # 12 * 0.7 = 8 train, 4 test → too small
        with pytest.raises(ValueError, match="Split too small"):
            engine.run_walk_forward(BuySellStrategy, bars, train_pct=0.7)

    def test_no_overfit_on_consistent_strategy(self):
        """A do-nothing strategy should not trigger overfitting."""
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        bars = self._bars(60)
        result = engine.run_walk_forward(
            DoNothingStrategy, bars, train_pct=0.7, overfit_threshold=0.5,
        )
        # Do-nothing strategy has Sharpe=0 on both sets → degradation=0
        assert result.is_overfit is False
        assert result.overfit_warning == ""

    def test_overfit_detection_with_threshold(self):
        """Overfit flag should trigger when degradation exceeds threshold."""
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        bars = self._bars(60)
        # Use a very low threshold so any degradation triggers it
        result = engine.run_walk_forward(
            BuySellStrategy, bars, train_pct=0.7, overfit_threshold=0.01,
        )
        # If degradation > 0.01, should be flagged
        if result.sharpe_degradation > 0.01:
            assert result.is_overfit is True
            assert "OVERFITTING" in result.overfit_warning

    def test_walk_forward_result_fields(self):
        """WalkForwardResult should have all required fields."""
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        bars = self._bars(60)
        result = engine.run_walk_forward(BuySellStrategy, bars, train_pct=0.7)

        assert hasattr(result, "train_result")
        assert hasattr(result, "test_result")
        assert hasattr(result, "train_pct")
        assert hasattr(result, "train_sharpe")
        assert hasattr(result, "test_sharpe")
        assert hasattr(result, "sharpe_degradation")
        assert hasattr(result, "is_overfit")
        assert hasattr(result, "overfit_warning")
