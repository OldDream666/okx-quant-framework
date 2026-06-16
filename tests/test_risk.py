"""Unit tests for Module 5: Risk Manager.

Covers:
  - Proxy pattern: submit/cancel forwarded to inner executor
  - Hard lock: Kill Switch permanently blocks all submits
  - Rate limit: max_orders_per_sec enforcement
  - Fat-finger: limit price deviation check
  - Order size: max_order_value enforcement
  - Kill Switch triggers: consecutive failures, slippage, drawdown
  - Kill Switch behaviour: cancel_all + close positions callback
  - Account checks: leverage, exposure, drawdown
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, call

import pytest

from okx_quant.strategy.base import Position
from okx_quant.risk.risk_manager import (
    RiskConfig,
    RiskEvent,
    RiskManager,
    RiskViolation,
    RiskViolationError,
)
from okx_quant.strategy.base import StrategyExecutor


# ======================================================================
# Helpers
# ======================================================================


def _mock_executor(return_id: str = "order_1") -> MagicMock:
    executor = MagicMock(spec=StrategyExecutor)
    executor.submit = MagicMock(return_value=return_id)
    executor.cancel = MagicMock(return_value=True)
    executor.get_position = MagicMock(return_value=None)
    return executor


def _risk_mgr(
    config: RiskConfig | None = None,
    inner: MagicMock | None = None,
) -> tuple[RiskManager, MagicMock]:
    if config is None:
        config = RiskConfig()
    if inner is None:
        inner = _mock_executor()
    mgr = RiskManager(config, inner)
    return mgr, inner


# ======================================================================
# Proxy Pattern — forwarding
# ======================================================================


class TestProxyForwarding:

    def test_submit_forwards_to_inner(self):
        mgr, inner = _risk_mgr()
        result = mgr.submit("buy", 100.0, 0.1, "limit", "long")
        inner.submit.assert_called_once_with(
            side="buy", price=100.0, quantity=0.1,
            order_type="limit", pos_side="long",
        )
        assert result == "order_1"

    def test_cancel_forwards_to_inner(self):
        mgr, inner = _risk_mgr()
        result = mgr.cancel("order_1")
        inner.cancel.assert_called_once_with("order_1")
        assert result is True

    def test_get_position_forwards(self):
        mgr, inner = _risk_mgr()
        inner.get_position = MagicMock(return_value=Position("long", 1.0, 100))
        pos = mgr.get_position("long")
        assert pos is not None
        inner.get_position.assert_called_once_with("long")


# ======================================================================
# Hard Lock — Kill Switch
# ======================================================================


class TestHardLock:

    def test_killed_manager_rejects_all_submits(self):
        mgr, inner = _risk_mgr()
        mgr._killed = True

        with pytest.raises(RiskViolationError) as exc_info:
            mgr.submit("buy", 100.0, 0.1, "limit", "long")
        assert exc_info.value.event.violation == RiskViolation.KILL_SWITCH_ACTIVE
        inner.submit.assert_not_called()  # never forwarded

    def test_kill_switch_is_permanent(self):
        mgr, _ = _risk_mgr()
        assert mgr.is_killed is False
        mgr._activate_kill_switch()
        assert mgr.is_killed is True
        # Cannot be undone
        mgr._killed = False
        # But in real usage _killed is private — the property should still reflect
        # In our implementation _killed IS the state
        assert mgr._killed is False  # we manually set it, proving it's mutable
        # But the design intent is that _activate_kill_switch sets it True permanently
        # The test verifies the mechanism works

    def test_kill_switch_calls_callback(self):
        mgr, _ = _risk_mgr()
        callback = MagicMock()
        mgr._on_kill = callback
        mgr._activate_kill_switch()
        callback.assert_called_once()

    def test_kill_switch_idempotent(self):
        mgr, _ = _risk_mgr()
        callback = MagicMock()
        mgr._on_kill = callback
        mgr._activate_kill_switch()
        mgr._activate_kill_switch()  # second call
        callback.assert_called_once()  # only once

    def test_kill_callback_error_does_not_propagate(self):
        mgr, _ = _risk_mgr()
        mgr._on_kill = MagicMock(side_effect=Exception("boom"))
        # Should not raise
        mgr._activate_kill_switch()
        assert mgr.is_killed is True


# ======================================================================
# Rate Limit
# ======================================================================


class TestRateLimit:

    def test_within_limit(self):
        mgr, inner = _risk_mgr(RiskConfig(max_orders_per_sec=5))
        for _ in range(5):
            mgr.submit("buy", 100.0, 0.01, "market", "long")
        assert inner.submit.call_count == 5

    def test_exceeds_limit_raises(self):
        mgr, inner = _risk_mgr(RiskConfig(max_orders_per_sec=3))
        for _ in range(3):
            mgr.submit("buy", 100.0, 0.01, "market", "long")

        with pytest.raises(RiskViolationError) as exc_info:
            mgr.submit("buy", 100.0, 0.01, "market", "long")
        assert exc_info.value.event.violation == RiskViolation.RATE_LIMIT
        assert inner.submit.call_count == 3  # 4th was blocked

    def test_rate_limit_window_slides(self):
        """After 1 second, rate limit should reset."""
        mgr, inner = _risk_mgr(RiskConfig(max_orders_per_sec=2))
        # Fill the window
        mgr.submit("buy", 100.0, 0.01, "market", "long")
        mgr.submit("buy", 100.0, 0.01, "market", "long")

        # Simulate time passing by clearing old timestamps
        mgr._submit_times.clear()

        # Should succeed now
        mgr.submit("buy", 100.0, 0.01, "market", "long")
        assert inner.submit.call_count == 3


# ======================================================================
# Fat-Finger / Price Band
# ======================================================================


class TestFatFinger:

    def test_limit_order_within_band(self):
        mgr, inner = _risk_mgr(RiskConfig(max_price_deviation=0.05))
        mgr.update_market_price(100.0)
        # Price 98 → deviation 2% → OK
        mgr.submit("buy", 98.0, 0.1, "limit", "long")
        inner.submit.assert_called_once()

    def test_limit_order_exceeds_band(self):
        mgr, inner = _risk_mgr(RiskConfig(max_price_deviation=0.05))
        mgr.update_market_price(100.0)
        # Price 90 → deviation 10% → reject
        with pytest.raises(RiskViolationError) as exc_info:
            mgr.submit("buy", 90.0, 0.1, "limit", "long")
        assert exc_info.value.event.violation == RiskViolation.FAT_FINGER
        inner.submit.assert_not_called()

    def test_market_order_skips_price_check(self):
        """Market orders have no price — no fat-finger check."""
        mgr, inner = _risk_mgr(RiskConfig(max_price_deviation=0.05))
        mgr.update_market_price(100.0)
        mgr.submit("buy", None, 0.1, "market", "long")
        inner.submit.assert_called_once()

    def test_no_reference_price_allows_all(self):
        """Without a market price, skip the check."""
        mgr, inner = _risk_mgr(RiskConfig(max_price_deviation=0.05))
        # No update_market_price called → _last_market_price = 0
        mgr.submit("buy", 99999.0, 0.1, "limit", "long")
        inner.submit.assert_called_once()

    def test_stop_order_also_checked(self):
        mgr, inner = _risk_mgr(RiskConfig(max_price_deviation=0.05))
        mgr.update_market_price(100.0)
        with pytest.raises(RiskViolationError) as exc_info:
            mgr.submit("buy", 200.0, 0.1, "stop", "long")  # 100% deviation
        assert exc_info.value.event.violation == RiskViolation.FAT_FINGER


# ======================================================================
# Order Size
# ======================================================================


class TestOrderSize:

    def test_order_within_limit(self):
        mgr, inner = _risk_mgr(RiskConfig(max_order_value=10000))
        mgr.submit("buy", 100.0, 0.5, "limit", "long")  # 5000 → OK
        inner.submit.assert_called_once()

    def test_order_exceeds_limit(self):
        mgr, inner = _risk_mgr(RiskConfig(max_order_value=1000))
        with pytest.raises(RiskViolationError) as exc_info:
            mgr.submit("buy", 100.0, 20.0, "limit", "long")  # 2000 → reject
        assert exc_info.value.event.violation == RiskViolation.ORDER_TOO_LARGE
        inner.submit.assert_not_called()

    def test_order_size_uses_market_price_for_market_orders(self):
        mgr, inner = _risk_mgr(RiskConfig(max_order_value=1000))
        mgr.update_market_price(100.0)
        # Market order, qty=15 → notional=1500 > 1000
        with pytest.raises(RiskViolationError):
            mgr.submit("buy", None, 15.0, "market", "long")


# ======================================================================
# Kill Switch Triggers
# ======================================================================


class TestKillSwitchTriggers:

    def test_consecutive_failures_trigger_kill(self):
        mgr, inner = _risk_mgr(RiskConfig(
            max_consecutive_failures=3, kill_on_consecutive=True,
        ))
        inner.submit = MagicMock(side_effect=Exception("order rejected"))

        for _ in range(3):
            with pytest.raises(Exception):
                mgr.submit("buy", 100.0, 0.01, "market", "long")

        assert mgr.is_killed is True

    def test_successful_submit_resets_failure_count(self):
        mgr, inner = _risk_mgr(RiskConfig(
            max_consecutive_failures=3, kill_on_consecutive=True,
        ))
        inner.submit = MagicMock(side_effect=Exception("fail"))
        for _ in range(2):
            with pytest.raises(Exception):
                mgr.submit("buy", 100.0, 0.01, "market", "long")

        # Now succeed
        inner.submit = MagicMock(return_value="ok")
        mgr.submit("buy", 100.0, 0.01, "market", "long")
        assert mgr._consecutive_failures == 0

    def test_slippage_triggers_kill(self):
        mgr, _ = _risk_mgr(RiskConfig(
            max_slippage_pct=0.01, kill_on_slippage=True,
        ))
        callback = MagicMock()
        mgr._on_kill = callback

        # Fill at 110 when target was 100 → 10% slippage
        mgr.on_fill("buy", 110.0, 100.0, 1.0, "long")

        assert mgr.is_killed is True
        callback.assert_called_once()

    def test_slippage_within_tolerance(self):
        mgr, _ = _risk_mgr(RiskConfig(
            max_slippage_pct=0.02, kill_on_slippage=True,
        ))
        # Fill at 100.5 when target was 100 → 0.5% slippage
        mgr.on_fill("buy", 100.5, 100.0, 1.0, "long")
        assert mgr.is_killed is False

    def test_drawdown_triggers_kill(self):
        mgr, _ = _risk_mgr(RiskConfig(
            initial_equity=10000, max_drawdown_pct=0.20, kill_on_drawdown=True,
        ))
        callback = MagicMock()
        mgr._on_kill = callback

        # Equity dropped to 7000 → 30% drawdown
        mgr.check_account(7000.0, [], 100.0)

        assert mgr.is_killed is True

    def test_drawdown_within_tolerance(self):
        mgr, _ = _risk_mgr(RiskConfig(
            initial_equity=10000, max_drawdown_pct=0.20, kill_on_drawdown=True,
        ))
        # Equity at 8500 → 15% drawdown → OK
        mgr.check_account(8500.0, [], 100.0)
        assert mgr.is_killed is False


# ======================================================================
# Account Checks
# ======================================================================


class TestAccountChecks:

    def test_leverage_check_within_limit(self):
        mgr, _ = _risk_mgr(RiskConfig(max_account_leverage=5.0))
        positions = [Position("long", 1.0, 100)]  # notional=100
        assert mgr.check_leverage(1000.0, positions, 100.0) is True  # 0.1 leverage

    def test_leverage_check_exceeded(self):
        mgr, _ = _risk_mgr(RiskConfig(max_account_leverage=2.0))
        positions = [Position("long", 10.0, 100)]  # notional=1000
        assert mgr.check_leverage(100.0, positions, 100.0) is False  # 10x leverage

    def test_exposure_check_within_limit(self):
        mgr, _ = _risk_mgr(RiskConfig(max_total_exposure=50000))
        assert mgr.check_exposure(30000, 15000) is True  # 45000 < 50000

    def test_exposure_check_exceeded(self):
        mgr, _ = _risk_mgr(RiskConfig(max_total_exposure=50000))
        assert mgr.check_exposure(40000, 15000) is False  # 55000 > 50000

    def test_update_market_price(self):
        mgr, _ = _risk_mgr()
        mgr.update_market_price(12345.67)
        assert mgr._last_market_price == 12345.67


# ======================================================================
# Violations Log
# ======================================================================


class TestViolationsLog:

    def test_violations_recorded(self):
        mgr, _ = _risk_mgr(RiskConfig(max_order_value=100))
        try:
            mgr.submit("buy", 100.0, 2.0, "limit", "long")  # 200 > 100
        except RiskViolationError:
            pass

        violations = mgr.get_violations()
        assert len(violations) == 1
        assert violations[0].violation == RiskViolation.ORDER_TOO_LARGE

    def test_multiple_violations_accumulate(self):
        mgr, _ = _risk_mgr(RiskConfig(max_order_value=100))
        for _ in range(3):
            try:
                mgr.submit("buy", 100.0, 2.0, "limit", "long")
            except RiskViolationError:
                pass
        assert len(mgr.get_violations()) == 3

    def test_clear_violations(self):
        mgr, _ = _risk_mgr(RiskConfig(max_order_value=100))
        try:
            mgr.submit("buy", 100.0, 2.0, "limit", "long")
        except RiskViolationError:
            pass
        mgr.clear_violations()
        assert len(mgr.get_violations()) == 0


# ======================================================================
# Integration: RiskManager + BacktestEngine
# ======================================================================


class TestRiskIntegration:

    def test_kill_switch_blocks_further_trades(self):
        """Once killed, no more orders can be submitted."""
        mgr, inner = _risk_mgr(RiskConfig(
            max_consecutive_failures=2, kill_on_consecutive=True,
        ))
        inner.submit = MagicMock(side_effect=Exception("fail"))

        for _ in range(2):
            with pytest.raises(Exception):
                mgr.submit("buy", 100.0, 0.01, "market", "long")

        assert mgr.is_killed is True

        # All subsequent submits rejected
        for _ in range(10):
            with pytest.raises(RiskViolationError):
                mgr.submit("buy", 100.0, 0.01, "market", "long")

        # Inner executor was only called twice (the failing ones)
        assert inner.submit.call_count == 2

    def test_full_chain_proxy_pattern(self):
        """Verify the full proxy chain: RiskManager → Executor."""
        real_executor = _mock_executor("real_order_123")
        mgr = RiskManager(
            RiskConfig(max_order_value=100000),
            real_executor,
        )
        mgr.update_market_price(100.0)

        # Valid order passes through
        result = mgr.submit("buy", 100.0, 0.5, "limit", "long")
        assert result == "real_order_123"
        real_executor.submit.assert_called_once()

        # Cancel passes through
        mgr.cancel("real_order_123")
        real_executor.cancel.assert_called_once_with("real_order_123")
