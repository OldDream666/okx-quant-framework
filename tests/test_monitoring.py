"""Unit tests for Module 6: Logging & Monitoring.

Covers:
  - setup_logger: configuration, InterceptHandler, stdlib bridging
  - MetricsCollector: incremental counters, O(1) queries, reset
  - HeartbeatMonitor: tick/check, stale detection, recovery
  - Alerter: threshold checks, dedup, async webhook delivery
  - TradeMetrics: win_rate, drawdown, Sharpe, profit_factor
"""

from __future__ import annotations

import asyncio
import logging
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from okx_quant.monitoring.logger import InterceptHandler, get_logger, setup_logger
from okx_quant.monitoring.metrics import (
    AlertConfig,
    AlertLevel,
    Alerter,
    HeartbeatMonitor,
    MetricsCollector,
    TradeMetrics,
    TradeRecord,
)


# ======================================================================
# setup_logger
# ======================================================================


class TestSetupLogger:

    def test_setup_does_not_raise(self, tmp_path):
        """Basic configuration should complete without error."""
        setup_logger(log_dir=str(tmp_path / "logs"), level="DEBUG", console=False)
        # Verify log file was created (may be empty until first log)
        assert (tmp_path / "logs").exists()

    def test_setup_with_console(self, tmp_path):
        setup_logger(log_dir=str(tmp_path / "logs"), level="INFO", console=True)
        # Should not raise

    def test_intercept_handler_captures_stdlib(self, tmp_path):
        """stdlib logging should be routed through loguru after setup."""
        setup_logger(log_dir=str(tmp_path / "logs"), level="DEBUG", console=False)
        stdlib_logger = logging.getLogger("test.module")
        stdlib_logger.info("Hello from stdlib")
        # If InterceptHandler works, this doesn't crash and the message is logged

    def test_get_logger_returns_bound_logger(self):
        log = get_logger(strategy="test", symbol="BTC-USDT")
        # Should be a loguru logger with bound context
        assert log is not None

    def test_setup_idempotent(self, tmp_path):
        """Calling setup_logger twice should not crash."""
        setup_logger(log_dir=str(tmp_path / "logs"), level="INFO", console=False)
        setup_logger(log_dir=str(tmp_path / "logs"), level="DEBUG", console=False)


# ======================================================================
# MetricsCollector — incremental O(1)
# ======================================================================


class TestMetricsCollector:

    def test_initial_state(self):
        mc = MetricsCollector(initial_equity=10000)
        m = mc.current_metrics()
        assert m.total_trades == 0
        assert m.win_rate == 0.0
        assert m.total_pnl == 0.0
        assert m.max_drawdown == 0.0
        assert m.current_equity == 10000
        assert m.peak_equity == 10000

    def test_record_winning_trade(self):
        mc = MetricsCollector(initial_equity=10000)
        mc.record_trade(TradeRecord(pnl=500.0, bars_held=10))
        m = mc.current_metrics()
        assert m.total_trades == 1
        assert m.winning_trades == 1
        assert m.losing_trades == 0
        assert m.win_rate == 1.0
        assert m.total_pnl == 500.0
        assert m.gross_profit == 500.0
        assert m.gross_loss == 0.0
        assert m.profit_factor == float("inf")
        assert m.consecutive_losses == 0

    def test_record_losing_trade(self):
        mc = MetricsCollector(initial_equity=10000)
        mc.record_trade(TradeRecord(pnl=-200.0, bars_held=3))
        m = mc.current_metrics()
        assert m.total_trades == 1
        assert m.winning_trades == 0
        assert m.losing_trades == 1
        assert m.win_rate == 0.0
        assert m.total_pnl == -200.0
        assert m.gross_loss == 200.0
        assert m.consecutive_losses == 1

    def test_win_rate_mixed(self):
        mc = MetricsCollector(initial_equity=10000)
        mc.record_trade(TradeRecord(pnl=100, bars_held=5))
        mc.record_trade(TradeRecord(pnl=-50, bars_held=3))
        mc.record_trade(TradeRecord(pnl=200, bars_held=8))
        mc.record_trade(TradeRecord(pnl=-30, bars_held=2))
        m = mc.current_metrics()
        assert m.total_trades == 4
        assert m.winning_trades == 2
        assert m.win_rate == 0.5
        assert m.total_pnl == 220.0

    def test_consecutive_losses_counter(self):
        mc = MetricsCollector(initial_equity=10000)
        mc.record_trade(TradeRecord(pnl=-10, bars_held=1))
        mc.record_trade(TradeRecord(pnl=-20, bars_held=1))
        mc.record_trade(TradeRecord(pnl=-30, bars_held=1))
        assert mc.current_metrics().consecutive_losses == 3

        # Winning trade resets counter
        mc.record_trade(TradeRecord(pnl=100, bars_held=5))
        assert mc.current_metrics().consecutive_losses == 0

    def test_incremental_drawdown(self):
        """Drawdown should track peak → trough incrementally."""
        mc = MetricsCollector(initial_equity=10000)
        mc.record_equity(10000)
        mc.record_equity(11000)   # new peak
        mc.record_equity(9500)    # drawdown from 11000
        m = mc.current_metrics()
        # DD = (11000 - 9500) / 11000 ≈ 13.6%
        assert abs(m.max_drawdown - 1500 / 11000) < 0.001
        assert m.peak_equity == 11000
        assert m.current_equity == 9500

    def test_drawdown_never_decreases_peak(self):
        mc = MetricsCollector(initial_equity=10000)
        mc.record_equity(12000)
        mc.record_equity(11000)   # lower than peak
        assert mc.current_metrics().peak_equity == 12000

    def test_avg_bars_held(self):
        mc = MetricsCollector(initial_equity=10000)
        mc.record_trade(TradeRecord(pnl=100, bars_held=5))
        mc.record_trade(TradeRecord(pnl=-50, bars_held=10))
        assert mc.current_metrics().avg_bars_held == 7.5

    def test_profit_factor(self):
        mc = MetricsCollector(initial_equity=10000)
        mc.record_trade(TradeRecord(pnl=300, bars_held=5))
        mc.record_trade(TradeRecord(pnl=200, bars_held=3))
        mc.record_trade(TradeRecord(pnl=-100, bars_held=2))
        m = mc.current_metrics()
        # PF = 500 / 100 = 5.0
        assert abs(m.profit_factor - 5.0) < 0.01

    def test_reset(self):
        mc = MetricsCollector(initial_equity=10000)
        mc.record_trade(TradeRecord(pnl=100, bars_held=5))
        mc.record_equity(10100)
        mc.reset()
        m = mc.current_metrics()
        assert m.total_trades == 0
        assert m.current_equity == 10000

    def test_returns_capped(self):
        """Returns list should not grow unbounded."""
        mc = MetricsCollector(initial_equity=10000)
        mc._max_returns = 10
        for i in range(100):
            mc.record_equity(10000 + i * 10)
        assert len(mc._returns) <= 10

    def test_sharpe_positive_for_uptrend(self):
        mc = MetricsCollector(initial_equity=10000)
        for i in range(30):
            mc.record_equity(10000 + i * 100)
        m = mc.current_metrics()
        assert m.sharpe_ratio > 0


# ======================================================================
# HeartbeatMonitor
# ======================================================================


class TestHeartbeatMonitor:

    def test_fresh_heartbeat(self):
        hb = HeartbeatMonitor(timeout=5.0)
        hb.tick()
        assert hb.check() is True
        assert hb.is_stale is False

    def test_stale_heartbeat(self):
        hb = HeartbeatMonitor(timeout=0.1)
        hb.tick()
        time.sleep(0.15)
        assert hb.check() is False
        assert hb.is_stale is True

    def test_stale_calls_callback(self):
        callback = MagicMock()
        hb = HeartbeatMonitor(timeout=0.1, on_stale=callback)
        hb.tick()
        time.sleep(0.15)
        hb.check()
        callback.assert_called_once()

    def test_recovery_after_stale(self):
        hb = HeartbeatMonitor(timeout=0.1)
        hb.tick()
        time.sleep(0.15)
        hb.check()  # stale
        assert hb.is_stale is True

        hb.tick()  # recovery
        assert hb.is_stale is False
        assert hb._stale_count == 0

    def test_seconds_since_last_tick(self):
        hb = HeartbeatMonitor(timeout=10.0)
        hb.tick()
        time.sleep(0.05)
        elapsed = hb.seconds_since_last_tick
        assert elapsed >= 0.04
        assert elapsed < 1.0

    def test_reset(self):
        hb = HeartbeatMonitor(timeout=0.1)
        hb.tick()
        time.sleep(0.15)
        hb.check()  # stale
        hb.reset()
        assert hb.is_stale is False
        assert hb.check() is True  # fresh after reset


# ======================================================================
# Alerter — threshold checks
# ======================================================================


class TestAlerterThresholds:

    def test_drawdown_alert(self):
        callback = MagicMock()
        config = AlertConfig(drawdown_threshold=0.10, on_alert=callback)
        alerter = Alerter(config)

        # 20% drawdown exceeds 10% threshold
        alerter.check(equity=8000, initial_equity=10000, metrics=TradeMetrics())

        callback.assert_called_once()
        level, msg = callback.call_args[0]
        assert level == AlertLevel.WARNING
        assert "20" in msg  # 20% drawdown

    def test_drawdown_within_threshold(self):
        callback = MagicMock()
        config = AlertConfig(drawdown_threshold=0.10, on_alert=callback)
        alerter = Alerter(config)

        # 5% drawdown — below 10% threshold
        alerter.check(equity=9500, initial_equity=10000, metrics=TradeMetrics())

        callback.assert_not_called()

    def test_consecutive_losses_alert(self):
        callback = MagicMock()
        config = AlertConfig(consecutive_losses=3, on_alert=callback)
        alerter = Alerter(config)

        metrics = TradeMetrics(consecutive_losses=5)
        alerter.check(equity=10000, initial_equity=10000, metrics=metrics)

        callback.assert_called_once()
        level, msg = callback.call_args[0]
        assert "5 consecutive" in msg

    def test_heartbeat_stale_alert(self):
        callback = MagicMock()
        config = AlertConfig(on_alert=callback)
        alerter = Alerter(config)

        alerter.check_heartbeat(is_stale=True, seconds=45.0)

        callback.assert_called_once()
        level, msg = callback.call_args[0]
        assert level == AlertLevel.CRITICAL
        assert "45" in msg

    def test_heartbeat_fresh_no_alert(self):
        callback = MagicMock()
        config = AlertConfig(on_alert=callback)
        alerter = Alerter(config)

        alerter.check_heartbeat(is_stale=False, seconds=5.0)

        callback.assert_not_called()


# ======================================================================
# Alerter — dedup
# ======================================================================


class TestAlerterDedup:

    def test_duplicate_suppressed(self):
        callback = MagicMock()
        config = AlertConfig(on_alert=callback)
        alerter = Alerter(config)

        alerter.alert(AlertLevel.WARNING, "same message")
        alerter.alert(AlertLevel.WARNING, "same message")  # suppressed

        assert callback.call_count == 1

    def test_different_messages_not_suppressed(self):
        callback = MagicMock()
        config = AlertConfig(on_alert=callback)
        alerter = Alerter(config)

        alerter.alert(AlertLevel.WARNING, "message A")
        alerter.alert(AlertLevel.WARNING, "message B")

        assert callback.call_count == 2

    def test_same_message_after_window_not_suppressed(self):
        callback = MagicMock()
        config = AlertConfig(on_alert=callback)
        alerter = Alerter(config)
        alerter._dedup_window = 0.0  # no dedup window

        alerter.alert(AlertLevel.WARNING, "msg")
        alerter.alert(AlertLevel.WARNING, "msg")

        assert callback.call_count == 2


# ======================================================================
# Alerter — async webhook
# ======================================================================


class TestAlerterAsync:

    @pytest.mark.asyncio
    async def test_start_stop(self):
        config = AlertConfig()
        alerter = Alerter(config)
        await alerter.start()
        assert alerter._running is True
        await alerter.stop()
        assert alerter._running is False

    @pytest.mark.asyncio
    async def test_alert_enqueued(self):
        config = AlertConfig()
        alerter = Alerter(config)
        alerter.alert(AlertLevel.WARNING, "test alert")
        assert not alerter._queue.empty()
        level, msg = await alerter._queue.get()
        assert level == AlertLevel.WARNING
        assert msg == "test alert"

    @pytest.mark.asyncio
    async def test_webhook_delivery_mocked(self):
        """Verify webhook POST is attempted when URL is configured."""
        config = AlertConfig(feishu_webhook="https://fake.feishu.com/webhook")
        alerter = Alerter(config)

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_instance

            await alerter.start()
            alerter.alert(AlertLevel.WARNING, "webhook test")

            # Let delivery loop process
            await asyncio.sleep(0.5)
            await alerter.stop()

            # Verify POST was called
            mock_instance.post.assert_called_once()
            call_args = mock_instance.post.call_args
            assert "feishu" in call_args[0][0] or call_args[0][0] == "https://fake.feishu.com/webhook"

    @pytest.mark.asyncio
    async def test_callback_error_does_not_block(self):
        """Alert callback raising should not prevent queueing."""
        config = AlertConfig(on_alert=MagicMock(side_effect=Exception("boom")))
        alerter = Alerter(config)
        # Should not raise
        alerter.alert(AlertLevel.WARNING, "test")
        assert not alerter._queue.empty()


# ======================================================================
# Integration: MetricsCollector + Alerter
# ======================================================================


class TestMonitoringIntegration:

    def test_full_metrics_lifecycle(self):
        """Simulate a full trading session with metrics tracking."""
        mc = MetricsCollector(initial_equity=10000)

        # Winning trades
        mc.record_trade(TradeRecord(pnl=300, bars_held=5))
        mc.record_trade(TradeRecord(pnl=200, bars_held=3))
        mc.record_equity(10500)

        # Losing trades
        mc.record_trade(TradeRecord(pnl=-100, bars_held=2))
        mc.record_equity(10400)

        m = mc.current_metrics()
        assert m.total_trades == 3
        assert m.winning_trades == 2
        assert m.win_rate == pytest.approx(2 / 3)
        assert m.total_pnl == 400
        assert m.profit_factor == pytest.approx(500 / 100)

    def test_alerter_with_metrics_collector(self):
        """Alerter checks against MetricsCollector output."""
        callback = MagicMock()
        config = AlertConfig(
            drawdown_threshold=0.10,
            consecutive_losses=3,
            on_alert=callback,
        )
        alerter = Alerter(config)
        mc = MetricsCollector(initial_equity=10000)

        # Simulate drawdown
        mc.record_equity(8500)
        alerter.check(
            equity=8500,
            initial_equity=10000,
            metrics=mc.current_metrics(),
        )
        callback.assert_called_once()

    def test_heartbeat_with_alerter(self):
        """HeartbeatMonitor → Alerter integration."""
        callback = MagicMock()
        config = AlertConfig(on_alert=callback)
        alerter = Alerter(config)

        hb = HeartbeatMonitor(
            timeout=0.1,
            on_stale=lambda: alerter.check_heartbeat(
                is_stale=True,
                seconds=hb.seconds_since_last_tick,
            ),
        )
        hb.tick()
        time.sleep(0.15)
        hb.check()  # triggers stale → alerter

        callback.assert_called_once()
        level, msg = callback.call_args[0]
        assert level == AlertLevel.CRITICAL
