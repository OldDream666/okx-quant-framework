"""Unit tests for TradeLedger."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okx_quant.monitoring.ledger import TradeLedger


class TestTradeLedger:

    @pytest.fixture
    def ledger(self, tmp_path: Path) -> TradeLedger:
        return TradeLedger(data_dir=str(tmp_path / "data"), symbol="BTC-USDT")

    def test_append_trade_creates_file(self, ledger: TradeLedger):
        ledger.append_trade({"side": "long", "pnl": 100.0})
        assert ledger._trades_path.exists()

    def test_append_trade_format(self, ledger: TradeLedger):
        ledger.append_trade({"side": "long", "pnl": 100.0, "fee": 0.5})
        lines = ledger._trades_path.read_text().strip().split("\n")
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["symbol"] == "BTC-USDT"
        assert record["side"] == "long"
        assert record["pnl"] == 100.0
        assert "ts" in record

    def test_append_multiple_trades(self, ledger: TradeLedger):
        ledger.append_trade({"side": "long", "pnl": 100})
        ledger.append_trade({"side": "short", "pnl": -50})
        ledger.append_trade({"side": "long", "pnl": 200})
        assert ledger.trade_count == 3

    def test_append_equity(self, ledger: TradeLedger):
        ledger.append_equity(equity=10000, cash=9500, position_value=500, drawdown=0.02)
        assert ledger.equity_count == 1
        records = ledger.query_equity()
        assert records[0]["equity"] == 10000
        assert records[0]["cash"] == 9500

    def test_append_equity_with_timestamp(self, ledger: TradeLedger):
        ledger.append_equity(equity=10000, ts=1718448000000)
        records = ledger.query_equity()
        assert "2024" in records[0]["ts"] or "2025" in records[0]["ts"] or "2026" in records[0]["ts"]

    def test_query_trades_filter_symbol(self, ledger: TradeLedger):
        ledger.append_trade({"side": "long", "pnl": 100})
        ledger._symbol = "ETH-USDT"
        ledger.append_trade({"side": "short", "pnl": -50})
        all_trades = ledger.query_trades()
        assert len(all_trades) == 2
        btc_only = ledger.query_trades(symbol="BTC-USDT")
        assert len(btc_only) == 1

    def test_query_trades_filter_side(self, ledger: TradeLedger):
        ledger.append_trade({"side": "long", "pnl": 100})
        ledger.append_trade({"side": "short", "pnl": -50})
        longs = ledger.query_trades(side="long")
        assert len(longs) == 1
        assert longs[0]["pnl"] == 100

    def test_summary_empty(self, ledger: TradeLedger):
        s = ledger.summary()
        assert s["total_trades"] == 0
        assert s["win_rate"] == 0.0

    def test_summary_with_trades(self, ledger: TradeLedger):
        ledger.append_trade({"pnl": 100, "fee": 1})
        ledger.append_trade({"pnl": -30, "fee": 0.5})
        ledger.append_trade({"pnl": 200, "fee": 2})
        s = ledger.summary()
        assert s["total_trades"] == 3
        assert s["wins"] == 2
        assert s["losses"] == 1
        assert s["win_rate"] == pytest.approx(2 / 3)
        assert s["total_pnl"] == 270
        assert s["total_fees"] == 3.5

    def test_flush_trades(self, ledger: TradeLedger):
        trades = [
            {"side": "long", "pnl": 100},
            {"side": "short", "pnl": -50},
        ]
        ledger.flush_trades(trades)
        assert ledger.trade_count == 2

    def test_flush_equity(self, ledger: TradeLedger):
        snapshots = [
            {"ts": "2026-01-01T00:00:00", "equity": 10000},
            {"ts": "2026-01-01T01:00:00", "equity": 10100},
        ]
        ledger.flush_equity(snapshots)
        assert ledger.equity_count == 2

    def test_export_daily_summary(self, ledger: TradeLedger):
        ledger.append_trade({"pnl": 100, "fee": 1})
        ledger.append_trade({"pnl": -30, "fee": 0.5})
        ledger.append_equity(equity=10100, drawdown=0.01)
        ledger.append_equity(equity=10050, drawdown=0.02)
        out = ledger.export_daily_summary()
        assert out.exists()
        content = out.read_text()
        assert "date" in content
        assert "pnl" in content
        assert "max_drawdown" in content
        assert "equity_close" in content
        # Should have header + 1 data row
        lines = content.strip().split("\n")
        assert len(lines) == 2

    def test_clear(self, ledger: TradeLedger):
        ledger.append_trade({"pnl": 100})
        ledger.append_equity(equity=10000)
        assert ledger.trade_count == 1
        ledger.clear()
        assert ledger.trade_count == 0
        assert ledger.equity_count == 0

    def test_empty_query(self, ledger: TradeLedger):
        assert ledger.query_trades() == []
        assert ledger.query_equity() == []

    def test_auto_ts_and_symbol(self, ledger: TradeLedger):
        ledger.append_trade({"side": "long", "pnl": 100})
        record = ledger.query_trades()[0]
        assert "ts" in record
        assert record["symbol"] == "BTC-USDT"


class TestTradeLedgerIntegration:

    def test_backtest_with_ledger(self, tmp_path: Path):
        """BacktestEngine should write trades + equity to ledger."""
        from okx_quant.backtest import BacktestEngine, ExchangeConfig
        from okx_quant.strategy.base import BaseStrategy, Signal
        from okx_quant.models.market import BarData

        # Minimal strategy
        class BuyOnce(BaseStrategy):
            name = "buy_once"
            def on_bar(self, bar: BarData) -> Signal | None:
                if self.state.bar_index == 0:
                    self.buy(self.state.cash * 0.5 / bar.close)
                return None

        # Generate simple bars
        bars = []
        for i in range(30):
            p = 100 + i * 0.5
            bars.append(BarData("BTC-USDT", p, p + 0.5, p - 0.5, p, 100,
                                1000000 + i * 60000, True))

        ledger = TradeLedger(data_dir=str(tmp_path / "data"), symbol="BTC-USDT")
        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(
            slippage_base=0.0, taker_fee_rate=0.0, latency_bars=1,
        ))
        strategy = BuyOnce()
        strategy.on_init({})

        result = engine.run(strategy, bars, ledger=ledger)

        # Ledger should have equity snapshots
        assert ledger.equity_count == len(bars)

        # If there were trades, ledger should have them
        if result.total_trades > 0:
            assert ledger.trade_count == result.total_trades
