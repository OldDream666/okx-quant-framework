"""本地交易账本 — 追加写入 JSONL + CSV 汇总。

无外部依赖（仅使用 stdlib）。适用于回测和实盘。

存储结构::

    data/
    ├── trades.jsonl          ← 每行一个 JSON 对象（追加写入）
    ├── equity.jsonl          ← 每行一个权益快照（追加写入）
    └── daily_summary.csv     ← 每天一行（按需生成）

用法::

    ledger = TradeLedger(data_dir="data", symbol="ETH-USDT-SWAP")

    # 记录交易
    ledger.append_trade({
        "side": "long",
        "entry_price": 1792.5,
        "exit_price": 1810.0,
        "quantity": 0.45,
        "pnl": 7.88,
        "fee": 0.16,
        "bars_held": 12,
        "reason": "signal_open_long",
    })

    # 记录权益快照
    ledger.append_equity(equity=80691.5, cash=78691.5, position_value=2000.0)

    # 查询
    trades = ledger.query_trades(symbol="ETH-USDT-SWAP")
    summary = ledger.summary()
    ledger.export_daily_summary("data/daily_summary.csv")
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class TradeLedger:
    """仅追加写入的本地交易日志。

    Parameters:
        data_dir: JSONL/CSV 文件目录（自动创建）。
        symbol:   交易标的（用于交易记录）。
    """

    def __init__(self, data_dir: str = "data", symbol: str = "") -> None:
        self._data_dir = Path(data_dir)
        self._symbol = symbol
        self._trades_path = self._data_dir / "trades.jsonl"
        self._equity_path = self._data_dir / "equity.jsonl"
        self._data_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def append_trade(self, trade: dict[str, Any]) -> None:
        """向 trades.jsonl 追加一条交易记录。

        若缺少 ``ts``（ISO 时间戳）和 ``symbol``，会自动添加。
        """
        record = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "symbol": self._symbol,
            **trade,
        }
        self._append_jsonl(self._trades_path, record)

    def append_equity(
        self,
        equity: float,
        cash: float = 0.0,
        position_value: float = 0.0,
        drawdown: float = 0.0,
        ts: int | None = None,
    ) -> None:
        """向 equity.jsonl 追加一条权益快照。

        Parameters:
            equity:         总权益（现金 + 未实现盈亏）。
            cash:           可用现金。
            position_value: 持仓总价值。
            drawdown:       当前回撤比例（0.01 = 1%）。
            ts:             Unix 时间戳毫秒（可选，为 None 时使用当前时间）。
        """
        if ts:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            ts_str = dt.isoformat(timespec="seconds")
        else:
            ts_str = datetime.now(timezone.utc).isoformat(timespec="seconds")

        record = {
            "ts": ts_str,
            "equity": round(equity, 2),
            "cash": round(cash, 2),
            "position_value": round(position_value, 2),
            "drawdown": round(drawdown, 6),
        }
        self._append_jsonl(self._equity_path, record)

    def flush_trades(self, trades: list[dict[str, Any]]) -> None:
        """批量写入多条交易记录（用于回测）。"""
        with open(self._trades_path, "a", encoding="utf-8") as f:
            for trade in trades:
                record = {
                    "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "symbol": self._symbol,
                    **trade,
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def flush_equity(self, snapshots: list[dict[str, Any]]) -> None:
        """批量写入多条权益快照（用于回测）。"""
        with open(self._equity_path, "a", encoding="utf-8") as f:
            for snap in snapshots:
                f.write(json.dumps(snap, ensure_ascii=False) + "\n")

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def query_trades(
        self,
        symbol: str | None = None,
        start: str | None = None,
        end: str | None = None,
        side: str | None = None,
    ) -> list[dict[str, Any]]:
        """从 trades.jsonl 读取交易记录，支持可选筛选条件。

        Parameters:
            symbol: 按标的筛选（None = 全部）。
            start:  按时间戳 >= start 筛选（ISO 格式）。
            end:    按时间戳 <= end 筛选（ISO 格式）。
            side:   按方向筛选（"long" / "short"）。
        """
        records = self._read_jsonl(self._trades_path)
        return self._filter(records, symbol=symbol, start=start, end=end, side=side)

    def query_equity(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict[str, Any]]:
        """从 equity.jsonl 读取权益快照。"""
        records = self._read_jsonl(self._equity_path)
        return self._filter(records, start=start, end=end)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self) -> dict[str, Any]:
        """根据所有已记录交易计算汇总统计。"""
        trades = self.query_trades()
        if not trades:
            return {
                "total_trades": 0, "wins": 0, "losses": 0,
                "win_rate": 0.0, "total_pnl": 0.0, "total_fees": 0.0,
                "avg_pnl": 0.0, "max_win": 0.0, "max_loss": 0.0,
                "profit_factor": 0.0,
            }

        wins = [t for t in trades if t.get("pnl", 0) > 0]
        losses = [t for t in trades if t.get("pnl", 0) <= 0]
        total_pnl = sum(t.get("pnl", 0) for t in trades)
        total_fees = sum(t.get("fee", 0) for t in trades)
        gross_profit = sum(t["pnl"] for t in wins)
        gross_loss = abs(sum(t["pnl"] for t in losses))

        return {
            "total_trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / len(trades) if trades else 0,
            "total_pnl": round(total_pnl, 2),
            "total_fees": round(total_fees, 2),
            "avg_pnl": round(total_pnl / len(trades), 2) if trades else 0,
            "max_win": round(max((t["pnl"] for t in trades), default=0), 2),
            "max_loss": round(min((t["pnl"] for t in trades), default=0), 2),
            "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else float("inf"),
        }

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_daily_summary(self, path: str | Path | None = None) -> Path:
        """根据交易和权益数据生成 daily_summary.csv。

        合并交易盈亏与权益曲线，生成包含最大回撤和收盘权益的
        完整每日财务报告。

        Parameters:
            path: 输出路径（默认：``data_dir/daily_summary.csv``）。

        Returns:
            写入的文件路径。
        """
        out = Path(path) if path else self._data_dir / "daily_summary.csv"

        trades = self.query_trades()
        equities = self.query_equity()

        # Group data by date
        daily_data: dict[str, dict[str, Any]] = {}

        # 1. Aggregate trade P&L
        for t in trades:
            ts = t.get("ts", "")
            date = ts[:10] if len(ts) >= 10 else "unknown"
            if date == "unknown":
                continue
            d = daily_data.setdefault(date, {
                "trades": 0, "wins": 0, "losses": 0,
                "pnl": 0.0, "fees": 0.0,
                "max_drawdown": 0.0, "equity_close": 0.0,
            })
            d["trades"] += 1
            if t.get("pnl", 0) > 0:
                d["wins"] += 1
            else:
                d["losses"] += 1
            d["pnl"] += t.get("pnl", 0)
            d["fees"] += t.get("fee", 0)

        # 2. Aggregate equity curve (max drawdown + closing equity)
        for eq in equities:
            ts = eq.get("ts", "")
            date = ts[:10] if len(ts) >= 10 else "unknown"
            if date == "unknown":
                continue
            d = daily_data.setdefault(date, {
                "trades": 0, "wins": 0, "losses": 0,
                "pnl": 0.0, "fees": 0.0,
                "max_drawdown": 0.0, "equity_close": 0.0,
            })
            # Track worst drawdown of the day
            d["max_drawdown"] = max(d["max_drawdown"], eq.get("drawdown", 0.0))
            # Last equity snapshot of the day = closing equity
            d["equity_close"] = eq.get("equity", 0.0)

        # 3. Write CSV
        fieldnames = ["date", "trades", "wins", "losses", "pnl", "fees", "max_drawdown", "equity_close"]
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for date in sorted(daily_data.keys()):
                row = {"date": date, **daily_data[date]}
                row["pnl"] = round(row["pnl"], 2)
                row["fees"] = round(row["fees"], 4)
                row["max_drawdown"] = round(row["max_drawdown"], 4)
                row["equity_close"] = round(row["equity_close"], 2)
                writer.writerow(row)

        return out

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """删除所有账本文件（重新开始）。"""
        for p in (self._trades_path, self._equity_path):
            if p.exists():
                p.unlink()
        csv_path = self._data_dir / "daily_summary.csv"
        if csv_path.exists():
            csv_path.unlink()

    @property
    def trade_count(self) -> int:
        """已记录交易数量。"""
        return sum(1 for _ in self._read_jsonl(self._trades_path))

    @property
    def equity_count(self) -> int:
        """已记录权益快照数量。"""
        return sum(1 for _ in self._read_jsonl(self._equity_path))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
        """向文件追加一行 JSON。"""
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        """从文件读取所有 JSON 行。"""
        if not path.exists():
            return []
        records = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    @staticmethod
    def _filter(
        records: list[dict[str, Any]],
        symbol: str | None = None,
        start: str | None = None,
        end: str | None = None,
        side: str | None = None,
    ) -> list[dict[str, Any]]:
        """按可选条件筛选记录。"""
        result = []
        for r in records:
            if symbol and r.get("symbol") != symbol:
                continue
            if start and r.get("ts", "") < start:
                continue
            if end and r.get("ts", "") > end:
                continue
            if side and r.get("side") != side:
                continue
            result.append(r)
        return result
