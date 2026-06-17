"""
RSI 反转 + EMA 趋势滤网 + ATR 动态止损策略
============================================

适用场景：小资金（50U+）、短周期（5m/15m）、高杠杆（20x）

逻辑：
  多头入场：RSI < 超卖阈值 + 价格 > 宏观 EMA（趋势向上）
  空头入场：RSI > 超买阈值 + 价格 < 宏观 EMA（趋势向下）
  止损：入场价 ± ATR × 倍数（动态适应波动率）
  止盈：入场价 ∓ ATR × 倍数（风险回报比 1:2）

优势：
  - RSI 反转信号在震荡行情胜率高
  - EMA 趋势滤网避免逆势抄底
  - ATR 动态止损比固定百分比更适应市场波动
  - 快进快出，单根 K 线内完成信号判断
"""

from __future__ import annotations

import math
from okx_quant.strategy.base import BaseStrategy, Signal, Position
from okx_quant.models.market import BarData


class RsiReversalStrategy(BaseStrategy):
    """RSI 反转 + 趋势滤网 + ATR 动态止损。"""

    name = "rsi_reversal"

    def on_init(self, params: dict) -> None:
        super().on_init(params)
        # RSI 参数
        self._rsi_period = int(params.get("rsi_period", 14))
        self._rsi_oversold = float(params.get("rsi_oversold", 30))
        self._rsi_overbought = float(params.get("rsi_overbought", 70))

        # 趋势滤网
        self._trend_ema_period = int(params.get("trend_ema_period", 200))

        # ATR 止损/止盈
        self._atr_period = int(params.get("atr_period", 14))
        self._atr_sl_mult = float(params.get("atr_sl_mult", 1.5))  # 止损 = ATR × 1.5
        self._atr_tp_mult = float(params.get("atr_tp_mult", 3.0))  # 止盈 = ATR × 3.0

        # 仓位
        self._position_pct = float(params.get("position_pct", 0.05))

        # 静默模式
        self._silent = bool(params.get("silent", False))

    def _log(self, msg: str) -> None:
        if not self._silent:
            print(msg)

    def on_start(self) -> None:
        self._log(
            f"🚀 RSI反转策略启动: RSI({self._rsi_period}) "
            f"超卖={self._rsi_oversold} 超买={self._rsi_overbought} | "
            f"趋势EMA({self._trend_ema_period}) | "
            f"ATR({self._atr_period}) SL={self._atr_sl_mult}x TP={self._atr_tp_mult}x"
        )

    def on_bar(self, bar: BarData) -> Signal | None:
        # 需要足够的数据
        required = max(self._rsi_period, self._trend_ema_period, self._atr_period) + 1
        if len(self.bars) < required:
            if len(self.bars) % 50 == 0:
                self._log(f"   ⏳ 加载数据中... {len(self.bars)}/{required}")
            return None

        # 计算指标
        rsi = self._calc_rsi(self.bars, self._rsi_period)
        trend_ema = self._calc_ema(self.bars, self._trend_ema_period)
        atr = self._calc_atr(self.bars, self._atr_period)

        # 当前持仓
        long_pos = self.position_long.quantity if self.position_long else 0
        short_pos = self.position_short.quantity if self.position_short else 0

        # ─── 多头入场 ───
        if rsi < self._rsi_oversold and bar.close > trend_ema:
            if long_pos == 0:
                size = self.state.cash * self._position_pct / bar.close
                stop_price = bar.close - atr * self._atr_sl_mult
                tp_price = bar.close + atr * self._atr_tp_mult

                self.buy(size)
                self.sell(size, price=stop_price, order_type="stop")
                self.sell(size, price=tp_price, order_type="limit")

                self._log(
                    f"   🟢 RSI反转做多: {size:.6f} @ {bar.close:.2f} | "
                    f"RSI={rsi:.1f} EMA={trend_ema:.2f} ATR={atr:.2f} | "
                    f"SL={stop_price:.2f} TP={tp_price:.2f}"
                )

        # ─── 空头入场 ───
        elif rsi > self._rsi_overbought and bar.close < trend_ema:
            if short_pos == 0:
                size = self.state.cash * self._position_pct / bar.close
                stop_price = bar.close + atr * self._atr_sl_mult
                tp_price = bar.close - atr * self._atr_tp_mult

                self.sell(size)
                self.buy(size, price=stop_price, order_type="stop")
                self.buy(size, price=tp_price, order_type="limit")

                self._log(
                    f"   🔴 RSI反转做空: {size:.6f} @ {bar.close:.2f} | "
                    f"RSI={rsi:.1f} EMA={trend_ema:.2f} ATR={atr:.2f} | "
                    f"SL={stop_price:.2f} TP={tp_price:.2f}"
                )

        return None

    def on_stop(self) -> None:
        self._log("🛑 RSI反转策略已停止")

    # ─── 指标计算 ───

    @staticmethod
    def _calc_rsi(bars: list[BarData], period: int) -> float:
        """计算 RSI（相对强弱指数）。"""
        if len(bars) < period + 1:
            return 50.0

        gains = []
        losses = []
        for i in range(-period, 0):
            change = bars[i].close - bars[i - 1].close
            if change > 0:
                gains.append(change)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(change))

        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _calc_ema(bars: list[BarData], period: int) -> float:
        """计算 EMA。"""
        if len(bars) < period:
            return bars[-1].close
        multiplier = 2.0 / (period + 1)
        ema = bars[0].close
        for bar in bars[1:]:
            ema = (bar.close - ema) * multiplier + ema
        return ema

    @staticmethod
    def _calc_atr(bars: list[BarData], period: int) -> float:
        """计算 ATR（平均真实波幅）。"""
        if len(bars) < period + 1:
            return abs(bars[-1].high - bars[-1].low)

        trs = []
        for i in range(-period, 0):
            high = bars[i].high
            low = bars[i].low
            prev_close = bars[i - 1].close
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)

        return sum(trs) / len(trs)
