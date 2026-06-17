"""
自适应 EMA 策略：根据行情状态动态切换参数
==========================================

核心逻辑：
  趋势行情（ADX > 25）→ 快速 EMA + 窄止损，追趋势
  震荡行情（ADX < 20）→ 慢速 EMA + 宽止损，减少假信号
  极端波动（ATR > 2x 均值）→ 降仓位 + 最宽止损，保命

指标：
  ADX（平均趋向指数）→ 趋势强度，>25 趋势，<20 震荡
  ATR（平均真实波幅）→ 波动率，用于动态止损和极端检测
  EMA（指数移动平均）→ 交叉信号
"""

from __future__ import annotations

from okx_quant.strategy.base import BaseStrategy, Signal
from okx_quant.models.market import BarData


class AdaptiveEmaStrategy(BaseStrategy):
    """自适应 EMA 策略：趋势/震荡/极端 三档切换。"""

    name = "adaptive_ema"

    def on_init(self, params: dict) -> None:
        super().on_init(params)
        # 趋势行情参数
        self._trend_fast = int(params.get("trend_fast", 10))
        self._trend_slow = int(params.get("trend_slow", 50))
        self._trend_sl = float(params.get("trend_sl", 0.05))
        self._trend_position_pct = float(params.get("trend_position_pct", 0.05))

        # 震荡行情参数
        self._range_fast = int(params.get("range_fast", 20))
        self._range_slow = int(params.get("range_slow", 30))
        self._range_sl = float(params.get("range_sl", 0.08))
        self._range_position_pct = float(params.get("range_position_pct", 0.03))

        # 极端波动参数
        self._extreme_sl = float(params.get("extreme_sl", 0.12))
        self._extreme_position_pct = float(params.get("extreme_position_pct", 0.02))

        # 状态检测参数
        self._adx_period = int(params.get("adx_period", 14))
        self._adx_trend = float(params.get("adx_trend", 25))
        self._adx_range = float(params.get("adx_range", 20))
        self._atr_period = int(params.get("atr_period", 14))
        self._atr_extreme_mult = float(params.get("atr_extreme_mult", 2.0))

        # 宏观滤网
        self._macro_period = int(params.get("macro_period", 0))

        # 静默模式
        self._silent = bool(params.get("silent", False))

        # 状态跟踪
        self._current_regime = "unknown"
        self._adx_value = 0.0
        self._atr_value = 0.0
        self._atr_avg = 0.0

    def _log(self, msg: str) -> None:
        if not self._silent:
            print(msg)

    def on_start(self) -> None:
        self._log(
            f"🚀 自适应EMA启动 | "
            f"趋势: EMA({self._trend_fast}/{self._trend_slow}) SL={self._trend_sl:.0%} 仓位={self._trend_position_pct:.0%} | "
            f"震荡: EMA({self._range_fast}/{self._range_slow}) SL={self._range_sl:.0%} 仓位={self._range_position_pct:.0%} | "
            f"极端: SL={self._extreme_sl:.0%} 仓位={self._extreme_position_pct:.0%} | "
            f"ADX({self._adx_period}) 趋势>{self._adx_trend} 震荡<{self._adx_range}"
        )

    def on_bar(self, bar: BarData) -> Signal | None:
        required = max(self._range_slow, self._adx_period, self._atr_period, self._macro_period) + 10
        if len(self.bars) < required:
            if len(self.bars) % 50 == 0:
                self._log(f"   ⏳ 加载数据中... {len(self.bars)}/{required}")
            return None

        # ─── 计算指标 ───
        adx = self._calc_adx(self.bars, self._adx_period)
        atr = self._calc_atr(self.bars, self._atr_period)
        atr_avg = self._calc_atr(self.bars, 50)  # 50 周期 ATR 均值

        self._adx_value = adx
        self._atr_value = atr
        self._atr_avg = atr_avg

        # ─── 检测行情状态 ───
        prev_regime = self._current_regime

        if atr > atr_avg * self._atr_extreme_mult and atr_avg > 0:
            self._current_regime = "extreme"
            fast, slow, sl, pos_pct = (
                self._trend_fast, self._trend_slow,
                self._extreme_sl, self._extreme_position_pct,
            )
        elif adx > self._adx_trend:
            self._current_regime = "trend"
            fast, slow, sl, pos_pct = (
                self._trend_fast, self._trend_slow,
                self._trend_sl, self._trend_position_pct,
            )
        elif adx < self._adx_range:
            self._current_regime = "range"
            fast, slow, sl, pos_pct = (
                self._range_fast, self._range_slow,
                self._range_sl, self._range_position_pct,
            )
        else:
            # 中间地带，保持上一个状态
            if self._current_regime == "unknown":
                self._current_regime = "range"
            fast, slow, sl, pos_pct = (
                self._range_fast, self._range_slow,
                self._range_sl, self._range_position_pct,
            )

        # 状态切换日志
        if self._current_regime != prev_regime:
            self._log(
                f"   🔄 行情切换: {prev_regime} → {self._current_regime} | "
                f"ADX={adx:.1f} ATR={atr:.4f} ATR均值={atr_avg:.4f}"
            )

        # ─── 计算 EMA ───
        fast_ema = self._calc_ema(self.bars, fast)
        slow_ema = self._calc_ema(self.bars, slow)
        fast_prev = self._calc_ema(self.bars[:-1], fast)
        slow_prev = self._calc_ema(self.bars[:-1], slow)

        # 宏观滤网
        if self._macro_period > 0:
            macro_ema = self._calc_ema(self.bars, self._macro_period)
        else:
            macro_ema = None

        # ─── 当前持仓 ───
        long_pos = self.position_long.quantity if self.position_long else 0
        short_pos = self.position_short.quantity if self.position_short else 0

        # ─── 金叉做多 ───
        if fast_prev <= slow_prev and fast_ema > slow_ema:
            # 宏观滤网：价格必须在宏观 EMA 上方
            if macro_ema is not None and bar.close < macro_ema:
                self._log(f"   ⚠️ 金叉被宏观滤网拦截 ({self._current_regime})")
                return None

            if long_pos == 0:
                size = self.state.cash * pos_pct / bar.close
                stop_price = bar.close * (1 - sl)
                self.buy(size)
                self.sell(size, price=stop_price, order_type="stop")
                self._log(
                    f"   🟢 [{self._current_regime}] 开多: {size:.6f} @ {bar.close:.2f} | "
                    f"EMA({fast}/{slow}) SL={sl:.0%} 止损={stop_price:.2f}"
                )

        # ─── 死叉做空 ───
        elif fast_prev >= slow_prev and fast_ema < slow_ema:
            if macro_ema is not None and bar.close > macro_ema:
                self._log(f"   ⚠️ 死叉被宏观滤网拦截 ({self._current_regime})")
                return None

            if short_pos == 0:
                size = self.state.cash * pos_pct / bar.close
                stop_price = bar.close * (1 + sl)
                self.sell(size)
                self.buy(size, price=stop_price, order_type="stop")
                self._log(
                    f"   🔴 [{self._current_regime}] 开空: {size:.6f} @ {bar.close:.2f} | "
                    f"EMA({fast}/{slow}) SL={sl:.0%} 止损={stop_price:.2f}"
                )

        return None

    def on_stop(self) -> None:
        self._log("🛑 自适应EMA策略已停止")

    # ─── 指标计算 ───

    @staticmethod
    def _calc_ema(bars: list[BarData], period: int) -> float:
        if len(bars) < period:
            return bars[-1].close
        multiplier = 2.0 / (period + 1)
        ema = bars[0].close
        for bar in bars[1:]:
            ema = (bar.close - ema) * multiplier + ema
        return ema

    @staticmethod
    def _calc_atr(bars: list[BarData], period: int) -> float:
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

    @staticmethod
    def _calc_adx(bars: list[BarData], period: int) -> float:
        """计算 ADX（平均趋向指数）。"""
        if len(bars) < period * 2 + 1:
            return 25.0  # 默认趋势

        # 计算 +DM, -DM, TR
        plus_dm_list = []
        minus_dm_list = []
        tr_list = []

        for i in range(-period * 2, 0):
            high = bars[i].high
            low = bars[i].low
            prev_high = bars[i - 1].high
            prev_low = bars[i - 1].low
            prev_close = bars[i - 1].close

            up_move = high - prev_high
            down_move = prev_low - low

            plus_dm = up_move if (up_move > down_move and up_move > 0) else 0
            minus_dm = down_move if (down_move > up_move and down_move > 0) else 0

            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))

            plus_dm_list.append(plus_dm)
            minus_dm_list.append(minus_dm)
            tr_list.append(tr)

        # Wilder 平滑
        atr = sum(tr_list[:period]) / period
        plus_di_smooth = sum(plus_dm_list[:period]) / period
        minus_di_smooth = sum(minus_dm_list[:period]) / period

        for i in range(period, len(tr_list)):
            atr = (atr * (period - 1) + tr_list[i]) / period
            plus_di_smooth = (plus_di_smooth * (period - 1) + plus_dm_list[i]) / period
            minus_di_smooth = (minus_di_smooth * (period - 1) + minus_dm_list[i]) / period

        if atr == 0:
            return 0.0

        plus_di = 100 * plus_di_smooth / atr
        minus_di = 100 * minus_di_smooth / atr

        di_sum = plus_di + minus_di
        if di_sum == 0:
            return 0.0

        dx = 100 * abs(plus_di - minus_di) / di_sum

        # 简化：直接返回 DX 作为 ADX 近似值
        return dx
