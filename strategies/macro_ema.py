"""宏观 EMA 交叉策略。

双 EMA 交叉配合宏观趋势过滤器（更高时间周期的 EMA200）。
仅沿宏观趋势方向交易。

Parameters（从配置加载）：
    fast_period (int):   快速 EMA 周期。
    slow_period (int):   慢速 EMA 周期。
    macro_period (int):  宏观趋势 EMA 周期（0 = 禁用）。
    stop_loss_pct (float): 止损距离比例。
    position_pct (float):  每笔交易的权益比例（默认 0.025）。
"""

from okx_quant.models.market import BarData
from okx_quant.strategy.base import BaseStrategy, Signal


class MacroEmaStrategy(BaseStrategy):
    """EMA 交叉 + 宏观趋势过滤 + 止损。"""

    name = "macro_ema"

    # Parameters (overridden by on_init)
    _fast_period: int = 15
    _slow_period: int = 40
    _macro_period: int = 800
    _stop_loss_pct: float = 0.05
    _position_pct: float = 0.025

    # EMA state
    _ema_fast: float = 0.0
    _ema_slow: float = 0.0
    _ema_macro: float = 0.0
    _prev_ema_fast: float = 0.0
    _prev_ema_slow: float = 0.0
    _ema_initialized: bool = False

    # Stop-loss order IDs
    _long_stop_id: str = ""
    _short_stop_id: str = ""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def on_init(self, params: dict) -> None:
        super().on_init(params)
        self._fast_period = int(params.get("fast_period", 15))
        self._slow_period = int(params.get("slow_period", 40))
        self._macro_period = int(params.get("macro_period", 800))
        self._stop_loss_pct = float(params.get("stop_loss_pct", 0.05))
        self._position_pct = float(params.get("position_pct", 0.025))

    def on_start(self) -> None:
        macro_info = f" + 宏观EMA({self._macro_period})" if self._macro_period > 0 else ""
        print(f"🚀 策略启动: EMA({self._fast_period}/{self._slow_period}){macro_info}")
        print(f"   止损: {self._stop_loss_pct:.0%} | 仓位: {self._position_pct:.1%} / 笔")

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    def on_bar(self, bar: BarData) -> Signal | None:
        """处理每根 K 线：更新 EMA → 检测交叉 → 交易。"""
        required = max(self._slow_period, self._macro_period) if self._macro_period > 0 else self._slow_period
        if len(self.bars) < required:
            if len(self.bars) % 100 == 0:
                print(f"   ⏳ 加载数据中... {len(self.bars)}/{required} 根 K 线")
            return None

        # Update EMAs
        self._update_ema(bar.close)
        if not self._ema_initialized:
            return None

        # Detect crossover
        golden_cross = self._prev_ema_fast <= self._prev_ema_slow and self._ema_fast > self._ema_slow
        death_cross = self._prev_ema_fast >= self._prev_ema_slow and self._ema_fast < self._ema_slow

        if not (golden_cross or death_cross):
            return None

        # Macro trend filter
        macro_pass = True
        if self._macro_period > 0 and self._ema_macro > 0:
            if golden_cross:
                macro_pass = bar.close > self._ema_macro
            else:
                macro_pass = bar.close < self._ema_macro

        # Execute trades
        if golden_cross and macro_pass:
            self._go_long(bar)
            return Signal("OPEN_LONG", bar.close, 0.8,
                          f"金叉 + 宏观多头 (EMA{self._fast_period}={self._ema_fast:.2f} > "
                          f"EMA{self._slow_period}={self._ema_slow:.2f})")
        elif death_cross and macro_pass:
            self._go_short(bar)
            return Signal("OPEN_SHORT", bar.close, 0.8,
                          f"死叉 + 宏观空头 (EMA{self._fast_period}={self._ema_fast:.2f} < "
                          f"EMA{self._slow_period}={self._ema_slow:.2f})")
        else:
            direction = "金叉" if golden_cross else "死叉"
            print(f"   ⚠️ {direction}被宏观滤网拦截 (价格={bar.close:.2f}, EMA{self._macro_period}={self._ema_macro:.2f})")
            return None

    # ------------------------------------------------------------------
    # Trading actions
    # ------------------------------------------------------------------

    def _go_long(self, bar: BarData) -> None:
        # Cancel existing short stop
        if self._short_stop_id:
            self.cancel(self._short_stop_id)
            self._short_stop_id = ""

        # Close short if open
        if self.position_short and self.position_short.quantity > 0:
            self.close_short()
            print(f"   ⚪ 平空")

        # Open long
        if not self.position_long or self.position_long.quantity == 0:
            size = self.state.cash * self._position_pct / bar.close
            self.buy(size)
            stop_price = bar.close * (1 - self._stop_loss_pct)
            self._long_stop_id = self.sell(size, price=stop_price, order_type="stop")
            print(f"   🟢 开多: {size:.6f} @ {bar.close:.2f}, 止损 @ {stop_price:.2f}")

    def _go_short(self, bar: BarData) -> None:
        # Cancel existing long stop
        if self._long_stop_id:
            self.cancel(self._long_stop_id)
            self._long_stop_id = ""

        # Close long if open
        if self.position_long and self.position_long.quantity > 0:
            self.close_long()
            print(f"   ⚪ 平多")

        # Open short
        if not self.position_short or self.position_short.quantity == 0:
            size = self.state.cash * self._position_pct / bar.close
            self.sell(size)
            stop_price = bar.close * (1 + self._stop_loss_pct)
            self._short_stop_id = self.buy(size, price=stop_price, order_type="stop")
            print(f"   🔴 开空: {size:.6f} @ {bar.close:.2f}, 止损 @ {stop_price:.2f}")

    # ------------------------------------------------------------------
    # EMA calculation
    # ------------------------------------------------------------------

    def _update_ema(self, price: float) -> None:
        self._prev_ema_fast = self._ema_fast
        self._prev_ema_slow = self._ema_slow

        if not self._ema_initialized:
            seed_period = max(self._slow_period, self._macro_period) if self._macro_period > 0 else self._slow_period
            if len(self.bars) >= seed_period:
                closes = [b.close for b in self.bars[-seed_period:]]
                self._ema_slow = sum(closes[-self._slow_period:]) / self._slow_period
                self._ema_fast = sum(closes[-self._fast_period:]) / self._fast_period
                if self._macro_period > 0:
                    self._ema_macro = sum(closes) / len(closes)
                self._ema_initialized = True
            return

        k_fast = 2.0 / (self._fast_period + 1)
        k_slow = 2.0 / (self._slow_period + 1)
        self._ema_fast = price * k_fast + self._ema_fast * (1 - k_fast)
        self._ema_slow = price * k_slow + self._ema_slow * (1 - k_slow)
        if self._macro_period > 0:
            k_macro = 2.0 / (self._macro_period + 1)
            self._ema_macro = price * k_macro + self._ema_macro * (1 - k_macro)

    def on_stop(self) -> None:
        if self._long_stop_id:
            self.cancel(self._long_stop_id)
        if self._short_stop_id:
            self.cancel(self._short_stop_id)
        print("🛑 策略已停止")
