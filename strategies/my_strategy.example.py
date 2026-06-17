"""
策略模板 — 新策略从此文件开始
==============================

复制后修改:
    cp strategies/my_strategy.example.py strategies/my_strategy.py

在配置中引用:
    strategy: "strategies.my_strategy.MyStrategy"

策略生命周期:
    on_init()  → 初始化参数
    on_start() → 策略启动（可选）
    on_bar()   → 每根 K 线闭合时调用（核心逻辑）
    on_stop()  → 策略停止（可选）
"""

from okx_quant.strategy.base import BaseStrategy, Signal
from okx_quant.models.market import BarData


class MyStrategy(BaseStrategy):
    """自定义策略示例。"""

    name = "my_strategy"

    def on_init(self, params: dict) -> None:
        """初始化策略参数。

        Args:
            params: 来自 YAML 配置的 strategy_params 段。
        """
        super().on_init(params)
        self.fast = params.get("fast_period", 10)
        self.slow = params.get("slow_period", 50)
        self.stop_loss_pct = params.get("stop_loss_pct", 0.05)
        self.position_pct = params.get("position_pct", 0.025)

    def on_start(self) -> None:
        """策略启动时调用（可选）。"""
        print(f"策略启动: EMA({self.fast}/{self.slow}), 止损={self.stop_loss_pct:.0%}")

    def on_bar(self, bar: BarData) -> Signal | None:
        """每根 K 线闭合时调用。

        这里写你的交易逻辑。

        Args:
            bar: 当前闭合的 K 线数据。
                 bar.open, bar.high, bar.low, bar.close, bar.volume

        Returns:
            None（通过 self.buy()/self.sell() 下单）
            或 Signal(action="BUY"/"SELL", price=..., confidence=...)
        """
        # ---- 示例：双均线交叉 ----

        # 等待足够的 K 线数据
        if len(self.bars) < self.slow + 1:
            return None

        # 计算 EMA
        fast_ema = self._ema(self.bars, self.fast)
        slow_ema = self._ema(self.bars, self.slow)
        fast_prev = self._ema(self.bars[:-1], self.fast)
        slow_prev = self._ema(self.bars[:-1], self.slow)

        # 金叉 → 开多
        if fast_prev <= slow_prev and fast_ema > slow_ema:
            if self.position_long is None or self.position_long.quantity == 0:
                size = self.state.cash * self.position_pct / bar.close
                self.buy(size)
                stop_price = bar.close * (1 - self.stop_loss_pct)
                self.sell(size, price=stop_price, order_type="stop")

        # 死叉 → 开空
        elif fast_prev >= slow_prev and fast_ema < slow_ema:
            if self.position_short is None or self.position_short.quantity == 0:
                size = self.state.cash * self.position_pct / bar.close
                self.sell(size)
                stop_price = bar.close * (1 + self.stop_loss_pct)
                self.buy(size, price=stop_price, order_type="stop")

        return None

    def on_stop(self) -> None:
        """策略停止时调用（可选）。"""
        print("策略已停止")

    # ---- 辅助方法 ----

    @staticmethod
    def _ema(bars: list[BarData], period: int) -> float:
        """计算 EMA（指数移动平均线）。"""
        if len(bars) < period:
            return bars[-1].close
        multiplier = 2 / (period + 1)
        ema = bars[0].close
        for bar in bars[1:]:
            ema = (bar.close - ema) * multiplier + ema
        return ema
