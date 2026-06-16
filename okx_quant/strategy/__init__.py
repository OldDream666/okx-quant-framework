"""策略框架 — 基类定义。

Backtest engine → ``okx_quant.backtest``
Live runner     → ``okx_quant.live``
"""

from okx_quant.strategy.base import (
    BaseStrategy,
    PendingOrder,
    Position,
    Signal,
    StrategyExecutor,
    StrategyState,
    generate_order_id,
)

__all__ = [
    "BaseStrategy",
    "PendingOrder",
    "Position",
    "Signal",
    "StrategyExecutor",
    "StrategyState",
    "generate_order_id",
]
