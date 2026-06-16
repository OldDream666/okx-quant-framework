"""策略基类、数据模型与生命周期钩子。

本模块定义了所有策略实现的抽象接口。同一个 ``on_bar()`` 回调在回测（同步执行器）
和实盘交易（通过队列的异步桥接）中行为完全一致——策略代码永远不会直接接触 ``asyncio``。

关键设计决策：

1. **基于动作的下单** —— 策略在 ``on_bar()`` 内部调用 ``self.buy()`` / ``self.sell()`` /
   ``self.close_long()`` / ``self.close_short()``，允许单根 K 线触发多个订单
   （如入场 + 止盈 + 止损同时触发）。

2. **OKX 双仓位模式** —— 多头和空头仓位独立共存。``close_long()`` 和 ``close_short()``
   携带隐式 ``posSide``，无需 ``reduce_only`` 标志。

3. **无前视偏差** —— 在第 *i* 根 K 线提交的市价单在第 *i+1* 根 K 线的开盘价执行。
   限价/止损单根据第 *i+1* 根 K 线的价格范围进行检查。

4. **异步桥接（实盘交易）** —— ``buy()`` / ``sell()`` 为**同步**调用以优化回测性能。
   在实盘模式下，``_executor`` 将订单意图推入 ``asyncio.Queue``，
   由网关协程消费。策略代码完全不涉及 ``async/await``。
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from okx_quant.models.market import BarData, OrderData


# ---------------------------------------------------------------------------
# Signal — pure prediction output (for AI scoring / logging)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Signal:
    """策略预测输出。

    Attributes:
        action:     方向性提示 —— ``BUY``、``SELL``、``OPEN_LONG``、
                    ``OPEN_SHORT``、``CLOSE_LONG``、``CLOSE_SHORT``、``HOLD``。
        price:      建议执行价格。
        confidence: 0.0–1.0。回测引擎忽略低于 0.6 的信号。
        reason:     人类可读的解释说明。
    """

    action: str = "HOLD"
    price: float = 0.0
    confidence: float = 0.0
    reason: str = ""


# ---------------------------------------------------------------------------
# Position — OKX dual-position model
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Position:
    """OKX 双仓位模式下的单边仓位（多头或空头）。

    ``highest_price`` / ``lowest_price`` 每根 K 线更新，供策略用于追踪止损逻辑。
    """

    side: str                          # "long" or "short"
    quantity: float = 0.0
    avg_price: float = 0.0
    entry_time: int = 0                # Unix ms
    entry_bar: int = 0                 # bar index at entry
    highest_price: float = 0.0         # updated each bar (long)
    lowest_price: float = float("inf") # updated each bar (short)
    realized_pnl: float = 0.0
    funding_paid: float = 0.0          # accumulated funding

    def unrealized_pnl(self, current_price: float) -> float:
        """按市价计算未实现盈亏。"""
        if self.quantity == 0:
            return 0.0
        if self.side == "long":
            return (current_price - self.avg_price) * self.quantity
        return (self.avg_price - current_price) * self.quantity

    def update_extremes(self, high: float, low: float) -> None:
        """追踪最高/最低价格，用于追踪止损。"""
        if self.side == "long":
            self.highest_price = max(self.highest_price, high)
        else:
            self.lowest_price = min(self.lowest_price, low)


# ---------------------------------------------------------------------------
# PendingOrder — limit / stop orders awaiting execution
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PendingOrder:
    """等待成交的限价或止损订单。

    Attributes:
        order_id:    唯一标识符。
        side:        ``buy`` 或 ``sell``。
        price:       限价/止损触发价格。
        quantity:    委托数量。
        order_type:  ``limit`` 或 ``stop``。
        pos_side:    ``long`` 或 ``short`` —— 决定开仓/减仓的方向（OKX 双仓位模式）。
        created_bar: 下单时的 K 线索引。
    """

    order_id: str
    side: str           # "buy" or "sell"
    price: float
    quantity: float
    order_type: str     # "limit" or "stop"
    pos_side: str       # "long" or "short"
    created_bar: int = 0


# ---------------------------------------------------------------------------
# StrategyState — mutable runtime state injected by the engine
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class StrategyState:
    """由回测引擎/实盘 OMS 管理的运行时状态。

    策略应将这些字段视为**只读**，``custom`` 除外（用于策略内部数据的自由格式字典）。
    """

    symbol: str = ""
    timeframe: str = ""
    bar_index: int = 0

    # Position bookkeeping (engine/OMS updates these)
    position_long: Position | None = None
    position_short: Position | None = None

    # Pending orders
    pending_orders: list[PendingOrder] = field(default_factory=list)

    # Cash equity (backtest only; live uses account balance)
    cash: float = 0.0

    # Free-form strategy data
    custom: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# BaseStrategy
# ---------------------------------------------------------------------------


class BaseStrategy(ABC):
    """所有交易策略的抽象基类。

    生命周期::

        on_init(params)   ← 使用用户参数调用一次
        on_start()        ← 在第一根 K 线之前调用
        on_bar(bar)       ← 每根新 K 线调用（核心逻辑）
        on_order_update() ← 订单状态变化时调用（仅实盘）
        on_stop()         ← 最后一根 K 线之后调用

    下单方式::

        self.buy(size, price=None, order_type="market") → order_id
        self.sell(size, price=None, order_type="market") → order_id
        self.close_long(size=None) → order_id   # None = 平掉全部仓位
        self.close_short(size=None) → order_id
        self.cancel(order_id) → bool

    这些方法在回测模式下为**同步**调用（推入逐 K 线队列）。在实盘模式下，
    注入的 ``_executor`` 将订单推入由网关消费的 ``asyncio.Queue``——
    策略对异步边界无感知。
    """

    name: str = "base"
    params: dict[str, Any] = {}

    # Injected by engine / live runner
    state: StrategyState
    bars: list[BarData]  # rolling window, engine keeps the last N bars
    _executor: Any       # StrategyExecutor protocol (engine or live bridge)

    def __init__(self) -> None:
        self.state = StrategyState()
        self.bars = []
        self._executor = None

    # ------------------------------------------------------------------
    # Lifecycle hooks (override in subclasses)
    # ------------------------------------------------------------------

    def on_init(self, params: dict[str, Any]) -> None:
        """策略加载时调用一次。在此处存储参数。"""
        self.params = params or {}

    def on_start(self) -> None:
        """在第一根 K 线之前调用。在此处设置指标等。"""

    @abstractmethod
    def on_bar(self, bar: BarData) -> Signal | None:
        """每根新 K 线调用。

        可以：
        - 调用 ``self.buy()`` / ``self.sell()`` / ``self.close_long()`` /
          ``self.close_short()`` 下单。
        - 返回 ``Signal`` 用于 AI 评分（可选）。

        两种方式可以在同一次调用中共存。
        """
        ...

    def on_order_update(self, order: OrderData) -> None:
        """订单状态变化时调用（仅实盘模式）。"""

    def on_stop(self) -> None:
        """最后一根 K 线之后调用。用于清理、记录总结等。"""

    # ------------------------------------------------------------------
    # Order actions — synchronous, push to executor queue
    # ------------------------------------------------------------------

    def buy(
        self,
        size: float,
        price: float | None = None,
        order_type: str = "market",
    ) -> str:
        """提交**买入**订单。

        在 OKX 双仓位模式下：
        - 如果没有空头仓位 → **开多**。
        - 如果存在空头仓位 → **平空**（由执行器决定）。

        Returns:
            订单 ID 字符串。
        """
        return self._executor.submit(
            side="buy",
            price=price,
            quantity=size,
            order_type=order_type,
            pos_side="long",  # default; engine may override for close
        )

    def sell(
        self,
        size: float,
        price: float | None = None,
        order_type: str = "market",
    ) -> str:
        """提交**卖出**订单。

        在 OKX 双仓位模式下：
        - 如果没有多头仓位 → **开空**。
        - 如果存在多头仓位 → **平多**（由执行器决定）。

        Returns:
            订单 ID 字符串。
        """
        return self._executor.submit(
            side="sell",
            price=price,
            quantity=size,
            order_type=order_type,
            pos_side="short",
        )

    def close_long(self, size: float | None = None) -> str:
        """平掉全部或部分**多头**仓位。

        Parameters:
            size: 平仓数量。``None`` = 平掉全部仓位。

        Returns:
            订单 ID 字符串。
        """
        qty = size if size is not None else (
            self.state.position_long.quantity if self.state.position_long else 0
        )
        if qty <= 0:
            return ""
        return self._executor.submit(
            side="sell",
            price=None,
            quantity=qty,
            order_type="market",
            pos_side="long",
        )

    def close_short(self, size: float | None = None) -> str:
        """平掉全部或部分**空头**仓位。

        Parameters:
            size: 平仓数量。``None`` = 平掉全部仓位。

        Returns:
            订单 ID 字符串。
        """
        qty = size if size is not None else (
            self.state.position_short.quantity if self.state.position_short else 0
        )
        if qty <= 0:
            return ""
        return self._executor.submit(
            side="buy",
            price=None,
            quantity=qty,
            order_type="market",
            pos_side="short",
        )

    def cancel(self, order_id: str) -> bool:
        """按 ID 撤销待执行订单。"""
        return self._executor.cancel(order_id)

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def position_long(self) -> Position | None:
        return self.state.position_long

    @property
    def position_short(self) -> Position | None:
        return self.state.position_short

    @property
    def has_position(self) -> bool:
        """如果任一方向有持仓则为 True。"""
        return (
            (self.state.position_long is not None and self.state.position_long.quantity > 0)
            or (self.state.position_short is not None and self.state.position_short.quantity > 0)
        )

    @property
    def current_bar(self) -> BarData | None:
        """最新的 K 线，如果还没有 K 线则为 None。"""
        return self.bars[-1] if self.bars else None


# ---------------------------------------------------------------------------
# Executor protocol (implemented by BacktestEngine and live bridge)
# ---------------------------------------------------------------------------


class StrategyExecutor:
    """引擎/实盘桥接必须实现的协议。

    在**回测**模式下：
    - ``submit()`` 推入 ``_pending_market_orders``（下一 K 线执行）。
    - ``cancel()`` 从 ``_pending_orders`` 中移除。

    在**实盘**模式下：
    - ``submit()`` 将订单意图推入 ``asyncio.Queue``。
    - 网关协程消费队列并调用 OKX REST。
    """

    def submit(
        self,
        side: str,
        price: float | None,
        quantity: float,
        order_type: str,
        pos_side: str,
    ) -> str:
        raise NotImplementedError

    def cancel(self, order_id: str) -> bool:
        raise NotImplementedError

    def get_position(self, side: str) -> Position | None:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def generate_order_id() -> str:
    """生成唯一订单 ID（UUID4，短格式）。"""
    return uuid.uuid4().hex[:12]
