"""事件驱动回测引擎，支持真实市场模拟。

本模块实现：

- ``ExchangeConfig``：手续费模型、动态滑点、tick/lot 取整、杠杆、保证金、资金费率。
- ``BacktestEngine``：逐 K 线事件循环，将 ``BarData`` 喂入 ``BaseStrategy`` 并模拟订单成交。
- ``BacktestResult``：完整的权益曲线、交易日志和汇总指标。

**无前视偏差保证：**

- 在第 *i* 根 K 线提交的市价单在第 *i+1* 根 K 线的**开盘价**执行。
- 限价/止损单根据第 *i+1* 根 K 线的最高/最低价范围进行检查。
- 策略永远不会看到未来数据。

**异步桥接（实盘交易说明）：**

- ``BaseStrategy.buy()`` / ``sell()`` 为同步调用——推入逐 K 线队列。回测模式下引擎在
  下一根 K 线消费队列。实盘模式下 ``_executor`` 将订单意图推入由网关协程消费的
  ``asyncio.Queue``，策略对 ``async/await`` 无感知。
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

from okx_quant.models.market import BarData
from okx_quant.strategy.base import (
    BaseStrategy,
    PendingOrder,
    Position,
    Signal,
    StrategyExecutor,
    StrategyState,
    generate_order_id,
)


# ---------------------------------------------------------------------------
# ExchangeConfig
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ExchangeConfig:
    """真实交易所模拟参数。

    Attributes:
        maker_fee_rate:            Maker 手续费（增加流动性的限价单）。
        taker_fee_rate:            Taker 手续费（市价单/越过价差的限价单）。
        slippage_base:             基础滑点比例（0.0003 = 0.03%）。
        slippage_volatility_factor: 基于振幅的滑点乘数。
        tick_size:                 最小价格变动单位。
        lot_size:                  最小数量步长。
        min_order_qty:             最小委托数量。
        contract_multiplier:       合约面值（现货为 1.0）。
        leverage:                  仓位杠杆（1 = 现货/无保证金）。
        initial_margin_ratio:      1 / leverage。
        maintenance_margin_ratio:  强平阈值。
        enable_liquidation:        开启保证金强平检查。
        funding_rate:              每期资金费率（永续合约）。
        funding_interval_hours:    资金费结算间隔（小时）。
        latency_bars:              信号到执行的延迟（K 线数）。
        signal_confidence_threshold: Signal 执行的最低置信度。
    """

    # Fees
    maker_fee_rate: float = 0.0002
    taker_fee_rate: float = 0.0005

    # Dynamic slippage
    slippage_base: float = 0.0003
    slippage_volatility_factor: float = 2.0

    # Asset specification
    tick_size: float = 0.01
    lot_size: float = 0.00000001
    min_order_qty: float = 0.00001
    contract_multiplier: float = 1.0

    # Leverage & margin
    leverage: int = 1
    initial_margin_ratio: float = 0.1
    maintenance_margin_ratio: float = 0.005
    enable_liquidation: bool = False

    # Funding rate (perpetuals)
    funding_rate: float = 0.0001
    funding_interval_hours: int = 8

    # Execution
    latency_bars: int = 1
    signal_confidence_threshold: float = 0.6

    # ------------------------------------------------------------------
    # Rounding helpers
    # ------------------------------------------------------------------

    def round_price(self, price: float) -> float:
        """将价格取整到最近的 tick_size（四舍五入）。"""
        if self.tick_size <= 0:
            return price
        from decimal import Decimal, ROUND_HALF_UP
        tick = Decimal(str(self.tick_size))
        rounded = Decimal(str(price)).quantize(tick, rounding=ROUND_HALF_UP)
        return float(rounded)

    def round_quantity(self, qty: float) -> float:
        """将数量取整到最近的 lot_size，并强制最小值。"""
        if self.lot_size <= 0:
            return qty
        from decimal import Decimal, ROUND_HALF_UP
        lot = Decimal(str(self.lot_size))
        rounded = float(Decimal(str(qty)).quantize(lot, rounding=ROUND_HALF_UP))
        if rounded < self.min_order_qty:
            return 0.0  # too small to fill
        return rounded

    # ------------------------------------------------------------------
    # Fee & slippage
    # ------------------------------------------------------------------

    def calc_fee(self, price: float, qty: float, is_taker: bool = True) -> float:
        """计算以计价货币计的交易手续费。"""
        rate = self.taker_fee_rate if is_taker else self.maker_fee_rate
        return price * qty * self.contract_multiplier * rate

    def calc_slippage(
        self, price: float, side: str, bar_high: float, bar_low: float
    ) -> float:
        """基于 K 线振幅计算动态滑点。

        返回**含滑点的价格**（买入时更高，卖出时更低）。

        滑点随 K 线相对振幅缩放：
        - 平静市场（小振幅）→ 接近 ``slippage_base``。
        - 波动市场（大振幅）→ 最高 10 倍基础值（上限 1%）。
        """
        if price <= 0:
            return price
        amplitude = (bar_high - bar_low) / price
        avg_amplitude = 0.01  # 1% reference amplitude
        factor = 1.0 + self.slippage_volatility_factor * (amplitude / avg_amplitude)
        slippage_pct = min(self.slippage_base * factor, 0.01)  # cap 1%
        slip = price * slippage_pct
        return price + slip if side == "buy" else price - slip

    # ------------------------------------------------------------------
    # Liquidation
    # ------------------------------------------------------------------

    def check_liquidation(
        self,
        entry_price: float,
        current_price: float,
        side: str,
        margin: float,
    ) -> bool:
        """检查仓位是否应被强平。

        简化模型：
        - long 亏损 = entry_price - current_price（价格下跌才亏）
        - short 亏损 = current_price - entry_price（价格上涨才亏）
        - 仅当亏损 > 0 时才触发强平检查
        """
        if not self.enable_liquidation or self.leverage <= 1:
            return False
        if side == "long":
            loss = entry_price - current_price
        else:
            loss = current_price - entry_price
        if loss <= 0:
            return False  # 盈利中，不触发强平
        threshold = entry_price * self.maintenance_margin_ratio * self.leverage
        return loss >= threshold

    # ------------------------------------------------------------------
    # Latency helpers
    # ------------------------------------------------------------------

    def execution_bar(self, signal_bar: int) -> int:
        """返回在 *signal_bar* 提交的信号实际执行的 K 线索引（signal_bar + latency_bars）。"""
        return signal_bar + self.latency_bars


# ---------------------------------------------------------------------------
# Trade record
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Trade:
    """一笔完成的交易（入场 → 出场）。"""

    entry_bar: int
    exit_bar: int
    entry_time: int          # Unix ms
    exit_time: int
    side: str                # "long" or "short"
    entry_price: float       # theoretical price
    exit_price: float
    entry_fill: float        # actual fill (with slippage)
    exit_fill: float
    quantity: float
    entry_fee: float
    exit_fee: float
    funding_paid: float
    pnl: float               # net P&L (after fees + funding)
    bars_held: int
    reason: str = ""


# ---------------------------------------------------------------------------
# BacktestResult
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class BacktestResult:
    """完整的回测输出。"""

    initial_capital: float
    final_equity: float
    total_return: float         # fraction (0.05 = 5%)
    annual_return: float
    max_drawdown: float
    sharpe_ratio: float
    win_rate: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    total_fees: float
    total_funding: float
    avg_bars_held: float
    profit_factor: float
    equity_curve: list[float]
    trades: list[Trade]
    config: dict[str, Any]


@dataclass(slots=True)
class WalkForwardResult:
    """Walk-forward 验证输出。

    Attributes:
        train_result:    训练集上的 BacktestResult。
        test_result:     测试集（样本外）上的 BacktestResult。
        train_pct:       用于训练的数据比例。
        train_sharpe:    训练集的 Sharpe 比率。
        test_sharpe:     测试集的 Sharpe 比率。
        sharpe_ratio:    test_sharpe / train_sharpe（越接近 1.0 越稳健）。
        is_overfit:      如果 Sharpe 衰减超过阈值则为 True。
        overfit_warning: 人类可读的警告信息（未过拟合时为空）。
    """

    train_result: BacktestResult
    test_result: BacktestResult
    train_pct: float
    train_sharpe: float
    test_sharpe: float
    sharpe_degradation: float   # 1 - test/train (0 = perfect, >0.5 = likely overfit)
    is_overfit: bool
    overfit_warning: str


# ---------------------------------------------------------------------------
# BacktestEngine
# ---------------------------------------------------------------------------


class BacktestEngine:
    """事件驱动回测引擎。

    用法::

        engine = BacktestEngine(initial_capital=10000, config=ExchangeConfig(...))
        result = engine.run(MyStrategy(), bars)
    """

    def __init__(
        self,
        initial_capital: float = 10_000.0,
        config: ExchangeConfig | None = None,
        data_dir: str = "data/backtest",
    ) -> None:
        self.initial_capital = initial_capital
        self.config = config or ExchangeConfig()
        self._data_dir = data_dir

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        strategy: BaseStrategy,
        bars: list[BarData],
        contract_mode: bool = False,
        ledger: Any = None,  # TradeLedger (optional, auto-created if None)
    ) -> BacktestResult:
        """运行回测。

        Parameters:
            strategy:      已实例化的策略（``on_init`` 已调用）。
            bars:          按时间顺序排列的历史 K 线。
            contract_mode: 启用双多空仓位交易。
            ledger:        可选的 :class:`TradeLedger`。如未提供则使用构造函数中的
                           ``data_dir`` 自动创建。

        Returns:
            包含完整指标的 :class:`BacktestResult`。
        """
        cfg = self.config

        # Auto-create ledger if not provided
        if ledger is None:
            from okx_quant.monitoring.ledger import TradeLedger
            symbol = bars[0].symbol if bars else "unknown"
            ledger = TradeLedger(data_dir=self._data_dir, symbol=symbol)

        # -- Initialize strategy state ---------------------------------
        state = StrategyState(
            symbol=bars[0].symbol if bars else "",
            cash=self.initial_capital,
        )
        strategy.state = state
        strategy.bars = []
        strategy._executor = _BacktestExecutor(self, strategy, contract_mode)

        strategy.on_start()

        # -- Internal tracking -----------------------------------------
        equity_curve: list[float] = []
        trades: list[_Trade] = []
        total_fees = 0.0
        total_funding = 0.0

        # Per-bar order queue: orders submitted at bar[i] execute at bar[i+latency]
        # _order_queue[exec_bar] = list of (side, price, qty, order_type, pos_side, reason)
        _order_queue: dict[int, list[tuple[str, float | None, float, str, str, str]]] = {}

        # Timestamp tracking for funding
        last_funding_ts = bars[0].timestamp if bars else 0
        funding_interval_ms = cfg.funding_interval_hours * 3600 * 1000

        strategy._executor._order_queue = _order_queue  # type: ignore[attr-defined]

        # -- Main loop -------------------------------------------------
        for i, bar in enumerate(bars):
            strategy.bars.append(bar)
            state.bar_index = i

            # Keep bars window reasonable (last 1000)
            if len(strategy.bars) > 1000:
                strategy.bars = strategy.bars[-1000:]

            # 1. Update position extremes (trailing stop data)
            if state.position_long and state.position_long.quantity > 0:
                state.position_long.update_extremes(bar.high, bar.low)
            if state.position_short and state.position_short.quantity > 0:
                state.position_short.update_extremes(bar.high, bar.low)

            # 2. Check liquidation
            if cfg.enable_liquidation:
                self._check_liquidation(state, bar, i, trades, cfg)

            # 3. Execute queued market orders (from previous bar's on_bar)
            exec_orders = _order_queue.pop(i, [])
            for side, price, qty, otype, pos_side, reason in exec_orders:
                if otype == "market":
                    # Execute at this bar's open + dynamic slippage
                    fill = cfg.calc_slippage(bar.open, side, bar.high, bar.low)
                    fill = cfg.round_price(fill)
                    qty = cfg.round_quantity(qty)
                    if qty <= 0:
                        continue
                    fee = cfg.calc_fee(fill, qty, is_taker=True)
                    self._execute_fill(
                        state, side, fill, qty, fee, pos_side, i, bar.timestamp,
                        trades, cfg, reason,
                    )
                    # Note: total_fees accumulated via trades at step 4
                else:
                    # Limit/stop placed at previous bar — add to pending
                    assert price is not None
                    state.pending_orders.append(PendingOrder(
                        order_id=generate_order_id(),
                        side=side,
                        price=price,
                        quantity=qty,
                        order_type=otype,
                        pos_side=pos_side,
                        created_bar=i - 1,
                    ))

            # 4. Execute pending limit / stop orders
            self._execute_pending(state, bar, i, trades, cfg)
            total_fees += sum(t.entry_fee + t.exit_fee for t in trades if t.exit_bar == i)

            # 5. Apply funding rate (perpetual contracts)
            if contract_mode and bar.timestamp - last_funding_ts >= funding_interval_ms:
                funding = self._apply_funding(state, bar)
                total_funding += funding
                last_funding_ts = bar.timestamp

            # 6. Call strategy
            signal = strategy.on_bar(bar)

            # 7. Process Signal return value
            if signal and signal.action != "HOLD":
                self._process_signal(signal, i, state, _order_queue, cfg)

            # 8. Record equity
            equity = self._calc_equity(state, bar.close)
            equity_curve.append(equity)

            # 9. Persist to ledger (if provided)
            if ledger is not None:
                ledger.append_equity(
                    equity=equity,
                    cash=state.cash,
                    position_value=equity - state.cash,
                    drawdown=max(
                        (max(equity_curve) - equity) / max(equity_curve)
                        if equity_curve else 0, 0
                    ),
                    ts=bar.timestamp,
                )

        # -- Cleanup ---------------------------------------------------
        strategy.on_stop()

        # Force-close remaining positions at last bar's close
        last_bar = bars[-1] if bars else None
        if last_bar:
            self._force_close(state, last_bar, len(bars) - 1, trades, cfg)
            total_fees += sum(t.entry_fee + t.exit_fee for t in trades if t.exit_bar == len(bars) - 1)

        final_equity = self._calc_equity(state, last_bar.close if last_bar else 0)

        result = self._build_result(
            final_equity, equity_curve, trades, total_fees, total_funding, bars,
        )

        # Persist trades to ledger (if provided)
        if ledger is not None:
            ledger.flush_trades([
                {
                    "side": t.side,
                    "entry_price": t.entry_price,
                    "exit_price": t.exit_price,
                    "entry_fill": t.entry_fill,
                    "exit_fill": t.exit_fill,
                    "quantity": t.quantity,
                    "pnl": round(t.realized_pnl, 2),
                    "fee": round(t.entry_fee + t.exit_fee, 2),
                    "funding_paid": round(t.funding_paid, 2),
                    "bars_held": t.bars_held,
                    "entry_bar": t.entry_bar,
                    "exit_bar": t.exit_bar,
                    "reason": t.reason,
                }
                for t in trades
            ])

        return result

    # ------------------------------------------------------------------
    # Walk-Forward Validation
    # ------------------------------------------------------------------

    def run_walk_forward(
        self,
        strategy_factory: type[BaseStrategy],
        bars: list[BarData],
        params: dict[str, Any] | None = None,
        train_pct: float = 0.7,
        overfit_threshold: float = 0.5,
        contract_mode: bool = False,
    ) -> WalkForwardResult:
        """运行 Walk-forward 验证：训练 → 测试 → 比较。

        将 *bars* 分为训练集（前 ``train_pct``）和测试集（剩余部分）。
        在**两个**集合上独立运行策略，比较 Sharpe 比率以检测过拟合。

        Parameters:
            strategy_factory:  策略**类**（非实例）。为每个集合创建新实例。
            bars:              完整的历史数据集。
            params:            传递给 ``strategy.on_init()`` 的参数。
            train_pct:         用于训练的 K 线比例（默认 0.7）。
            overfit_threshold: Sharpe 衰减阈值（默认 0.5）。
                               如果 ``(1 - test_sharpe/train_sharpe) > threshold``，
                               结果被标记为过拟合。
            contract_mode:     启用双仓位交易。

        Returns:
            包含训练/测试结果和过拟合分析的 :class:`WalkForwardResult`。
        """
        if train_pct <= 0 or train_pct >= 1:
            raise ValueError(f"train_pct must be in (0, 1), got {train_pct}")
        if len(bars) < 10:
            raise ValueError(f"Need at least 10 bars, got {len(bars)}")

        # Split data
        split_idx = int(len(bars) * train_pct)
        train_bars = bars[:split_idx]
        test_bars = bars[split_idx:]

        if len(train_bars) < 5 or len(test_bars) < 5:
            raise ValueError(
                f"Split too small: train={len(train_bars)}, test={len(test_bars)}. "
                f"Need at least 5 bars in each set."
            )

        # Run on training set
        train_strategy = strategy_factory()
        train_strategy.on_init(params or {})
        train_result = self.run(train_strategy, train_bars, contract_mode)

        # Run on test set (fresh strategy instance, same params)
        test_strategy = strategy_factory()
        test_strategy.on_init(params or {})
        test_result = self.run(test_strategy, test_bars, contract_mode)

        # Compare Sharpe ratios
        train_sharpe = train_result.sharpe_ratio
        test_sharpe = test_result.sharpe_ratio

        # Sharpe degradation: how much worse is test vs train?
        if train_sharpe > 0:
            degradation = 1.0 - (test_sharpe / train_sharpe)
        elif test_sharpe < 0:
            degradation = 1.0  # both negative → fully degraded
        else:
            degradation = 0.0  # train was bad, test is good → not overfit

        is_overfit = degradation > overfit_threshold

        warning = ""
        if is_overfit:
            warning = (
                f"⚠️ OVERFITTING WARNING: Sharpe degradation {degradation:.1%} "
                f"exceeds threshold {overfit_threshold:.1%}. "
                f"Train Sharpe={train_sharpe:.2f}, Test Sharpe={test_sharpe:.2f}. "
                f"The strategy may be over-optimized for the training period."
            )

        return WalkForwardResult(
            train_result=train_result,
            test_result=test_result,
            train_pct=train_pct,
            train_sharpe=train_sharpe,
            test_sharpe=test_sharpe,
            sharpe_degradation=degradation,
            is_overfit=is_overfit,
            overfit_warning=warning,
        )

    # ------------------------------------------------------------------
    # Internal: execution
    # ------------------------------------------------------------------

    def _execute_fill(
        self,
        state: StrategyState,
        side: str,
        fill_price: float,
        qty: float,
        fee: float,
        pos_side: str,
        bar_idx: int,
        timestamp: int,
        trades: list[_Trade],
        cfg: ExchangeConfig,
        reason: str = "",
    ) -> None:
        """执行成交并更新仓位。"""
        pos = state.position_long if pos_side == "long" else state.position_short

        if side == "buy":
            if pos is None or pos.quantity == 0:
                # Open new position
                new_pos = Position(
                    side=pos_side,
                    quantity=qty,
                    avg_price=fill_price,
                    contract_multiplier=cfg.contract_multiplier,
                    entry_time=timestamp,
                    entry_bar=bar_idx,
                    highest_price=fill_price,
                    lowest_price=fill_price,
                )
                if pos_side == "long":
                    state.position_long = new_pos
                else:
                    state.position_short = new_pos
                # Track entry trade
                trades.append(_Trade(
                    entry_bar=bar_idx, side=pos_side,
                    entry_price=fill_price, entry_fill=fill_price,
                    quantity=qty, entry_fee=fee, entry_time=timestamp,
                    reason=reason,
                ))
            else:
                # Add to existing position or close
                self._reduce_or_close(
                    state, pos, pos_side, "buy", fill_price, qty, fee,
                    bar_idx, timestamp, trades, cfg, reason,
                )
        else:  # sell
            if pos is None or pos.quantity == 0:
                # Open new short position (only in contract mode)
                new_pos = Position(
                    side=pos_side,
                    quantity=qty,
                    avg_price=fill_price,
                    contract_multiplier=cfg.contract_multiplier,
                    entry_time=timestamp,
                    entry_bar=bar_idx,
                    highest_price=fill_price,
                    lowest_price=fill_price,
                )
                if pos_side == "short":
                    state.position_short = new_pos
                else:
                    state.position_long = new_pos
                trades.append(_Trade(
                    entry_bar=bar_idx, side=pos_side,
                    entry_price=fill_price, entry_fill=fill_price,
                    quantity=qty, entry_fee=fee, entry_time=timestamp,
                    reason=reason,
                ))
            else:
                self._reduce_or_close(
                    state, pos, pos_side, "sell", fill_price, qty, fee,
                    bar_idx, timestamp, trades, cfg, reason,
                )

        # Deduct fee from cash
        state.cash -= fee

    def _reduce_or_close(
        self,
        state: StrategyState,
        pos: Position,
        pos_side: str,
        side: str,
        fill_price: float,
        qty: float,
        fee: float,
        bar_idx: int,
        timestamp: int,
        trades: list[_Trade],
        cfg: ExchangeConfig,
        reason: str,
    ) -> None:
        """减仓或平仓。"""
        close_qty = min(qty, pos.quantity)

        # P&L calculation
        if pos_side == "long":
            pnl = (fill_price - pos.avg_price) * close_qty * pos.contract_multiplier
        else:
            pnl = (pos.avg_price - fill_price) * close_qty * pos.contract_multiplier

        pos.realized_pnl += pnl
        pos.quantity -= close_qty
        state.cash += pnl

        # Close the matching entry trade
        for t in reversed(trades):
            if t.side == pos_side and not t.is_closed:
                t.close(bar_idx, timestamp, fill_price, fee, pnl, pos.funding_paid)
                break

        if pos.quantity <= 0:
            # Fully closed
            if pos_side == "long":
                state.position_long = None
            else:
                state.position_short = None

        # If qty > close_qty, open a new position in opposite direction
        remaining = qty - close_qty
        if remaining > cfg.min_order_qty:
            new_side = "short" if pos_side == "long" else "long"
            new_pos = Position(
                side=new_side,
                quantity=remaining,
                avg_price=fill_price,
                contract_multiplier=cfg.contract_multiplier,
                entry_time=timestamp,
                entry_bar=bar_idx,
                highest_price=fill_price,
                lowest_price=fill_price,
            )
            if new_side == "long":
                state.position_long = new_pos
            else:
                state.position_short = new_pos
            trades.append(_Trade(
                entry_bar=bar_idx, side=new_side,
                entry_price=fill_price, entry_fill=fill_price,
                quantity=remaining, entry_fee=fee, entry_time=timestamp,
                reason=reason,
            ))

    def _execute_pending(
        self,
        state: StrategyState,
        bar: BarData,
        bar_idx: int,
        trades: list[_Trade],
        cfg: ExchangeConfig,
    ) -> None:
        """根据当前 K 线执行待执行的限价/止损单。"""
        remaining: list[PendingOrder] = []

        for order in state.pending_orders:
            fill_price: float | None = None

            if order.order_type == "limit":
                if order.side == "buy" and bar.low <= order.price:
                    # Limit order fills at exact limit price — no slippage (Maker)
                    fill_price = order.price
                elif order.side == "sell" and bar.high >= order.price:
                    fill_price = order.price
            elif order.order_type == "stop":
                if order.side == "buy" and bar.high >= order.price:
                    fill_price = cfg.calc_slippage(order.price, "buy", bar.high, bar.low)
                elif order.side == "sell" and bar.low <= order.price:
                    fill_price = cfg.calc_slippage(order.price, "sell", bar.high, bar.low)

            if fill_price is not None:
                fill_price = cfg.round_price(fill_price)
                qty = cfg.round_quantity(order.quantity)
                if qty <= 0:
                    continue
                fee = cfg.calc_fee(fill_price, qty, is_taker=(order.order_type != "limit"))
                self._execute_fill(
                    state, order.side, fill_price, qty, fee,
                    order.pos_side, bar_idx, bar.timestamp,
                    trades, cfg, f"pending_{order.order_type}",
                )
            else:
                remaining.append(order)

        state.pending_orders = remaining

    def _process_signal(
        self,
        signal: Signal,
        bar_idx: int,
        state: StrategyState,
        queue: dict[int, list],
        cfg: ExchangeConfig,
    ) -> None:
        """处理 on_bar() 返回的 Signal。"""
        if signal.confidence < cfg.signal_confidence_threshold:
            return

        exec_bar = cfg.execution_bar(bar_idx)

        action_map = {
            "BUY":         ("buy",  "long",  "market", "signal_buy"),
            "SELL":        ("sell", "short", "market", "signal_sell"),
            "OPEN_LONG":   ("buy",  "long",  "market", "signal_open_long"),
            "OPEN_SHORT":  ("sell", "short", "market", "signal_open_short"),
            "CLOSE_LONG":  ("sell", "long",  "market", "signal_close_long"),
            "CLOSE_SHORT": ("buy",  "short", "market", "signal_close_short"),
        }

        entry = action_map.get(signal.action)
        if entry is None:
            return

        side, pos_side, otype, reason = entry
        qty = state.cash * 0.1 / signal.price if signal.price > 0 else 0  # 10% of cash

        if exec_bar not in queue:
            queue[exec_bar] = []
        queue[exec_bar].append((side, signal.price, qty, otype, pos_side, reason))

    # ------------------------------------------------------------------
    # Internal: liquidation & funding
    # ------------------------------------------------------------------

    def _check_liquidation(
        self,
        state: StrategyState,
        bar: BarData,
        bar_idx: int,
        trades: list[_Trade],
        cfg: ExchangeConfig,
    ) -> None:
        """强平低于维持保证金的仓位。"""
        for pos_side in ("long", "short"):
            pos = state.position_long if pos_side == "long" else state.position_short
            if pos is None or pos.quantity == 0:
                continue
            if cfg.check_liquidation(pos.avg_price, bar.close, pos_side, state.cash):
                # Force close — use worst-case intra-bar price + slippage
                worst_price = bar.low if pos_side == "long" else bar.high
                close_price = cfg.calc_slippage(worst_price, "sell" if pos_side == "long" else "buy", bar.high, bar.low)
                close_price = cfg.round_price(close_price)
                close_qty = pos.quantity
                fee = cfg.calc_fee(close_price, close_qty, is_taker=True)
                pnl = pos.unrealized_pnl(close_price)
                pos.realized_pnl += pnl
                state.cash += pnl - fee

                for t in reversed(trades):
                    if t.side == pos_side and not t.is_closed:
                        t.close(bar_idx, bar.timestamp, close_price, fee, pnl, pos.funding_paid)
                        break

                if pos_side == "long":
                    state.position_long = None
                else:
                    state.position_short = None

    def _apply_funding(self, state: StrategyState, bar: BarData) -> float:
        """对持仓应用资金费率。"""
        total = 0.0
        for pos_side in ("long", "short"):
            pos = state.position_long if pos_side == "long" else state.position_short
            if pos is None or pos.quantity == 0:
                continue
            value = pos.quantity * bar.close * pos.contract_multiplier
            funding = value * self.config.funding_rate
            if pos_side == "long":
                funding = -funding  # long pays
            pos.funding_paid += funding
            state.cash += funding
            total += abs(funding)
        return total

    def _force_close(
        self,
        state: StrategyState,
        bar: BarData,
        bar_idx: int,
        trades: list[_Trade],
        cfg: ExchangeConfig,
    ) -> None:
        """在回测结束时强制平掉所有仓位。"""
        for pos_side in ("long", "short"):
            pos = state.position_long if pos_side == "long" else state.position_short
            if pos is None or pos.quantity == 0:
                continue
            close_price = cfg.calc_slippage(bar.close, "sell" if pos_side == "long" else "buy", bar.high, bar.low)
            close_price = cfg.round_price(close_price)
            fee = cfg.calc_fee(close_price, pos.quantity, is_taker=True)
            pnl = pos.unrealized_pnl(close_price)
            pos.realized_pnl += pnl
            state.cash += pnl - fee

            for t in reversed(trades):
                if t.side == pos_side and not t.is_closed:
                    t.close(bar_idx, bar.timestamp, close_price, fee, pnl, pos.funding_paid)
                    break

            if pos_side == "long":
                state.position_long = None
            else:
                state.position_short = None

    # ------------------------------------------------------------------
    # Internal: equity & metrics
    # ------------------------------------------------------------------

    def _calc_equity(self, state: StrategyState, current_price: float) -> float:
        """总权益 = 现金 + 所有持仓的未实现盈亏。"""
        equity = state.cash
        for pos in (state.position_long, state.position_short):
            if pos and pos.quantity > 0:
                equity += pos.unrealized_pnl(current_price)
        return equity

    def _build_result(
        self,
        final_equity: float,
        equity_curve: list[float],
        trades: list[_Trade],
        total_fees: float,
        total_funding: float,
        bars: list[BarData],
    ) -> BacktestResult:
        """计算汇总指标并构建 BacktestResult。"""

        init = self.initial_capital
        total_return = (final_equity - init) / init if init else 0.0

        # Max drawdown
        max_dd = 0.0
        peak = init
        for eq in equity_curve:
            peak = max(peak, eq)
            dd = (peak - eq) / peak if peak > 0 else 0
            max_dd = max(max_dd, dd)

        # Sharpe ratio (annualized)
        if len(equity_curve) > 1:
            returns = [
                (equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1]
                for i in range(1, len(equity_curve))
                if equity_curve[i - 1] > 0
            ]
            if returns:
                avg_ret = sum(returns) / len(returns)
                std_ret = (sum((r - avg_ret) ** 2 for r in returns) / len(returns)) ** 0.5
                # Annualize: 根据 K 线间隔动态计算年化系数
                bars_per_year = BacktestEngine._bars_per_year(bars)
                sharpe = (avg_ret / std_ret * math.sqrt(bars_per_year)) if std_ret > 0 else 0.0
            else:
                sharpe = 0.0
        else:
            sharpe = 0.0

        # Trade statistics (only closed trades)
        closed = [t for t in trades if t.is_closed]
        winning = [t for t in closed if t.realized_pnl > 0]
        losing = [t for t in closed if t.realized_pnl <= 0]
        win_rate = len(winning) / len(closed) if closed else 0.0
        avg_bars = sum(t.bars_held for t in closed) / len(closed) if closed else 0.0

        gross_profit = sum(t.realized_pnl for t in winning)
        gross_loss = abs(sum(t.realized_pnl for t in losing))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        # Convert _Trade → Trade for result
        trade_records = [
            Trade(
                entry_bar=t.entry_bar,
                exit_bar=t.exit_bar,
                entry_time=t.entry_time,
                exit_time=t.exit_time,
                side=t.side,
                entry_price=t.entry_price,
                exit_price=t.exit_price,
                entry_fill=t.entry_fill,
                exit_fill=t.exit_fill,
                quantity=t.quantity,
                entry_fee=t.entry_fee,
                exit_fee=t.exit_fee,
                funding_paid=t.funding_paid,
                pnl=t.realized_pnl,
                bars_held=t.bars_held,
                reason=t.reason,
            )
            for t in trades
        ]

        return BacktestResult(
            initial_capital=init,
            final_equity=final_equity,
            total_return=total_return,
            annual_return=total_return,  # simplified; could be time-weighted
            max_drawdown=max_dd,
            sharpe_ratio=sharpe,
            win_rate=win_rate,
            total_trades=len(closed),
            winning_trades=len(winning),
            losing_trades=len(losing),
            total_fees=total_fees,
            total_funding=total_funding,
            avg_bars_held=avg_bars,
            profit_factor=profit_factor,
            equity_curve=equity_curve,
            trades=trade_records,
            config={
                "maker_fee": self.config.maker_fee_rate,
                "taker_fee": self.config.taker_fee_rate,
                "slippage_base": self.config.slippage_base,
                "leverage": self.config.leverage,
                "tick_size": self.config.tick_size,
                "latency_bars": self.config.latency_bars,
            },
        )


# ---------------------------------------------------------------------------
# Internal trade tracker (mutable, with close method)
# ---------------------------------------------------------------------------



    @staticmethod
    def _bars_per_year(bars: list[BarData]) -> float:
        """根据 K 线间隔推断年化系数（bars/年）。"""
        if len(bars) < 2:
            return 365.0
        sample = bars[:min(100, len(bars))]
        intervals = [sample[i].timestamp - sample[i-1].timestamp for i in range(1, len(sample))]
        if not intervals:
            return 365.0
        avg_ms = sum(intervals) / len(intervals)
        avg_minutes = avg_ms / 60_000
        if avg_minutes <= 0:
            return 365.0
        return 525_600 / avg_minutes  # 一年 ≈ 525,600 分钟

class _Trade:
    """回测中使用的可变交易跟踪器（不暴露在结果中）。"""

    __slots__ = (
        "entry_bar", "exit_bar", "entry_time", "exit_time", "side",
        "entry_price", "exit_price", "entry_fill", "exit_fill",
        "quantity", "entry_fee", "exit_fee", "funding_paid",
        "realized_pnl", "reason", "is_closed",
    )

    def __init__(
        self,
        entry_bar: int,
        side: str,
        entry_price: float,
        entry_fill: float,
        quantity: float,
        entry_fee: float,
        entry_time: int,
        reason: str = "",
    ) -> None:
        self.entry_bar = entry_bar
        self.exit_bar = -1
        self.entry_time = entry_time
        self.exit_time = 0
        self.side = side
        self.entry_price = entry_price
        self.exit_price = 0.0
        self.entry_fill = entry_fill
        self.exit_fill = 0.0
        self.quantity = quantity
        self.entry_fee = entry_fee
        self.exit_fee = 0.0
        self.funding_paid = 0.0
        self.realized_pnl = 0.0
        self.reason = reason
        self.is_closed = False

    def close(
        self,
        exit_bar: int,
        exit_time: int,
        exit_price: float,
        exit_fee: float,
        pnl: float,
        funding: float,
    ) -> None:
        self.exit_bar = exit_bar
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.exit_fill = exit_price
        self.exit_fee = exit_fee
        self.funding_paid = funding
        self.realized_pnl = pnl - self.entry_fee - exit_fee
        self.is_closed = True

    @property
    def bars_held(self) -> int:
        if self.exit_bar < 0:
            return 0
        return self.exit_bar - self.entry_bar


# ---------------------------------------------------------------------------
# Backtest executor (injected into strategy._executor)
# ---------------------------------------------------------------------------


class _BacktestExecutor(StrategyExecutor):
    """用于回测的 StrategyExecutor 实现。

    ``submit()`` 将订单推入逐 K 线队列；引擎在下一根 K 线消费队列
    （默认 latency_bars=1 → 无前视偏差）。

    在实盘交易中，此类被异步桥接替代，将订单意图推入 ``asyncio.Queue`` 供网关协程消费。
    """

    def __init__(
        self,
        engine: BacktestEngine,
        strategy: BaseStrategy,
        contract_mode: bool,
    ) -> None:
        self._engine = engine
        self._strategy = strategy
        self._contract_mode = contract_mode
        self._order_queue: dict[int, list] = {}

    def submit(
        self,
        side: str,
        price: float | None,
        quantity: float,
        order_type: str,
        pos_side: str,
    ) -> str:
        """将订单推入队列，在下一根 K 线执行。"""
        cfg = self._engine.config
        bar_idx = self._strategy.state.bar_index
        exec_bar = cfg.execution_bar(bar_idx)
        state = self._strategy.state

        # 现货模式校验：不允许裸卖空
        if not self._contract_mode and side == "sell":
            has_long = state.position_long is not None and state.position_long.quantity > 0
            if not has_long:
                logger.warning(
                    "现货模式拒绝卖空: 无多仓可平 (bar=%d)", bar_idx
                )
                return ""

        # 限价/止损单 → 加入 pending_orders
        if order_type in ("limit", "stop"):
            self._strategy.state.pending_orders.append(PendingOrder(
                order_id=generate_order_id(),
                side=side,
                price=price or 0.0,
                quantity=quantity,
                order_type=order_type,
                pos_side=pos_side,
                created_bar=bar_idx,
            ))
            return ""

        # Market orders → queue for next bar
        if exec_bar not in self._order_queue:
            self._order_queue[exec_bar] = []
        self._order_queue[exec_bar].append(
            (side, price, quantity, order_type, pos_side, f"strategy_{side}")
        )
        return generate_order_id()

    def cancel(self, order_id: str) -> bool:
        """按 ID 移除待执行订单。"""
        state = self._strategy.state
        before = len(state.pending_orders)
        state.pending_orders = [
            o for o in state.pending_orders if o.order_id != order_id
        ]
        return len(state.pending_orders) < before

    def get_position(self, side: str) -> Position | None:
        state = self._strategy.state
        if side == "long":
            return state.position_long
        return state.position_short
