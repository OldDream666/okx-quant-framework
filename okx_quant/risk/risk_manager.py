"""风控管理器 —— 策略与执行层之间的中间件。

实现**代理模式**：``RiskManager`` 包装 ``StrategyExecutor``，对每次 ``submit()`` /
``cancel()`` 调用进行交易前检查拦截。策略对风控层无感知——照常调用 ``self.buy()``。

防御层级（按执行顺序）：

1. **硬锁定检查** —— 如果 Kill Switch 已激活，立即拒绝。
2. **频率限制** —— 每个滚动秒最多 ``max_orders_per_sec`` 次提交。
3. **胖手指/价格带** —— 限价单价格必须在最新市价的 ``max_price_deviation`` 范围内。
4. **订单大小** —— ``quantity × price`` 不得超过 ``max_order_value``。
5. **仓位敞口** —— 所有仓位的总名义价值不得超过 ``max_total_exposure``。
6. **杠杆监控** —— 如果估计杠杆 > ``max_account_leverage``，拒绝开新仓。

成交后引擎调用 ``on_fill()``，风控管理器可以：

- 跟踪滑点并在超额时触发 Kill Switch。
- 统计连续下单失败次数。

Kill Switch 行为（``_activate_kill_switch``）：

- 撤销所有待执行订单。
- 以市价平掉所有持仓。
- **永久**设置 ``is_killed = True`` —— 所有后续 ``submit()`` 调用将抛出 ``RiskViolationError``。
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from okx_quant.strategy.base import Position, StrategyExecutor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Risk events & errors
# ---------------------------------------------------------------------------


class RiskViolation(str, Enum):
    """风控违规类型。"""

    KILL_SWITCH_ACTIVE = "kill_switch_active"
    RATE_LIMIT = "rate_limit"
    FAT_FINGER = "fat_finger"
    ORDER_TOO_LARGE = "order_too_large"
    EXPOSURE_EXCEEDED = "exposure_exceeded"
    LEVERAGE_EXCEEDED = "leverage_exceeded"
    CONSECUTIVE_FAILURES = "consecutive_failures"
    SLIPPAGE_EXCEEDED = "slippage_exceeded"
    DRAWDOWN_EXCEEDED = "drawdown_exceeded"


@dataclass(slots=True, frozen=True)
class RiskEvent:
    """风控违规记录。"""

    violation: RiskViolation
    message: str
    timestamp: float = field(default_factory=time.time)
    order_side: str = ""
    quantity: float = 0.0
    price: float = 0.0


class RiskViolationError(Exception):
    """风控检查阻止订单时抛出。"""

    def __init__(self, event: RiskEvent) -> None:
        self.event = event
        super().__init__(f"[{event.violation.value}] {event.message}")


# ---------------------------------------------------------------------------
# Risk configuration
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class RiskConfig:
    """风控管理器配置。

    所有金额限制以**计价货币**（如 USDT）为单位。比例为小数（0.05 = 5%）。
    """

    # --- Pre-trade checks ---
    max_order_value: float = 50_000.0          # single order max notional
    max_total_exposure: float = 200_000.0      # total across all positions
    max_price_deviation: float = 0.05          # 5% from last close
    max_orders_per_sec: int = 10               # rolling 1-second rate limit

    # --- Account-level ---
    max_account_leverage: float = 5.0          # refuse new positions above this

    # --- Kill Switch triggers ---
    max_consecutive_failures: int = 5          # N failed orders → kill
    max_slippage_pct: float = 0.01             # 1% fill slippage → kill
    max_drawdown_pct: float = 0.20             # 20% equity drawdown → kill
    initial_equity: float = 0.0                # set at start for drawdown calc

    # --- Kill Switch behaviour ---
    kill_on_slippage: bool = True
    kill_on_consecutive: bool = True
    kill_on_drawdown: bool = True


# ---------------------------------------------------------------------------
# Risk Manager
# ---------------------------------------------------------------------------


class RiskManager(StrategyExecutor):
    """包装 ``StrategyExecutor`` 并添加风控检查的中间件。

    用法::

        executor = _BacktestExecutor(engine, strategy, contract_mode)
        risk = RiskManager(RiskConfig(...), executor)
        strategy._executor = risk   # strategy.buy() → risk.submit() → executor.submit()
    """

    def __init__(
        self,
        config: RiskConfig,
        inner: StrategyExecutor,
    ) -> None:
        self._config = config
        self._inner = inner

        # Hard lock — once True, never resets
        self._killed: bool = False

        # Rate limiting: deque of submission timestamps
        self._submit_times: deque[float] = deque()

        # Failure tracking
        self._consecutive_failures: int = 0

        # Market price anchor (updated each bar)
        self._last_market_price: float = 0.0

        # Account state (updated by engine each bar via check_account)
        self._current_equity: float = 0.0
        self._current_positions: list[Position] | None = None
        self._current_exposure: float = 0.0
        self._high_water_mark: float = 0.0  # 回撤高水位追踪

        # Violation log
        self._violations: list[RiskEvent] = []

        # Kill switch callback (set by engine)
        self._on_kill: Any = None  # Callable[[StrategyState], None]

    # ------------------------------------------------------------------
    # StrategyExecutor protocol
    # ------------------------------------------------------------------

    def submit(
        self,
        side: str,
        price: float | None,
        quantity: float,
        order_type: str,
        pos_side: str,
    ) -> str:
        """在转发给内部执行器之前进行拦截和验证。

        Raises:
            RiskViolationError: 任何检查失败时抛出。
        """
        # 1. Hard lock — immutable gate
        if self._killed:
            raise RiskViolationError(RiskEvent(
                violation=RiskViolation.KILL_SWITCH_ACTIVE,
                message="Kill switch active — all trading halted.",
                order_side=side, quantity=quantity, price=price or 0,
            ))

        # 2. Rate limit
        self._check_rate_limit()

        # 3. Fat-finger / price band (limit orders only)
        if order_type in ("limit", "stop") and price is not None:
            self._check_price_band(price, side)

        # 4. Order size
        self._check_order_size(quantity, price or self._last_market_price)

        # 5. Exposure check
        if self._config.max_total_exposure < float("inf"):
            new_notional = quantity * (price or self._last_market_price)
            if not self.check_exposure(self._current_exposure, new_notional):
                event = RiskEvent(
                    violation=RiskViolation.EXPOSURE_EXCEEDED,
                    message=(
                        f"Exposure ${self._current_exposure + new_notional:,.0f} "
                        f"exceeds max ${self._config.max_total_exposure:,.0f}"
                    ),
                    order_side=side, quantity=quantity, price=price or 0,
                )
                self._record_violation(event)
                raise RiskViolationError(event)

        # 6. Leverage check (block new opens only, not closes)
        if (self._current_equity > 0
                and self._current_positions is not None
                and self._last_market_price > 0):
            is_open = ((side == "buy" and pos_side == "long")
                       or (side == "sell" and pos_side == "short"))
            if is_open and not self.check_leverage(
                self._current_equity, self._current_positions,
                self._last_market_price,
            ):
                event = RiskEvent(
                    violation=RiskViolation.LEVERAGE_EXCEEDED,
                    message=(
                        f"Estimated leverage exceeds max "
                        f"{self._config.max_account_leverage:.1f}x"
                    ),
                    order_side=side, quantity=quantity, price=price or 0,
                )
                self._record_violation(event)
                raise RiskViolationError(event)

        # 7. Forward to inner executor
        try:
            order_id = self._inner.submit(
                side=side, price=price, quantity=quantity,
                order_type=order_type, pos_side=pos_side,
            )
            self._consecutive_failures = 0
            return order_id
        except Exception as exc:
            self._record_failure(str(exc))
            raise

    def cancel(self, order_id: str) -> bool:
        return self._inner.cancel(order_id)

    def get_position(self, side: str) -> Position | None:
        return self._inner.get_position(side)

    # ------------------------------------------------------------------
    # Post-fill callback (called by engine after each fill)
    # ------------------------------------------------------------------

    def on_fill(
        self,
        side: str,
        fill_price: float,
        target_price: float,
        quantity: float,
        pos_side: str,
    ) -> None:
        """监控成交滑点违规。

        Parameters:
            side:         ``buy`` 或 ``sell``。
            fill_price:   实际成交价格（含滑点）。
            target_price: 预期价格（订单价格或 K 线开盘价）。
            quantity:     成交数量。
            pos_side:     ``long`` 或 ``short``。
        """
        if target_price <= 0:
            return
        slippage_pct = abs(fill_price - target_price) / target_price

        if slippage_pct > self._config.max_slippage_pct and self._config.kill_on_slippage:
            event = RiskEvent(
                violation=RiskViolation.SLIPPAGE_EXCEEDED,
                message=(
                    f"Slippage {slippage_pct:.4%} exceeds threshold "
                    f"{self._config.max_slippage_pct:.4%}"
                ),
                order_side=side, quantity=quantity, price=fill_price,
            )
            self._record_violation(event)
            self._activate_kill_switch()

    # ------------------------------------------------------------------
    # Account-level checks (called by engine each bar)
    # ------------------------------------------------------------------

    def check_account(
        self,
        equity: float,
        positions: list[Position],
        market_price: float,
    ) -> None:
        """每根 K 线的账户健康检查。

        由引擎在每根 K 线后调用，监控：
        - 杠杆率
        - 回撤

        Parameters:
            equity:        账户总权益。
            positions:     所有持仓。
            market_price:  当前市价（K 线收盘价）。
        """
        self._last_market_price = market_price
        self._current_equity = equity
        self._current_positions = positions

        # Update high water mark
        if equity > self._high_water_mark:
            self._high_water_mark = equity

        # 基于真实持仓重新计算敞口（覆盖，而非累加）
        self._current_exposure = sum(
            abs(p.quantity * market_price) for p in positions if p.quantity != 0
        )

        # Drawdown check (基于高水位，而非固定初始权益)
        ref_equity = self._high_water_mark or self._config.initial_equity
        if ref_equity > 0 and self._config.kill_on_drawdown:
            drawdown = (ref_equity - equity) / ref_equity
            if drawdown > self._config.max_drawdown_pct:
                event = RiskEvent(
                    violation=RiskViolation.DRAWDOWN_EXCEEDED,
                    message=(
                        f"Drawdown {drawdown:.2%} exceeds threshold "
                        f"{self._config.max_drawdown_pct:.2%}"
                    ),
                )
                self._record_violation(event)
                self._activate_kill_switch()

        # Leverage check (informational — blocks new opens via check_leverage)
        total_notional = sum(
            abs(p.quantity * market_price) for p in positions if p.quantity != 0
        )
        if equity > 0:
            est_leverage = total_notional / equity
            if est_leverage > self._config.max_account_leverage:
                logger.warning(
                    "Account leverage %.2f exceeds max %.2f",
                    est_leverage, self._config.max_account_leverage,
                )

    def check_leverage(self, equity: float, positions: list[Position], market_price: float) -> bool:
        """检查开新仓是否会超过杠杆限制。

        Returns:
            如果安全可开仓返回 ``True``，被阻止返回 ``False``。
        """
        total_notional = sum(
            abs(p.quantity * market_price) for p in positions if p.quantity != 0
        )
        if equity <= 0:
            return False
        return (total_notional / equity) <= self._config.max_account_leverage

    def check_exposure(self, current_exposure: float, new_order_value: float) -> bool:
        """检查新订单是否会超过总敞口限制。"""
        return (current_exposure + new_order_value) <= self._config.max_total_exposure

    # ------------------------------------------------------------------
    # Market price anchor
    # ------------------------------------------------------------------

    def update_market_price(self, price: float) -> None:
        """更新胖手指检查的参考价格。

        由引擎在每根 K 线开始时调用（通常为 K 线收盘价）。
        """
        self._last_market_price = price

    # ------------------------------------------------------------------
    # Kill Switch
    # ------------------------------------------------------------------

    def _activate_kill_switch(self) -> None:
        """激活 Kill Switch —— 不可逆。

        1. 永久设置 ``is_killed = True``。
        2. 调用 kill 回调（引擎撤销所有订单 + 平掉所有仓位）。
        3. 记录事件。
        """
        if self._killed:
            return  # already killed
        self._killed = True
        logger.critical("KILL SWITCH 已触发 — 所有交易永久停止")
        if self._on_kill is not None:
            try:
                self._on_kill()
            except Exception as exc:
                logger.error("Kill Switch 回调错误: %s", exc)

    @property
    def is_killed(self) -> bool:
        """Kill Switch 是否已被激活（永久）。"""
        return self._killed

    # ------------------------------------------------------------------
    # Violations log
    # ------------------------------------------------------------------

    def get_violations(self) -> list[RiskEvent]:
        """返回所有已记录的风控违规。"""
        return list(self._violations)

    def clear_violations(self) -> None:
        self._violations.clear()

    # ------------------------------------------------------------------
    # Internal: checks
    # ------------------------------------------------------------------

    def _check_rate_limit(self) -> None:
        """如果上一秒内超过 ``max_orders_per_sec`` 次提交则拒绝。"""
        now = time.monotonic()
        cutoff = now - 1.0
        # Remove expired entries
        while self._submit_times and self._submit_times[0] < cutoff:
            self._submit_times.popleft()
        if len(self._submit_times) >= self._config.max_orders_per_sec:
            event = RiskEvent(
                violation=RiskViolation.RATE_LIMIT,
                message=(
                    f"Rate limit exceeded: {len(self._submit_times)} orders "
                    f"in last second (max {self._config.max_orders_per_sec})"
                ),
            )
            self._record_violation(event)
            raise RiskViolationError(event)
        self._submit_times.append(now)

    def _check_price_band(self, price: float, side: str) -> None:
        """拒绝价格偏离市价过远的限价单。"""
        if self._last_market_price <= 0:
            return  # no reference price yet — allow
        deviation = abs(price - self._last_market_price) / self._last_market_price
        if deviation > self._config.max_price_deviation + 1e-8:
            event = RiskEvent(
                violation=RiskViolation.FAT_FINGER,
                message=(
                    f"Price {price} deviates {deviation:.2%} from market "
                    f"{self._last_market_price} (max {self._config.max_price_deviation:.2%})"
                ),
                order_side=side, price=price,
            )
            self._record_violation(event)
            raise RiskViolationError(event)

    def _check_order_size(self, quantity: float, price: float) -> None:
        """拒绝超过最大名义价值的订单。"""
        notional = quantity * price
        if notional > self._config.max_order_value:
            event = RiskEvent(
                violation=RiskViolation.ORDER_TOO_LARGE,
                message=(
                    f"Order value {notional:.2f} exceeds max "
                    f"{self._config.max_order_value:.2f}"
                ),
                quantity=quantity, price=price,
            )
            self._record_violation(event)
            raise RiskViolationError(event)

    def _record_failure(self, reason: str) -> None:
        """跟踪连续下单失败。"""
        self._consecutive_failures += 1
        if (self._consecutive_failures >= self._config.max_consecutive_failures
                and self._config.kill_on_consecutive):
            event = RiskEvent(
                violation=RiskViolation.CONSECUTIVE_FAILURES,
                message=(
                    f"{self._consecutive_failures} consecutive failures "
                    f"(max {self._config.max_consecutive_failures}): {reason}"
                ),
            )
            self._record_violation(event)
            self._activate_kill_switch()

    def _record_violation(self, event: RiskEvent) -> None:
        """记录并存储风控违规。"""
        self._violations.append(event)
        logger.warning("风控违规 [%s]: %s", event.violation.value, event.message)
