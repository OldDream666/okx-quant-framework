"""实时指标采集、心跳监控与异步报警。

核心设计决策：

1. **增量指标** — ``MetricsCollector`` 维护运行中的计数器
   （``max_equity``、``win_count`` 等），因此 ``current_metrics()`` 为 O(1)，
   无论已记录多少交易。无需遍历完整列表。

2. **心跳监控** — ``HeartbeatMonitor`` 跟踪最近一次接收行情数据的时间。
   若间隔超过 ``timeout`` 秒，触发 CRITICAL 级别报警。
   通过 ``check()``（每根 K 线 / tick 调用）进行检查。

3. **异步报警** — ``Alerter`` 将报警推入 ``asyncio.Queue``，
   由后台任务消费并通过 POST 发送到 webhook URL（飞书 / Telegram）。
   策略引擎不会被网络 I/O 阻塞。
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Coroutine

from loguru import logger


# ---------------------------------------------------------------------------
# Alert level
# ---------------------------------------------------------------------------


class AlertLevel(IntEnum):
    """报警严重级别（数值越大越严重）。"""
    INFO = 0
    WARNING = 1
    ERROR = 2
    CRITICAL = 3


# ---------------------------------------------------------------------------
# Trade record (lightweight, for metrics only)
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class TradeRecord:
    """用于指标计算的最小交易记录。"""
    pnl: float
    bars_held: int
    timestamp: int = 0          # Unix ms


# ---------------------------------------------------------------------------
# MetricsCollector — incremental O(1) computation
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class TradeMetrics:
    """当前交易指标快照。"""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    avg_bars_held: float = 0.0
    sharpe_ratio: float = 0.0
    consecutive_losses: int = 0
    current_equity: float = 0.0
    peak_equity: float = 0.0


class MetricsCollector:
    """增量交易指标 — 每次更新 O(1)。

    所有计数器维护为运行总计，因此 ``current_metrics()``
    无需遍历完整交易历史。

    用法::

        collector = MetricsCollector(initial_equity=10000)
        collector.record_trade(TradeRecord(pnl=150.0, bars_held=5))
        collector.record_equity(10150.0, timestamp=1234567890)
        metrics = collector.current_metrics()
    """

    def __init__(self, initial_equity: float = 0.0) -> None:
        self._initial_equity = initial_equity

        # Incremental counters
        self._total_trades: int = 0
        self._win_count: int = 0
        self._loss_count: int = 0
        self._total_pnl: float = 0.0
        self._gross_profit: float = 0.0
        self._gross_loss: float = 0.0
        self._total_bars: int = 0
        self._consecutive_losses: int = 0

        # Equity tracking (incremental max drawdown)
        self._current_equity: float = initial_equity
        self._peak_equity: float = initial_equity
        self._max_drawdown: float = 0.0

        # Returns for Sharpe calculation (keep last N)
        self._returns: list[float] = []
        self._max_returns: int = 1000  # cap memory usage
        self._last_equity: float = initial_equity

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_trade(self, trade: TradeRecord) -> None:
        """记录已完成交易 — 增量更新所有计数器。"""
        self._total_trades += 1
        self._total_pnl += trade.pnl
        self._total_bars += trade.bars_held

        if trade.pnl > 0:
            self._win_count += 1
            self._gross_profit += trade.pnl
            self._consecutive_losses = 0
        else:
            self._loss_count += 1
            self._gross_loss += abs(trade.pnl)
            self._consecutive_losses += 1

    def record_equity(self, equity: float, timestamp: int = 0) -> None:
        """记录当前权益 — 更新峰值、回撤和 Sharpe 输入。

        Parameters:
            equity:    当前总权益（现金 + 未实现盈亏）。
            timestamp: Unix 毫秒（可选，用于日志记录）。
        """
        self._current_equity = equity

        # Incremental peak update
        if equity > self._peak_equity:
            self._peak_equity = equity

        # Incremental max drawdown
        if self._peak_equity > 0:
            dd = (self._peak_equity - equity) / self._peak_equity
            if dd > self._max_drawdown:
                self._max_drawdown = dd

        # Return for Sharpe
        if self._last_equity > 0:
            ret = (equity - self._last_equity) / self._last_equity
            self._returns.append(ret)
            if len(self._returns) > self._max_returns:
                self._returns = self._returns[-self._max_returns:]
        self._last_equity = equity

    # ------------------------------------------------------------------
    # Query — O(1)
    # ------------------------------------------------------------------

    def current_metrics(self) -> TradeMetrics:
        """返回当前指标快照 — 全部 O(1) 查询。"""
        win_rate = self._win_count / self._total_trades if self._total_trades else 0.0
        profit_factor = (
            self._gross_profit / self._gross_loss
            if self._gross_loss > 0 else float("inf")
        )
        avg_bars = self._total_bars / self._total_trades if self._total_trades else 0.0

        # Incremental Sharpe
        sharpe = 0.0
        if len(self._returns) > 1:
            avg = sum(self._returns) / len(self._returns)
            var = sum((r - avg) ** 2 for r in self._returns) / len(self._returns)
            std = math.sqrt(var)
            sharpe = (avg / std * math.sqrt(365)) if std > 0 else 0.0

        return TradeMetrics(
            total_trades=self._total_trades,
            winning_trades=self._win_count,
            losing_trades=self._loss_count,
            win_rate=win_rate,
            total_pnl=self._total_pnl,
            gross_profit=self._gross_profit,
            gross_loss=self._gross_loss,
            profit_factor=profit_factor,
            max_drawdown=self._max_drawdown,
            avg_bars_held=avg_bars,
            sharpe_ratio=sharpe,
            consecutive_losses=self._consecutive_losses,
            current_equity=self._current_equity,
            peak_equity=self._peak_equity,
        )

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def reset(self, initial_equity: float | None = None) -> None:
        """重置所有计数器。"""
        eq = initial_equity if initial_equity is not None else self._initial_equity
        self.__init__(initial_equity=eq)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Heartbeat Monitor
# ---------------------------------------------------------------------------


class HeartbeatMonitor:
    """检测过期的行情数据 / 网络连接。

    引擎每次接收到行情数据时调用 ``tick()``。
    ``check()`` 将经过的时间与 ``timeout`` 进行比较，
    若心跳过期则触发 CRITICAL 级别报警。

    用法::

        hb = HeartbeatMonitor(timeout=30.0, on_stale=my_alert_func)
        # 在数据循环中：
        hb.tick()                          # received data
        hb.check()                         # 检查是否过期（正常返回 True）
    """

    def __init__(
        self,
        timeout: float = 30.0,
        on_stale: Callable[[], None] | None = None,
    ) -> None:
        self._timeout = timeout
        self._last_tick: float = time.monotonic()
        self._on_stale = on_stale
        self._stale_count: int = 0
        self._is_stale: bool = False

    def tick(self) -> None:
        """记录心跳 — 每次收到行情数据时调用。"""
        self._last_tick = time.monotonic()
        if self._is_stale:
            logger.info("Heartbeat recovered after %d stale checks", self._stale_count)
        self._is_stale = False
        self._stale_count = 0

    def check(self) -> bool:
        """检查心跳是否仍然新鲜。

        Returns:
            ``True`` 表示正常（数据新鲜），``False`` 表示过期。
        """
        elapsed = time.monotonic() - self._last_tick
        if elapsed > self._timeout:
            self._stale_count += 1
            self._is_stale = True
            if self._on_stale is not None:
                self._on_stale()
            return False
        return True

    @property
    def is_stale(self) -> bool:
        return self._is_stale

    @property
    def seconds_since_last_tick(self) -> float:
        return time.monotonic() - self._last_tick

    def reset(self) -> None:
        self._last_tick = time.monotonic()
        self._is_stale = False
        self._stale_count = 0


# ---------------------------------------------------------------------------
# Alert configuration
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AlertConfig:
    """报警器配置。

    Attributes:
        drawdown_threshold:   触发 WARNING 的回撤比例（0.10 = 10%）。
        loss_threshold:       触发 WARNING 的单笔亏损金额。
        consecutive_losses:   连续亏损 N 次 → WARNING。
        heartbeat_timeout:    心跳过期秒数 → CRITICAL。
        feishu_webhook:       飞书机器人 webhook URL（可选）。
        telegram_webhook:     Telegram 机器人 webhook URL（可选）。
        on_alert:             同步回调 ``(level, message) -> None``。
    """

    drawdown_threshold: float = 0.10
    loss_threshold: float = 500.0
    consecutive_losses: int = 5
    heartbeat_timeout: float = 30.0

    feishu_webhook: str = ""
    telegram_webhook: str = ""

    on_alert: Callable[[AlertLevel, str], None] | None = None


# ---------------------------------------------------------------------------
# Alerter — async webhook + local callbacks
# ---------------------------------------------------------------------------


class Alerter:
    """带异步 webhook 推送的报警管理器。

    - ``check()`` 每根 K 线调用，评估各项阈值。
    - ``alert()`` 可手动调用，用于任意事件。
    - Webhook 推送为非阻塞：报警推入 ``asyncio.Queue``，
      由后台任务发送。

    用法::

        alerter = Alerter(AlertConfig(feishu_webhook="https://..."))
        await alerter.start()
        alerter.check(equity=9000, initial=10000, metrics=collector.current_metrics())
        await alerter.stop()
    """

    def __init__(self, config: AlertConfig) -> None:
        self._config = config
        self._queue: asyncio.Queue[tuple[AlertLevel, str]] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._running = False

        # Dedup: suppress repeated identical alerts within a window
        self._recent: dict[str, float] = {}  # message → timestamp
        self._dedup_window: float = 60.0     # seconds

        # Counters for threshold tracking
        self._last_drawdown_alert: float = 0.0
        self._last_loss_alert: float = 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动后台 webhook 推送任务。"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._delivery_loop())
        logger.info("Alerter started — webhooks: feishu={}, telegram={}",
                     bool(self._config.feishu_webhook),
                     bool(self._config.telegram_webhook))

    async def stop(self) -> None:
        """停止推送任务并刷新剩余报警。"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------
    # Threshold checks (synchronous — called each bar)
    # ------------------------------------------------------------------

    def check(
        self,
        equity: float,
        initial_equity: float,
        metrics: TradeMetrics,
    ) -> None:
        """根据当前状态评估报警阈值。

        此方法为**同步**且非阻塞 — 仅将报警入队以供异步推送。
        """
        # Drawdown
        if initial_equity > 0:
            dd = (initial_equity - equity) / initial_equity
            if dd >= self._config.drawdown_threshold and dd > self._last_drawdown_alert:
                self._last_drawdown_alert = dd
                self.alert(
                    AlertLevel.WARNING,
                    f"Drawdown {dd:.2%} exceeds threshold "
                    f"{self._config.drawdown_threshold:.2%} "
                    f"(equity={equity:.2f}, initial={initial_equity:.2f})",
                )

        # Single-trade loss (via metrics)
        if metrics.total_pnl < -self._config.loss_threshold:
            if metrics.total_pnl < self._last_loss_alert:
                self._last_loss_alert = metrics.total_pnl
                self.alert(
                    AlertLevel.WARNING,
                    f"Total P&L {metrics.total_pnl:.2f} exceeds loss threshold "
                    f"{self._config.loss_threshold:.2f}",
                )

        # Consecutive losses
        if metrics.consecutive_losses >= self._config.consecutive_losses:
            self.alert(
                AlertLevel.WARNING,
                f"{metrics.consecutive_losses} consecutive losses "
                f"(threshold: {self._config.consecutive_losses})",
            )

    def check_heartbeat(self, is_stale: bool, seconds: float) -> None:
        """检查心跳状态 — 由 HeartbeatMonitor 回调调用。"""
        if is_stale:
            self.alert(
                AlertLevel.CRITICAL,
                f"No market data for {seconds:.1f}s — possible connection loss",
            )

    # ------------------------------------------------------------------
    # Manual alert
    # ------------------------------------------------------------------

    def alert(self, level: AlertLevel, message: str) -> None:
        """将报警入队以供异步推送。

        Parameters:
            level:   严重级别。
            message: 人类可读的报警文本。
        """
        # Dedup check
        key = f"{level.name}:{message}"
        now = time.monotonic()
        last = self._recent.get(key, 0.0)
        if now - last < self._dedup_window:
            return  # suppress duplicate
        self._recent[key] = now

        # Stale entry cleanup
        if len(self._recent) > 100:
            cutoff = now - self._dedup_window * 2
            self._recent = {k: v for k, v in self._recent.items() if v > cutoff}

        logger.log(level.name, "[ALERT] {}", message)

        # Sync callback
        if self._config.on_alert is not None:
            try:
                self._config.on_alert(level, message)
            except Exception as exc:
                logger.error("Alert callback error: {}", exc)

        # Async webhook queue (non-blocking put)
        try:
            self._queue.put_nowait((level, message))
        except asyncio.QueueFull:
            logger.warning("Alert queue full — dropping alert: {}", message)

    # ------------------------------------------------------------------
    # Async delivery loop
    # ------------------------------------------------------------------

    async def _delivery_loop(self) -> None:
        """后台任务：消费报警队列并 POST 到 webhook。"""
        while self._running:
            try:
                level, message = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return

            # Send to all configured webhooks
            tasks: list[asyncio.Task[bool]] = []
            if self._config.feishu_webhook:
                tasks.append(asyncio.create_task(
                    self._send_webhook(self._config.feishu_webhook, level, message, "feishu")
                ))
            if self._config.telegram_webhook:
                tasks.append(asyncio.create_task(
                    self._send_webhook(self._config.telegram_webhook, level, message, "telegram")
                ))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_webhook(
        self,
        url: str,
        level: AlertLevel,
        message: str,
        name: str,
    ) -> bool:
        """向 webhook URL POST 报警信息。

        支持飞书（Lark）和 Telegram 机器人格式。
        """
        try:
            import httpx

            if "feishu" in name or "lark" in url.lower():
                payload = {
                    "msg_type": "text",
                    "content": {
                        "text": f"[{level.name}] {message}"
                    },
                }
            elif "telegram" in name:
                payload = {
                    "text": f"[{level.name}] {message}",
                }
            else:
                payload = {"text": f"[{level.name}] {message}"}

            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code < 300:
                    logger.debug("Webhook {} sent OK (status={})", name, resp.status_code)
                    return True
                else:
                    logger.warning("Webhook {} returned status {}", name, resp.status_code)
                    return False
        except Exception as exc:
            logger.error("Webhook {} failed: {}", name, exc)
            return False
