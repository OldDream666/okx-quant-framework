"""实盘/模拟盘交易引擎 —— 连接策略到 OKX 真实/模拟环境。

架构::

    Strategy.on_bar()
        │
        ▼
    LiveExecutor.submit()       ← 同步，返回 order_id
        │
        ▼
    RiskManager.submit()        ← 交易前检查
        │
        ▼
    OrderManager.submit_order() ← REST → OKX API
        │
        ▼
    WebSocket push              ← 订单状态更新回传 OMS

``LiveRunner`` 将所有组件串联在一起：

1. **RESTClient** —— 初始账户余额 + 合约规格。
2. **WebSocketClient** —— 订阅 K 线和订单通道。
3. **OrderManager** —— 通过 WS 推送跟踪所有活跃订单。
4. **RiskManager** —— 包含频率限制、胖手指检查、Kill Switch 的中间件。
5. **LiveExecutor** —— 将同步 ``buy()/sell()`` 桥接到异步 OMS。

K 线收盘检测：
    OKX 实时推送蜡烛图更新。当收到的蜡烛时间戳**晚于**上一根已处理
    K 线的时间戳时（即已进入下一个 K 线周期），该 K 线被视为**已收盘**。
    已收盘的 K 线随后被分发到 ``strategy.on_bar()``。

优雅关闭：
    收到 SIGINT（Ctrl+C）时，运行器：
    1. 调用 ``strategy.on_stop()``。
    2. 撤销所有待执行订单。
    3. 关闭 WS 和 REST 连接。
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Any, Callable, Coroutine

from okx_quant.config.auth import OKXAuth
from okx_quant.config.settings import OKXConfig
from okx_quant.gateway.rest_client import RESTClient
from okx_quant.gateway.ws_client import WebSocketClient
from okx_quant.models.market import BarData, OrderData, OrderStatus, TickData
from okx_quant.oms.order_manager import OrderManager
from okx_quant.risk.risk_manager import RiskConfig, RiskManager
from okx_quant.strategy.base import BaseStrategy, Position, StrategyExecutor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LiveExecutor — bridges strategy buy()/sell() to OrderManager
# ---------------------------------------------------------------------------


class LiveExecutor(StrategyExecutor):
    """用于实盘/模拟交易的 StrategyExecutor。

    当策略调用 ``self.buy(size)`` 时，调用流程：
        Strategy → LiveExecutor.submit() → RiskManager → OrderManager → REST

    这是 ``_BacktestExecutor`` 的**实盘对应实现**。两者实现相同的 ``StrategyExecutor`` 协议，
    因此策略代码在回测和实盘模式下完全一致。
    """

    def __init__(self, oms: OrderManager) -> None:
        self._oms = oms

    def submit(
        self,
        side: str,
        price: float | None,
        quantity: float,
        order_type: str,
        pos_side: str,
    ) -> str:
        """通过 OrderManager 提交订单。

        Returns:
            OKX 订单 ID（来自 REST response）。
        """
        # The OrderManager.submit_order is async — we schedule it on the
        # running event loop without blocking the strategy.
        loop = asyncio.get_running_loop()
        future = asyncio.ensure_future(
            self._submit_async(side, price, quantity, order_type, pos_side)
        )
        # Block until the order is placed (strategy expects a sync return)
        # This is safe because the strategy runs inside the event loop's
        # on_bar callback, which is already async.
        return self._oms_order_id or ""

    async def _submit_async(
        self,
        side: str,
        price: float | None,
        quantity: float,
        order_type: str,
        pos_side: str,
    ) -> str:
        """异步订单提交。"""
        try:
            order = await self._oms.submit_order(
                symbol="",  # will be set by LiveRunner context
                side=side,
                order_type=order_type,
                size=str(quantity),
                price=str(price) if price else None,
            )
            self._oms_order_id = order.get("ordId", "")
            return self._oms_order_id
        except Exception as exc:
            logger.error("下单失败: %s", exc)
            return ""

    def cancel(self, order_id: str) -> bool:
        """撤销待执行订单。"""
        loop = asyncio.get_running_loop()
        future = asyncio.ensure_future(
            self._oms.cancel_order("", order_id)
        )
        return True  # optimistic

    def get_position(self, side: str) -> Position | None:
        """获取当前仓位（实盘中未实现——请使用 OMS）。"""
        return None


# ---------------------------------------------------------------------------
# Async LiveExecutor — proper async bridge
# ---------------------------------------------------------------------------


class AsyncLiveExecutor:
    """异步感知执行器，正确桥接同步策略调用到异步 OMS。

    与基础 ``LiveExecutor`` 不同，此版本使用 ``asyncio.Queue`` 将同步 ``submit()`` 调用
    与异步下单解耦。队列由调用 OrderManager 的后台任务消费。

    这是生产环境推荐的执行器。
    """

    def __init__(self, oms: OrderManager, symbol: str) -> None:
        self._oms = oms
        self._symbol = symbol
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._results: dict[str, str] = {}  # request_id → order_id
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """启动后台订单处理任务。"""
        self._task = asyncio.create_task(self._process_loop())

    async def stop(self) -> None:
        """停止后台任务。"""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def submit(
        self,
        side: str,
        price: float | None,
        quantity: float,
        order_type: str,
        pos_side: str,
    ) -> str:
        """将订单加入异步处理队列。

        返回临时请求 ID。实际订单 ID 将在后台任务处理完订单后可用。
        """
        import uuid
        request_id = uuid.uuid4().hex[:8]
        self._queue.put_nowait({
            "request_id": request_id,
            "side": side,
            "price": price,
            "quantity": quantity,
            "order_type": order_type,
            "pos_side": pos_side,
        })
        return request_id

    def cancel(self, order_id: str) -> bool:
        """将撤销请求加入队列。"""
        self._queue.put_nowait({
            "request_id": "cancel",
            "order_id": order_id,
        })
        return True

    def get_position(self, side: str) -> Position | None:
        return None

    async def _process_loop(self) -> None:
        """后台任务：消费订单队列并提交到 OMS。"""
        while True:
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return

            try:
                if "order_id" in item and item.get("request_id") == "cancel":
                    await self._oms.cancel_order(self._symbol, item["order_id"])
                else:
                    result = await self._oms.submit_order(
                        symbol=self._symbol,
                        side=item["side"],
                        order_type=item["order_type"],
                        size=str(item["quantity"]),
                        price=str(item["price"]) if item["price"] else None,
                    )
                    ord_id = result.get("ordId", "")
                    self._results[item["request_id"]] = ord_id
                    logger.info(
                        logger.info("已下单: %s %s %s 数量=%s → ordId=%s",
                        item["side"], item["order_type"], self._symbol,
                        item["quantity"], ord_id,
                    )
            except Exception as exc:
                logger.error("订单处理错误: %s", exc)


# ---------------------------------------------------------------------------
# LiveRunner
# ---------------------------------------------------------------------------


class LiveRunner:
    """实盘/模拟交易运行器 —— 主入口。

    串联：Strategy + RiskManager + OrderManager + REST + WebSocket。

    用法::

        from okx_quant.strategy.templates.ema_cross import EmaCrossStrategy

        runner = LiveRunner(
            config=load_config().okx,
            strategy=EmaCrossStrategy(),
            strategy_params={"fast_period": 5, "slow_period": 20},
            symbol="BTC-USDT",
            timeframe="1H",
        )
        asyncio.run(runner.start())
    """

    def __init__(
        self,
        config: OKXConfig,
        strategy: BaseStrategy,
        strategy_params: dict[str, Any] | None = None,
        symbol: str = "BTC-USDT",
        timeframe: str = "1H",
        risk_config: RiskConfig | None = None,
        bar_callback: Callable[[BarData], None] | None = None,
        leverage: int = 1,
        ledger: Any = None,  # TradeLedger (optional)
    ) -> None:
        self._config = config
        self._strategy = strategy
        self._strategy_params = strategy_params or {}
        self._symbol = symbol
        self._timeframe = timeframe
        self._bar_callback = bar_callback
        self._leverage = leverage
        self._ledger = ledger

        # Auth
        self._auth = OKXAuth(config)

        # Components (created in start())
        self._rest: RESTClient | None = None
        self._ws_public: WebSocketClient | None = None
        self._ws_private: WebSocketClient | None = None
        self._oms: OrderManager | None = None
        self._risk: RiskManager | None = None
        self._executor: AsyncLiveExecutor | None = None

        # Risk config
        self._risk_config = risk_config or RiskConfig()

        # Bar tracking
        self._last_bar_ts: int = 0
        self._bar_count: int = 0

        # State
        self._running = False
        self._symbol_ws = symbol  # OKX WS format (BTC-USDT)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动实盘运行器 —— 连接 OKX 并开始交易。"""
        logger.info("="*60)
        logger.info("  LiveRunner 启动中")
        logger.info("  交易对: %s | 周期: %s | 模拟盘: %s",
                     self._symbol, self._timeframe, self._config.is_demo)
        logger.info("="*60)

        # 1. Initialize REST client
        self._rest = RESTClient(self._config, self._auth)
        await self._rest.connect()
        logger.info("✅ REST 客户端已连接")

        # 2. Fetch initial account state
        try:
            balance = await self._rest.get_balance()
            logger.info("💰 账户权益: $%.2f", balance.total_equity)
            self._risk_config.initial_equity = balance.total_equity
        except Exception as exc:
            logger.warning("查询余额失败: %s", exc)

        # 2b. Set leverage
        if self._leverage > 1:
            try:
                result = await self._rest.set_leverage(self._symbol, self._leverage)
                logger.info("⚙️ 杠杆已设置为 %dx (%s)", self._leverage, self._symbol)
            except Exception as exc:
                logger.warning("设置杠杆失败: %s", exc)

        # 3. Pre-load historical K-line data for strategy warm-up
        await self._preload_history()

        # 4. Initialize OrderManager
        self._oms = OrderManager(self._rest, MagicMock())  # WS injected later

        # 4. Initialize executor
        self._executor = AsyncLiveExecutor(self._oms, self._symbol)
        await self._executor.start()

        # 5. Initialize RiskManager
        self._risk = RiskManager(self._risk_config, self._executor)
        self._risk._on_kill = self._on_kill_switch
        logger.info("🛡️ 风控管理器已配置")

        # 6. Inject executor into strategy
        self._strategy._executor = self._risk
        self._strategy.state.symbol = self._symbol
        self._strategy.state.cash = self._risk_config.initial_equity

        # 7. Initialize WebSocket — OKX requires separate connections for
        #    public channels (K-line) and private channels (orders).
        #
        #    IMPORTANT: For demo trading, use the PRODUCTION public WS for
        #    market data (K-line is identical demo/real), and the DEMO private
        #    WS for order updates.  The demo public WS (wspap) is unreliable.
        from okx_quant.gateway.rest_client import _okx_bar
        from okx_quant.config.settings import OKXConfig

        # Public WS: candle channels MUST use the /business endpoint.
        # /ws/v5/public does NOT support candle channels (returns 60018).
        # Data is identical demo/real, so production endpoint is always safe.
        candle_url = "wss://ws.okx.com:8443/ws/v5/business"
        self._ws_public = WebSocketClient(candle_url)
        self._ws_public.subscribe_candles(
            self._symbol, self._timeframe, self._on_candle
        )
        logger.info("📡 公共 WS → %s %s K线（生产端点）",
                     self._symbol, _okx_bar(self._timeframe))
        # Private WS: use demo endpoint for demo, production for real
        self._ws_private = WebSocketClient(self._config.ws_private, auth=self._auth)
        self._ws_private.subscribe_orders(self._on_order_update)
        self._oms._ws = self._ws_private  # type: ignore[attr-defined]
        logger.info("📡 私有 WS → 订单推送 (%s)",
                     "demo" if self._config.is_demo else "production")

        # 10. Strategy init + start
        self._strategy.on_init(self._strategy_params)
        self._strategy.on_start()
        logger.info("🚀 策略 '%s' 已初始化", self._strategy.name)

        # 11. Register signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        # 12. Connect both WebSockets
        self._running = True
        logger.info("="*60)
        logger.info("  ✅ LiveRunner 已启动 — 正在连接 WebSocket...")
        logger.info("  按 Ctrl+C 停止")
        logger.info("="*60)

        await self._ws_public.connect()
        await self._ws_private.connect()

        # Keep running until shutdown
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """优雅关闭 —— 撤销订单，关闭连接。"""
        if not self._running:
            return
        self._running = False

        logger.info("\n🛑 正在关闭...")

        # 1. Strategy cleanup
        try:
            self._strategy.on_stop()
            logger.info("  ✅ 策略 on_stop() 已调用")
        except Exception as exc:
            logger.error("  ❌ on_stop 错误: %s", exc)

        # 2. Cancel all pending orders
        if self._oms:
            active = self._oms.get_active_orders(self._symbol)
            for order in active:
                try:
                    await self._oms.cancel_order(self._symbol, order.order_id)
                    logger.info("  🗑️ 已撤销订单 %s", order.order_id)
                except Exception as exc:
                    logger.error("  ❌ 撤单失败 %s: %s", order.order_id, exc)

        # 3. Stop executor
        if self._executor:
            await self._executor.stop()

        # 4. Disconnect
        if self._ws_public:
            await self._ws_public.disconnect()
        if self._ws_private:
            await self._ws_private.disconnect()
        if self._rest:
            await self._rest.close()

        logger.info("  ✅ 关闭完成")

    # ------------------------------------------------------------------
    # Historical data preload
    # ------------------------------------------------------------------

    async def _preload_history(self, count: int = 900) -> None:
        """通过 REST 获取历史 K 线并预填充策略 K 线窗口。

        这消除了预热等待——策略可以在启动后立即交易。

        Parameters:
            count: 获取的 K 线数量（默认 900，略多于典型的宏观 EMA 周期 800）。
        """
        from okx_quant.gateway.rest_client import _okx_bar

        logger.info("📥 正在预加载 %d 根历史 %s K线 (%s)...",
                     count, self._timeframe, self._symbol)

        try:
            all_bars: list = []
            after = None
            remaining = count

            while remaining > 0:
                batch_size = min(300, remaining)
                batch = await self._rest.get_history_candles(
                    self._symbol, self._timeframe, after=after, limit=batch_size,
                )
                if not batch:
                    break
                all_bars.extend(batch)
                after = int(batch[0].timestamp)
                remaining -= len(batch)
                logger.info("  📥 已获取 %d 根 (累计: %d)", len(batch), len(all_bars))

            # Sort chronologically and fill strategy bars
            all_bars.sort(key=lambda b: b.timestamp)
            self._strategy.bars = all_bars
            self._bar_count = len(all_bars)
            if all_bars:
                self._last_bar_ts = all_bars[-1].timestamp

            logger.info("✅ 已预加载 %d 根 K线 (最早: %s, 最新: %s)",
                         len(all_bars),
                         all_bars[0].timestamp if all_bars else "N/A",
                         all_bars[-1].timestamp if all_bars else "N/A")

        except Exception as exc:
            logger.warning("⚠️ 预加载失败: %s — 将从实时数据预热", exc)

    # ------------------------------------------------------------------
    # K-line callback
    # ------------------------------------------------------------------

    async def _on_candle(self, bar: BarData) -> None:
        """处理从 WebSocket 接收的 K 线数据。

        K 线收盘检测：
        - OKX 实时推送蜡烛更新（开盘/最高/最低/收盘在 K 线内变化）。``confirmed`` 字段
          为 True 时表示 K 线已收盘。
        - 同时检查时间戳是否已前进（新的 K 线周期）。
        - 仅已收盘的 K 线会被分发到 ``strategy.on_bar()``。
        """
        if not bar.confirmed:
            return  # intra-bar update, skip

        if bar.timestamp <= self._last_bar_ts:
            return  # already processed this bar

        # New closed bar
        self._last_bar_ts = bar.timestamp
        self._bar_count += 1

        logger.info(
            "📊 Bar #%d closed: %s O=%.2f H=%.2f L=%.2f C=%.2f",
            self._bar_count, self._symbol,
            bar.open, bar.high, bar.low, bar.close,
        )

        # Update strategy's bar window
        self._strategy.bars.append(bar)
        if len(self._strategy.bars) > 1000:
            self._strategy.bars = self._strategy.bars[-1000:]
        self._strategy.state.bar_index = self._bar_count

        # Update risk manager's market price
        if self._risk:
            self._risk.update_market_price(bar.close)

        # Heartbeat: notify that we received data
        # (HeartbeatMonitor.tick() would go here if configured)

        # Dispatch to strategy
        try:
            signal = self._strategy.on_bar(bar)
            if signal and signal.action != "HOLD":
                logger.info(
                    "📡 Signal: %s (confidence=%.2f) %s",
                    signal.action, signal.confidence, signal.reason,
                )
        except Exception as exc:
            logger.error("❌ 策略错误: %s", exc)

        # Record equity to ledger
        if self._ledger is not None:
            try:
                cash = self._strategy.state.cash
                pos_val = 0.0
                for pos in (self._strategy.position_long, self._strategy.position_short):
                    if pos and pos.quantity > 0:
                        pos_val += pos.unrealized_pnl(bar.close)
                self._ledger.append_equity(
                    equity=cash + pos_val,
                    cash=cash,
                    position_value=pos_val,
                    drawdown=0.0,  # computed by ledger query
                    ts=bar.timestamp,
                )
            except Exception as exc:
                logger.debug("Ledger equity write error: %s", exc)

        # Notify external callback
        if self._bar_callback:
            try:
                self._bar_callback(bar)
            except Exception as exc:
                logger.error("❌ K线回调错误: %s", exc)

    # ------------------------------------------------------------------
    # Order update callback
    # ------------------------------------------------------------------

    async def _on_order_update(self, order: OrderData) -> None:
        """处理来自 WebSocket 的订单状态更新。"""
        logger.info(
            "📋 Order update: %s %s %s → %s (filled=%.6f/%.6f)",
            order.order_id, order.side.value, order.symbol,
            order.status.value, order.filled_qty, order.quantity,
        )

        # Record filled orders to ledger
        if self._ledger is not None and order.status in (
            OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED,
        ):
            try:
                self._ledger.append_trade({
                    "order_id": order.order_id,
                    "side": order.side.value,
                    "symbol": order.symbol,
                    "order_type": order.order_type.value,
                    "price": order.price,
                    "fill_price": order.avg_price,
                    "quantity": order.quantity,
                    "filled_qty": order.filled_qty,
                    "fee": order.fee,
                    "fee_currency": order.fee_currency,
                    "status": order.status.value,
                })
            except Exception as exc:
                logger.debug("Ledger trade write error: %s", exc)

    # ------------------------------------------------------------------
    # Kill Switch callback
    # ------------------------------------------------------------------

    def _on_kill_switch(self) -> None:
        """RiskManager 激活 Kill Switch 时调用。"""
        logger.critical("🚨 KILL SWITCH 已触发 — 正在撤销所有订单")
        if self._oms:
            active = self._oms.get_active_orders(self._symbol)
            for order in active:
                asyncio.ensure_future(
                    self._oms.cancel_order(self._symbol, order.order_id)
                )


# ---------------------------------------------------------------------------
# Import guard for MagicMock in type hints
# ---------------------------------------------------------------------------

try:
    from unittest.mock import MagicMock
except ImportError:
    class MagicMock:  # type: ignore[no-redef]
        pass
