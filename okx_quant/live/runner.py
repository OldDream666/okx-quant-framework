"""实盘/模拟盘交易引擎 — 连接策略到 OKX 真实/模拟环境。

架构::

    Strategy.on_bar()
        │
        ▼
    self.buy() / self.sell()
        │
        ▼
    RiskManager.submit()          ← 预交易检查
        │
        ▼
    LiveExecutor.submit()         ← 同步接口，推入异步队列
        │
        ▼
    OrderManager.submit_order()   ← REST → OKX API
        │
        ▼
    WebSocket push                ← 订单状态更新回传

K 线闭合检测：
    OKX 实时推送 candle 更新。当 ``confirmed`` 字段为 True 且时间戳
    超过上次处理的时间戳时，判定为新闭合 K 线，触发 ``strategy.on_bar()``。

优雅退出：
    捕获 SIGINT (Ctrl+C)，自动调用 ``strategy.on_stop()``，
    撤销所有挂单，关闭连接。
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Any, Callable, Coroutine
import uuid

from okx_quant.config.auth import OKXAuth
from okx_quant.config.settings import OKXConfig
from okx_quant.gateway.rest_client import RESTClient
from okx_quant.gateway.ws_client import WebSocketClient
from okx_quant.models.market import BarData, OKXAPIError, OrderData, OrderStatus
from okx_quant.oms.order_manager import OrderManager
from okx_quant.risk.risk_manager import RiskConfig, RiskManager
from okx_quant.strategy.base import BaseStrategy, Position, StrategyExecutor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LiveExecutor — 同步接口 + 异步队列
# ---------------------------------------------------------------------------


class LiveExecutor(StrategyExecutor):
    """同步策略接口 → 异步订单队列的桥接执行器。

    当策略调用 ``self.buy(size)`` 时，订单被推入 ``asyncio.Queue``，
    由后台任务消费并提交到 OrderManager。

    - 市价单：fire-and-forget，返回空字符串
    - 限价/止损单：等待实际 order_id 后返回（用于后续撤单）
    """

    def __init__(self, oms: OrderManager, symbol: str) -> None:
        self._oms = oms
        self._symbol = symbol
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

        # order_id 响应通道：request_id → asyncio.Future[str]
        self._pending_futures: dict[str, asyncio.Future[str]] = {}
        # request_id → 实际 order_id 映射（用于 cancel 时查找）
        self._request_to_order: dict[str, str] = {}

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
        """提交订单到异步队列。

        返回 request_id 字符串作为订单句柄，供后续 cancel() 使用。
        后台任务处理完成后，request_id 会被映射到真实的 OKX order_id。
        """
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
        """将撤销请求加入队列。

        order_id 可以是 request_id（未处理完的订单）或实际的 OKX order_id。
        """
        if not order_id:
            return False
        # 如果是 request_id 且已映射到实际 order_id，直接用实际 order_id
        real_order_id = self._request_to_order.get(order_id, order_id)
        self._queue.put_nowait({
            "request_id": "cancel",
            "order_id": real_order_id,
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
                request_id = item.get("request_id", "")

                # 撤单请求
                if request_id == "cancel":
                    order_id = item.get("order_id", "")
                    await self._oms.cancel_order(self._symbol, order_id)
                    logger.info("已撤单: %s", order_id)
                    continue

                # 下单请求
                result = await self._oms.submit_order(
                    symbol=self._symbol,
                    side=item["side"],
                    order_type=item["order_type"],
                    size=str(item["quantity"]),
                    price=str(item["price"]) if item["price"] else None,
                    pos_side=item.get("pos_side"),
                )
                ord_id = result.order_id  # OrderData 属性，非 dict.get()

                logger.info(
                    "已下单: %s %s %s 数量=%s → ordId=%s",
                    item["side"], item["order_type"], self._symbol,
                    item["quantity"], ord_id,
                )

                # 如果有等待的 Future（限价/止损单），设置结果
                future = self._pending_futures.pop(request_id, None)
                if future and not future.done():
                    future.set_result(ord_id)

                # 记录 request_id → order_id 映射
                self._request_to_order[request_id] = ord_id

            except (OKXAPIError, OSError, asyncio.CancelledError) as exc:
                logger.error("订单处理错误: %s", exc)
                # 设置异常给等待的 Future
                request_id = item.get("request_id", "")
                future = self._pending_futures.pop(request_id, None)
                if future and not future.done():
                    future.set_exception(exc)
            except Exception as exc:
                logger.error("订单处理未知错误: %s", exc, exc_info=True)
                request_id = item.get("request_id", "")
                future = self._pending_futures.pop(request_id, None)
                if future and not future.done():
                    future.set_exception(exc)


# ---------------------------------------------------------------------------
# LiveRunner
# ---------------------------------------------------------------------------


class LiveRunner:
    """实盘/模拟交易运行器 — 主入口。

    串联：Strategy + RiskManager + OrderManager + REST + WebSocket。
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
        ledger: Any = None,
        preload_bars: int = 900,
    ) -> None:
        self._config = config
        self._strategy = strategy
        self._strategy_params = strategy_params or {}
        self._symbol = symbol
        self._timeframe = timeframe
        self._bar_callback = bar_callback
        self._leverage = leverage
        self._ledger = ledger
        self._preload_bars = preload_bars

        # 认证
        self._auth = OKXAuth(config)

        # 组件（在 start() 中创建）
        self._rest: RESTClient | None = None
        self._ws_public: WebSocketClient | None = None
        self._ws_private: WebSocketClient | None = None
        self._oms: OrderManager | None = None
        self._risk: RiskManager | None = None
        self._executor: LiveExecutor | None = None

        # 风控配置
        self._risk_config = risk_config or RiskConfig()

        # K 线跟踪
        self._last_bar_ts: int = 0
        self._processed_bars: set[int] = set()  # 防止并发重复处理
        self._last_sync_time: float = 0.0  # 持仓同步节流
        self._bar_count: int = 0

        # 状态
        self._running = False

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动实盘运行器。"""
        logger.info("=" * 60)
        logger.info("  LiveRunner 启动中")
        logger.info("  交易对: %s | 周期: %s | 模拟盘: %s",
                     self._symbol, self._timeframe, self._config.is_demo)
        logger.info("=" * 60)

        # 1. 初始化 REST 客户端
        self._rest = RESTClient(self._config, self._auth)
        await self._rest.connect()
        logger.info("✅ REST 客户端已连接")

        # 2. 查询账户余额
        try:
            balance = await self._rest.get_balance()
            logger.info("💰 账户权益: $%.2f", balance.total_equity)
            self._risk_config.initial_equity = balance.total_equity
            self._strategy.state.cash = balance.total_equity
        except Exception as exc:
            logger.warning("查询余额失败: %s", exc)

        # 2b. 查询现有持仓，初始化策略仓位
        try:
            positions = await self._rest.get_positions()
            for pos in positions:
                if pos.symbol != self._symbol:
                    continue
                strategy_pos = Position(
                    side=pos.side.value if pos.side.value in ("long", "short") else "long",
                    quantity=abs(pos.quantity),
                    avg_price=pos.avg_price,
                    contract_multiplier=pos.contract_multiplier,
                )
                if pos.side.value == "long" or (pos.side.value == "net" and pos.quantity >= 0):
                    self._strategy.state.position_long = strategy_pos
                    logger.info("📈 同步多头持仓: %.6f @ %.2f", strategy_pos.quantity, strategy_pos.avg_price)
                else:
                    self._strategy.state.position_short = strategy_pos
                    logger.info("📉 同步空头持仓: %.6f @ %.2f", strategy_pos.quantity, strategy_pos.avg_price)
            if not positions:
                logger.info("📭 无现有持仓")
        except Exception as exc:
            logger.warning("查询持仓失败: %s", exc)

        # 3. 设置杠杆
        if self._leverage > 1:
            try:
                await self._rest.set_leverage(self._symbol, self._leverage)
                logger.info("⚙️ 杠杆已设置为 %dx (%s)", self._leverage, self._symbol)
            except Exception as exc:
                logger.warning("设置杠杆失败: %s", exc)

        # 4. 预加载历史 K 线
        await self._preload_history(self._preload_bars)

        # 5. 初始化 OrderManager
        self._oms = OrderManager(self._rest, None)

        # 6. 初始化执行器
        self._executor = LiveExecutor(self._oms, self._symbol)
        await self._executor.start()

        # 7. 初始化 RiskManager
        self._risk = RiskManager(self._risk_config, self._executor)
        self._risk._on_kill = self._on_kill_switch
        logger.info("🛡️ 风控管理器已配置")

        # 8. 注入执行器到策略
        self._strategy._executor = self._risk
        self._strategy.state.symbol = self._symbol
        self._strategy.state.cash = self._risk_config.initial_equity

        # 9. 初始化 WebSocket
        # 公共 WS：candle 频道（必须用 /business 端点）
        candle_url = "wss://ws.okx.com:8443/ws/v5/business"
        self._ws_public = WebSocketClient(candle_url)
        self._ws_public.subscribe_candles(
            self._symbol, self._timeframe, self._on_candle
        )
        logger.info("📡 公共 WS → %s %s K线（生产端点）",
                     self._symbol, self._timeframe)

        # 私有 WS：订单推送
        self._ws_private = WebSocketClient(self._config.ws_private, auth=self._auth)
        self._ws_private.subscribe_orders(self._on_order_update)
        self._oms._ws = self._ws_private
        logger.info("📡 私有 WS → 订单推送 (%s)",
                     "demo" if self._config.is_demo else "production")

        # 注册订单推送订阅（必须在 WS 注入之后）
        await self._oms.start(inst_type="SWAP")

        # 10. 策略初始化
        self._strategy.on_init(self._strategy_params)
        self._strategy.on_start()
        logger.info("🚀 策略 '%s' 已初始化", self._strategy.name)

        # 11. 注册信号处理
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        # 12. 连接 WebSocket
        self._running = True
        logger.info("=" * 60)
        logger.info("  ✅ LiveRunner 已启动 — 正在连接 WebSocket...")
        logger.info("  按 Ctrl+C 停止")
        logger.info("=" * 60)

        await self._ws_public.connect()
        await self._ws_private.connect()

        # 保持运行直到关闭
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """优雅关闭。"""
        if not self._running:
            return
        self._running = False

        logger.info("\n🛑 正在关闭...")

        # 1. 策略清理
        try:
            self._strategy.on_stop()
            logger.info("  ✅ 策略 on_stop() 已调用")
        except Exception as exc:
            logger.error("  ❌ on_stop 错误: %s", exc)

        # 2. 撤销所有挂单
        if self._oms:
            active = self._oms.get_active_orders(self._symbol)
            for order in active:
                try:
                    await self._oms.cancel_order(self._symbol, order.order_id)
                    logger.info("  🗑️ 已撤销订单 %s", order.order_id)
                except Exception as exc:
                    logger.error("  ❌ 撤单失败 %s: %s", order.order_id, exc)

        # 3. 停止执行器
        if self._executor:
            await self._executor.stop()

        # 4. 断开连接
        if self._ws_public:
            await self._ws_public.disconnect()
        if self._ws_private:
            await self._ws_private.disconnect()
        if self._rest:
            await self._rest.close()

        logger.info("  ✅ 关闭完成")

    # ------------------------------------------------------------------
    # 历史数据预加载
    # ------------------------------------------------------------------

    async def _preload_history(self, count: int = 900) -> None:
        """拉取历史 K 线并预填充策略 bar 窗口。"""
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
                # batch 已升序（最旧在前），after 参数含义是"取比它更旧的"
                # 所以用 batch[0]（本批最旧）作为下一页起点
                after = int(batch[0].timestamp)
                remaining -= len(batch)
                logger.info("  📥 已获取 %d 根 (累计: %d)", len(batch), len(all_bars))

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
    # K 线回调
    # ------------------------------------------------------------------

    async def _on_candle(self, bar: BarData) -> None:
        """处理 WebSocket 推送的 K 线数据。"""
        if not bar.confirmed:
            return  # K 线未闭合

        if bar.timestamp <= self._last_bar_ts:
            return  # 已处理

        # 并发防重：如果另一个协程正在处理同一根 bar，跳过
        if bar.timestamp in self._processed_bars:
            return
        self._processed_bars.add(bar.timestamp)

        # 新的闭合 K 线
        self._last_bar_ts = bar.timestamp
        self._bar_count += 1

        # 定期清理已处理 K 线集合，防止内存泄漏
        if len(self._processed_bars) > 10000:
            self._processed_bars = {bar.timestamp}

        logger.info(
            "📊 Bar #%d closed: %s O=%.2f H=%.2f L=%.2f C=%.2f",
            self._bar_count, self._symbol,
            bar.open, bar.high, bar.low, bar.close,
        )

        # 更新策略 K 线窗口
        self._strategy.bars.append(bar)
        if len(self._strategy.bars) > 1000:
            self._strategy.bars = self._strategy.bars[-1000:]
        self._strategy.state.bar_index = self._bar_count

        # 更新风控参考价
        if self._risk:
            self._risk.update_market_price(bar.close)

        # 调用风控账户检查（敞口/杠杆/回撤/Kill Switch）
        if self._risk:
            positions = []
            equity = self._strategy.state.cash
            for p in (self._strategy.position_long, self._strategy.position_short):
                if p and p.quantity > 0:
                    positions.append(p)
                    equity += p.unrealized_pnl(bar.close)
            try:
                self._risk.check_account(equity, positions, bar.close)
            except Exception as exc:
                logger.error("❌ 风控检查错误: %s", exc)

        # 分发给策略
        try:
            signal = self._strategy.on_bar(bar)
            if signal and signal.action != "HOLD":
                logger.info(
                    "📡 信号: %s (置信度=%.2f) %s",
                    signal.action, signal.confidence, signal.reason,
                )
        except Exception as exc:
            logger.error("❌ 策略错误: %s", exc)

        # 记录权益到账本
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
                    drawdown=0.0,
                    ts=bar.timestamp,
                )
            except Exception as exc:
                logger.debug("账本权益写入错误: %s", exc)

        # 外部回调
        if self._bar_callback:
            try:
                self._bar_callback(bar)
            except Exception as exc:
                logger.error("❌ K线回调错误: %s", exc)

    # ------------------------------------------------------------------
    # 订单更新回调
    # ------------------------------------------------------------------

    async def _on_order_update(self, order: OrderData) -> None:
        """处理 WebSocket 推送的订单状态更新。"""
        logger.info(
            "📋 订单更新: %s %s %s → %s (已成交=%.6f/%.6f)",
            order.order_id, order.side.value, order.symbol,
            order.status.value, order.filled_qty, order.quantity,
        )

        # 通知风控（滑点监控）
        if self._risk and order.status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED):
            self._risk.on_fill(
                side=order.side.value,
                fill_price=order.avg_price,
                target_price=order.price,
                quantity=order.filled_qty,
                pos_side=order.pos_side,
            )

        # 成交后同步策略持仓（从 OKX 查询真实持仓，避免增量计算误差）
        if order.status == OrderStatus.FILLED:
            await self._sync_positions_from_exchange()
        elif order.status == OrderStatus.PARTIALLY_FILLED:
            # 节流：部分成交频繁，避免触发 OKX 频率限制
            now = time.monotonic()
            if now - self._last_sync_time > 5.0:  # 最多 5 秒同步一次
                await self._sync_positions_from_exchange()
                self._last_sync_time = now

        # 记录到账本
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
                logger.debug("账本成交写入错误: %s", exc)

    async def _sync_positions_from_exchange(self) -> None:
        """从 OKX 查询真实持仓并同步到策略。"""
        if not self._rest:
            return
        try:
            positions = await self._rest.get_positions()
            self._strategy.state.position_long = None
            self._strategy.state.position_short = None
            for pos in positions:
                if pos.symbol != self._symbol or pos.quantity == 0:
                    continue
                strategy_pos = Position(
                    side=pos.side.value if pos.side.value in ("long", "short") else "long",
                    quantity=abs(pos.quantity),
                    avg_price=pos.avg_price,
                    contract_multiplier=pos.contract_multiplier,
                )
                if pos.side.value == "long" or (pos.side.value == "net" and pos.quantity >= 0):
                    self._strategy.state.position_long = strategy_pos
                else:
                    self._strategy.state.position_short = strategy_pos
            logger.info(
                "🔄 持仓同步: long=%s short=%s",
                f"{self._strategy.position_long.quantity:.6f}" if self._strategy.position_long else "无",
                f"{self._strategy.position_short.quantity:.6f}" if self._strategy.position_short else "无",
            )
        except Exception as exc:
            logger.warning("持仓同步失败: %s", exc)

    # ------------------------------------------------------------------
    # Kill Switch 回调
    # ------------------------------------------------------------------

    def _on_kill_switch(self) -> None:
        """RiskManager 触发 Kill Switch 时调用。"""
        logger.critical("🚨 KILL SWITCH 已触发 — 正在撤销所有订单")
        if self._oms:
            active = self._oms.get_active_orders(self._symbol)
            for order in active:
                asyncio.ensure_future(
                    self._oms.cancel_order(self._symbol, order.order_id)
                )

        # 市价平掉所有持仓
        pos_long = self._strategy.position_long
        if pos_long and pos_long.quantity > 0:
            logger.critical("🚨 Kill Switch: 市价平多仓 %.6f", pos_long.quantity)
            if self._oms:
                asyncio.ensure_future(self._oms.submit_order(
                    self._symbol, "sell", "market", str(pos_long.quantity),
                    pos_side="long",
                ))

        pos_short = self._strategy.position_short
        if pos_short and pos_short.quantity > 0:
            logger.critical("🚨 Kill Switch: 市价平空仓 %.6f", pos_short.quantity)
            if self._oms:
                asyncio.ensure_future(self._oms.submit_order(
                    self._symbol, "buy", "market", str(pos_short.quantity),
                    pos_side="short",
                ))
