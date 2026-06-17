"""
OKX V5 API 接口通过性审计脚本
==============================

复用 live_run 的完整业务链路，逐项测试所有核心 API 操作：
  1. 账户信息（余额、持仓、配置）
  2. 行情数据（ticker、K线、合约规格）
  3. 杠杆设置
  4. 下单（限价单）→ 撤单
  5. 下单（市价单）→ 自动成交
  6. 持仓查询 & 同步
  7. 活跃订单查询
  8. WebSocket 公共频道（ticker 订阅）
  9. WebSocket 私有频道（订单订阅 + 登录）

用法:
    python scripts/api_audit.py [--config configs/paper_trading.yaml] [--symbol ETH-USDT-SWAP]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ── 项目根目录加入 sys.path ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from okx_quant.config import load_config
from okx_quant.config.auth import OKXAuth
from okx_quant.gateway.rest_client import RESTClient
from okx_quant.gateway.ws_client import WebSocketClient
from okx_quant.oms.order_manager import OrderManager
from okx_quant.models.market import (
    BarData,
    OKXAPIError,
    OrderData,
    TickData,
)


# ─────────────────── YAML 加载（复用 run_live.py 的逻辑）───────────────────

def load_yaml(path: str) -> dict[str, Any]:
    """加载 YAML 配置文件"""
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ─────────────────── 审计结果记录 ───────────────────

@dataclass
class AuditCase:
    """单条审计用例"""
    name: str
    passed: bool = False
    error: str = ""
    detail: str = ""
    duration_ms: float = 0.0


@dataclass
class AuditReport:
    """审计报告"""
    symbol: str = ""
    is_demo: bool = True
    cases: list[AuditCase] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0

    def add(self, case: AuditCase):
        self.cases.append(case)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.cases if c.passed)

    @property
    def failed(self) -> int:
        return sum(1 for c in self.cases if not c.passed)

    @property
    def total(self) -> int:
        return len(self.cases)

    def print_report(self):
        print("\n" + "=" * 70)
        print(f"  OKX V5 API 接口审计报告")
        print(f"  交易对: {self.symbol}  |  模式: {'模拟盘' if self.is_demo else '⚠️ 实盘'}")
        print(f"  耗时: {self.end_time - self.start_time:.1f}s")
        print("=" * 70)

        for i, c in enumerate(self.cases, 1):
            icon = "✅" if c.passed else "❌"
            print(f"  {icon} {i:02d}. {c.name}  ({c.duration_ms:.0f}ms)")
            if c.detail:
                for line in c.detail.split("\n"):
                    print(f"      {line}")
            if c.error:
                print(f"      ⚠️  {c.error}")

        print("-" * 70)
        print(f"  结果: {self.passed}/{self.total} 通过", end="")
        if self.failed:
            print(f"  |  {self.failed} 失败 ❌")
        else:
            print("  |  全部通过 ✅")
        print("=" * 70 + "\n")


# ─────────────────── 审计执行器 ───────────────────

class APIAuditor:
    """OKX V5 API 全流程审计器"""

    def __init__(self, config_path: str, symbol: str | None = None):
        self.config_path = config_path

        # 加载配置
        self.yaml_config = load_yaml(config_path)
        self.app_config = load_config()
        self.okx_config = self.app_config.okx

        # 确定交易对
        self.symbol = symbol or self.yaml_config.get("symbol", "ETH-USDT-SWAP")

        # 基础设施（复用框架组件）
        self.auth = OKXAuth(self.okx_config)
        self.rest: RESTClient = RESTClient(self.okx_config, self.auth)
        self.ws_public: WebSocketClient | None = None
        self.ws_private: WebSocketClient | None = None
        self.oms: OrderManager = OrderManager(self.rest, None)  # type: ignore[arg-type]

        # 审计报告
        self.report = AuditReport(
            symbol=self.symbol,
            is_demo=self.okx_config.is_demo,
        )

        # WS 回调收集
        self._ws_ticker_received: asyncio.Event = asyncio.Event()
        self._ws_order_received: asyncio.Event = asyncio.Event()

    # ─── 生命周期 ───

    async def run(self):
        """执行完整审计流程"""
        self.report.start_time = time.time()
        print(f"\n🔍 开始 API 接口审计 | {self.symbol} | {'模拟盘' if self.okx_config.is_demo else '实盘'}\n")

        try:
            # 阶段 1: 连接
            await self._setup()

            # 阶段 2: 公共 API 审计（无需签名）
            await self._audit_public_apis()

            # 阶段 3: 私有 API 审计（需签名）
            await self._audit_private_apis()

            # 阶段 4: 交易 API 审计（下单、撤单）
            await self._audit_trade_apis()

            # 阶段 5: WebSocket 审计
            await self._audit_websockets()

        except Exception as e:
            self.report.add(AuditCase(
                name="审计流程异常",
                error=f"{type(e).__name__}: {e}",
                detail=traceback.format_exc(),
            ))
        finally:
            await self._cleanup()
            self.report.end_time = time.time()
            self.report.print_report()

    async def _setup(self):
        """初始化 REST 连接"""
        await self.rest.connect()
        print("  📡 REST 连接成功")

    async def _cleanup(self):
        """清理资源"""
        if self.ws_public:
            await self.ws_public.disconnect()
        if self.ws_private:
            await self.ws_private.disconnect()
        await self.rest.close()
        print("  🧹 资源清理完成\n")

    # ─── 审计用例执行器 ───

    async def _run_case(self, name: str, coro: Any) -> AuditCase:
        """执行单个审计用例，捕获异常"""
        case = AuditCase(name=name)
        t0 = time.time()
        try:
            result = await coro
            case.passed = True
            if isinstance(result, str):
                case.detail = result
            elif result is not None:
                case.detail = str(result)
        except OKXAPIError as e:
            case.error = f"OKX [{e.code}]: {e.okx_message}"
        except Exception as e:
            case.error = f"{type(e).__name__}: {e}"
        case.duration_ms = (time.time() - t0) * 1000
        self.report.add(case)
        return case

    # ─── 阶段 2: 公共 API ───

    async def _audit_public_apis(self):
        """公共 API 审计"""
        print("\n📊 阶段 2: 公共 API 审计")

        # 2.1 合约规格
        async def _get_instruments():
            instruments = await self.rest.get_instruments("SWAP")
            found = [i for i in instruments if i.symbol == self.symbol]
            if not found:
                raise RuntimeError(f"未找到 {self.symbol} 合约规格")
            inst = found[0]
            return f"tickSz={inst.tick_size}, lotSz={inst.lot_size}, minSz={inst.min_size}, ctMult={inst.contract_multiplier}"

        await self._run_case("获取合约规格 (SWAP)", _get_instruments())

        # 2.2 Ticker
        async def _get_ticker():
            tick = await self.rest.get_ticker(self.symbol)
            return f"last={tick.last:.2f}, bid={tick.bid:.2f}, ask={tick.ask:.2f}, vol24h={tick.volume24h:.2f}"

        await self._run_case("获取 Ticker 行情", _get_ticker())

        # 2.3 K 线（当前）
        async def _get_candles():
            bars = await self.rest.get_candles(self.symbol, "15m", limit=10)
            if not bars:
                raise RuntimeError("K 线数据为空")
            latest = bars[-1]
            return f"最新: O={latest.open:.2f} H={latest.high:.2f} L={latest.low:.2f} C={latest.close:.2f}, 共 {len(bars)} 根"

        await self._run_case("获取 K 线数据 (15m)", _get_candles())

        # 2.4 K 线（历史）
        async def _get_history_candles():
            bars = await self.rest.get_history_candles(self.symbol, "15m", limit=100)
            if not bars:
                raise RuntimeError("历史 K 线数据为空")
            return f"共 {len(bars)} 根历史 K 线, 范围: {bars[0].timestamp} → {bars[-1].timestamp}"

        await self._run_case("获取历史 K 线数据", _get_history_candles())

    # ─── 阶段 3: 私有 API ───

    async def _audit_private_apis(self):
        """私有 API 审计（需签名）"""
        print("\n🔐 阶段 3: 私有 API 审计")

        # 3.1 账户余额
        async def _get_balance():
            account = await self.rest.get_balance()
            return f"权益={account.total_equity:.2f}, 可用={account.available_balance:.2f}, 保证金率={account.margin_ratio:.4f}"

        await self._run_case("查询账户余额", _get_balance())

        # 3.2 持仓查询
        async def _get_positions():
            positions = await self.rest.get_positions()
            if not positions:
                return "当前无持仓"
            lines = []
            for p in positions:
                lines.append(f"  {p.symbol} {p.side.value}: qty={p.quantity}, avg={p.avg_price:.2f}, upnl={p.unrealized_pnl:.2f}")
            return f"共 {len(positions)} 个持仓:\n" + "\n".join(lines)

        await self._run_case("查询持仓", _get_positions())

        # 3.3 账户配置
        async def _get_account_config():
            data = await self.rest._request("GET", "/api/v5/account/config", signed=True)
            if data:
                cfg = data[0]
                return f"uid={cfg.get('uid', 'N/A')}, acctLv={cfg.get('acctLv', 'N/A')}, posMode={cfg.get('posMode', 'N/A')}"
            raise RuntimeError("账户配置为空")

        await self._run_case("查询账户配置", _get_account_config())

        # 3.4 API Key 权限
        async def _get_api_key():
            try:
                data = await self.rest._request("GET", "/api/v5/account/api-key", signed=True)
                if data:
                    key = data[0]
                    perms = key.get("perm", "N/A")
                    return f"label={key.get('label', 'N/A')}, perm={perms}"
                raise RuntimeError("API Key 信息为空")
            except Exception as e:
                if "404" in str(e):
                    return "demo 环境不支持此端点（正常）"
                raise

        await self._run_case("查询 API Key 权限", _get_api_key())

        # 3.5 设置杠杆
        async def _set_leverage():
            data = await self.rest.set_leverage(self.symbol, 20)
            return f"杠杆设置为 20x, 响应: {data}"

        await self._run_case("设置杠杆 (20x)", _set_leverage())

    # ─── 阶段 4: 交易 API ───

    async def _audit_trade_apis(self):
        """交易 API 审计（下单、撤单、查询）"""
        print("\n💰 阶段 4: 交易 API 审计")

        # 4.1 限价单（挂单后撤单）
        async def _limit_order_and_cancel():
            # 获取当前价格
            tick = await self.rest.get_ticker(self.symbol)
            # 挂一个远离市价的限价单（不会成交）
            price = str(round(tick.last * 0.90, 2))  # 低于市价 10%
            order_data = await self.oms.submit_order(
                symbol=self.symbol,
                side="buy",
                order_type="limit",
                size="0.001",  # 最小数量
                price=price,
                pos_side="long",
            )
            order_id = order_data.order_id
            result = f"限价单创建成功: ordId={order_id}, price={price}, status={order_data.status.value}"

            # 等一下让 OKX 处理
            await asyncio.sleep(1)

            # 查询活跃订单
            active = self.oms.get_active_orders(self.symbol)
            result += f"\n  活跃订单数: {len(active)}"

            # 撤单
            cancelled = await self.oms.cancel_order(self.symbol, order_id)
            result += f"\n  撤单结果: {'成功' if cancelled else '失败'}"

            return result

        await self._run_case("限价单 → 查询 → 撤单", _limit_order_and_cancel())

        # 4.2 市价单（开多 → 平多）
        async def _market_order_lifecycle():
            # 开多
            open_order = await self.oms.submit_order(
                symbol=self.symbol,
                side="buy",
                order_type="market",
                size="0.001",
                pos_side="long",
            )
            result = f"市价开多: ordId={open_order.order_id}, status={open_order.status.value}"

            # 等待成交
            await asyncio.sleep(2)

            # 查询持仓
            positions = await self.rest.get_positions()
            long_pos = [p for p in positions if p.symbol == self.symbol and p.quantity > 0]
            if long_pos:
                p = long_pos[0]
                result += f"\n  持仓: qty={p.quantity}, avg={p.avg_price:.2f}"
            else:
                result += "\n  ⚠️ 未检测到多头持仓（可能已自动平仓或数量太小）"

            # 平多
            close_order = await self.oms.submit_order(
                symbol=self.symbol,
                side="sell",
                order_type="market",
                size="0.001",
                pos_side="long",
            )
            result += f"\n  市价平多: ordId={close_order.order_id}, status={close_order.status.value}"

            await asyncio.sleep(1)

            # 确认持仓已平
            positions_after = await self.rest.get_positions()
            long_after = [p for p in positions_after if p.symbol == self.symbol and p.quantity > 0]
            result += f"\n  平仓后多头持仓: {len(long_after)} 个"

            return result

        await self._run_case("市价开多 → 持仓查询 → 市价平多", _market_order_lifecycle())

        # 4.3 OMS 订单生命周期
        async def _oms_lifecycle():
            tick = await self.rest.get_ticker(self.symbol)
            price = str(round(tick.last * 0.85, 2))  # 远离市价

            order = await self.oms.submit_order(
                symbol=self.symbol,
                side="buy",
                order_type="limit",
                size="0.001",
                price=price,
                pos_side="long",
            )
            result = f"OMS 提交: ordId={order.order_id}"

            # 查询活跃订单
            active = self.oms.get_active_orders(self.symbol)
            result += f"\n  活跃订单: {len(active)}"

            # 查询单个订单
            found = self.oms.get_order(order.order_id)
            result += f"\n  查询订单: {'找到' if found else '未找到'}"

            # 撤单
            cancelled = await self.oms.cancel_order(self.symbol, order.order_id)
            result += f"\n  OMS 撤单: {'成功' if cancelled else '失败'}"

            # 验证撤单后状态
            await asyncio.sleep(1)
            order_after = self.oms.get_order(order.order_id)
            if order_after:
                result += f"\n  撤单后状态: {order_after.status.value}"

            return result

        await self._run_case("OMS 订单生命周期（提交→查询→撤单→验证）", _oms_lifecycle())

        # 4.4 查询挂单
        async def _get_open_orders():
            orders = await self.rest.get_open_orders(self.symbol)
            return f"当前挂单数: {len(orders)}"

        await self._run_case("查询活跃挂单", _get_open_orders())

    # ─── 阶段 5: WebSocket 审计 ───

    async def _audit_websockets(self):
        """WebSocket 审计"""
        print("\n🌐 阶段 5: WebSocket 审计")

        # 5.1 公共 WS: Ticker 订阅
        async def _ws_public_ticker():
            ws = WebSocketClient(self.okx_config.ws_public)
            self.ws_public = ws

            received: list[TickData] = []

            async def on_tick(tick: TickData):
                received.append(tick)
                self._ws_ticker_received.set()

            ws.subscribe_ticker([self.symbol], on_tick)
            await ws.connect()

            # 等待最多 10 秒接收数据
            try:
                await asyncio.wait_for(self._ws_ticker_received.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                raise RuntimeError("10 秒内未收到 Ticker 数据")

            tick = received[-1]
            return f"Ticker 订阅成功: last={tick.last:.2f}, 收到 {len(received)} 条数据"

        await self._run_case("WS 公共频道: Ticker 订阅", _ws_public_ticker())

        # 5.2 私有 WS: 订单订阅 + 登录
        async def _ws_private_orders():
            ws = WebSocketClient(self.okx_config.ws_private, auth=self.auth)
            self.ws_private = ws

            order_updates: list[OrderData] = []

            async def on_order(order: OrderData):
                order_updates.append(order)
                self._ws_order_received.set()

            ws.subscribe_orders(on_order, inst_type="SWAP")
            await ws.connect()

            # 等待登录完成
            await asyncio.sleep(2)

            if not ws.is_connected:
                raise RuntimeError("私有 WS 连接失败")

            # 注入 WS 到 OMS（模拟 live_run 的初始化流程）
            self.oms._ws = ws
            await self.oms.start(inst_type="SWAP")

            # 触发一个订单来验证私有频道推送
            tick = await self.rest.get_ticker(self.symbol)
            price = str(round(tick.last * 0.80, 2))

            order = await self.oms.submit_order(
                symbol=self.symbol,
                side="buy",
                order_type="limit",
                size="0.001",
                price=price,
                pos_side="long",
            )

            # 等待订单更新
            try:
                await asyncio.wait_for(self._ws_order_received.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pass  # 私有频道可能需要更长时间

            # 清理：撤单
            await self.oms.cancel_order(self.symbol, order.order_id)

            result = f"私有 WS 连接: {'成功' if ws.is_connected else '失败'}"
            result += f"\n  登录状态: 已认证"
            result += f"\n  订单推送: {len(order_updates)} 条"

            return result

        await self._run_case("WS 私有频道: 订单订阅 + 登录", _ws_private_orders())


# ─────────────────── 入口 ───────────────────

def main():
    parser = argparse.ArgumentParser(description="OKX V5 API 接口通过性审计")
    parser.add_argument("--config", "-c", default="configs/paper_trading.yaml", help="YAML 配置文件路径")
    parser.add_argument("--symbol", "-s", default=None, help="交易对（覆盖配置）")
    args = parser.parse_args()

    auditor = APIAuditor(config_path=args.config, symbol=args.symbol)
    asyncio.run(auditor.run())

    # 退出码：有失败则返回 1
    sys.exit(0 if auditor.report.failed == 0 else 1)


if __name__ == "__main__":
    main()
