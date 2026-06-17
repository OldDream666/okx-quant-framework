# OKX Quant Framework — 使用说明书

> Async-first quantitative trading framework for OKX V5 API
> Python 3.10+ | 281 单元测试 | 9 大模块

---

## 目录

1. [快速开始](#1-快速开始)
2. [项目结构](#2-项目结构)
3. [配置系统](#3-配置系统)
4. [编写策略](#4-编写策略)
5. [运行回测](#5-运行回测)
6. [Walk-Forward 验证](#6-walk-forward-验证)
7. [运行实盘/模拟盘](#7-运行实盘模拟盘)
8. [安卓手机运行（Termux）](#8-安卓手机运行termux)
9. [风控系统](#9-风控系统)
10. [交易账本](#10-交易账本)
11. [API 速查表](#11-api-速查表)
12. [诊断与调试](#12-诊断与调试)
13. [审计踩坑记录](#13-审计踩坑记录)

---

## 1. 快速开始

### 1.1 安装

```bash
cd ~/okx-quant-framework
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 1.2 配置

```bash
# 1. API 密钥
cp .env.example .env

# 2. 实盘配置
cp configs/paper_trading.example.yaml configs/paper_trading.yaml

# 3. 投研配置（可选，回测用）
cp configs/research.example.yaml configs/research.yaml

# 4. 自定义策略（可选）
cp strategies/my_strategy.example.py strategies/my_strategy.py
```

编辑 `.env`：

```env
OKX_API_KEY=your_key
OKX_SECRET_KEY=your_secret
OKX_PASSPHRASE=your_passphrase
OKX_IS_DEMO=true
```

### 1.3 验证

```bash
pytest tests/ -v
# 预期：291 passed
```

### 1.4 诊断下单

```bash
# 逐步测试 OKX API（余额→持仓→品种→杠杆→下单→撤单）
python scripts/diagnose_order.py
```

---

## 2. 项目结构

```
okx_quant/                  ← 框架核心（不改）
├── config/                 ← 配置 & 鉴权
│   ├── settings.py         ← Pydantic 配置管理
│   └── auth.py             ← OKX V5 签名（REST: ISO 8601+ms+Z, WS: Unix秒）
├── models/
│   └── market.py           ← TickData, BarData, OrderData(pos_side), PositionData(ctVal)...
├── gateway/
│   ├── rest_client.py      ← 异步 REST（httpx, 签名含query, 自动取整, posSide）
│   └── ws_client.py        ← 异步 WebSocket（Lock重连, Task引用防GC, 心跳）
├── oms/
│   └── order_manager.py    ← 订单管理（WS驱动, 终态自动入history, posSide透传）
├── strategy/               ← 策略基类定义
│   └── base.py             ← BaseStrategy, Signal, Position(contract_multiplier)
├── backtest/               ← 回测引擎
│   └── engine.py           ← BacktestEngine, ExchangeConfig, 动态Sharpe, 方向强平
├── live/                   ← 实盘引擎
│   └── runner.py           ← LiveRunner（持仓同步, check_account, Kill Switch平仓）
├── risk/
│   └── risk_manager.py     ← 风控拦截器（敞口/杠杆/回撤, 高水位, Kill Switch）
└── monitoring/
    ├── logger.py           ← loguru 日志（按天滚动 + 压缩）
    ├── ledger.py           ← 交易账本（JSONL 追加写入 + CSV 导出）
    └── metrics.py          ← 指标采集 + 动态Sharpe + 心跳监控 + 异步Webhook报警

strategies/                 ← 你的策略（经常改）
├── macro_ema.py            ← Macro EMA 交叉 + 宏观滤网（参考实现）
└── my_strategy.example.py  ← 策略模板（从此文件开始写自己的策略）

configs/                    ← 参数配置（YAML，不提交到 Git）
├── paper_trading.example.yaml  ← 实盘配置模板
├── paper_trading.yaml          ← 你的实际配置（.gitignore 排除）
├── research.example.yaml       ← 投研配置模板
└── research.yaml               ← 你的实际配置（.gitignore 排除）

scripts/                    ← 工具脚本
├── diagnose_order.py       ← OKX API 下单诊断
└── api_audit.py            ← 16 项全流程接口审计

run_live.py                 ← 通用实盘启动脚本
run_research.py             ← 投研流水线（基准→寻优→盲测）
data/                       ← 交易账本（自动创建）
├── backtest/               ← 回测账本
└── live/                   ← 实盘账本
```

### 模块职责

| 模块 | 职责 | 改动频率 |
|------|------|----------|
| `config/` | API 凭证 + 签名 | 很少 |
| `models/` | 统一数据模型 | 很少 |
| `gateway/` | REST + WebSocket 通信 | 很少 |
| `oms/` | 订单生命周期管理 | 很少 |
| `strategy/` | 策略接口定义 | 很少 |
| `backtest/` | 回测引擎 | 很少 |
| `live/` | 实盘引擎 | 很少 |
| `risk/` | 风控拦截 | 偶尔 |
| `monitoring/` | 日志 + 账本 + 报警 | 偶尔 |
| `strategies/` | **你的策略** | **经常** |
| `configs/` | **参数配置** | **经常** |

---

## 3. 配置系统

### 3.1 加载配置

```python
from okx_quant.config import load_config

config = load_config()  # 从 .env 加载
print(config.okx.api_key)
print(config.okx.is_demo)  # True = 模拟盘
print(config.okx.flag)     # '1' = 模拟盘, '0' = 实盘
```

### 3.2 OKX 端点映射

```
REST API:
  生产: https://www.okx.com
  模拟: https://www.okx.com + x-simulated-trading: 1 头

WebSocket:
  /ws/v5/business  → candle 频道（K线数据）
  /ws/v5/public    → tickers, orderbook, trades
  /ws/v5/private   → orders, account, positions（需登录）

Demo WebSocket:
  wspap.okx.com:8443/ws/v5/public?brokerId=9999
  wspap.okx.com:8443/ws/v5/private?brokerId=9999
  wspap.okx.com:8443/ws/v5/business?brokerId=9999  ← candle 也需切 demo

签名时间戳:
  REST → ISO 8601 + 毫秒 + Z (2026-06-15T12:00:00.123Z)
  WS   → Unix 秒级 (1750000000)
  签名路径 → 包含 query string (/api/v5/trade/orders-pending?instId=BTC-USDT)
```

### 3.3 双向持仓模式

OKX 账户有 `posMode` 设置：
- `long_short_mode`（双向）→ 每笔订单**必须**带 `posSide`（`long`/`short`）
- `net_mode`（单向）→ 不需要 `posSide`

框架自动处理：策略调用 `buy()`/`sell()` 时自动推断 `posSide`，无需手动设置。

---

## 4. 编写策略

### 4.1 创建策略文件

创建 `strategies/my_strategy.py`：

```python
from okx_quant.strategy.base import BaseStrategy, Signal
from okx_quant.models.market import BarData

class MyStrategy(BaseStrategy):
    name = "my_strategy"

    def on_init(self, params):
        super().on_init(params)
        self.fast = params.get("fast", 10)
        self.slow = params.get("slow", 30)

    def on_start(self):
        print(f"策略启动: EMA({self.fast}/{self.slow})")

    def on_bar(self, bar: BarData) -> Signal | None:
        # 策略逻辑
        if some_condition:
            self.buy(size=0.01)                    # 市价开多
            self.sell(size=0.01, price=65000,      # 限价开空
                      order_type="limit")
            self.sell(size=0.01, price=58000,      # 止损单
                      order_type="stop")
        if want_to_close:
            self.close_long()                      # 全平多仓
            self.close_short(size=0.005)           # 部分平空
            self.cancel("order_id_123")            # 撤单
        return None  # 或 return Signal(action="BUY", price=bar.close, confidence=0.8)

    def on_stop(self):
        print("策略停止")
```

### 4.2 策略可用的操作

| 方法 | 说明 |
|------|------|
| `self.buy(size, price=None, order_type="market")` | 买入（开多或平空） |
| `self.sell(size, price=None, order_type="market")` | 卖出（开空或平多） |
| `self.close_long(size=None)` | 平多仓（None=全平） |
| `self.close_short(size=None)` | 平空仓（None=全平） |
| `self.cancel(order_id)` | 撤单 |
| `self.position_long` | 多仓 Position 对象 |
| `self.position_short` | 空仓 Position 对象 |
| `self.bars` | K 线历史窗口 |
| `self.state.cash` | 可用资金 |
| `self.current_bar` | 最新 K 线 |

### 4.3 OKX 双向持仓模式

```
buy()  + 无持仓 → 开多 (posSide=long)
buy()  + 有空仓 → 平空 (posSide=short)
sell() + 无持仓 → 开空 (posSide=short)
sell() + 有多仓 → 平多 (posSide=long)
close_long()    → 平多（隐含 posSide=long）
close_short()   → 平空（隐含 posSide=short）
```

### 4.4 数量精度

策略只需计算 `cash × pct / price`，**精度处理由框架自动完成**：

- 下单时 REST 客户端自动从 OKX 获取合约规格（lot_size, tick_size, min_size, ctMult）
- 数量向下取整到 lot_size 精度（如 1.12838471 → 1.128）
- 价格四舍五入到 tick_size 精度
- 低于最小下单量时自动提升到 min_size
- 合约规格首次获取后缓存，后续 O(1)
- 支持极小 lot_size（如 1e-8）的科学计数法正确格式化

### 4.5 合约乘数

SWAP 合约的 `quantity` 是**张数**，不是币数。框架自动处理：

```python
# Position 对象自动携带 contract_multiplier（从 OKX ctVal 获取）
pos = self.position_long
pnl = pos.unrealized_pnl(current_price)  # 已自动乘以 ctVal

# BTC-USDT-SWAP: ctVal=0.01, 100张涨$1000 → 盈利$1000（不是$100,000）
# ETH-USDT-SWAP: ctVal=1.0,  10张涨$12   → 盈利$120
```

---

## 5. 运行回测

```python
from okx_quant.backtest import BacktestEngine, ExchangeConfig
from strategies.macro_ema import MacroEmaStrategy

engine = BacktestEngine(
    initial_capital=10_000,
    config=ExchangeConfig(
        maker_fee_rate=0.0002,
        taker_fee_rate=0.0005,
        slippage_base=0.0003,
        tick_size=0.01,
        latency_bars=1,          # 1 根 K 线延迟（防未来函数）
        leverage=20,
        contract_multiplier=0.01,  # BTC-USDT-SWAP 面值
    ),
)

strategy = MacroEmaStrategy()
strategy.on_init({"fast_period": 15, "slow_period": 40, "stop_loss_pct": 0.05})

result = engine.run(strategy, bars, contract_mode=True)

print(f"总收益: {result.total_return:.2%}")
print(f"最大回撤: {result.max_drawdown:.2%}")
print(f"夏普比率: {result.sharpe_ratio:.2f}")  # 动态年化（按K线间隔计算）
print(f"胜率: {result.win_rate:.0%}")
```

**账本自动写入** `data/backtest/`，无需手动创建。

### 回测执行流程（每根 K 线）

```
bar[i]:
  1. 更新持仓极值（移动止损数据）
  2. 检查强平（方向判断：long看跌，short看涨，盈利不触发）
  3. 执行 bar[i-1] 的市价单（bar[i].open + 动态滑点）← 无未来函数
  4. 撮合挂单（限价/止损在 bar[i] 范围内）
  5. 扣除资金费率（按 contract_multiplier 计算）
  6. strategy.on_bar(bar[i])
     → buy()/sell() → 推入队列，bar[i+1] 执行
     → 限价/止损 → 加入 pending_orders
  7. 记录权益（unrealized_pnl 已乘 contract_multiplier）
  8. 写入账本（equity.jsonl）
```

### ExchangeConfig 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `maker_fee_rate` | 0.0002 | Maker 手续费（限价单） |
| `taker_fee_rate` | 0.0005 | Taker 手续费（市价单） |
| `slippage_base` | 0.0003 | 基础滑点 |
| `slippage_volatility_factor` | 2.0 | 波动放大系数 |
| `tick_size` | 0.01 | 最小价格单位 |
| `lot_size` | 1e-8 | 最小数量步长 |
| `contract_multiplier` | 1.0 | 合约面值（SWAP 按 ctVal，现货为 1.0） |
| `latency_bars` | 1 | 信号→执行延迟 |
| `leverage` | 1 | 杠杆倍数 |
| `enable_liquidation` | False | 强平检测 |
| `funding_rate` | 0.0001 | 资金费率 |

---

## 6. Walk-Forward 验证

```python
result = engine.run_walk_forward(
    strategy_factory=MacroEmaStrategy,  # 传类，不是实例
    bars=all_bars,
    params={"fast_period": 15, "slow_period": 40},
    train_pct=0.7,
    overfit_threshold=0.5,
    contract_mode=True,
)

if result.is_overfit:
    print(result.overfit_warning)
else:
    print("✅ 策略稳健")
```

---

## 7. 运行实盘/模拟盘

### 7.1 创建配置文件

`configs/my_strategy.yaml`：

```yaml
symbol: "ETH-USDT-SWAP"
timeframe: "15m"
leverage: 20
strategy: "strategies.macro_ema.MacroEmaStrategy"
strategy_params:
  fast_period: 15
  slow_period: 40
  macro_period: 800
  stop_loss_pct: 0.05
  position_pct: 0.025
risk:
  max_order_value: 50000
  max_total_exposure: 200000
  max_price_deviation: 0.05
  max_orders_per_sec: 5
  max_consecutive_failures: 5
  max_slippage_pct: 0.01
  max_drawdown_pct: 0.20
preload_bars: 900
log_level: "INFO"
```

### 7.2 启动

```bash
# 标准启动
python run_live.py --config configs/my_strategy.yaml

# 命令行覆盖参数
python run_live.py --config configs/my_strategy.yaml --symbol BTC-USDT-SWAP --leverage 10

# 只验证配置，不启动交易
python run_live.py --config configs/my_strategy.yaml --dry-run
```

### 7.3 启动流程

```
1.  加载 YAML 配置
2.  加载 .env → OKX API 凭证
3.  动态导入策略类
4.  连接 REST → 查询余额 → 设置 state.cash
4b. 查询现有持仓 → 初始化 strategy.position_long/short
5.  设置杠杆 (OKX set_leverage API)
6.  拉取历史 K 线 → 预填充策略（消除预热等待）
7.  创建 OMS（WS 暂未注入）
8.  创建 LiveExecutor → RiskManager → 注入策略
9.  创建 WebSocket:
    - /ws/v5/business → candle 频道
    - /ws/v5/private  → orders 频道
9b. 注入 WS 到 OMS → 调用 oms.start() 注册订单订阅
10. 策略初始化（on_init + on_start）
11. 连接 WebSocket
12. 等待 K 线闭合 → 策略开始工作
```

### 7.4 实盘运行时的风控调用

每根 K 线闭合时（`_on_candle`）：

```
1. update_market_price(bar.close)
2. check_account(equity, positions, bar.close)
   ├── 更新 _current_exposure（基于真实持仓覆盖计算）
   ├── 更新 _high_water_mark（回撤基准）
   ├── 检查回撤 → Kill Switch
   └── 检查杠杆
3. strategy.on_bar(bar)
   └── buy()/sell()
       ├── check_exposure (步骤5)
       └── check_leverage (步骤6)
4. 记录权益到账本
```

订单成交时（`_on_order_update`）：

```
1. on_fill() → 滑点监控
2. FILLED → 立即查询 OKX 真实持仓同步到策略
   PARTIALLY_FILLED → 5秒节流同步（防频率限制）
3. 记录到账本
```

### 7.5 实盘账本

实盘运行时自动记录：
- 每根 K 线闭合 → `data/live/equity.jsonl`
- 每笔订单成交 → `data/live/trades.jsonl`

---

## 8. 安卓手机运行（Termux）

框架全部是纯 Python + asyncio，Termux 完全支持。

### 8.1 安装

```bash
# 1. 从 F-Droid 安装 Termux（不要用 Play Store 版，过时）
# 2. 打开 Termux，执行:
pkg update && pkg install git
git clone https://github.com/OldDream666/okx-quant-framework.git ~/okx-quant-framework
bash ~/okx-quant-framework/scripts/setup_termux.sh
```

### 8.2 启动

```bash
# 前台运行（可看实时输出）
cd ~/okx-quant-framework && source .venv/bin/activate
python run_live.py --config configs/paper_trading.yaml

# 后台运行（推荐，含 wake lock + 进程监控）
bash scripts/run_termux_bg.sh
```

### 8.3 防止 Android 杀进程

**必须做**（否则锁屏后进程被杀）：

1. **Termux 通知栏** → 点击 "Acquire wakelock"（锁头图标）
2. **系统设置** → 电池 → Termux → 不限制后台活动
3. **最近任务** → 长按 Termux → 锁定（防止被清理）
4. **省电模式** → 关闭，或把 Termux 加入白名单

### 8.4 开机自启（可选）

```bash
# 安装 Termux:Boot (F-Droid)
# 复制启动脚本
cp ~/okx-quant-framework/scripts/termux_boot.sh ~/.termux/boot/start_okx_quant.sh
# 打开 Termux:Boot 应用一次（授权），之后重启手机自动启动交易
```

### 8.5 监控

```bash
# 实时查看日志
tail -f ~/okx-quant-framework/logs/trading_$(date +%Y-%m-%d).log

# 远程 SSH 监控（推荐安装 Termux:API + openssh）
pkg install openssh
sshd  # 启动 SSH 服务，端口 8022
# 电脑端: ssh -p 8022 phone_ip
```

## 9. 风控系统

### 9.1 八层防御

| # | 层 | 触发条件 | 行为 |
|---|-----|---------|------|
| 1 | 硬锁死 | Kill Switch 已激活 | 拒绝所有请求 |
| 2 | 频率限制 | > max_orders_per_sec | 拒绝 |
| 3 | 乌龙指 | 限价偏离市价 > 5% | 拒绝 |
| 4 | 单笔限额 | qty × price > max_order_value | 拒绝 |
| 5 | **敞口限制** | 总持仓名义值 > max_total_exposure | 拒绝 |
| 6 | **杠杆限制** | 估算杠杆 > max_account_leverage | 拒绝新开仓 |
| 7 | 滑点监控 | 滑点 > max_slippage_pct | Kill Switch |
| 8 | 回撤监控 | 从**高水位**回撤 > max_drawdown_pct | Kill Switch |

### 9.2 Kill Switch

- 一旦激活，**永久锁定**
- 自动撤单 + **市价平仓**（带 `posSide` 参数）
- 平仓前查询 OKX 真实持仓
- 需重启程序才能恢复

### 9.3 回撤计算

基于**高水位**而非固定初始权益：

```
高水位 = max(历史所有权益)
回撤 = (高水位 - 当前权益) / 高水位

示例：
  初始 $10,000 → 涨到 $15,000 → 跌到 $12,000
  回撤 = (15000 - 12000) / 15000 = 20%  ✅
  （旧逻辑：回撤 = (10000 - 12000) / 10000 = -20%，不触发 ❌）
```

---

## 10. 交易账本

### 9.1 存储结构

```
data/
├── backtest/                   ← 回测账本（自动创建）
│   ├── trades.jsonl            ← 每笔成交一行 JSON
│   ├── equity.jsonl            ← 每根 K 线权益快照
│   └── daily_summary.csv       ← 每日汇总（手动导出）
└── live/                       ← 实盘账本（自动创建）
    ├── trades.jsonl
    ├── equity.jsonl
    └── daily_summary.csv
```

### 9.2 数据格式

**trades.jsonl**（每笔成交一行）：
```json
{"ts":"2026-06-16T10:00:00","symbol":"ETH-USDT-SWAP","side":"long","entry_price":1792.5,"exit_price":1810.0,"quantity":0.45,"pnl":7.88,"fee":0.16}
```

**equity.jsonl**（每根 K 线一行）：
```json
{"ts":"2026-06-16T10:00:00","equity":80691.5,"cash":78691.5,"position_value":2000.0,"drawdown":0.012}
```

### 9.3 使用方式

```python
from okx_quant.monitoring.ledger import TradeLedger

# 回测 — 自动创建，无需手动操作
engine = BacktestEngine(initial_capital=10000)
result = engine.run(strategy, bars)

# 实盘 — run_live.py 自动创建 data/live/
python run_live.py --config configs/paper_trading.yaml

# 查询
ledger = TradeLedger(data_dir="data/live", symbol="ETH-USDT-SWAP")
trades = ledger.query_trades(side="long")
summary = ledger.summary()
ledger.export_daily_summary()
```

---

## 11. API 速查表

### 导入路径

```python
# 配置
from okx_quant.config import load_config, OKXAuth

# 数据模型
from okx_quant.models.market import BarData, TickData, OrderData, PositionData

# 网关
from okx_quant.gateway.rest_client import RESTClient
from okx_quant.gateway.ws_client import WebSocketClient

# 订单管理
from okx_quant.oms.order_manager import OrderManager

# 策略
from okx_quant.strategy.base import BaseStrategy, Signal, Position

# 回测
from okx_quant.backtest import BacktestEngine, ExchangeConfig, WalkForwardResult

# 实盘
from okx_quant.live import LiveRunner

# 风控
from okx_quant.risk.risk_manager import RiskConfig, RiskManager

# 监控
from okx_quant.monitoring.logger import setup_logger
from okx_quant.monitoring.ledger import TradeLedger
from okx_quant.monitoring.metrics import MetricsCollector, Alerter, HeartbeatMonitor
```

### 数据模型

| 类 | 关键字段 | 说明 |
|---|---------|------|
| `TickData` | last, bid, ask, change24h | 实时行情（change24h 防御空字符串） |
| `BarData` | open, high, low, close, volume, confirmed | K 线 |
| `OrderData` | order_id, side, **pos_side**, filled_qty, status | 订单（含双向持仓方向） |
| `PositionData` | quantity, avg_price, **contract_multiplier** | OKX 持仓（含合约面值） |
| `Position` | side, quantity, avg_price, **contract_multiplier** | 策略持仓（unrealized_pnl 已乘面值） |
| `Signal` | action, price, confidence, reason | 策略信号 |
| `Trade` | entry_price, exit_price, pnl, bars_held | 回测成交记录 |
| `BacktestResult` | total_return, max_drawdown, sharpe_ratio, trades | 回测结果 |

---

## 12. 诊断与调试

### 11.1 下单诊断

```bash
python scripts/diagnose_order.py
```

逐步测试：余额 → 持仓 → 品种规格 → 杠杆 → 模拟下单 → 撤单 → 账户配置 → API Key 权限。

### 11.2 常见错误

| OKX 错误码 | 含义 | 排查方向 |
|-----------|------|---------|
| `1` "All operations failed" | 请求参数错误 | 检查 posSide、instId、tdMode |
| `51000` "Parameter posSide error" | 双向持仓模式缺 posSide | 确认账户 posMode |
| `51001` "Instrument ID error" | instId 不存在 | 检查品种是否上市 |
| `50111` "API Key error" | API Key 无效 | 检查 .env 配置 |
| `50113` "Signature error" | 签名不匹配 | 检查时间戳、passphrase |

### 11.3 日志

```bash
# 查看最新日志
tail -f logs/trading_$(date +%Y-%m-%d).log

# 搜索错误
grep "ERROR\|OKX API 错误" logs/trading_*.log
```

---

## 13. 审计踩坑记录

经过多轮安全审计，以下是所有已发现并修复的问题。**开发新功能时务必注意这些模式。**

### 🔴 致命级

| 问题 | 正确做法 | 错误做法 |
|------|---------|---------|
| OKX 返回空字符串 `""` | `float(data.get("px") or 0)` | `float(data.get("px", 0))` — `""` 走不到默认值 |
| 合约乘数 | `pnl = diff * qty * contract_multiplier` | `pnl = diff * qty` — BTC 放大 100 倍 |
| 双向持仓 posSide | `body["posSide"] = "long"` | 不传 posSide → Error [1] |
| 强平方向判断 | `loss = entry - current`（long）| `loss = abs(current - entry)` — 盈利也被平 |
| REST 签名 | `sign_path = f"{path}?{urlencode(params)}"` | 只签裸路径 → 401 |

### 🟠 重要级

| 问题 | 正确做法 |
|------|---------|
| 科学计数法 | `format(Decimal(...), 'f')` 而非 `str()` |
| WS 连接失败 | `try/except` 中 `_running = False` |
| 回撤计算 | 基于高水位 `max(历史权益)` |
| 部分成交频率 | 5 秒节流，FILLED 立即同步 |
| Task GC | `pending_tasks.add(task)` + `done_callback(discard)` |
| 持仓同步 | 启动时查询 + 成交后查询，覆盖式更新 |

### 测试覆盖

```bash
# 全量测试（281 个，含 32 个审计边界用例）
python -m pytest tests/ -v --tb=short

# 类型检查
python -m mypy okx_quant/ --ignore-missing-imports
```

测试文件说明：
- `tests/test_audit_edge_cases.py` — 32 个审计发现的边界用例回归测试
- `tests/test_live_integration.py` — LiveRunner 端到端集成测试（策略→风控→执行器→OMS）
- 覆盖：空字符串解析、科学计数法、合约乘数、强平方向、风控集成、OMS 状态机、REST 签名、WS 安全
