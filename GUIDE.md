# OKX Quant Framework — 使用说明书

> Async-first quantitative trading framework for OKX V5 API
> Python 3.10+ | 249+ 单元测试 | 9 大模块

---

## 目录

1. [快速开始](#1-快速开始)
2. [项目结构](#2-项目结构)
3. [配置系统](#3-配置系统)
4. [编写策略](#4-编写策略)
5. [运行回测](#5-运行回测)
6. [Walk-Forward 验证](#6-walk-forward-验证)
7. [运行实盘/模拟盘](#7-运行实盘模拟盘)
8. [风控系统](#8-风控系统)
9. [交易账本](#9-交易账本)
10. [API 速查表](#10-api-速查表)

---

## 1. 快速开始

### 1.1 安装

```bash
cd ~/okx-quant-framework
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 1.2 配置 API 密钥

```bash
cp .env.example .env
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
# 预期：249 passed
```

---

## 2. 项目结构

```
okx_quant/                  ← 框架核心（不改）
├── config/                 ← 配置 & 鉴权
│   ├── settings.py         ← Pydantic 配置管理
│   └── auth.py             ← OKX V5 签名（REST: ISO 8601, WS: Unix秒）
├── models/
│   └── market.py           ← TickData, BarData, OrderData, PositionData...
├── gateway/
│   ├── rest_client.py      ← 异步 REST（httpx, tdMode=cross 合约模式）
│   └── ws_client.py        ← 异步 WebSocket（自动重连, 心跳）
├── oms/
│   └── order_manager.py    ← 订单管理（WS 驱动状态更新）
├── strategy/               ← 策略基类定义
│   └── base.py             ← BaseStrategy, Signal, Position
├── backtest/               ← 回测引擎
│   └── engine.py           ← BacktestEngine, ExchangeConfig, WalkForwardResult
├── live/                   ← 实盘引擎
│   └── runner.py           ← LiveRunner（K线驱动 + 历史预加载 + 杠杆设置）
├── risk/
│   └── risk_manager.py     ← 风控拦截器（代理模式, Kill Switch）
└── monitoring/
    ├── logger.py           ← loguru 日志（按天滚动 + 压缩）
    ├── ledger.py           ← 交易账本（JSONL 追加写入 + CSV 导出）
    └── metrics.py          ← 指标采集 + 心跳监控 + 异步 Webhook 报警

strategies/                 ← 你的策略（经常改）
└── macro_ema.py            ← Macro EMA 交叉 + 宏观滤网

configs/                    ← 参数配置（YAML）
└── paper_trading.yaml      ← 模拟盘配置

run_live.py                 ← 通用启动脚本
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
  wspap.okx.com:8443/ws/v5/private?brokerId=9999

签名时间戳:
  REST → ISO 8601 + 毫秒 + Z (2026-06-15T12:00:00.123Z)
  WS   → Unix 秒级 (1750000000)
```

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
buy()  + 无持仓 → 开多
buy()  + 有空仓 → 平空
sell() + 无持仓 → 开空
sell() + 有多仓 → 平多
close_long()    → 平多（隐含 posSide=long）
close_short()   → 平空（隐含 posSide=short）
```

### 4.4 数量精度

策略只需计算 `cash × pct / price`，**精度处理由框架自动完成**：

- 下单时 REST 客户端自动从 OKX 获取合约规格（lot_size, tick_size, min_size）
- 数量向下取整到 lot_size 精度（如 1.12838471 → 1.128）
- 价格四舍五入到 tick_size 精度
- 低于最小下单量时自动提升到 min_size
- 合约规格首次获取后缓存，后续 O(1)

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
    ),
)

strategy = MacroEmaStrategy()
strategy.on_init({"fast_period": 15, "slow_period": 40, "stop_loss_pct": 0.05})

result = engine.run(strategy, bars, contract_mode=True)

print(f"总收益: {result.total_return:.2%}")
print(f"最大回撤: {result.max_drawdown:.2%}")
print(f"夏普比率: {result.sharpe_ratio:.2f}")
print(f"胜率: {result.win_rate:.0%}")
```

**账本自动写入** `data/backtest/`，无需手动创建。

### 回测执行流程（每根 K 线）

```
bar[i]:
  1. 更新持仓极值（移动止损数据）
  2. 检查强平
  3. 执行 bar[i-1] 的市价单（bar[i].open + 动态滑点）← 无未来函数
  4. 撮合挂单（限价/止损在 bar[i] 范围内）
  5. 扣除资金费率
  6. strategy.on_bar(bar[i])
     → buy()/sell() → 推入队列，bar[i+1] 执行
     → 限价/止损 → 加入 pending_orders
  7. 记录权益
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
1. 加载 YAML 配置
2. 加载 .env → OKX API 凭证
3. 动态导入策略类
4. 连接 REST → 查询余额
5. 设置杠杆 (OKX set_leverage API)
6. 拉取历史 K 线 → 预填充策略（消除预热等待）
7. 创建账本 → data/live/
8. 连接 WebSocket:
   - /ws/v5/business → candle 频道
   - /ws/v5/private  → orders 频道
9. 等待 K 线闭合 → 策略开始工作
```

### 7.4 实盘账本

实盘运行时自动记录：
- 每根 K 线闭合 → `data/live/equity.jsonl`
- 每笔订单成交 → `data/live/trades.jsonl`

---

## 8. 风控系统

### 8.1 六层防御

| # | 层 | 触发条件 | 行为 |
|---|-----|---------|------|
| 1 | 硬锁死 | Kill Switch 已激活 | 拒绝所有请求 |
| 2 | 频率限制 | > max_orders_per_sec | 拒绝 |
| 3 | 乌龙指 | 限价偏离市价 > 5% | 拒绝 |
| 4 | 单笔限额 | qty × price > max_order_value | 拒绝 |
| 5 | 滑点监控 | 滑点 > max_slippage_pct | Kill Switch |
| 6 | 回撤监控 | 回撤 > max_drawdown_pct | Kill Switch |

### 8.2 Kill Switch

- 一旦激活，**永久锁定**
- 自动撤单 + 市价平仓
- 需重启程序才能恢复

---

## 9. 交易账本

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

**daily_summary.csv**：
```csv
date,trades,wins,losses,pnl,fees,max_drawdown,equity_close
2026-06-16,5,3,2,152.30,8.5000,0.0230,80843.80
```

### 9.3 使用方式

```python
from okx_quant.monitoring.ledger import TradeLedger

# 回测 — 自动创建，无需手动操作
engine = BacktestEngine(initial_capital=10000)
result = engine.run(strategy, bars)
# 账本在 data/backtest/

# 回测 — 自定义目录
engine = BacktestEngine(initial_capital=10000, data_dir="data/my_test")
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

## 10. API 速查表

### 导入路径

```python
# 配置
from okx_quant.config import load_config, OKXAuth

# 数据模型
from okx_quant.models.market import BarData, TickData, OrderData

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

| 类 | 说明 |
|---|------|
| `TickData` | 实时行情 |
| `BarData` | K 线 |
| `OrderData` | 订单 |
| `PositionData` | 持仓 |
| `Signal` | 策略信号 |
| `Position` | 策略持仓（多/空独立） |
| `Trade` | 回测成交记录 |
| `BacktestResult` | 回测结果 |
| `WalkForwardResult` | WF 验证结果 |

---

## 常见问题

### Q: 模拟盘和实盘价格一样吗？

K 线和买一卖一**基本一致**（共享真实行情），成交量不同（模拟盘有独立成交）。

### Q: 策略信号不受影响吗？

EMA 基于收盘价，差异在 0.01% 以内，**不影响策略信号**。

### Q: Kill Switch 触发后怎么恢复？

**无法恢复** — 重启程序。这是设计意图，防止暴走策略造成更大损失。

### Q: 回测账本和实盘账本会混在一起吗？

不会。回测写入 `data/backtest/`，实盘写入 `data/live/`，完全隔离。

### Q: 如何新增策略？

1. 创建 `strategies/my_strategy.py`
2. 创建 `configs/my_strategy.yaml`（指定 `strategy: "strategies.my_strategy.MyStrategy"`）
3. `python run_live.py --config configs/my_strategy.yaml`
