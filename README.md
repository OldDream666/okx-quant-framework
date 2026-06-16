# 🚀 OKX Quant Framework

<p align="center">
  <strong>基于 OKX V5 API 的异步量化交易框架</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Tests-249%20passed-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/License-MIT-yellow" alt="License">
  <img src="https://img.shields.io/badge/OKX-V5%20API-orange" alt="OKX">
  <img src="https://img.shields.io/badge/async-asyncio-purple" alt="Async">
</p>

---

## ✨ 特性

| 特性 | 说明 |
|------|------|
| 🔄 **全异步架构** | REST（httpx）+ WebSocket（websockets）+ asyncio |
| 🔐 **OKX 官方接口** | V5 API 签名鉴权，REST + WS 双通道 |
| 📊 **事件驱动回测** | 无未来函数，动态滑点，双向合约，资金费率 |
| 🔍 **Walk-Forward 验证** | 训练/测试集分割，过拟合自动检测 |
| 🚀 **实盘引擎** | 历史 K 线预加载，自动杠杆设置，K 线驱动 |
| 🛡️ **六层风控** | 频率限制 → 乌龙指 → 单笔限额 → 滑点 → 回撤 → Kill Switch |
| 📒 **交易账本** | JSONL 追加写入，CSV 每日汇总，回测/实盘隔离 |
| 📈 **实时监控** | loguru 日志 + 心跳检测 + 飞书/Telegram Webhook 报警 |

---

## 📁 项目结构

```
okx_quant/
├── config/           # 🔐 配置 & 鉴权（Pydantic v2, .env）
├── models/           # 📦 统一数据模型（TickData, BarData, OrderData...）
├── gateway/          # 🌐 OKX 网关（REST + WebSocket, 自动重连）
├── oms/              # 📋 订单管理（WS 驱动状态更新）
├── strategy/         # 🎯 策略基类（BaseStrategy, Signal, Position）
├── backtest/         # 📊 回测引擎（事件驱动, 动态滑点, Walk-Forward）
├── live/             # 🚀 实盘引擎（K 线驱动, 历史预加载）
├── risk/             # 🛡️ 风控拦截器（代理模式, Kill Switch）
└── monitoring/       # 📒 日志 + 账本 + 报警（loguru, JSONL, Webhook）

strategies/           # 📝 用户策略
configs/              # ⚙️ YAML 配置文件
run_live.py           # 🏃 通用启动脚本
scripts/              # 🔧 诊断脚本
data/                 # 💾 交易账本（自动创建）
```

---

## ⚡ 快速开始

### 1. 安装

```bash
git clone https://github.com/OldDream666/okx-quant-framework.git
cd okx-quant-framework
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. 配置

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

### 3. 验证

```bash
pytest tests/ -v
# ✅ 249 passed in 2s
```

---

## 📝 编写策略

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

    def on_bar(self, bar: BarData) -> Signal | None:
        if golden_cross:
            self.buy(size=0.01)                     # 市价开多
            self.sell(size=0.01, price=58000,        # 止损单
                      order_type="stop")
        if want_to_close:
            self.close_long()                        # 全平多仓
        return None
```

### 策略 API

| 方法 | 说明 |
|------|------|
| `self.buy(size, price, order_type)` | 买入（开多或平空） |
| `self.sell(size, price, order_type)` | 卖出（开空或平多） |
| `self.close_long(size=None)` | 平多仓 |
| `self.close_short(size=None)` | 平空仓 |
| `self.cancel(order_id)` | 撤单 |
| `self.position_long` / `self.position_short` | 持仓查询 |
| `self.bars` | K 线历史窗口 |
| `self.state.cash` | 可用资金 |

> 💡 数量精度由框架自动处理 — 策略只需算 `cash × pct / price`，REST 层自动获取合约规格并取整。

---

## 📊 回测

```python
from okx_quant.backtest import BacktestEngine, ExchangeConfig
from strategies.macro_ema import MacroEmaStrategy

engine = BacktestEngine(
    initial_capital=10_000,
    config=ExchangeConfig(
        taker_fee_rate=0.0005,
        slippage_base=0.0003,
        latency_bars=1,    # 无未来函数
        leverage=20,
    ),
)

strategy = MacroEmaStrategy()
strategy.on_init({"fast_period": 15, "slow_period": 40, "stop_loss_pct": 0.05})

result = engine.run(strategy, bars, contract_mode=True)

print(f"收益: {result.total_return:.2%}")   # +3.66%
print(f"回撤: {result.max_drawdown:.2%}")   # 0.92%
print(f"夏普: {result.sharpe_ratio:.2f}")   # 5.05
```

账本自动写入 `data/backtest/`（JSONL + CSV）。

### Walk-Forward 验证

```python
wf = engine.run_walk_forward(
    strategy_factory=MacroEmaStrategy,
    bars=all_bars,
    params={"fast_period": 15, "slow_period": 40},
    train_pct=0.7,
)

if wf.is_overfit:
    print(wf.overfit_warning)
    # ⚠️ OVERFITTING WARNING: Sharpe degradation 65.2%...
else:
    print("✅ 策略稳健")
```

---

## 🚀 实盘 / 模拟盘

### 配置文件

`configs/paper_trading.yaml`：

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
```

### 启动

```bash
# 标准启动
python run_live.py --config configs/paper_trading.yaml

# 覆盖参数
python run_live.py --config configs/paper_trading.yaml --symbol BTC-USDT-SWAP --leverage 10

# 验证配置
python run_live.py --config configs/paper_trading.yaml --dry-run
```

### 启动流程

```
1. 加载 YAML 配置 + .env API Key
2. 动态导入策略类
3. 连接 REST → 查余额 → 设杠杆
4. 拉取 900 根历史 K 线 → 预填充策略（无需等待预热）
5. 连接 WebSocket（K 线 + 订单）
6. 等待 K 线闭合 → 策略开始工作
```

---

## 🛡️ 风控

```
策略 self.buy()
    ↓
RiskManager.submit()
    ├── 1. 硬锁检查（Kill Switch?）
    ├── 2. 频率限制（> max_orders_per_sec?）
    ├── 3. 乌龙指（限价偏离 > 5%?）
    ├── 4. 单笔限额（> max_order_value?）
    └── 5. 通过 → 下单
```

| 触发条件 | 行为 |
|---------|------|
| 连续 N 次下单失败 | Kill Switch → 撤单 + 平仓 |
| 滑点 > 阈值 | Kill Switch |
| 回撤 > 阈值 | Kill Switch |

> ⚠️ Kill Switch 一旦触发**永久锁定**，需重启程序。

---

## 📒 交易账本

```
data/
├── backtest/               ← 回测账本（自动）
│   ├── trades.jsonl
│   ├── equity.jsonl
│   └── daily_summary.csv
└── live/                   ← 实盘账本（自动）
    ├── trades.jsonl
    ├── equity.jsonl
    └── daily_summary.csv
```

```python
from okx_quant.monitoring.ledger import TradeLedger

ledger = TradeLedger(data_dir="data/live", symbol="ETH-USDT-SWAP")
print(ledger.summary())
# {'total_trades': 5, 'wins': 3, 'win_rate': 0.6, 'total_pnl': 270.0, ...}

ledger.export_daily_summary()
# → data/live/daily_summary.csv
```

---

## 🌐 OKX 端点

| 端点 | 用途 |
|------|------|
| `https://www.okx.com` | REST API |
| `/ws/v5/business` | K 线数据（candle 频道） |
| `/ws/v5/public` | 行情（tickers, orderbook） |
| `/ws/v5/private` | 订单 & 账户（需登录） |

---

## 🧪 测试

```bash
pytest tests/ -v

# 测试套件
tests/test_config_auth.py      31 passed   # 配置 & 签名
tests/test_gateway.py          40 passed   # REST + WebSocket
tests/test_oms.py              20 passed   # 订单管理
tests/test_strategy.py         56 passed   # 策略 + 回测 + Walk-Forward
tests/test_integration.py      13 passed   # 全链路集成
tests/test_risk.py             35 passed   # 风控
tests/test_monitoring.py       38 passed   # 日志 + 指标 + 报警
tests/test_ledger.py           16 passed   # 交易账本
─────────────────────────────────────────
Total                         249 passed
```

---

## 📦 依赖

| 包 | 用途 |
|---|------|
| `pydantic >= 2.0` | 配置管理 |
| `httpx >= 0.27` | 异步 REST |
| `websockets >= 13.0` | 异步 WebSocket |
| `loguru >= 0.7` | 日志 |
| `pyyaml >= 6.0` | YAML 配置 |
| `python-dotenv >= 1.0` | .env 加载 |

---

## 📖 文档

- [GUIDE.md](GUIDE.md) — 完整使用说明

---

## 📄 License

MIT
