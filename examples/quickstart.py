"""
OKX Quant Framework — 5 分钟快速上手

直接运行：
    cd ~/okx-quant-framework
    source .venv/bin/activate
    python examples/quickstart.py

不需要 API Key，不需要网络，用合成数据演示完整流程。
"""

import sys
import os

# 确保能导入 okx_quant
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from okx_quant.models.market import BarData
from okx_quant.backtest.engine import BacktestEngine, ExchangeConfig
from okx_quant.strategy.templates.ema_cross import EmaCrossStrategy


# ============================================================
# 第一步：生成模拟 K 线数据（先涨后跌，触发金叉和死叉）
# ============================================================

def generate_bars(n=60, start=100.0):
    """生成 60 根 K 线：前 30 根涨，后 30 根跌。带真实波动。"""
    import random
    random.seed(42)  # 固定种子，结果可复现

    bars = []
    price = start
    for i in range(n):
        if i < 30:
            trend = 0.8   # 上涨趋势
        else:
            trend = -0.9  # 下跌趋势

        # 随机波动 ±1.5
        noise = random.uniform(-1.5, 1.5)
        price = price + trend + noise
        price = max(price, 50)  # 防止负数

        volatility = random.uniform(1.0, 3.0)  # 每根 K 线振幅 1~3 美元
        bars.append(BarData(
            symbol="BTC-USDT",
            open=price - volatility * 0.3,
            high=price + volatility * 0.5,
            low=price - volatility * 0.5,
            close=price,
            volume=random.uniform(500, 2000),
            timestamp=1_000_000 + i * 3600_000,
            confirmed=True,
        ))
    return bars


# ============================================================
# 第二步：配置交易所参数
# ============================================================

exchange_config = ExchangeConfig(
    maker_fee_rate=0.0002,       # Maker 手续费 0.02%
    taker_fee_rate=0.0005,       # Taker 手续费 0.05%
    slippage_base=0.0003,        # 基础滑点 0.03%
    tick_size=0.01,              # BTC 最小价格单位
    latency_bars=1,              # 信号延迟 1 根 K 线
)


# ============================================================
# 第三步：运行回测
# ============================================================

def run_backtest():
    print("=" * 60)
    print("  OKX Quant Framework — 回测演示")
    print("=" * 60)

    # 生成数据
    bars = generate_bars(60, start=100.0)
    print(f"\n📊 数据：{len(bars)} 根 1H K 线")
    print(f"   起始价：${bars[0].close:.2f}  →  最高价：${max(b.close for b in bars):.2f}  →  结束价：${bars[-1].close:.2f}")

    # 创建引擎 + 策略
    engine = BacktestEngine(initial_capital=10_000, config=exchange_config)
    strategy = EmaCrossStrategy()
    strategy.on_init({
        "fast_period": 3,
        "slow_period": 15,
        "stop_loss_pct": 0.05,  # 8% 止损（适配波动行情）
    })

    # 运行回测
    print("\n⏳ 正在回测...")
    result = engine.run(strategy, bars, contract_mode=True)

    # 打印结果
    print(f"\n{'─' * 60}")
    print(f"  📈 回测结果")
    print(f"{'─' * 60}")
    print(f"  初始资金：  ${result.initial_capital:>12,.2f}")
    print(f"  最终权益：  ${result.final_equity:>12,.2f}")
    print(f"  总收益：    {result.total_return:>12.2%}")
    print(f"  最大回撤：  {result.max_drawdown:>12.2%}")
    print(f"  夏普比率：  {result.sharpe_ratio:>12.2f}")
    print(f"  胜率：      {result.win_rate:>12.0%}")
    print(f"  总交易：    {result.total_trades:>12} 笔")
    print(f"  总手续费：  ${result.total_fees:>12,.2f}")
    print(f"{'─' * 60}")

    # 打印交易明细
    if result.trades:
        print(f"\n  📋 交易明细：")
        for i, trade in enumerate(result.trades, 1):
            side = "🟢 开多" if trade.side == "long" else "🔴 开空"
            pnl = f"+${trade.pnl:.2f}" if trade.pnl > 0 else f"-${abs(trade.pnl):.2f}"
            print(f"    #{i}  Bar {trade.entry_bar:>2}: {side} @ ${trade.entry_fill:>8.2f}"
                  f"  →  Bar {trade.exit_bar:>2} @ ${trade.exit_fill:>8.2f}"
                  f"  PnL={pnl:>10}  ({trade.reason})")
    else:
        print("\n  ⚠️ 没有产生交易（数据不足以触发信号）")

    return result


# ============================================================
# 第四步：Walk-Forward 验证（检测过拟合）
# ============================================================

def run_walk_forward():
    print(f"\n\n{'=' * 60}")
    print(f"  Walk-Forward 验证（过拟合检测）")
    print(f"{'=' * 60}")

    # 用更多数据做 WF
    bars = generate_bars(100, start=100.0)
    print(f"\n📊 数据：{len(bars)} 根 K 线（70 根训练 / 30 根测试）")

    engine = BacktestEngine(initial_capital=10_000, config=exchange_config)

    print("⏳ 正在验证...")
    result = engine.run_walk_forward(
        strategy_factory=EmaCrossStrategy,
        bars=bars,
        params={"fast_period": 5, "slow_period": 20, "stop_loss_pct": 0.08},
        train_pct=0.7,
        overfit_threshold=0.5,
        contract_mode=True,
    )

    print(f"\n{'─' * 60}")
    print(f"  📊 Walk-Forward 结果")
    print(f"{'─' * 60}")
    print(f"  训练集 Sharpe：  {result.train_sharpe:>8.2f}")
    print(f"  测试集 Sharpe：  {result.test_sharpe:>8.2f}")
    print(f"  Sharpe 退化：    {result.sharpe_degradation:>8.1%}")
    print(f"  训练集收益：     {result.train_result.total_return:>8.2%}")
    print(f"  测试集收益：     {result.test_result.total_return:>8.2%}")
    print(f"{'─' * 60}")

    if result.is_overfit:
        print(f"\n  ⚠️  {result.overfit_warning}")
    else:
        print(f"\n  ✅ 策略稳健，未检测到过拟合")

    return result


# ============================================================
# 运行
# ============================================================

if __name__ == "__main__":
    backtest_result = run_backtest()
    wf_result = run_walk_forward()

    print(f"\n\n{'=' * 60}")
    print(f"  ✅ 完成！")
    print(f"{'=' * 60}")
    print(f"\n  下一步：")
    print(f"  1. 修改 examples/quickstart.py 中的策略参数试试")
    print(f"  2. 编写自己的策略（继承 BaseStrategy）")
    print(f"  3. 配置 .env 文件连接 OKX API 拉取真实数据")
    print(f"  4. 阅读 GUIDE.md 了解更多功能")
    print()
