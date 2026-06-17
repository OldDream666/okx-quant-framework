"""
投研流水线：基准回归 → 网格寻优 → Walk-Forward 盲测

用法:
    python run_research.py --config configs/research.yaml
    python run_research.py --config configs/research.yaml --mode baseline
    python run_research.py --config configs/research.yaml --mode grid
    python run_research.py --config configs/research.yaml --mode walkforward
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

# 项目根目录
sys.path.insert(0, str(Path(__file__).resolve().parent))

from okx_quant.config import load_config
from okx_quant.config.auth import OKXAuth
from okx_quant.gateway.rest_client import RESTClient
from okx_quant.models.market import BarData
from okx_quant.backtest import BacktestEngine, ExchangeConfig
from okx_quant.strategy.base import BaseStrategy


# ======================================================================
# 配置加载
# ======================================================================


def load_yaml(path: str) -> dict[str, Any]:
    """加载 YAML 配置文件。"""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ======================================================================
# 数据加载
# ======================================================================


async def fetch_history(
    rest: RESTClient,
    symbol: str,
    bar: str,
    days: int,
) -> list[BarData]:
    """从 OKX REST API 拉取历史 K 线数据。

    使用 RESTClient 的 get_history_candles 分页拉取，
    自动处理降序→升序翻转。
    """
    all_bars: list[BarData] = []
    after: int | None = None
    cutoff_ms = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

    print(f"⏳ 拉取 {symbol} 最近 {days} 天 {bar} K 线...")

    while True:
        batch = await rest.get_history_candles(symbol, bar, after=after, limit=100)
        if not batch:
            break

        # 过滤超出时间范围的
        filtered = [b for b in batch if b.timestamp >= cutoff_ms]
        all_bars.extend(filtered)

        # 如果这批里有超出范围的，说明已经到底
        if len(filtered) < len(batch):
            break

        # 游标：取这批最早的时间戳
        after = batch[0].timestamp
        print(f"   📥 已拉取 {len(all_bars)} 根，追溯至 "
              f"{datetime.fromtimestamp(after / 1000):%Y-%m-%d %H:%M}")

        await asyncio.sleep(0.15)  # 防频率限制

    # 按时间升序排列
    all_bars.sort(key=lambda b: b.timestamp)

    if all_bars:
        start = datetime.fromtimestamp(all_bars[0].timestamp / 1000)
        end = datetime.fromtimestamp(all_bars[-1].timestamp / 1000)
        print(f"✅ 共 {len(all_bars)} 根 K 线 | {start:%Y-%m-%d} → {end:%Y-%m-%d}")

    return all_bars


# ======================================================================
# 策略工厂
# ======================================================================


def load_strategy_class(class_path: str) -> type[BaseStrategy]:
    """动态加载策略类。格式: 'module.ClassName'"""
    module_path, class_name = class_path.rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


# ======================================================================
# 回测引擎工厂
# ======================================================================


def create_engine(config: dict[str, Any]) -> BacktestEngine:
    """从配置创建回测引擎。"""
    exchange = config.get("exchange", {})
    return BacktestEngine(
        initial_capital=config.get("initial_capital", 10_000),
        config=ExchangeConfig(
            maker_fee_rate=exchange.get("maker_fee_rate", 0.0002),
            taker_fee_rate=exchange.get("taker_fee_rate", 0.0005),
            slippage_base=exchange.get("slippage_base", 0.0003),
            tick_size=exchange.get("tick_size", 0.01),
            latency_bars=exchange.get("latency_bars", 1),
            leverage=exchange.get("leverage", 20),
            contract_multiplier=exchange.get("contract_multiplier", 1.0),
            funding_rate=exchange.get("funding_rate", 0.0001),
        ),
    )


# ======================================================================
# 步骤一：基准回归
# ======================================================================


def run_baseline(
    config: dict[str, Any],
    bars: list[BarData],
    strategy_class: type[BaseStrategy],
) -> dict[str, Any]:
    """基准回归测试：用当前配置参数跑一次。"""
    print("\n" + "=" * 50)
    print(" 🛠️ 步骤一：基准回归测试")
    print("=" * 50)

    params = config["strategy_params"]
    engine = create_engine(config)
    strategy = strategy_class()
    strategy.on_init(params)

    result = engine.run(strategy, bars, contract_mode=True)

    print(f"📊 参数: {params}")
    print(f"📈 收益:   {result.total_return:.2%}")
    print(f"📉 回撤:   {result.max_drawdown:.2%}")
    print(f"⚖️ 夏普:   {result.sharpe_ratio:.2f}")
    print(f"🎯 胜率:   {result.win_rate:.0%}")
    print(f"📋 交易数: {result.total_trades}")

    return {
        "params": params,
        "return": result.total_return,
        "drawdown": result.max_drawdown,
        "sharpe": result.sharpe_ratio,
        "win_rate": result.win_rate,
        "trades": result.total_trades,
    }


# ======================================================================
# 步骤二：网格寻优
# ======================================================================


def run_grid_search(
    config: dict[str, Any],
    bars: list[BarData],
    strategy_class: type[BaseStrategy],
) -> dict[str, Any]:
    """网格参数寻优。"""
    print("\n" + "=" * 50)
    print(" 🔍 步骤二：网格寻优")
    print("=" * 50)

    grid = config.get("grid_search", {})
    fast_periods = grid.get("fast_periods", [10, 15, 20])
    slow_periods = grid.get("slow_periods", [30, 40, 50])
    stop_losses = grid.get("stop_losses", [0.05, 0.08, 0.10])
    macro_period = config["strategy_params"].get("macro_period", 800)
    position_pct = config["strategy_params"].get("position_pct", 0.5)

    base_params = {k: v for k, v in config["strategy_params"].items()
                   if k not in ("fast_period", "slow_period", "stop_loss_pct")}

    combinations = [
        (f, s, sl)
        for f, s, sl in itertools.product(fast_periods, slow_periods, stop_losses)
        if f < s
    ]
    print(f"🚀 共 {len(combinations)} 组参数组合\n")

    best_sharpe = -999.0
    best_params: dict[str, Any] = {}
    results: list[dict[str, Any]] = []

    for i, (fast, slow, sl) in enumerate(combinations, 1):
        params = {
            **base_params,
            "fast_period": fast,
            "slow_period": slow,
            "stop_loss_pct": sl,
        }

        engine = create_engine(config)
        strategy = strategy_class()
        strategy.on_init(params)
        result = engine.run(strategy, bars, contract_mode=True)

        row = {
            "fast": fast, "slow": slow, "sl": sl,
            "return": result.total_return,
            "sharpe": result.sharpe_ratio,
            "drawdown": result.max_drawdown,
            "win_rate": result.win_rate,
            "trades": result.total_trades,
        }
        results.append(row)

        marker = ""
        if result.sharpe_ratio > best_sharpe and result.total_return > 0:
            best_sharpe = result.sharpe_ratio
            best_params = params
            marker = " ⭐"

        print(f"  [{i:2d}/{len(combinations)}] "
              f"EMA({fast}/{slow}) SL={sl:.0%} | "
              f"夏普={result.sharpe_ratio:+.2f} "
              f"收益={result.total_return:+.2%} "
              f"回撤={result.max_drawdown:.2%} "
              f"胜率={result.win_rate:.0%}"
              f"{marker}")

    print(f"\n✅ 最优参数: {best_params}")
    print(f"   夏普={best_sharpe:.2f}")

    return {"best_params": best_params, "best_sharpe": best_sharpe, "all_results": results}


# ======================================================================
# 步骤三：Walk-Forward 盲测
# ======================================================================


def run_walk_forward(
    config: dict[str, Any],
    bars: list[BarData],
    strategy_class: type[BaseStrategy],
    params: dict[str, Any],
) -> dict[str, Any]:
    """Walk-Forward 滚动前向验证。"""
    print("\n" + "=" * 50)
    print(" 🛡️ 步骤三：Walk-Forward 盲测")
    print("=" * 50)

    wf_config = config.get("walk_forward", {})
    train_pct = wf_config.get("train_pct", 0.7)
    overfit_threshold = wf_config.get("overfit_threshold", 0.5)

    engine = create_engine(config)
    result = engine.run_walk_forward(
        strategy_factory=strategy_class,
        bars=bars,
        params=params,
        train_pct=train_pct,
        overfit_threshold=overfit_threshold,
        contract_mode=True,
    )

    if result.is_overfit:
        print(f"❌ 过拟合警告: {result.overfit_warning}")
        return {"is_overfit": True, "warning": result.overfit_warning}
    else:
        print("✅ 策略稳健：通过未知数据盲测！")
        return {"is_overfit": False}


# ======================================================================
# 主入口
# ======================================================================


async def main():
    parser = argparse.ArgumentParser(description="OKX 量化投研流水线")
    parser.add_argument("--config", "-c", default="configs/research.yaml",
                        help="投研配置文件路径")
    parser.add_argument("--mode", "-m", default="all",
                        choices=["baseline", "grid", "walkforward", "all"],
                        help="运行模式")
    args = parser.parse_args()

    # 加载配置
    config = load_yaml(args.config)

    symbol = config.get("symbol", "ETH-USDT-SWAP")
    timeframe = config.get("timeframe", "15m")
    days = config.get("days", 90)
    strategy_path = config.get("strategy", "strategies.macro_ema.MacroEmaStrategy")

    print(f"🔬 OKX 投研流水线 | {symbol} | {timeframe} | {days}天")

    # 加载策略类
    strategy_class = load_strategy_class(strategy_path)

    # 连接 REST 拉取数据
    app_config = load_config()
    auth = OKXAuth(app_config.okx)
    rest = RESTClient(app_config.okx, auth)
    await rest.connect()

    try:
        bars = await fetch_history(rest, symbol, timeframe, days)
        if not bars:
            print("❌ 未获取到数据")
            return
    finally:
        await rest.close()

    # 执行投研流程
    if args.mode in ("baseline", "all"):
        run_baseline(config, bars, strategy_class)

    best_params = config["strategy_params"]
    if args.mode in ("grid", "all"):
        grid_result = run_grid_search(config, bars, strategy_class)
        best_params = grid_result["best_params"]

    if args.mode in ("walkforward", "all"):
        if args.mode == "all":
            # all 模式用网格寻优的最优参数
            pass
        run_walk_forward(config, bars, strategy_class, best_params)


if __name__ == "__main__":
    asyncio.run(main())
