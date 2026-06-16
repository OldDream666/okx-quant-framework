#!/usr/bin/env python3
"""
OKX Quant Framework — 通用实盘/模拟盘启动脚本

用法:
    # 使用配置文件
    python run_live.py --config configs/paper_trading.yaml

    # 命令行覆盖参数
    python run_live.py --config configs/paper_trading.yaml --leverage 10 --symbol BTC-USDT-SWAP

    # 查看所有选项
    python run_live.py --help

配置优先级: 命令行参数 > 配置文件 > 默认值
"""

import argparse
import asyncio
import importlib
import sys
import os
from pathlib import Path

# 确保项目根目录在 Python 路径中
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

import yaml
from okx_quant.config import load_config
from okx_quant.risk.risk_manager import RiskConfig
from okx_quant.strategy.base import BaseStrategy
from okx_quant.live import LiveRunner
from okx_quant.monitoring.logger import setup_logger
from okx_quant.monitoring.ledger import TradeLedger


def load_yaml(path: str) -> dict:
    """Load YAML config file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_strategy(class_path: str) -> type[BaseStrategy]:
    """Dynamically load a strategy class from a dotted path.

    Examples:
        "strategies.macro_ema.MacroEmaStrategy"
        "okx_quant.strategy.templates.ema_cross.EmaCrossStrategy"
    """
    module_path, class_name = class_path.rsplit(".", 1)
    # Try relative import from project root first
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    if not issubclass(cls, BaseStrategy):
        raise TypeError(f"{class_path} is not a subclass of BaseStrategy")
    return cls


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="OKX Quant Framework — Live/Paper Trading Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", "-c", type=str, required=True,
                        help="YAML config file path")
    parser.add_argument("--symbol", "-s", type=str, default=None,
                        help="Override trading symbol (e.g. BTC-USDT-SWAP)")
    parser.add_argument("--timeframe", "-t", type=str, default=None,
                        help="Override K-line timeframe (e.g. 15m, 1H)")
    parser.add_argument("--leverage", "-l", type=int, default=None,
                        help="Override leverage (1-125)")
    parser.add_argument("--log-level", type=str, default=None,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Override log level")
    parser.add_argument("--dry-run", action="store_true",
                        help="Load config and strategy, then exit (no trading)")
    return parser.parse_args()


def main():
    args = parse_args()

    # ── 1. Load YAML config ──
    print(f"📋 加载配置: {args.config}")
    cfg = load_yaml(args.config)

    # ── 2. Command-line overrides ──
    symbol = args.symbol or cfg.get("symbol", "BTC-USDT-SWAP")
    timeframe = args.timeframe or cfg.get("timeframe", "1H")
    leverage = args.leverage if args.leverage is not None else cfg.get("leverage", 1)
    log_level = args.log_level or cfg.get("log_level", "INFO")
    preload_bars = cfg.get("preload_bars", 900)

    # ── 3. Setup logging ──
    log_dir = cfg.get("log_dir", "logs")
    setup_logger(log_dir=log_dir, level=log_level, console=True)

    # ── 4. Load OKX credentials ──
    env_config = load_config()
    okx_config = env_config.okx
    print(f"🔑 Environment: {'模拟盘' if okx_config.is_demo else '⚠️ 实盘'}")
    print(f"   API Key: {okx_config.api_key[:8]}...")

    # ── 5. Load strategy dynamically ──
    strategy_class_path = cfg.get("strategy", "")
    if not strategy_class_path:
        print("❌ 错误: 配置文件中未指定 strategy")
        sys.exit(1)

    strategy_class = load_strategy(strategy_class_path)
    strategy = strategy_class()
    strategy_params = cfg.get("strategy_params", {})

    print(f"📊 策略: {strategy_class.__name__} ({strategy.name})")

    # ── 6. Build risk config ──
    risk_cfg = cfg.get("risk", {})
    risk_config = RiskConfig(
        max_order_value=risk_cfg.get("max_order_value", 50_000),
        max_total_exposure=risk_cfg.get("max_total_exposure", 200_000),
        max_price_deviation=risk_cfg.get("max_price_deviation", 0.05),
        max_orders_per_sec=risk_cfg.get("max_orders_per_sec", 5),
        max_consecutive_failures=risk_cfg.get("max_consecutive_failures", 5),
        max_slippage_pct=risk_cfg.get("max_slippage_pct", 0.01),
        max_drawdown_pct=risk_cfg.get("max_drawdown_pct", 0.20),
    )

    # ── 7. Print summary ──
    print(f"\n{'─' * 50}")
    print(f"  交易对:    {symbol}")
    print(f"  周期:      {timeframe}")
    print(f"  杠杆:      {leverage}x")
    print(f"  预加载:    {preload_bars} 根 K 线")
    print(f"  策略参数:  {strategy_params}")
    print(f"  风控:")
    print(f"    单笔上限:    ${risk_config.max_order_value:,.0f}")
    print(f"    总持仓上限:  ${risk_config.max_total_exposure:,.0f}")
    print(f"    回撤阈值:    {risk_config.max_drawdown_pct:.0%}")
    print(f"{'─' * 50}\n")

    # ── 8. Dry run or live ──
    if args.dry_run:
        print("🏁 验证完成 — 未启动交易。")
        return

    # ── 9. Create and start LiveRunner ──
    ledger = TradeLedger(data_dir="data/live", symbol=symbol)

    runner = LiveRunner(
        config=okx_config,
        strategy=strategy,
        strategy_params=strategy_params,
        symbol=symbol,
        timeframe=timeframe,
        risk_config=risk_config,
        bar_callback=lambda bar: None,
        leverage=leverage,
        ledger=ledger,
    )

    asyncio.run(runner.start())


if __name__ == "__main__":
    main()
