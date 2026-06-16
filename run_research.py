"""
投研流水线：基准回归 -> 网格寻优 -> Walk-Forward 盲测
"""
import httpx
import asyncio
import time
from datetime import datetime, timedelta
from okx_quant.models.market import BarData
import itertools
from okx_quant.backtest import BacktestEngine, ExchangeConfig
from okx_quant.models.market import BarData
from strategies.macro_ema import MacroEmaStrategy


# ==========================================
# 1. 配置实盘级别的硬核回测环境
# ==========================================
def create_strict_engine() -> BacktestEngine:
    """创建带有真实摩擦成本的回测引擎"""
    return BacktestEngine(
        initial_capital=10_000,
        config=ExchangeConfig(
            maker_fee_rate=0.0002,         # Maker 手续费
            taker_fee_rate=0.0005,         # Taker 手续费
            slippage_base=0.0003,          # 基础滑点
            tick_size=0.01,                # ETH 最小变动价位
            latency_bars=1,                # 1 根 K 线延迟，绝对防未来函数
            leverage=20,                   # 20倍杠杆
            contract_multiplier=0.1,       # ETH-USDT-SWAP 最新合约面值
            funding_rate=0.0001            # 模拟资金费率磨损
        )
    )

# ==========================================
# 2. 从 OKX 真实接口加载历史数据
# ==========================================
def load_historical_data() -> list[BarData]:
    """
    同步包装器：供主程序直接调用。
    拉取最近 90 天 ETH-USDT-SWAP 的 15m K线数据。
    """
    return asyncio.run(_fetch_okx_history())

async def _fetch_okx_history() -> list[BarData]:
    symbol = "ETH-USDT-SWAP"
    bar_size = "15m"
    days_to_fetch = 90
    limit_per_request = 100

    # 计算目标时间截点
    end_time_ms = int(time.time() * 1000)
    start_time_ms = int((datetime.now() - timedelta(days=days_to_fetch)).timestamp() * 1000)

    bars = []
    after_ts = ""  # 游标初始为空，代表从当前最新时间开始拉取

    print(f"⏳ 开始从 OKX 拉取 {symbol} 最近 {days_to_fetch} 天的 {bar_size} K线数据...")

    async with httpx.AsyncClient() as client:
        while True:
            url = "https://www.okx.com/api/v5/market/history-candles"
            params = {
                "instId": symbol,
                "bar": bar_size,
                "limit": limit_per_request
            }
            if after_ts:
                params["after"] = after_ts

            try:
                # 设置 10 秒超时时间，防止网络假死
                response = await client.get(url, params=params, timeout=10.0)
                response.raise_for_status()
                data = response.json()

                if data.get("code") != "0" or not data.get("data"):
                    print(f"⚠️ 数据拉取到底或出现异常: {data.get('msg')}")
                    break

                batch_data = data["data"]
                for row in batch_data:
                    # OKX 返回格式: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                    ts = int(row[0])
                    
                    # 如果已经触及 90 天前的时间线，直接截断
                    if ts < start_time_ms:
                        break

                    bar = BarData(
                        symbol=symbol,
                        timestamp=ts,
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                        confirmed=int(row[8])
                    )
                    bars.append(bar)

                # 获取这批数据中最老的一根的时间戳，作为下一次请求的游标
                last_ts = int(batch_data[-1][0])
                
                # 如果最老的一根已经超出了我们需要的天数，或者返回数据不足 100 条（说明没数据了），退出循环
                if last_ts < start_time_ms or len(batch_data) < limit_per_request:
                    break

                after_ts = str(last_ts)
                current_date = datetime.fromtimestamp(last_ts/1000).strftime('%Y-%m-%d %H:%M')
                print(f"   📥 已拉取 {len(bars)} 根 K 线，当前进度追溯至: {current_date}")

                # ⚠️ 核心防御：防止触发 OKX 接口频率限制 (20次/2秒)
                await asyncio.sleep(0.15)

            except Exception as e:
                print(f"❌ 网络请求异常: {e}，休眠 2 秒后自动重试...")
                await asyncio.sleep(2)

    # 🚀 致命修复：OKX 返回的数据是按时间倒序的（最新在最前），回测引擎必须按时间正序消费
    bars.reverse()
    
    if bars:
        start_date = datetime.fromtimestamp(bars[0].timestamp/1000).strftime('%Y-%m-%d %H:%M')
        end_date = datetime.fromtimestamp(bars[-1].timestamp/1000).strftime('%Y-%m-%d %H:%M')
        print(f"✅ 拉取完成！共获取 {len(bars)} 根有效 K 线。")
        print(f"   跨度: {start_date} -> {end_date}")
    
    return bars

# ==========================================
# 步骤一：基准回归测试 (Baseline Regression)
# ==========================================
def run_baseline(bars: list[BarData]):
    print("\n" + "="*50)
    print(" 🛠️ 步骤一：基准回归测试 (15-40-800)")
    print("="*50)
    
    engine = create_strict_engine()
    strategy = MacroEmaStrategy()
    strategy.on_init({
        "fast_period": 15, 
        "slow_period": 40, 
        "macro_period": 800,
        "stop_loss_pct": 0.05,
        "position_pct": 0.025
    })
    
    result = engine.run(strategy, bars, contract_mode=True)
    
    print(f"📊 基准收益:   {result.total_return:.2%}")
    print(f"📉 最大回撤:   {result.max_drawdown:.2%}")
    print(f"⚖️ 夏普比率:   {result.sharpe_ratio:.2f}")
    print(f"🎯 胜率:       {result.win_rate:.0%}")
    print("💡 结论：评估修复 P0 漏洞后，原策略是否依然具备正向 Alpha。")

# ==========================================
# 步骤二：网格寻优 (Grid Parameter Tuning)
# ==========================================
def run_grid_search(bars: list[BarData]) -> dict:
    print("\n" + "="*50)
    print(" 🔍 步骤二：网格寻优 (Grid Search)")
    print("="*50)
    
    # 定义搜索空间
    fast_periods = [10, 15, 20]
    slow_periods = [30, 40, 50]
    stop_losses = [0.05, 0.08, 0.10]
    
    best_sharpe = -999
    best_params = {}
    
    combinations = list(itertools.product(fast_periods, slow_periods, stop_losses))
    print(f"🚀 共需测试 {len(combinations)} 组参数组合...")
    
    for fast, slow, sl in combinations:
        # 排除不合理的均线组合
        if fast >= slow:
            continue
            
        engine = create_strict_engine()
        strategy = MacroEmaStrategy()
        params = {
            "fast_period": fast,
            "slow_period": slow,
            "macro_period": 800, # 宏观滤网保持不动
            "stop_loss_pct": sl,
            "position_pct": 0.025
        }
        strategy.on_init(params)
        result = engine.run(strategy, bars, contract_mode=True)
        
        # 记录最优夏普比率
        if result.sharpe_ratio > best_sharpe and result.total_return > 0:
            best_sharpe = result.sharpe_ratio
            best_params = params
            print(f"   ⭐ 新最优: {fast}-{slow} (SL: {sl:.0%}) | 夏普: {best_sharpe:.2f} | 收益: {result.total_return:.2%}")
            
    print(f"✅ 寻优结束。最优参数为: {best_params}")
    return best_params

# ==========================================
# 步骤三：滚动前向验证 (Walk-Forward Validation)
# ==========================================
def run_walk_forward(bars: list[BarData], best_params: dict):
    print("\n" + "="*50)
    print(" 🛡️ 步骤三：Walk-Forward 滚动前向盲测")
    print("="*50)
    
    engine = create_strict_engine()
    
    # 按照 GUIDE.md 规范调用
    result = engine.run_walk_forward(
        strategy_factory=MacroEmaStrategy,  
        bars=bars,
        params=best_params,
        train_pct=0.7,             # 70% 训练寻找最优，30% 未知数据盲测
        overfit_threshold=0.5,     # 容忍夏普衰减 50%
        contract_mode=True
    )
    
    if result.is_overfit:
        print("❌ 警告：触发过拟合拦截！")
        print(f"原因: {result.overfit_warning}")
        print("建议：扩大历史数据样本，或减少策略参数维度。")
    else:
        print("✅ 策略稳健：顺利通过未知数据盲测！")
        print("现在你可以将这组参数写入 configs/live_50usdt.yaml 中进行实盘了。")

# ==========================================
# 主执行入口
# ==========================================
if __name__ == "__main__":
    historical_bars = load_historical_data()
    
    if not historical_bars:
        print("⚠️ 请先在 load_historical_data() 中接入 K 线数据源。")
    else:
        # 1. 测基准
        run_baseline(historical_bars)
        # 2. 找参数
        best = run_grid_search(historical_bars)
        # 3. 防过拟合盲测
        if best:
            run_walk_forward(historical_bars, best)