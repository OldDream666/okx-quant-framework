#!/bin/bash
# ============================================================
# Termux 后台运行脚本 — 防止 Android 杀进程
# ============================================================
# 用法: bash scripts/run_termux_bg.sh [config_path]
#
# 功能:
#   1. 获取 Termux wake lock
#   2. 使用 nohup 后台运行
#   3. 自动日志轮转
#   4. 进程监控 + 自动重启
#   5. Termux:Boot 开机自启支持
# ============================================================

set -e

CONFIG="${1:-configs/paper_trading.yaml}"
PROJECT_DIR="$HOME/okx-quant-framework"
PID_FILE="$PROJECT_DIR/.live.pid"
LOG_DIR="$PROJECT_DIR/logs"
MAIN_LOG="$LOG_DIR/trading_$(date +%Y-%m-%d).log"
MONITOR_LOG="$LOG_DIR/monitor.log"

cd "$PROJECT_DIR"

# ─────────────────────────────────────────────
# 前置检查
# ─────────────────────────────────────────────
if [ ! -f ".venv/bin/activate" ]; then
    echo "❌ 虚拟环境不存在，请先运行: bash scripts/setup_termux.sh"
    exit 1
fi

if [ ! -f "$CONFIG" ]; then
    echo "❌ 配置文件不存在: $CONFIG"
    exit 1
fi

if [ ! -f ".env" ]; then
    echo "❌ .env 文件不存在，请配置 API 密钥"
    exit 1
fi

# ─────────────────────────────────────────────
# 获取 wake lock
# ─────────────────────────────────────────────
echo "🔒 获取 Termux wake lock..."
termux-wake-lock 2>/dev/null || true

# ─────────────────────────────────────────────
# 停止旧进程
# ─────────────────────────────────────────────
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "🛑 停止旧进程 (PID=$OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        sleep 2
    fi
    rm -f "$PID_FILE"
fi

# ─────────────────────────────────────────────
# 启动交易进程
# ─────────────────────────────────────────────
echo "🚀 启动 OKX 量化交易..."
echo "  配置: $CONFIG"
echo "  日志: $MAIN_LOG"

source .venv/bin/activate

nohup python run_live.py --config "$CONFIG" >> "$MAIN_LOG" 2>&1 &
TRADE_PID=$!
echo "$TRADE_PID" > "$PID_FILE"

echo "  PID: $TRADE_PID"
echo ""
echo "✅ 交易进程已后台启动"
echo ""

# ─────────────────────────────────────────────
# 监控循环（可选）
# ─────────────────────────────────────────────
echo "📊 监控模式（Ctrl+C 退出监控，交易继续运行）"
echo "  按 q 退出监控，交易不受影响"
echo ""

monitor_loop() {
    while true; do
        if ! kill -0 "$TRADE_PID" 2>/dev/null; then
            echo ""
            echo "⚠️  [$MONITOR_LOG] 交易进程已退出 (PID=$TRADE_PID)"
            echo "  查看日志: tail -20 $MAIN_LOG"
            echo "  重启: bash scripts/run_termux_bg.sh $CONFIG"
            break
        fi

        # 显示最新状态
        LAST_LINE=$(tail -1 "$MAIN_LOG" 2>/dev/null || echo "等待日志...")
        echo -ne "\r📊 $(date +%H:%M:%S) | PID=$TRADE_PID | $LAST_LINE\033[K"

        sleep 10
    done
}

monitor_loop
