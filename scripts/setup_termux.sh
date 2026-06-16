#!/bin/bash
# ============================================================
# Termux 安装脚本 — 在安卓手机上运行 OKX 量化框架
# ============================================================
# 用法: bash scripts/setup_termux.sh
#
# 前提:
#   1. 安装 Termux (F-Droid 版本，不要用 Play Store 版)
#   2. 打开 Termux，执行本脚本
# ============================================================

set -e

echo "================================================"
echo "  OKX 量化框架 — Termux 安装"
echo "================================================"

# ─────────────────────────────────────────────
# 1. 基础依赖
# ─────────────────────────────────────────────
echo ""
echo "📦 [1/6] 安装系统依赖..."
pkg update -y
pkg install -y python rust binutils

# ─────────────────────────────────────────────
# 2. 克隆项目（如果还没克隆）
# ─────────────────────────────────────────────
echo ""
echo "📂 [2/6] 准备项目目录..."
if [ ! -d "$HOME/okx-quant-framework" ]; then
    echo "  请手动克隆项目:"
    echo "  git clone https://github.com/OldDream666/okx-quant-framework.git ~/okx-quant-framework"
    echo ""
    echo "  或者从电脑传输:"
    echo "  scp -r ~/okx-quant-framework phone:~/"
    exit 1
fi

cd ~/okx-quant-framework

# ─────────────────────────────────────────────
# 3. Python 虚拟环境
# ─────────────────────────────────────────────
echo ""
echo "🐍 [3/6] 创建 Python 虚拟环境..."
python -m venv .venv
source .venv/bin/activate

# ─────────────────────────────────────────────
# 4. 安装依赖
# ─────────────────────────────────────────────
echo ""
echo "📥 [4/6] 安装 Python 依赖..."
pip install --upgrade pip

# pydantic-core 需要 Rust 编译（Termux aarch64 没有预编译 wheel）
# 设置编译环境
export PYO3_CROSS_LIB_DIR=""
export PYO3_CROSS_PYTHON_VERSION=""

pip install -e ".[dev]" 2>&1 || {
    echo ""
    echo "⚠️  如果 pydantic 编译失败，尝试:"
    echo "    pkg install -y cmake ninja"
    echo "    pip install maturin"
    echo "    pip install -e ."
    echo ""
    echo "  或者使用纯 Python 模式:"
    echo "    PYO3_PYTHON=python pip install pydantic --no-binary pydantic-core"
    exit 1
}

# ─────────────────────────────────────────────
# 5. 配置文件
# ─────────────────────────────────────────────
echo ""
echo "🔑 [5/6] 配置 API 密钥..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo "  ⚠️  请编辑 .env 文件填入你的 OKX API 密钥:"
    echo "    nano .env"
    echo ""
    echo "  必填项:"
    echo "    OKX_API_KEY=your_key"
    echo "    OKX_SECRET_KEY=your_secret"
    echo "    OKX_PASSPHRASE=your_passphrase"
    echo "    OKX_IS_DEMO=true"
else
    echo "  ✅ .env 已存在"
fi

# ─────────────────────────────────────────────
# 6. Termux 保活配置
# ─────────────────────────────────────────────
echo ""
echo "🔒 [6/6] 配置 Termux 后台保活..."

# 获取 Termux wake lock（防止系统杀进程）
termux-wake-lock 2>/dev/null || echo "  ⚠️  termux-wake-lock 不可用，请手动在 Termux 通知栏点击 Acquire wakelock"

echo ""
echo "================================================"
echo "  ✅ 安装完成！"
echo "================================================"
echo ""
echo "  启动模拟盘:"
echo "    cd ~/okx-quant-framework"
echo "    source .venv/bin/activate"
echo "    python run_live.py --config configs/paper_trading.yaml"
echo ""
echo "  后台运行（推荐）:"
echo "    bash scripts/run_termux_bg.sh"
echo ""
echo "  查看日志:"
echo "    tail -f logs/trading_$(date +%Y-%m-%d).log"
echo ""
echo "  ⚠️  重要提醒:"
echo "    1. 在 Termux 通知栏开启 'Acquire wakelock'"
echo "    2. 系统设置 → 电池 → Termux → 不限制后台"
echo "    3. 锁定 Termux 在最近任务中（防止被清理）"
echo "================================================"
