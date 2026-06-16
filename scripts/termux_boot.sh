#!/bin/bash
# ============================================================
# Termux:Boot 开机自启脚本
# ============================================================
# 安装方法:
#   1. 安装 Termux:Boot (F-Droid)
#   2. 复制本文件到 ~/.termux/boot/
#      cp scripts/termux_boot.sh ~/.termux/boot/start_okx_quant.sh
#   3. 打开 Termux:Boot 应用一次（授权）
#   4. 重启手机后自动启动交易
# ============================================================

# 等待系统就绪
sleep 30

# 获取 wake lock
termux-wake-lock 2>/dev/null || true

# 启动交易
cd ~/okx-quant-framework
bash scripts/run_termux_bg.sh configs/paper_trading.yaml
