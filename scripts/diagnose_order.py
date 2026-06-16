#!/usr/bin/env python3
"""下单诊断脚本 — 逐步测试 OKX API，定位 Error [1] 根因。

用法: python scripts/diagnose_order.py

测试顺序:
1. 账户余额（确认 API Key 有效）
2. 持仓查询（确认签名正确）
3. 品种规格（确认 instId 存在）
4. 杠杆设置（确认交易权限）
5. 模拟下单（最小数量，定位 Error [1]）
"""

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(override=True)

from okx_quant.config.settings import load_config
from okx_quant.config.auth import OKXAuth
from okx_quant.gateway.rest_client import RESTClient


async def main():
    cfg = load_config()
    okx = cfg.okx
    auth = OKXAuth(okx)

    print("=" * 60)
    print("  OKX 下单诊断")
    print("=" * 60)
    print(f"  环境:     {'模拟盘' if okx.is_demo else '⚠️ 实盘'}")
    print(f"  Base URL: {okx.base_url}")
    print(f"  API Key:  {okx.api_key[:8]}...")
    print("=" * 60)

    async with RESTClient(okx, auth) as client:
        # ─────────────────────────────────────────────
        # Step 1: 账户余额
        # ─────────────────────────────────────────────
        print("\n📋 Step 1: 查询账户余额...")
        try:
            data = await client._request("GET", "/api/v5/account/balance", signed=True)
            bal = data[0] if data else {}
            total_eq = bal.get("totalEq", "N/A")
            details = bal.get("details", [])
            print(f"  ✅ 账户总权益: {total_eq} USD")
            for d in details[:3]:
                print(f"     {d.get('ccy', '?')}: avail={d.get('availBal', '0')}, eq={d.get('eq', '0')}")
        except Exception as e:
            print(f"  ❌ 查询余额失败: {e}")
            print("  → API Key 可能无效或权限不足")
            return

        # ─────────────────────────────────────────────
        # Step 2: 持仓查询
        # ─────────────────────────────────────────────
        print("\n📋 Step 2: 查询当前持仓...")
        try:
            data = await client._request("GET", "/api/v5/account/positions", signed=True)
            print(f"  ✅ 持仓查询成功, {len(data)} 个持仓")
            for p in data[:3]:
                print(f"     {p.get('instId', '?')}: pos={p.get('pos', '0')} side={p.get('posSide', 'net')}")
        except Exception as e:
            print(f"  ❌ 查询持仓失败: {e}")

        # ─────────────────────────────────────────────
        # Step 3: 品种规格
        # ─────────────────────────────────────────────
        symbol = "ETH-USDT-SWAP"
        print(f"\n📋 Step 3: 查询品种规格 {symbol}...")
        try:
            data = await client._request(
                "GET", "/api/v5/public/instruments",
                params={"instType": "SWAP"}, signed=False,
            )
            found = None
            for inst in data:
                if inst.get("instId") == symbol:
                    found = inst
                    break
            if found:
                print(f"  ✅ 品种存在:")
                print(f"     instId={found['instId']}")
                print(f"     lotSz={found.get('lotSz', '?')}")
                print(f"     minSz={found.get('minSz', '?')}")
                print(f"     tickSz={found.get('tickSz', '?')}")
                print(f"     ctMult={found.get('ctMult', '?')}")
                print(f"     state={found.get('state', '?')}")
            else:
                print(f"  ❌ 品种 {symbol} 不存在!")
                all_ids = [d["instId"] for d in data if "ETH" in d.get("instId", "")]
                print(f"     可用的 ETH 品种: {all_ids[:5]}")
                return
        except Exception as e:
            print(f"  ❌ 查询品种失败: {e}")
            return

        # ─────────────────────────────────────────────
        # Step 4: 设置杠杆
        # ─────────────────────────────────────────────
        print(f"\n📋 Step 4: 设置杠杆 20x ({symbol})...")
        try:
            data = await client._request(
                "POST", "/api/v5/account/set-leverage",
                body={"instId": symbol, "lever": "20", "mgnMode": "cross"},
                signed=True,
            )
            print(f"  ✅ 杠杆设置成功: {data}")
        except Exception as e:
            print(f"  ⚠️ 杠杆设置异常: {e}")

        # ─────────────────────────────────────────────
        # Step 5: 模拟下单 (最小数量)
        # ─────────────────────────────────────────────
        min_sz = found.get("minSz", "0.001")
        print(f"\n📋 Step 5: 模拟下单 {symbol} (最小数量 {min_sz}, posSide=long)...")
        body = {
            "instId": symbol,
            "tdMode": "cross",
            "side": "buy",
            "ordType": "market",
            "sz": min_sz,
            "posSide": "long",  # 双向持仓模式必须指定
        }
        print(f"  请求体: {json.dumps(body, indent=2)}")

        try:
            data = await client._request(
                "POST", "/api/v5/trade/order",
                body=body, signed=True,
            )
            if data:
                result = data[0]
                s_code = result.get("sCode", "?")
                s_msg = result.get("sMsg", "")
                ord_id = result.get("ordId", "")
                print(f"  sCode={s_code} sMsg={s_msg} ordId={ord_id}")
                if s_code == "0":
                    print(f"  ✅ 下单成功! ordId={ord_id}")
                    # 尝试撤单
                    print(f"\n📋 Step 5b: 撤单...")
                    try:
                        cancel_data = await client._request(
                            "POST", "/api/v5/trade/cancel-order",
                            body={"instId": symbol, "ordId": ord_id},
                            signed=True,
                        )
                        print(f"  ✅ 撤单成功: {cancel_data}")
                    except Exception as ce:
                        print(f"  ⚠️ 撤单失败: {ce}")
                else:
                    print(f"  ❌ 下单被拒绝!")
                    print(f"  → OKX 返回: sCode={s_code}, sMsg='{s_msg}'")
            else:
                print(f"  ❌ OKX 返回空 data")
        except Exception as e:
            err_str = str(e)
            print(f"  ❌ 下单失败: {err_str}")
            # 解析错误详情
            if "code=1" in err_str or "[1]" in err_str:
                print()
                print("  ╔══════════════════════════════════════════════════╗")
                print("  ║  OKX Error [1]: 请检查以下可能原因:              ║")
                print("  ║                                                  ║")
                print("  ║  1. API Key 权限: 去 OKX → API → 确认勾选了     ║")
                print("  ║     '交易' 权限（不只是'读取'）                  ║")
                print("  ║                                                  ║")
                print("  ║  2. 模拟盘合约: 确认 OKX 模拟盘已开通           ║")
                print("  ║     ETH-USDT-SWAP 永续合约交易                   ║")
                print("  ║                                                  ║")
                print("  ║  3. 账户模式: 确认账户已切换到'跨币种保证金'    ║")
                print("  ║     模式 (OKX App → 交易 → 账户模式)            ║")
                print("  ╚══════════════════════════════════════════════════╝")

        # ─────────────────────────────────────────────
        # Step 6: 补充 - 查询账户配置
        # ─────────────────────────────────────────────
        print(f"\n📋 Step 6: 查询账户配置...")
        try:
            data = await client._request("GET", "/api/v5/account/config", signed=True)
            if data:
                cfg_info = data[0]
                print(f"  uid={cfg_info.get('uid', '?')}")
                print(f"  acctLv={cfg_info.get('acctLv', '?')} (1=简单, 2=单币种, 3=跨币种, 4=组合)")
                print(f"  posMode={cfg_info.get('posMode', '?')} (long_short_mode / net_mode)")
                print(f"  autoLoan={cfg_info.get('autoLoan', '?')}")
        except Exception as e:
            print(f"  ⚠️ 查询账户配置失败: {e}")

        # ─────────────────────────────────────────────
        # Step 7: 补充 - 查询交易权限
        # ─────────────────────────────────────────────
        print(f"\n📋 Step 7: 查询 API Key 权限...")
        try:
            data = await client._request("GET", "/api/v5/account/api-key", signed=True)
            if data:
                key_info = data[0]
                print(f"  label={key_info.get('label', '?')}")
                print(f"  perm={key_info.get('perm', '?')}")
                print(f"  ip={key_info.get('ip', '无限制')}")
                print(f"  expire={key_info.get('expire', '永不过期')}")
        except Exception as e:
            print(f"  ⚠️ 查询权限失败: {e}")

    print("\n" + "=" * 60)
    print("  诊断完成")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
