# apex_client.py
# 主网专用版本（ApeX Omni Mainnet）
# ------------------------------------------------------------
# 重要：
#   - 只连接 ApeX 主网 (APEX_OMNI_HTTP_MAIN / NETWORKID_OMNI_MAIN_ARB)
#   - 必须设置 APEX_USE_MAINNET=true，否则拒绝初始化
#   - 使用 Omni Key seeds (APEX_ZK_SEEDS)，l2Key 允许为空字符串 ''
# ------------------------------------------------------------

import os
import time
from typing import Any, Dict

# ✅ 关键改动在这里：用 http_private_sign，而不是 http_private_v3
from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    NETWORKID_OMNI_MAIN_ARB,
)

# ----------------------------------------------------------------------
# 1. 环境变量读取 & 主网保护开关
# ----------------------------------------------------------------------

# DO 环境里：APEX_USE_MAINNET=true 才允许连主网
APEX_USE_MAINNET = os.getenv("APEX_USE_MAINNET", "false").lower() == "true"

if not APEX_USE_MAINNET:
    raise RuntimeError(
        "APEX_USE_MAINNET is not 'true'. "
        "This apex_client.py is MAINNET-ONLY. "
        "If you really want to use Apex mainnet, set APEX_USE_MAINNET=true in your environment."
    )

# ApeX 主网基础配置
BASE_URL = APEX_OMNI_HTTP_MAIN
NETWORK_ID = NETWORKID_OMNI_MAIN_ARB

# API key 三件套（在 Omni 主网 keyManagement 里生成）
API_KEY = os.getenv("APEX_API_KEY")
API_SECRET = os.getenv("APEX_API_SECRET")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE")

# 必填：Omni Key seeds（主网的那一串）
ZK_SEEDS = os.getenv("APEX_ZK_SEEDS")

# 可选：l2Key seeds（可以留空，方案 A）
L2KEY_SEEDS = os.getenv("APEX_L2KEY_SEEDS", "") or ""

# ----------------------------------------------------------------------
# 2. 基本检查（缺啥就直接抛异常）
# ----------------------------------------------------------------------


def _check_env() -> None:
    missing = []
    if not API_KEY:
        missing.append("APEX_API_KEY")
    if not API_SECRET:
        missing.append("APEX_API_SECRET")
    if not API_PASSPHRASE:
        missing.append("APEX_API_PASSPHRASE")
    if not ZK_SEEDS:
        missing.append("APEX_ZK_SEEDS")

    if missing:
        raise RuntimeError(
            f"[apex_client] Missing required environment variables for MAINNET: {', '.join(missing)}"
        )


# ----------------------------------------------------------------------
# 3. 初始化 HttpPrivateSign（主网）
# ----------------------------------------------------------------------


def init_client() -> HttpPrivateSign:
    """
    初始化 ApeX Omni 主网 HttpPrivateSign 客户端。

    - 使用主网 BASE_URL / NETWORK_ID
    - 使用 Omni 主网的 ZK_SEEDS
    - L2KEY_SEEDS 允许为空字符串 ''
    """
    _check_env()

    client = HttpPrivateSign(
        BASE_URL,
        network_id=NETWORK_ID,
        zk_seeds=ZK_SEEDS,
        zk_l2Key=L2KEY_SEEDS,  # 方案 A：允许为空字符串
        api_key_credentials={
            "key": API_KEY,
            "secret": API_SECRET,
            "passphrase": API_PASSPHRASE,
        },
    )

    # 官方建议：使用私有接口前先 configs_v3()
    client.configs_v3()

    return client


# 全局客户端实例（被 app.py 等文件复用）
client: HttpPrivateSign = init_client()

# ----------------------------------------------------------------------
# 4. 封装常用调用
# ----------------------------------------------------------------------


def get_account() -> Dict[str, Any]:
    """
    获取账户信息（主网）。
    """
    return client.get_account_v3()


def get_balances() -> Dict[str, Any]:
    """
    获取账户余额信息（主网）。
    """
    return client.get_account_balance_v3()


def create_market_order(
    symbol: str,
    side: str,
    size: str,
    price: str | None = None,
) -> Dict[str, Any]:
    """
    在 ApeX 主网创建一个简单的市价单。

    参数：
        symbol: 例如 "BTC-USDT"（务必用 ApeX 主网上的合约代码）
        side  : "BUY" 或 "SELL"
        size  : 例如 "0.001"
        price : 可选，市价单可以不传，如果传就当作参考价字符串
    """
    ts = int(time.time())

    params: Dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "size": size,
        "timestampSeconds": ts,
    }
    if price is not None:
        params["price"] = str(price)

    return client.create_order_v3(**params)


# ----------------------------------------------------------------------
# 5. 自测：本地 / DO console 里可以直接 python apex_client.py 看主网连通性
# ----------------------------------------------------------------------


def _self_test() -> None:
    print(f"[apex_client] MAINNET MODE ENABLED: {APEX_USE_MAINNET}")
    print(f"[apex_client] BASE_URL = {BASE_URL}, NETWORK_ID = {NETWORK_ID}")

    cfg = client.configs_v3()
    print(
        "[apex_client] configs_v3 OK, symbols:",
        len(cfg.get("data", {}).get("symbols", [])),
    )

    acc = client.get_account_v3()
    account_id = acc.get("data", {}).get("account", {}).get("id")
    print("[apex_client] get_account_v3 OK, account id:", account_id)


if __name__ == "__main__":
    _self_test()
