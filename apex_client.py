# apex_client.py
# 主网专用版本（ApeX Omni Mainnet）

import os
import time
from typing import Any, Dict

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    NETWORKID_OMNI_MAIN_ARB,
)

# ----------------------------------------------------
# 1. 环境变量 & 主网保护
# ----------------------------------------------------

APEX_USE_MAINNET = os.getenv("APEX_USE_MAINNET", "false").lower() == "true"

if not APEX_USE_MAINNET:
    raise RuntimeError(
        "APEX_USE_MAINNET is not 'true'. "
        "This apex_client.py is MAINNET-ONLY. "
        "Set APEX_USE_MAINNET=true in your App env vars if you really want mainnet."
    )

BASE_URL = APEX_OMNI_HTTP_MAIN
NETWORK_ID = NETWORKID_OMNI_MAIN_ARB

API_KEY = os.getenv("APEX_API_KEY")
API_SECRET = os.getenv("APEX_API_SECRET")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE")

ZK_SEEDS = os.getenv("APEX_ZK_SEEDS")
L2KEY_SEEDS = os.getenv("APEX_L2KEY_SEEDS", "") or ""


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
            f"[apex_client] Missing required env vars for MAINNET: {', '.join(missing)}"
        )


# ----------------------------------------------------
# 2. 初始化 HttpPrivateSign（关键：先 configs_v3 + get_account_v3）
# ----------------------------------------------------

def init_client() -> HttpPrivateSign:
    """
    初始化 ApeX Omni 主网 HttpPrivateSign 客户端。

    注意：
        必须先调用 configs_v3() 和 get_account_v3()
        这样 HttpPrivateSign 里才会生成 self.accountv3，后面 create_order_v3() 才不会报错。
    """
    _check_env()

    client = HttpPrivateSign(
        BASE_URL,
        network_id=NETWORK_ID,
        zk_seeds=ZK_SEEDS,
        zk_l2Key=L2KEY_SEEDS,
        api_key_credentials={
            "key": API_KEY,
            "secret": API_SECRET,
            "passphrase": API_PASSPHRASE,
        },
    )

    # 这两步很关键！
    configs = client.configs_v3()
    account_data = client.get_account_v3()

    # 打一点调试（只在容器日志里看到）
    print("[apex_client] configs_v3 loaded, symbols:",
          len(configs.get("data", {}).get("symbols", [])))
    print("[apex_client] get_account_v3 OK, account:",
          account_data.get("data", {}).get("account", {}).get("id"))

    return client


# 全局客户端
client: HttpPrivateSign = init_client()

# ----------------------------------------------------
# 3. 封装常用调用
# ----------------------------------------------------

def get_account() -> Dict[str, Any]:
    return client.get_account_v3()


def get_balances() -> Dict[str, Any]:
    return client.get_account_balance_v3()


def create_market_order(
    symbol: str,
    side: str,
    size: str,
    price: str | None = None,
) -> Dict[str, Any]:
    """
    在 ApeX 主网创建一个简单市价单。
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


# ----------------------------------------------------
# 4. 自测入口（可选）
# ----------------------------------------------------

def _self_test() -> None:
    print(f"[apex_client] MAINNET MODE ENABLED: {APEX_USE_MAINNET}")
    print(f"[apex_client] BASE_URL = {BASE_URL}, NETWORK_ID = {NETWORK_ID}")

    cfg = client.configs_v3()
    print(
        "[apex_client] configs_v3 symbols:",
        len(cfg.get("data", {}).get("symbols", [])),
    )

    acc = client.get_account_v3()
    print(
        "[apex_client] account id:",
        acc.get("data", {}).get("account", {}).get("id"),
    )


if __name__ == "__main__":
    _self_test()
