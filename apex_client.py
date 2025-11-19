# apex_client.py
# 方案 A：只用 Omni seeds，l2Key 允许为空字符串 ''
# 使用 apexomni 官方 SDK（apexpro-openapi / apexomni）

import os
import time
from typing import Any, Dict

from apexomni.http_private_v3 import HttpPrivateSign
from apexomni.constants import (
    # Testnet
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_TEST_BNB,
    # Mainnet
    APEX_OMNI_HTTP_MAIN,
    NETWORKID_OMNI_MAIN_ARB,
)

# ----------------------------------------------------------------------
# 1. 读取环境变量
# ----------------------------------------------------------------------

# 环境：test = 测试网，main = 主网
APEX_ENV = os.getenv("APEX_ENV", "test").lower()

if APEX_ENV == "main":
    BASE_URL = APEX_OMNI_HTTP_MAIN
    NETWORK_ID = NETWORKID_OMNI_MAIN_ARB
else:
    # 默认走测试网
    BASE_URL = APEX_OMNI_HTTP_TEST
    NETWORK_ID = NETWORKID_OMNI_TEST_BNB

API_KEY = os.getenv("APEX_API_KEY")
API_SECRET = os.getenv("APEX_API_SECRET")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE")

# 必须有的 zk seeds（Omni Key）
ZK_SEEDS = os.getenv("APEX_ZK_SEEDS")

# l2Key 可以为空，我们默认给 ''
L2KEY_SEEDS = os.getenv("APEX_L2KEY_SEEDS", "") or ""

# ----------------------------------------------------------------------
# 2. 基本检查（缺少任何关键参数，直接抛异常）
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
        # 这里抛出异常，在 DO 日志里会一眼看到缺什么
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


# ----------------------------------------------------------------------
# 3. 初始化 HttpPrivateSign 客户端（只用 seeds，l2Key 为空也可）
#    官方 README 原话：l2Key not shown in keyManagement; set to '' (empty)
# ----------------------------------------------------------------------


def init_client() -> HttpPrivateSign:
    """
    初始化 ApeX Omni HttpPrivateSign 客户端。
    使用：
        - BASE_URL / NETWORK_ID 根据 APEX_ENV 决定 test/main
        - zk_seeds 使用 Omni Key seeds
        - zk_l2Key 允许为空字符串 ''
    """
    _check_env()

    client = HttpPrivateSign(
        BASE_URL,
        network_id=NETWORK_ID,
        zk_seeds=ZK_SEEDS,
        zk_l2Key=L2KEY_SEEDS,  # 方案 A：这里可以是 ''
        api_key_credentials={
            "key": API_KEY,
            "secret": API_SECRET,
            "passphrase": API_PASSPHRASE,
        },
    )

    # 官方 Best Practice 要求：用私有接口前要先 configs_v3() 一次
    client.configs_v3()

    return client


# 全局复用的 client 实例
client: HttpPrivateSign = init_client()

# ----------------------------------------------------------------------
# 4. 封装几个简单的调用（方便其他文件直接 import 使用）
# ----------------------------------------------------------------------


def get_account() -> Dict[str, Any]:
    """
    获取账户信息，用来测试连通性。
    等价于官方 demo 里的 client.get_account_v3()
    """
    return client.get_account_v3()


def get_balances() -> Dict[str, Any]:
    """
    获取账户余额信息。
    """
    return client.get_account_balance_v3()


def create_market_order(
    symbol: str,
    side: str,
    size: str,
    price: str | None = None,
) -> Dict[str, Any]:
    """
    创建一个简单的市价单（官方 create_order_v3 的封装）
    - symbol: 例如 "BTC-USDT"
    - side:   "BUY" 或 "SELL"
    - size:   例如 "0.001"
    - price:  市价单可以不传，如果你想传一个参考价就传字符串

    注意：这里是示例，你的 TradingView Webhook 那边可以再做一层封装。
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
# 5. 本文件直接运行时的小测试（可选）
#    本地 / DO console 里 python apex_client.py 看效果
# ----------------------------------------------------------------------


def _self_test() -> None:
    print(f"[apex_client] ENV = {APEX_ENV}, BASE_URL = {BASE_URL}, NETWORK_ID = {NETWORK_ID}")
    print("[apex_client] Testing configs_v3 & get_account_v3 ...")

    cfg = client.configs_v3()
    print("[apex_client] configs_v3 OK, symbols:", len(cfg.get("data", {}).get("symbols", [])))

    acc = client.get_account_v3()
    print("[apex_client] get_account_v3 OK, account id:", acc.get("data", {}).get("account", {}).get("id"))


if __name__ == "__main__":
    _self_test()
