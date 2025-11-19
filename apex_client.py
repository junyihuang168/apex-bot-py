# apex_client.py
# ------------------------------------------------------------
# ApeX Omni 主网客户端 (HttpPrivateSign)
#
# 特点：
#   - 强制使用主网 (APEX_OMNI_HTTP_MAIN / NETWORKID_OMNI_MAIN_ARB)
#   - 通过环境变量读取主网 API key 和 Omni seeds
#   - init 时自动调用 configs_v3() + get_account_v3()，避免 accountv3 报错
#   - create_market_order():
#         * 市价单，size 一律转字符串
#         * price 为 None 时不传给 Apex，由交易所自动按当前市价成交
#
# 需要在 DO 的 App-Level 环境变量中设置：
#   APEX_USE_MAINNET   = true
#   APEX_API_KEY       = <你的主网 api key>
#   APEX_API_SECRET    = <你的主网 api secret>
#   APEX_API_PASSPHRASE= <你的主网 api passphrase>
#   APEX_ZK_SEEDS      = <你的主网 omni seeds>
#   APEX_L2KEY_SEEDS   = "" (可留空，方案 A)
#   ENABLE_LIVE_TRADING= true/false (在 app.py 里用)
#
# ------------------------------------------------------------

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
    # 这里直接阻止启动，避免误连测试网 / 错网
    raise RuntimeError(
        "APEX_USE_MAINNET is not 'true'. "
        "This apex_client.py is MAINNET-ONLY. "
        "If you really want to use Apex mainnet, set APEX_USE_MAINNET=true in env."
    )

BASE_URL = APEX_OMNI_HTTP_MAIN
NETWORK_ID = NETWORKID_OMNI_MAIN_ARB

API_KEY = os.getenv("APEX_API_KEY")
API_SECRET = os.getenv("APEX_API_SECRET")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE")

# 必填：主网 Omni Key seeds
ZK_SEEDS = os.getenv("APEX_ZK_SEEDS")

# 可选：l2Key seeds（方案 A：可以留空字符串）
L2KEY_SEEDS = os.getenv("APEX_L2KEY_SEEDS", "") or ""


def _check_env() -> None:
    """检查必须的环境变量是否都存在。"""
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
        这样 HttpPrivateSign 里才会生成 self.accountv3，
        后面 create_order_v3() 才不会报 'accountv3' 的错误。
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

    # 先加载配置
    cfg = client.configs_v3()
    print(
        "[apex_client] configs_v3 loaded, symbols:",
        len(cfg.get("data", {}).get("symbols", [])),
    )

    # 再获取账户信息，顺便让 SDK 内部准备好 accountv3
    acc = client.get_account_v3()
    acc_id = acc.get("data", {}).get("account", {}).get("id")
    print("[apex_client] get_account_v3 OK, account id:", acc_id)

    return client


# 全局客户端（被 app.py 复用）
client: HttpPrivateSign = init_client()

# ----------------------------------------------------
# 3. 封装常用调用
# ----------------------------------------------------

def get_account() -> Dict[str, Any]:
    """返回 Apex 主网账户信息。"""
    return client.get_account_v3()


def get_balances() -> Dict[str, Any]:
    """返回主网账户的余额信息。"""
    return client.get_account_balance_v3()


def create_market_order(
    symbol: str,
    side: str,
    size: str | float | int,
    price: str | float | int | None = None,
) -> Dict[str, Any]:
    """
    在 ApeX 主网创建一个简单市价单。

    参数：
        symbol : 例如 "ZEC-USDT"（务必使用 ApeX 主网上的合约代码）
        side   : "BUY" 或 "SELL"
        size   : 下单数量，可以是数字也可以是字符串，内部会转成 str
        price  : 可选。市价单通常不需要传价，如果为 None，我们就完全不传该字段，
                 由交易所按当前市价撮合。

    注意：
        - 这是市价单 (type="MARKET")，真正成交价格由盘口决定。
        - 如果以后想做限价单，可以再扩展一个 create_limit_order()。
    """
    ts = int(time.time())

    params: Dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "size": str(size),
        "timestampSeconds": ts,
    }

    # 只有在你明确给出价格时才传给 Apex
    if price is not None:
        params["price"] = str(price)

    return client.create_order_v3(**params)


# ----------------------------------------------------
# 4. 自测入口（可选，本地/Console 里 python apex_client.py）
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
