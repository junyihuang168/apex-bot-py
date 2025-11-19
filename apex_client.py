import os
import time

from apexomni.http_private_v3 import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_TEST,
    APEX_OMNI_HTTP_MAIN,
    NETWORKID_TEST,
    NETWORKID_MAIN,
)

# 环境变量
API_KEY        = os.getenv("APEX_API_KEY", "")
API_SECRET     = os.getenv("APEX_API_SECRET", "")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE", "")

ZK_SEEDS       = os.getenv("APEX_ZK_SEEDS", "")
L2KEY_SEEDS    = os.getenv("APEX_L2KEY_SEEDS", "")

USE_MAINNET    = os.getenv("APEX_USE_MAINNET", "false").lower() == "true"
ENABLE_LIVE    = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"

_client = None


def _build_client():
    """创建 HttpPrivateSign 客户端，只初始化一次。"""
    endpoint   = APEX_OMNI_HTTP_MAIN if USE_MAINNET else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_MAIN if USE_MAINNET else NETWORKID_TEST

    print("[apex_client] init HttpPrivateSign, mainnet =", USE_MAINNET)
    client = HttpPrivateSign(
        endpoint,
        network_id=network_id,
        zk_seeds=ZK_SEEDS,
        zk_l2Key=L2KEY_SEEDS,
        api_key_credentials={
            "key": API_KEY,
            "secret": API_SECRET,
            "passphrase": API_PASSPHRASE,
        },
    )

    # 官方建议：先拉一次 configs 和 account
    try:
        cfg = client.configs_v3()
        print("[apex_client] configs_v3 ok")
        acc = client.get_account_v3()
        print("[apex_client] get_account_v3 ok")
    except Exception as e:
        print("[apex_client] warning: configs/account failed:", repr(e))

    return client


def _get_client():
    global _client
    if _client is None:
        _client = _build_client()
    return _client


def get_account():
    client = _get_client()
    return client.get_account_v3()


def _create_order(symbol, side, size, order_type, price, reduce_only, client_id, live):
    """
    统一的下单函数，最终调用 apexomni 的 create_order_v3。
    对 Omni 来说：price 不能是 None，必须是 Decimal 字符串。
    """
    client = _get_client()

    # TradingView 传过来是字符串，我们这里简单转成字符串，保持原样即可
    size_str  = str(size)
    price_str = str(price)  # 已经保证不是 None

    ts = int(time.time())

    params = {
        "symbol": symbol,          # 例如 "BTC-USDT"
        "side": side,              # "BUY" / "SELL"
        "type": order_type,        # "MARKET" / "LIMIT"
        "size": size_str,
        "price": price_str,
        "timestampSeconds": ts,
    }

    if reduce_only:
        params["reduceOnly"] = True
    if client_id:
        params["clientId"] = client_id
    # live 参数：True = 真实下单，False = 只验签不落单（视 Apex 实现）
    params["live"] = bool(live and ENABLE_LIVE)

    print("[apex_client] create_order_v3 params:", params)
    res = client.create_order_v3(**params)
    return res


def create_market_order(symbol, side, size, price, reduce_only=False, client_id=None, live=True):
    """
    市价单：Omni 也要求 price 字段，我们用当前 close 价格（来自 TV）。
    """
    return _create_order(symbol, side, size, "MARKET", price, reduce_only, client_id, live)


def create_limit_order(symbol, side, size, price, reduce_only=False, client_id=None, live=True):
    """
    限价单：price 就是你的挂单价格。
    """
    return _create_order(symbol, side, size, "LIMIT", price, reduce_only, client_id, live)
