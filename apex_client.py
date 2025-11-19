# apex_client.py
import os
import time

from apexomni.constants import (
    NETWORKID_TEST,
    NETWORKID_MAIN,
    APEX_OMNI_HTTP_TEST,
    APEX_OMNI_HTTP_MAIN,
)
from apexomni.http_private_sign import HttpPrivateSign


def _get_env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "y")


def get_client():
    """根据环境变量创建 Apex HttpPrivateSign 客户端"""

    use_mainnet = _get_env_bool("APEX_USE_MAINNET", False)

    api_key = os.getenv("APEX_API_KEY")
    api_secret = os.getenv("APEX_API_SECRET")
    api_passphrase = os.getenv("APEX_API_PASSPHRASE")

    zk_seeds = os.getenv("APEX_ZK_SEEDS")
    # 新版 SDK 可以只用 zk_seeds；如果你以后真的有 L2 key，也可以填
    zk_l2key = os.getenv("APEX_L2KEY_SEEDS")

    if use_mainnet:
        base_url = APEX_OMNI_HTTP_MAIN
        network_id = NETWORKID_MAIN
    else:
        base_url = APEX_OMNI_HTTP_TEST
        network_id = NETWORKID_TEST

    client = HttpPrivateSign(
        base_url,
        network_id=network_id,
        zk_seeds=zk_seeds,
        zk_l2Key=zk_l2key,
        api_key_credentials={
            "key": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        },
    )
    return client


def get_account():
    """给 app.py 用的账户查询"""
    client = get_client()
    return client.get_account_v3()


def create_market_order(symbol: str, side: str, size: float, price=None, live: bool = True):
    """
    给 app.py 用的下单函数。
    - symbol: 例如 'BTC-USDT'
    - side: 'BUY' 或 'SELL'
    - size:  数量（字符串或数字都可以，这里会转成字符串）
    - price: 如果是 MARKET，但 SDK 里需要 price，就由 TV 传过来的字符串/数字
    """

    client = get_client()

    ts = int(time.time())

    # 把 TradingView 传来的 price 处理成字符串；如果没有就不带这个字段
    price_str = None
    if price is not None:
        try:
            price_str = str(price)
        except Exception:
            price_str = None

    params = {
        "symbol": symbol,
        "side": side.upper(),
        "type": "MARKET",
        "size": str(size),
        "timestampSeconds": ts,
    }
    if price_str is not None:
        params["price"] = price_str

    resp = client.create_order_v3(**params)
    return resp
