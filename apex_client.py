# apex_client.py
import os
import time
import logging

from apexomni.constants import (
    NETWORKID_TEST,
    NETWORKID_MAIN,
    APEX_OMNI_HTTP_TEST,
    APEX_OMNI_HTTP_MAIN,
)
from apexomni.http_private_v3 import HttpPrivate_v3  # 关键：用 HttpPrivate_v3，而不是 HttpPrivateSign

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TEST 或 MAIN，通过 DO 的环境变量控制，不设默认 TEST
APEX_ENV = os.getenv("APEX_ENV", "TEST").upper()


def _build_client() -> HttpPrivate_v3:
    """从环境变量里创建一个 HttpPrivate_v3 客户端"""

    api_key = os.getenv("APEX_API_KEY", "")
    api_secret = os.getenv("APEX_API_SECRET", "")
    api_passphrase = os.getenv("APEX_API_PASSPHRASE", "")
    zk_seeds = os.getenv("APEX_ZK_SEEDS", "")

    if not (api_key and api_secret and api_passphrase and zk_seeds):
        raise RuntimeError(
            "Missing Apex credentials: APEX_API_KEY / APEX_API_SECRET / "
            "APEX_API_PASSPHRASE / APEX_ZK_SEEDS"
        )

    if APEX_ENV == "MAIN":
        base_url = APEX_OMNI_HTTP_MAIN
        network_id = NETWORKID_MAIN
    else:
        base_url = APEX_OMNI_HTTP_TEST
        network_id = NETWORKID_TEST

    logger.info("Init HttpPrivate_v3: env=%s, base_url=%s", APEX_ENV, base_url)

    client = HttpPrivate_v3(
        base_url,
        network_id=network_id,
        zk_seeds=zk_seeds,
        api_key_credentials={
            "key": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        },
    )
    return client


def get_account():
    """给 app.py 调试用：返回账户信息"""
    client = _build_client()
    return client.get_account()


def create_order(
    symbol: str,
    side: str,
    order_type: str,
    size,
    price=None,
    reduce_only: bool = False,
):
    """
    通用下单封装，直接调用 HttpPrivate_v3.create_order(...)
    symbol:  'BTC-USDT' / 'BNB-USDT' 之类
    side:    'BUY' / 'SELL'
    order_type: 'MARKET' / 'LIMIT'
    size:    下单数量（字符串或数字）
    price:   限价单价格（字符串或数字，市价单可以为 None）
    """

    client = _build_client()

    ts = int(time.time())
    kwargs = {
        "symbol": symbol,
        "side": side.upper(),
        "type": order_type.upper(),   # MARKET / LIMIT
        "size": str(size),
        "timestampSeconds": ts,
    }

    if price is not None and order_type.upper() == "LIMIT":
        kwargs["price"] = str(price)

    # 有些版本支持 reduceOnly，有些不支持，如果不支持会在响应里报错，到时再看日志
    if reduce_only:
        kwargs["reduceOnly"] = True

    logger.info("Sending order to Apex: %s", kwargs)
    resp = client.create_order(**kwargs)
    logger.info("Apex response: %s", resp)
    return resp


def create_market_order(symbol: str, side: str, size, reduce_only: bool = False):
    """
    给 app.py 用的简单封装：统一走市价单。
    """
    return create_order(
        symbol=symbol,
        side=side,
        order_type="MARKET",
        size=size,
        price=None,
        reduce_only=reduce_only,
    )
