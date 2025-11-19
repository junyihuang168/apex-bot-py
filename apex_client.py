# apex_client.py
import os
import time

from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_MAIN,
    NETWORKID_TEST,
)
from apexomni.http_private_v3 import HttpPrivate_v3

# ------------------------------------------------------------------
# 基础配置：根据 APEX_ENV 选择 test / main
# ------------------------------------------------------------------
APEX_ENV = os.getenv("APEX_ENV", "test").lower()

if APEX_ENV == "main":
    APEX_HTTP = APEX_OMNI_HTTP_MAIN
    NETWORK_ID = NETWORKID_MAIN
else:
    # 默认走 TESTNET
    APEX_HTTP = APEX_OMNI_HTTP_TEST
    NETWORK_ID = NETWORKID_TEST

API_KEY = os.getenv("APEX_API_KEY")
API_SECRET = os.getenv("APEX_API_SECRET")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE")

if not API_KEY or not API_SECRET or not API_PASSPHRASE:
    raise RuntimeError("APEX_API_KEY / APEX_API_SECRET / APEX_API_PASSPHRASE 未在环境变量中配置")

# 官方文档要求初始化后先 configs_v3 / get_account_v3 一次 
_client: HttpPrivate_v3 | None = None


def _get_client() -> HttpPrivate_v3:
    global _client
    if _client is None:
        c = HttpPrivate_v3(
            APEX_HTTP,
            network_id=NETWORK_ID,
            api_key_credentials={
                "key": API_KEY,
                "secret": API_SECRET,
                "passphrase": API_PASSPHRASE,
            },
        )
        # 这两行是官方推荐的“预热”，失败会直接抛错，方便在 DO 日志里看到问题
        c.configs_v3()
        c.get_account_v3()
        _client = c
    return _client


# ------------------------------------------------------------------
# 账户查询（给 /health 或调试用）
# ------------------------------------------------------------------
def get_account():
    c = _get_client()
    return c.get_account_v3()


# ------------------------------------------------------------------
# 下单封装：市价单 & 限价单
# TradingView 传来的 size / price 都是字符串，这里保持字符串即可
# ------------------------------------------------------------------
def _base_order_params(
    symbol: str,
    side: str,
    size: str,
    price: str,
    order_type: str,
    reduce_only: bool = False,
    client_id: str | None = None,
    time_in_force: str | None = None,
):
    ts = int(time.time())
    params: dict[str, object] = {
        "symbol": symbol,
        "side": side.upper(),          # BUY / SELL
        "type": order_type,            # "MARKET" / "LIMIT"
        "size": str(size),             # 必须是字符串
        "price": str(price),           # 必须是字符串 
        "timestampSeconds": ts,
        "reduceOnly": bool(reduce_only),
    }
    if client_id:
        # SDK 里字段叫 clientOrderId
        params["clientOrderId"] = client_id
    if time_in_force:
        params["timeInForce"] = time_in_force  # GOOD_TIL_CANCEL / FILL_OR_KILL / ...
    return params


def create_market_order(
    symbol: str,
    side: str,
    size: str,
    price: str,
    reduce_only: bool = False,
    client_id: str | None = None,
    leverage: float | None = None,
    live: bool = True,
    **_kwargs,
):
    """
    市价单封装。
    注意：Apex 这边的 MARKET 也需要 price，用来做最差成交价 / 费用上限。
    leverage 参数目前 Apex 的 create_order_v3 不用，这里只是为了兼容 app.py 的调用。
    """
    c = _get_client()
    params = _base_order_params(
        symbol=symbol,
        side=side,
        size=size,
        price=price,
        order_type="MARKET",
        reduce_only=reduce_only,
        client_id=client_id,
    )
    return c.create_order_v3(**params)


def create_limit_order(
    symbol: str,
    side: str,
    size: str,
    price: str,
    reduce_only: bool = False,
    client_id: str | None = None,
    leverage: float | None = None,
    live: bool = True,
    time_in_force: str = "GOOD_TIL_CANCEL",
    **_kwargs,
):
    """
    限价单封装。
    这里只是把参数拼成 create_order_v3 需要的格式，真正的撮合还是交易所在做。
    """
    c = _get_client()
    params = _base_order_params(
        symbol=symbol,
        side=side,
        size=size,
        price=price,
        order_type="LIMIT",
        reduce_only=reduce_only,
        client_id=client_id,
        time_in_force=time_in_force,
    )
    return c.create_order_v3(**params)
