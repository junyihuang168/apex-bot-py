import os
import time

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)


# ---------------------------
# 内部小工具函数
# ---------------------------

def _env_bool(name: str, default: bool = False) -> bool:
    """把环境变量字符串转成布尔值."""
    value = os.getenv(name, str(default))
    return value.lower() in ("1", "true", "yes", "y", "on")


def _get_base_and_network():
    """根据环境变量决定用 mainnet 还是 testnet。"""
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)

    # 兼容你额外加的 APEX_ENV=main
    env_name = os.getenv("APEX_ENV", "").lower()
    if env_name in ("main", "mainnet", "prod", "production"):
        use_mainnet = True

    base_url = APEX_OMNI_HTTP_MAIN if use_mainnet else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_OMNI_MAIN_ARB if use_mainnet else NETWORKID_TEST
    return base_url, network_id


def _get_api_credentials():
    """从环境变量里拿 API key / secret / passphrase。"""
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


# ---------------------------
# 创建 ApeX 客户端
# ---------------------------

def get_client() -> HttpPrivateSign:
    """
    返回带 zk 签名的 HttpPrivateSign 客户端，
    按官方 demo 的习惯，顺便预热 configs_v3 / get_account_v3，
    这样 create_order_v3 里用到的 configV3 / accountV3 就不会报错。
    """
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()

    zk_seeds = os.environ["APEX_ZK_SEEDS"]
    zk_l2 = os.getenv("APEX_L2KEY_SEEDS") or None

    client = HttpPrivateSign(
        base_url,
        network_id=network_id,
        zk_seeds=zk_seeds,
        zk_l2Key=zk_l2,
        api_key_credentials=api_creds,
    )

    # 预热配置 & 账户信息（官方 demo 也是这么做）
    try:
        client.configs_v3()
    except Exception as e:
        print("[apex_client] configs_v3() error:", e)

    try:
        client.get_account_v3()
    except Exception as e:
        print("[apex_client] get_account_v3() error:", e)

    return client


# ---------------------------
# 对外工具函数
# ---------------------------

def get_account():
    """查询账户信息，方便你在本地或日志里调试。"""
    client = get_client()
    return client.get_account_v3()


def get_market_price(symbol: str, side: str, size_for_quote: str = "1") -> str:
    """
    使用官方 GET /v3/get-worst-price 获取一个估算的“市价”。
    这里的 size_for_quote 理论上是币的数量，但我们只是用它来报价，
    对价格影响不大，所以默认用 1 即可。
    """
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()

    # 这里只需要 API key，不需要 zk 签名，所以用 HttpPrivate_v3。
    from apexomni.http_private_v3 import HttpPrivate_v3

    http_v3_client = HttpPrivate_v3(
        base_url,
        network_id=network_id,
        api_key_credentials=api_creds,
    )

    side = side.upper()
    size_str = str(size_for_quote)

    res = http_v3_client.get_worst_price_v3(
        symbol=symbol,
        size=size_str,
        side=side,
    )

    # 返回结构一般是:
    # {'worstPrice': '123.00', 'bidOnePrice': '...', 'askOnePrice': '...'}
    price = None
    if isinstance(res, dict):
        if "worstPrice" in res:
            price = res["worstPrice"]
        elif "data" in res and isinstance(res["data"], dict) and "worstPrice" in res["data"]:
            price = res["data"]["worstPrice"]

    if price is None:
        raise RuntimeError(f"[apex_client] get_worst_price_v3 返回异常: {res}")

    price_str = str(price)
    print(
        f"[apex_client] worst price for {symbol} {side} size={size_str}: {price_str}"
    )
    return price_str


def create_market_order(
    symbol: str,
    side: str,
    size,                 # 这里的 size 被当成 “USDT 金额”
    reduce_only: bool = False,
    client_id: str = None,
):
    """
    创建 ApeX 的 MARKET 市价单（按 USDT 金额下单）：

    - TradingView 传进来的 size 被当成 “USDT notional”
    - 先用 get_market_price 拿一个当前价格 price
    - 然后 coin_size = usdt_notional / price
    - 真正下单时把 coin_size 传给 create_order_v3 的 size
    """
    client = get_client()

    side = side.upper()
    usdt_notional = float(size)

    # 1) 先拿一个当前价格（这里用 size_for_quote=1 就够了）
    price_str = get_market_price(symbol, side, "1")
    price = float(price_str)

    if price <= 0:
        raise RuntimeError(f"[apex_client] 非法价格: {price_str}")

    # 2) USDT 金额 -> 币的数量
    coin_size = usdt_notional / price

    if coin_size <= 0:
        raise RuntimeError(
            f"[apex_client] 计算出来的币数量 <= 0, usdt_notional={usdt_notional}, price={price}"
        )

    # 控制一下小数位数，避免过长
    size_str = f"{coin_size:.6f}".rstrip("0").rstrip(".")
    if size_str == "":
        size_str = "0.000001"

    ts = int(time.time())
    if client_id is None:
        safe_symbol = symbol.replace("/", "-")
        client_id = f"tv-{safe_symbol}-{ts}"

    print(
        "[apex_client] create_market_order params:",
        {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "usdt_size": usdt_notional,
            "size": size_str,          # 真正下单用的“币数量”
            "price": price_str,
            "reduce_only": reduce_only,
            "client_id": client_id,
            "timestampSeconds": ts,
        },
    )

    # 按官方 SDK 的参数名调用：
    # - 注意：这里的 reduceOnly 是驼峰写法（之前用 reduce_only 会报 TypeError）
    order = client.create_order_v3(
        symbol=symbol,
        side=side,
        type="MARKET",
        size=size_str,
        timestampSeconds=ts,
        price=price_str,
        reduceOnly=reduce_only,
        # clientOrderId 可以不传，让交易所自己生成；不想再踩参数名的坑
    )

    print("[apex_client] order response:", order)
    return order
