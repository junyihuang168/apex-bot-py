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
    用于真正的下单（create_order_v3）。
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
    return client


# ---------------------------
# 对外工具函数
# ---------------------------

def get_account():
    """查询账户信息，方便你在本地或日志里调试。"""
    client = get_client()
    return client.get_account_v3()


def get_market_price(symbol: str, side: str, size: str) -> str:
    """
    使用官方推荐的 GET /v3/get-worst-price 来获取市价单应当使用的价格。
    - symbol: 例如 'BNB-USDT'
    - side: 'BUY' 或 'SELL'
    - size: 下单数量（字符串或数字均可）

    返回: 字符串形式的价格（比如 '532.15'）
    """
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()

    # 这里不用 zk 签名，只需要 API key，所以用 HttpPrivate_v3。
    from apexomni.http_private_v3 import HttpPrivate_v3

    http_v3_client = HttpPrivate_v3(
        base_url,
        network_id=network_id,
        api_key_credentials=api_creds,
    )

    side = side.upper()
    size_str = str(size)

    # 调用官方 get_worst_price_v3
    res = http_v3_client.get_worst_price_v3(
        symbol=symbol,
        size=size_str,
        side=side,
    )

    # 返回结构一般是: {'worstPrice': '123.00', 'bidOnePrice': '...', 'askOnePrice': '...'}
    price = None
    if isinstance(res, dict):
        if "worstPrice" in res:
            price = res["worstPrice"]
        elif "data" in res and isinstance(res["data"], dict) and "worstPrice" in res["data"]:
            price = res["data"]["worstPrice"]

    if price is None:
        raise RuntimeError(f"[apex_client] get_worst_price_v3 返回异常: {res}")

    price_str = str(price)
    print(f"[apex_client] worst price for {symbol} {side} size={size_str}: {price_str}")
    return price_str


def create_market_order(
    symbol: str,
    side: str,
    size: str,
    reduce_only: bool = False,   # 先保留在我们自己这一层，不直接传给 SDK
    client_id: str | None = None,
):
    """
    创建 ApeX 的 MARKET 市价单，会自动根据当前盘口计算 price。

    - symbol: 例如 'BNB-USDT'
    - side: 'BUY' 或 'SELL'
    - size: 数量（字符串或数字）
    - reduce_only: True 表示只减仓（目前只体现在日志里，不直接传给 SDK）
    - client_id: 你从 TradingView 传来的 client_id；如果为空会自动生成一个。
    """
    client = get_client()

    side = side.upper()
    size_str = str(size)

    # ★ 先用官方 API 拿 worstPrice，当作市价单的 price
    price_str = get_market_price(symbol, side, size_str)

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
            "size": size_str,
            "price": price_str,
            "reduce_only": reduce_only,
            "clientOrderId": client_id,
            "timestampSeconds": ts,
        },
    )

    # 关键：这里 **不再传 reduce_only**，否则 SDK 会报 TypeError
    order = client.create_order_v3(
        symbol=symbol,
        side=side,
        type="MARKET",
        size=size_str,
        timestampSeconds=ts,
        price=price_str,
        clientOrderId=client_id,
    )

    print("[apex_client] order response:", order)
    return order
