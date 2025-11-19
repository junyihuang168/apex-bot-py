# apex_client.py
import os
import time
import logging

from apexomni.http_private_v3 import HttpPrivateSign
from apexomni.constants import (
    NETWORKID_MAIN,
    NETWORKID_TEST,
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------- 创建底层 Http 客户端 ----------
def _build_http_client():
    use_mainnet = os.getenv("APEX_USE_MAINNET", "false").lower() == "true"

    api_url = APEX_OMNI_HTTP_MAIN if use_mainnet else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_MAIN if use_mainnet else NETWORKID_TEST

    api_key = os.environ["APEX_API_KEY"]
    api_secret = os.environ["APEX_API_SECRET"]
    api_passphrase = os.environ["APEX_API_PASSPHRASE"]
    zk_seeds = os.environ["APEX_ZK_SEEDS"]
    zk_l2 = os.getenv("APEX_L2KEY_SEEDS", "")

    logger.info(
        "[apex_client] init HttpPrivateSign use_mainnet=%s, api_url=%s",
        use_mainnet,
        api_url,
    )

    client = HttpPrivateSign(
        api_url,
        network_id=network_id,
        zk_seeds=zk_seeds,
        zk_l2Key=zk_l2 if zk_l2 else None,
        api_key_credentials={
            "key": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        },
    )
    return client


_http_client = None


def _get_client():
    global _http_client
    if _http_client is None:
        _http_client = _build_http_client()
    return _http_client


# ---------- 小工具：把 TV 的符号改成 Apex 的 ----------
def _normalize_symbol(symbol: str) -> str:
    # TV 通常是 BNBUSDT，Apex 是 BNB-USDT
    if "-" not in symbol and symbol.endswith("USDT"):
        return symbol[:-4] + "-USDT"
    return symbol


# ---------- 下单（统一走 MARKET） ----------
def create_market_order(
    symbol: str,
    side: str,
    size,
    price=None,
    leverage: float = 1.0,
    reduce_only: bool = False,
    client_id: str | None = None,
):
    """
    使用 Apex omni http 私有接口创建市价单。
    - symbol: 来自 TradingView 的 symbol（会自动转成 BNB-USDT 这种格式）
    - side: 'BUY' or 'SELL'
    - size: 你传过来的数量（我们会转成字符串）
    - price: 从 TradingView JSON 里拿的 price（字符串或数字，允许为 None）
    - leverage: 杠杆
    - reduce_only: 是否只减仓
    - client_id: 你从 TV 传过来的 client_id
    """

    client = _get_client()
    ts = int(time.time())

    symbol_norm = _normalize_symbol(symbol)
    side_up = side.upper()

    params = {
        "symbol": symbol_norm,
        "side": side_up,          # BUY / SELL
        "type": "MARKET",
        "size": str(size),
        "timestampSeconds": ts,
    }

    # 只有有价格才传，避免 Decimal ConversionSyntax
    if price not in (None, ""):
        params["price"] = str(price)

    params["reduceOnly"] = bool(reduce_only)
    params["leverage"] = str(leverage)
    params["clientId"] = client_id or f"tv-{symbol_norm}-{ts}"

    logger.info("[apex_client] create_order params: %s", params)

    # 兼容不同版本 SDK：优先用 create_order_v3，没有就退回 create_order
    fn = getattr(client, "create_order_v3", None)
    if fn is None:
        fn = getattr(client, "create_order", None)
    if fn is None:
        raise AttributeError(
            "Apex Http client has no 'create_order_v3' or 'create_order'"
        )

    return fn(**params)


def get_account():
    client = _get_client()
    fn = getattr(client, "get_account_v3", None)
    if fn is None:
        fn = getattr(client, "get_account", None)
    if fn is None:
        raise AttributeError(
            "Apex Http client has no 'get_account_v3' or 'get_account'"
        )
    return fn()
