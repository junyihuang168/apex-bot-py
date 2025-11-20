import os
import time
import math
from typing import Tuple, Dict, Any

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.http_private_v3 import HttpPrivate_v3
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)


def _env_bool(name: str, default: bool = False) -> bool:
    """把环境变量转成 bool。"""
    value = os.getenv(name, str(default))
    return value.lower() in ("1", "true", "yes", "y", "on")


def _get_base_and_network() -> Tuple[str, int]:
    """根据环境变量决定 mainnet / testnet。"""
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)
    env_name = os.getenv("APEX_ENV", "").lower()
    if env_name in ("main", "mainnet", "prod", "production"):
        use_mainnet = True

    base_url = APEX_OMNI_HTTP_MAIN if use_mainnet else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_OMNI_MAIN_ARB if use_mainnet else NETWORKID_TEST
    return base_url, network_id


def _get_api_credentials() -> Dict[str, str]:
    """从环境变量里读 API key / secret / passphrase。"""
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


def _make_signed_client() -> HttpPrivateSign:
    """带 zk 签名的客户端，用来真正下单 create_order_v3。"""
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()

    zk_seeds = os.environ["APEX_ZK_SEEDS"]
    zk_l2 = os.getenv("APEX_L2KEY_SEEDS") or None

    return HttpPrivateSign(
        base_url,
        network_id=network_id,
        zk_seeds=zk_seeds,
        zk_l2Key=zk_l2,
        api_key_credentials=api_creds,
    )


def _make_public_client() -> HttpPrivate_v3:
    """轻量级 v3 客户端，用来调公开接口（比如 get_worst_price_v3）。"""
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()
    return HttpPrivate_v3(
        base_url,
        network_id=network_id,
        api_key_credentials=api_creds,
    )


def floor_to_decimals(value: float, decimals: int) -> float:
    """向下取整到 N 位小数（永远不会四舍五入往上）。"""
    factor = 10 ** decimals
    return math.floor(value * factor) / factor


def _extract_worst_price(res: Any) -> str:
    """
    从 get_worst_price_v3 的返回里尽量把 worstPrice 抠出来。
    有些 SDK 可能包了一层 data，这里做一点兼容。
    """
    if isinstance(res, dict):
        if "worstPrice" in res:
            return str(res["worstPrice"])
        if "data" in res and isinstance(res["data"], dict) and "worstPrice" in res["data"]:
            return str(res["data"]["worstPrice"])
    # 实在不行就直接转成字符串（最保底的 fallback）
    return str(res)


def get_account():
    """方便在日志或本地调试时看账户信息。"""
    client = _make_signed_client()
    return client.get_account_v3()


def create_market_order(
    symbol: str,
    side: str,
    usdt_size: float,
    tv_price: float,
    reduce_only: bool = False,
    client_id: str | None = None,
):
    """
    用“USDT 金额”来创建 MARKET 市价单（方案 B）。

    - symbol: 例如 'ZEC-USDT'
    - side: 'BUY' / 'SELL'
    - usdt_size: TradingView 传过来的 USDT 金额（比如 10 表示 10 USDT 名义）
    - tv_price: TradingView 发过来的价格，用来估算数量
    - reduce_only: True=减仓/平仓，False=开仓
    - client_id: 从 TradingView 传来的 client_id（会转发给 Apex）
    """
    side = side.upper()
    usdt_size = float(usdt_size)
    tv_price = float(tv_price)

    if usdt_size <= 0:
        raise ValueError(f"usdt_size must be > 0, got {usdt_size}")

    # --- 1) USDT -> 币的数量（先用 TV 价格估算） ---
    raw_qty = usdt_size / tv_price
    # 大部分 perp 的 stepSize = 0.01，这里统一向下取 2 位小数，简单又安全
    qty = floor_to_decimals(raw_qty, 2)
    if qty <= 0:
        raise ValueError(f"computed qty <= 0 from usdt_size={usdt_size}, tv_price={tv_price}")

    qty_str = f"{qty:.2f}"

    # --- 2) 用官方 get_worst_price_v3 拿一个合法的价格 ---
    public_client = _make_public_client()
    worst_res = public_client.get_worst_price_v3(
        symbol=symbol,
        size=qty_str,
        side=side,
    )
    worst_price_str = _extract_worst_price(worst_res)

    # --- 3) 带 zk 签名的真正下单 ---
    ts = int(time.time())
    if client_id is None:
        safe_symbol = symbol.replace("/", "-")
        client_id = f"tv-{safe_symbol}-{ts}"

    signed_client = _make_signed_client()

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "size": qty_str,          # 注意：这里是“币的数量”，不是 USDT 金额
        "price": worst_price_str,
        "reduce_only": reduce_only,
        "client_id": client_id,   # ✅ 一定要用 client_id（蛇形），不要用 clientOrderId
        "timestampSeconds": ts,
    }
    print("[apex_client] create_market_order params:", params)

    order = signed_client.create_order_v3(**params)
    print("[apex_client] order response:", order)
    return order
