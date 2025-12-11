import os
import time
import random
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Union, Tuple

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

# -------------------------------------------------------------------
# 交易规则配置
# -------------------------------------------------------------------
DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),
    "step_size": Decimal("0.01"),
    "qty_decimals": 2,
}

SYMBOL_RULES: Dict[str, Dict[str, Any]] = {
    # "BTC-USDT": {"min_qty": Decimal("0.001"), "step_size": Decimal("0.001"), "qty_decimals": 3},
}

_CLIENT: Optional[HttpPrivateSign] = None


# [优化] 改为公有函数，供 app.py 风控计算使用
def get_symbol_rules(symbol: str) -> Dict[str, Any]:
    s = symbol.upper()
    rules = SYMBOL_RULES.get(s, {})
    return {**DEFAULT_SYMBOL_RULES, **rules}


# [优化] 改为公有函数，供 app.py 计算下单数量
def snap_quantity(symbol: str, theoretical_qty: Decimal) -> Decimal:
    rules = get_symbol_rules(symbol)
    step = rules["step_size"]
    min_qty = rules["min_qty"]
    decimals = rules["qty_decimals"]

    if theoretical_qty <= 0:
        raise ValueError("calculated quantity must be > 0")

    steps = (theoretical_qty // step)
    snapped = steps * step

    quantum = Decimal("1").scaleb(-decimals)
    snapped = snapped.quantize(quantum, rounding=ROUND_DOWN)

    if snapped < min_qty:
        raise ValueError(f"budget too small: snapped quantity {snapped} < minQty {min_qty}")

    return snapped


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, str(default))
    return value.lower() in ("1", "true", "yes", "y", "on")


def _get_base_and_network():
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)
    env_name = os.getenv("APEX_ENV", "").lower()
    if env_name in ("main", "mainnet", "prod", "production"):
        use_mainnet = True

    base_url = APEX_OMNI_HTTP_MAIN if use_mainnet else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_OMNI_MAIN_ARB if use_mainnet else NETWORKID_TEST
    return base_url, network_id


def _get_api_credentials():
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


def _random_client_id() -> str:
    return str(int(float(str(random.random())[2:])))


def get_client() -> HttpPrivateSign:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()
    zk_seeds = os.environ["APEX_ZK_SEEDS"]
    zk_l2 = os.getenv("APEX_L2KEY_SEEDS") or ""

    client = HttpPrivateSign(
        base_url,
        network_id=network_id,
        zk_seeds=zk_seeds,
        zk_l2Key=zk_l2,
        api_key_credentials=api_creds,
    )

    try:
        cfg = client.configs_v3()
        print(f"[apex_client] Connected to {base_url} (Config OK)")
    except Exception as e:
        print("[apex_client] WARNING configs_v3 error:", e)

    _CLIENT = client
    return client


def get_market_price(symbol: str, side: str, size: str) -> str:
    """
    获取真实可成交价格 (Worst Price)
    这是风控逻辑的核心，确保获取的是能成交的真实价格。
    """
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()

    from apexomni.http_private_v3 import HttpPrivate_v3
    http_v3_client = HttpPrivate_v3(base_url, network_id=network_id, api_key_credentials=api_creds)

    side = side.upper()
    size_str = str(size)

    try:
        res = http_v3_client.get_worst_price_v3(symbol=symbol, size=size_str, side=side)
    except Exception as e:
        raise RuntimeError(f"Network error fetching price: {e}")

    price = None
    if isinstance(res, dict):
        if "worstPrice" in res:
            price = res["worstPrice"]
        elif "data" in res and isinstance(res["data"], dict) and "worstPrice" in res["data"]:
            price = res["data"]["worstPrice"]

    if price is None:
        raise RuntimeError(f"[apex_client] get_worst_price_v3 failed: {res}")

    return str(price)


NumberLike = Union[str, float, int]


def _extract_order_ids(raw_order: Any) -> Tuple[Optional[str], Optional[str]]:
    order_id, client_order_id = None, None
    def _pick(d: dict, *keys):
        for k in keys:
            if d.get(k): return d.get(k)
        return None

    if isinstance(raw_order, dict):
        order_id = _pick(raw_order, "orderId", "id")
        client_order_id = _pick(raw_order, "clientOrderId", "clientId")
        data = raw_order.get("data")
        if isinstance(data, dict):
            order_id = order_id or _pick(data, "orderId", "id")
            client_order_id = client_order_id or _pick(data, "clientOrderId", "clientId")

    return (str(order_id) if order_id else None, str(client_order_id) if client_order_id else None)


def get_fill_summary(symbol: str, order_id: str, max_wait_sec: float = 2.0) -> Dict[str, Any]:
    """
    尝试获取订单成交详情
    """
    client = get_client()
    start = time.time()
    
    while True:
        try:
            # 尝试不同的SDK方法名，兼容性处理
            if hasattr(client, "get_order_v3"):
                raw = client.get_order_v3(orderId=order_id)
            elif hasattr(client, "get_order_detail_v3"):
                raw = client.get_order_detail_v3(order_id)
            else:
                raw = {} # Fallback

            # 简单的解析逻辑 (假设 data 里有 fillPrice)
            # 注意：实际 Apex 接口可能需要专门的 fills 接口，这里为了简化演示
            # 如果拿不到 fills，上层会回退使用 create_order 返回的 price
            if isinstance(raw, dict):
                data = raw.get("data", raw)
                if isinstance(data, dict):
                    filled_qty = Decimal(str(data.get("cumFilledSize") or data.get("filledSize") or "0"))
                    avg_price = Decimal(str(data.get("avgPrice") or data.get("fillAvgPrice") or "0"))
                    if filled_qty > 0 and avg_price > 0:
                        return {"filled_qty": filled_qty, "avg_fill_price": avg_price}
        except Exception:
            pass
        
        if time.time() - start > max_wait_sec:
            raise RuntimeError("Fill timeout")
        time.sleep(0.2)


def create_market_order(
    symbol: str,
    side: str,
    size: NumberLike | None = None,
    size_usdt: NumberLike | None = None,
    reduce_only: bool = False,
    client_id: Optional[str] = None,
) -> Dict[str, Any]:
    client = get_client()
    side = side.upper()
    rules = get_symbol_rules(symbol)
    decimals = rules["qty_decimals"]

    # 1. 计算数量
    if size_usdt is not None:
        budget = Decimal(str(size_usdt))
        min_qty = rules["min_qty"]
        ref_price = Decimal(get_market_price(symbol, side, str(min_qty)))
        snapped_qty = snap_quantity(symbol, budget / ref_price)
        price_str = get_market_price(symbol, side, str(snapped_qty)) # 二次确认价格
        size_str = format(snapped_qty, f".{decimals}f")
    else:
        if size is None: raise ValueError("size or size_usdt required")
        size_str = str(size)
        price_str = get_market_price(symbol, side, size_str)

    ts = int(time.time())
    apex_client_id = _random_client_id()
    if client_id: print(f"[apex_client] TV ClientID: {client_id}")

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "size": size_str,
        "price": price_str,
        "timestampSeconds": ts,
        "reduceOnly": reduce_only,
        "clientId": apex_client_id,
    }

    # 2. 发送请求
    print(f"[apex_client] Executing {side} {symbol} Qty={size_str} @ {price_str}")
    raw_order = client.create_order_v3(**params)
    
    order_id, client_order_id = _extract_order_ids(raw_order)
    
    return {
        "raw_order": raw_order,
        "order_id": order_id,
        "computed": {"price": price_str, "size": size_str}
    }
