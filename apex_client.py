import os
import time
import random
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Union

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

# -------------------------------------------------------------------
# 交易规则（默认：最小数量 0.01，步长 0.01，小数点后 2 位）
# 如果以后某个币不一样，可以在 SYMBOL_RULES 里单独覆盖
# -------------------------------------------------------------------

DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),      # 最小数量
    "step_size": Decimal("0.01"),    # 每档步长
    "qty_decimals": 2,               # 小数位数
}

SYMBOL_RULES: Dict[str, Dict[str, Any]] = {
    # 示例覆盖
    # "BTC-USDT": {"min_qty": Decimal("0.001"), "step_size": Decimal("0.001"), "qty_decimals": 3},
}

_CLIENT: Optional[HttpPrivateSign] = None


def _get_symbol_rules(symbol: str) -> Dict[str, Any]:
    s = symbol.upper()
    rules = SYMBOL_RULES.get(s, {})
    return {**DEFAULT_SYMBOL_RULES, **rules}


def _snap_quantity(symbol: str, theoretical_qty: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
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
        raise ValueError(
            f"budget too small: snapped quantity {snapped} < minQty {min_qty}"
        )

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
        print("[apex_client] configs_v3 ok:", cfg)
    except Exception as e:
        print("[apex_client] WARNING configs_v3 error:", e)

    try:
        acc = client.get_account_v3()
        print("[apex_client] get_account_v3 ok:", acc)
    except Exception as e:
        print("[apex_client] WARNING get_account_v3 error:", e)

    _CLIENT = client
    return client


def get_account():
    return get_client().get_account_v3()


def get_market_price(symbol: str, side: str, size: str) -> str:
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()

    from apexomni.http_private_v3 import HttpPrivate_v3

    http_v3_client = HttpPrivate_v3(
        base_url,
        network_id=network_id,
        api_key_credentials=api_creds,
    )

    side = side.upper()
    size_str = str(size)

    res = http_v3_client.get_worst_price_v3(
        symbol=symbol,
        size=size_str,
        side=side,
    )

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


NumberLike = Union[str, float, int]


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

    rules = _get_symbol_rules(symbol)
    decimals = rules["qty_decimals"]

    if size_usdt is not None:
        budget = Decimal(str(size_usdt))
        if budget <= 0:
            raise ValueError("size_usdt must be > 0")

        min_qty = rules["min_qty"]
        ref_price_decimal = Decimal(get_market_price(symbol, side, str(min_qty)))
        theoretical_qty = budget / ref_price_decimal
        snapped_qty = _snap_quantity(symbol, theoretical_qty)

        price_str = get_market_price(symbol, side, str(snapped_qty))
        price_decimal = Decimal(price_str)

        size_str = format(snapped_qty, f".{decimals}f")

        used_budget = (snapped_qty * price_decimal).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )

        print(
            f"[apex_client] budget={budget} USDT -> qty={size_str} "
            f"used≈{used_budget} USDT (price {price_str})"
        )
    else:
        if size is None:
            raise ValueError("size or size_usdt must be provided")

        size_str = str(size)
        price_str = get_market_price(symbol, side, size_str)
        price_decimal = Decimal(price_str)
        used_budget = (price_decimal * Decimal(size_str)).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )

    ts = int(time.time())
    apex_client_id = _random_client_id()
    tv_client_id = client_id

    if tv_client_id:
        print(f"[apex_client] tv_client_id={tv_client_id} -> apex_clientId={apex_client_id}")
    else:
        print(f"[apex_client] apex_clientId={apex_client_id} (no tv_client_id)")

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

    print("[apex_client] create_market_order params:", params)
    order = client.create_order_v3(**params)
    print("[apex_client] order response:", order)

    data = order["data"] if isinstance(order, dict) and "data" in order else order

    return {
        "data": data,
        "raw_order": order,
        "computed": {
            "symbol": symbol,
            "side": side,
            "size": size_str,
            "price": price_str,
            "used_budget": str(used_budget),
            "reduce_only": reduce_only,
        },
    }


def create_stop_market_order(
    symbol: str,
    side: str,
    size: NumberLike,
    trigger_price: NumberLike,
    reduce_only: bool = True,
    client_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    真正挂在 ApeX 的 STOP_MARKET 条件止损单。
    """
    client = get_client()

    try:
        if not hasattr(client, "configV3"):
            client.configs_v3()
        if not hasattr(client, "accountV3"):
            client.get_account_v3()
    except Exception as e:
        print("[apex_client] fallback init error (stop):", e)

    side = side.upper()
    size_str = str(size)

    trigger_price_dec = Decimal(str(trigger_price))

    # 保护价，让系统更容易接受
    if side == "SELL":
        limit_price_dec = trigger_price_dec * Decimal("0.99")
    else:
        limit_price_dec = trigger_price_dec * Decimal("1.01")

    price_str = str(limit_price_dec)
    trigger_price_str = str(trigger_price_dec)

    ts = int(time.time())
    apex_client_id = _random_client_id()
    tv_client_id = client_id

    if tv_client_id:
        print(f"[apex_client] (stop) tv_client_id={tv_client_id} -> apex_clientId={apex_client_id}")
    else:
        print(f"[apex_client] (stop) apex_clientId={apex_client_id} (no tv_client_id)")

    params = {
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "size": size_str,
        "price": price_str,
        "triggerPrice": trigger_price_str,
        "triggerPriceType": "MARKET",  # ✅ 增加兼容字段
        "reduceOnly": reduce_only,
        "clientId": apex_client_id,
        "timestampSeconds": ts,
    }

    print("[apex_client] create_stop_market_order params:", params)
    order = client.create_order_v3(**params)
    print("[apex_client] (stop) order response:", order)

    data = order["data"] if isinstance(order, dict) and "data" in order else order

    order_id = None
    client_order_id = None

    def _pick(d: dict, *keys):
        for k in keys:
            if k in d and d[k] is not None:
                return d[k]
        return None

    if isinstance(order, dict):
        order_id = _pick(order, "orderId", "id")
        client_order_id = _pick(order, "clientOrderId", "clientId")

    if isinstance(data, dict):
        order_id = order_id or _pick(data, "orderId", "id")
        client_order_id = client_order_id or _pick(data, "clientOrderId", "clientId")

    return {
        "data": data,
        "raw_order": order,
        "order_id": order_id,
        "client_order_id": client_order_id,
        "symbol": symbol,
        "side": side,
        "size": size_str,
        "trigger_price": trigger_price_str,
        "price": price_str,
    }


def cancel_order(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
):
    client = get_client()

    try:
        if hasattr(client, "delete_order_v3"):
            kwargs = {}
            if order_id:
                kwargs["orderId"] = order_id
            if client_order_id:
                kwargs["clientOrderId"] = client_order_id
            print(f"[apex_client] delete_order_v3 kwargs={kwargs}")
            res = client.delete_order_v3(**kwargs)
            print("[apex_client] cancel_order response:", res)
            return res

        if hasattr(client, "cancel_order_v3") and order_id:
            print(f"[apex_client] cancel_order_v3 orderId={order_id}")
            res = client.cancel_order_v3(orderId=order_id)
            print("[apex_client] cancel_order response:", res)
            return res

        if hasattr(client, "cancel_active_order_v3"):
            coid = client_order_id or order_id
            print(f"[apex_client] cancel_active_order_v3 symbol={symbol} clientOrderId={coid}")
            res = client.cancel_active_order_v3(symbol=symbol, clientOrderId=coid)
            print("[apex_client] cancel_order response:", res)
            return res

        print("[apex_client] WARNING: no cancel/delete API found on client")
        return {"error": "no_cancel_api"}

    except Exception as e:
        print("[apex_client] cancel_order error:", e)
        return {"error": str(e)}
