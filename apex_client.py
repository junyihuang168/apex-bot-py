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

# ============================================================
# 交易规则（默认：最小数量 0.01，步长 0.01，小数点后 2 位）
# ============================================================

DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),
    "step_size": Decimal("0.01"),
    "qty_decimals": 2,
}

SYMBOL_RULES: Dict[str, Dict[str, Any]] = {
    # Example override:
    # "BTC-USDT": {"min_qty": Decimal("0.001"), "step_size": Decimal("0.001"), "qty_decimals": 3},
}

_CLIENT: Optional[HttpPrivateSign] = None

NumberLike = Union[str, float, int]


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
        client.configs_v3()
        print("[apex_client] configs_v3 ok")
    except Exception as e:
        print("[apex_client] WARNING configs_v3 error:", e)

    try:
        client.get_account_v3()
        print("[apex_client] get_account_v3 ok")
    except Exception as e:
        print("[apex_client] WARNING get_account_v3 error:", e)

    _CLIENT = client
    return client


def get_account():
    client = get_client()
    return client.get_account_v3()


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
        raise RuntimeError(f"[apex_client] get_worst_price_v3 returned abnormal: {res}")

    price_str = str(price)
    print(f"[apex_client] worst price for {symbol} {side} size={size_str}: {price_str}")
    return price_str


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
            f"[apex_client] budget={budget} USDT -> qty={size_str}, "
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

    if client_id:
        print(f"[apex_client] tv_client_id={client_id} -> apex_clientId={apex_client_id}")
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


# ============================================================
# ✅ Real position helpers
# - Fix symbol format mismatch: ZEC-USDT vs ZECUSDT
# ============================================================

def _norm_symbol(s: str) -> str:
    return str(s or "").upper().replace("-", "").replace("_", "").strip()


def get_open_position_for_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        acc = get_account()
    except Exception as e:
        print("[apex_client] get_account error:", e)
        return None

    data = acc.get("data") if isinstance(acc, dict) else None
    if not isinstance(data, dict):
        data = acc if isinstance(acc, dict) else {}

    positions = (
        data.get("positions")
        or data.get("openPositions")
        or data.get("position")
        or data.get("positionV3")
        or []
    )

    if not isinstance(positions, list):
        return None

    target = _norm_symbol(symbol)

    for p in positions:
        if not isinstance(p, dict):
            continue

        psym = _norm_symbol(p.get("symbol", ""))
        if psym != target:
            continue

        size = p.get("size")
        side = str(p.get("side", "")).upper()  # LONG/SHORT
        entry = p.get("entryPrice")

        try:
            size_dec = Decimal(str(size or "0"))
        except Exception:
            size_dec = Decimal("0")

        if size_dec <= 0 or side not in ("LONG", "SHORT"):
            continue

        entry_dec = None
        try:
            if entry is not None:
                entry_dec = Decimal(str(entry))
        except Exception:
            entry_dec = None

        return {
            "symbol": str(p.get("symbol", symbol)),
            "side": side,
            "size": size_dec,
            "entryPrice": entry_dec,
            "raw": p,
        }

    return None


def map_position_side_to_exit_order_side(pos_side: str) -> str:
    s = str(pos_side).upper()
    return "SELL" if s == "LONG" else "BUY"
