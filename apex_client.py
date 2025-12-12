import os
import time
import random
import inspect
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Union, Tuple

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),
    "step_size": Decimal("0.01"),
    "qty_decimals": 2,
}

SYMBOL_RULES: Dict[str, Dict[str, Any]] = {
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

    _CLIENT = client
    return client


def get_account():
    client = get_client()
    return client.get_account_v3()


def _mk_http_v3():
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()
    from apexomni.http_private_v3 import HttpPrivate_v3
    return HttpPrivate_v3(
        base_url,
        network_id=network_id,
        api_key_credentials=api_creds,
    )


def get_market_price(symbol: str, side: str, size: str) -> str:
    http_v3_client = _mk_http_v3()
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
        raise RuntimeError(f"[apex_client] get_worst_price_v3 abnormal: {res}")

    price_str = str(price)
    print(f"[apex_client] worst price for {symbol} {side} size={size_str}: {price_str}")
    return price_str


NumberLike = Union[str, float, int]


def _extract_order_ids(raw_order: Any) -> Tuple[Optional[str], Optional[str]]:
    order_id = None
    client_order_id = None

    def _pick(d: dict, *keys):
        for k in keys:
            v = d.get(k)
            if v is not None:
                return v
        return None

    if isinstance(raw_order, dict):
        order_id = _pick(raw_order, "orderId", "id")
        client_order_id = _pick(raw_order, "clientOrderId", "clientId")

        data = raw_order.get("data")
        if isinstance(data, dict):
            order_id = order_id or _pick(data, "orderId", "id")
            client_order_id = client_order_id or _pick(data, "clientOrderId", "clientId")

    return (str(order_id) if order_id else None, str(client_order_id) if client_order_id else None)


def _call_flexible(fn, *, args_list=None, kwargs_list=None):
    args_list = args_list or []
    kwargs_list = kwargs_list or []
    last_err = None

    for a in args_list:
        try:
            return fn(*a)
        except Exception as e:
            last_err = e

    for kw in kwargs_list:
        try:
            return fn(**kw)
        except Exception as e:
            last_err = e

    raise last_err if last_err else RuntimeError("call failed")


def get_fill_summary(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    max_wait_sec: float = 3.0,
    poll_interval: float = 0.10,
) -> Dict[str, Any]:
    """
    ✅ 重点修复：不同 apexomni 版本 SDK 的方法签名不一样
    这里用 HttpPrivate_v3 优先（更稳定），并用 flexible-call 兼容参数名。
    """
    http_v3 = _mk_http_v3()
    start = time.time()
    last_err = None

    while True:
        try:
            order_obj = None
            fills_obj = None

            # ---- order detail candidates ----
            for name in ("get_order_v3", "get_order_detail_v3", "get_order_by_id_v3"):
                if hasattr(http_v3, name) and order_id:
                    fn = getattr(http_v3, name)

                    # 尝试多种签名
                    try:
                        order_obj = _call_flexible(
                            fn,
                            args_list=[(order_id,)],
                            kwargs_list=[
                                {"orderId": order_id},
                                {"order_id": order_id},
                                {"id": order_id},
                            ],
                        )
                        break
                    except Exception as e:
                        last_err = e

            # ---- fills candidates ----
            for name in ("get_fills_v3", "get_order_fills_v3", "get_trade_fills_v3"):
                if hasattr(http_v3, name):
                    fn = getattr(http_v3, name)
                    try:
                        fills_obj = _call_flexible(
                            fn,
                            args_list=[(order_id,)] if order_id else [],
                            kwargs_list=[
                                {"orderId": order_id} if order_id else {},
                                {"order_id": order_id} if order_id else {},
                                {"clientId": client_order_id} if client_order_id else {},
                                {"clientOrderId": client_order_id} if client_order_id else {},
                            ],
                        )
                        break
                    except Exception as e:
                        last_err = e

            def _as_list(x):
                if x is None:
                    return []
                if isinstance(x, list):
                    return x
                if isinstance(x, dict):
                    data = x.get("data")
                    if isinstance(data, list):
                        return data
                    if isinstance(data, dict):
                        if "fills" in data and isinstance(data["fills"], list):
                            return data["fills"]
                        if "list" in data and isinstance(data["list"], list):
                            return data["list"]
                return []

            fills_list = _as_list(fills_obj)

            total_qty = Decimal("0")
            total_notional = Decimal("0")

            for f in fills_list:
                if not isinstance(f, dict):
                    continue
                q = f.get("size") or f.get("qty") or f.get("filledSize") or f.get("fillSize")
                p = f.get("price") or f.get("fillPrice") or f.get("avgPrice")
                if q is None or p is None:
                    continue
                dq = Decimal(str(q))
                dp = Decimal(str(p))
                if dq <= 0 or dp <= 0:
                    continue
                total_qty += dq
                total_notional += dq * dp

            if total_qty > 0:
                avg = (total_notional / total_qty)
                return {
                    "symbol": symbol,
                    "order_id": order_id,
                    "client_order_id": client_order_id,
                    "filled_qty": total_qty,
                    "avg_fill_price": avg,
                    "raw_order": order_obj,
                    "raw_fills": fills_obj,
                }

            # 退而求其次：从 order_obj 里找 filledSize/avgPrice
            if isinstance(order_obj, dict):
                data = order_obj.get("data") if isinstance(order_obj.get("data"), dict) else order_obj
                if isinstance(data, dict):
                    fq = data.get("filledSize") or data.get("cumFilledSize") or data.get("sizeFilled")
                    ap = data.get("avgPrice") or data.get("averagePrice") or data.get("fillAvgPrice")
                    if fq and ap:
                        dq = Decimal(str(fq))
                        dp = Decimal(str(ap))
                        if dq > 0 and dp > 0:
                            return {
                                "symbol": symbol,
                                "order_id": order_id,
                                "client_order_id": client_order_id,
                                "filled_qty": dq,
                                "avg_fill_price": dp,
                                "raw_order": order_obj,
                                "raw_fills": fills_obj,
                            }

        except Exception as e:
            last_err = e

        if time.time() - start >= max_wait_sec:
            raise RuntimeError(f"fill summary unavailable, last_err={last_err!r}")

        time.sleep(poll_interval)


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

        print(f"[apex_client] budget={budget} -> qty={size_str}, used≈{used_budget} (price {price_str})")
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

    raw_order = client.create_order_v3(**params)

    print("[apex_client] order response:", raw_order)

    data = raw_order["data"] if isinstance(raw_order, dict) and "data" in raw_order else raw_order
    order_id, client_order_id = _extract_order_ids(raw_order)

    return {
        "data": data,
        "raw_order": raw_order,
        "order_id": order_id,
        "client_order_id": client_order_id,
        "computed": {
            "symbol": symbol,
            "side": side,
            "size": size_str,
            "price": price_str,
            "used_budget": str(used_budget),
            "reduce_only": reduce_only,
        },
    }


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
