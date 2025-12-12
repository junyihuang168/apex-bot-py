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

# -------------------------------------------------------------------
# Trading rules (default)
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


class FillUnavailableError(RuntimeError):
    """Raised when we cannot obtain real fills within allowed windows."""


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

    try:
        cfg = client.configs_v3()
        print("[apex_client] configs_v3 ok")
    except Exception as e:
        print("[apex_client] WARNING configs_v3 error:", e)

    try:
        acc = client.get_account_v3()
        print("[apex_client] get_account_v3 ok")
    except Exception as e:
        print("[apex_client] WARNING get_account_v3 error:", e)

    _CLIENT = client
    return client


def get_account():
    client = get_client()
    return client.get_account_v3()


def get_market_price(symbol: str, side: str, size: str) -> str:
    """
    NOTE:
    - This is ONLY used for order placement (required 'price' field) and budget->qty estimation.
    - It is NOT used as entry/exit baseline for SL/TP (Fill-First).
    """
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
        raise RuntimeError(f"[apex_client] get_worst_price_v3 unexpected: {res}")

    price_str = str(price)
    return price_str


NumberLike = Union[str, float, int]


def _extract_order_ids(raw_order: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    best-effort extraction: orderId / clientOrderId
    """
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


def _safe_call(fn, **kwargs):
    """
    Call function with only supported kwargs (robust across SDK versions).
    """
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        call_kwargs = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        return fn(**call_kwargs)
    except (ValueError, TypeError):
        # signature may fail for some callables; fallback
        return fn(**{k: v for k, v in kwargs.items() if v is not None})


def _as_dict_data(obj: Any) -> Any:
    if isinstance(obj, dict) and "data" in obj:
        return obj.get("data")
    return obj


def _try_parse_fill_from_order_obj(order_obj: Any) -> Optional[Dict[str, Any]]:
    """
    Parse cumulative fill from an order-like response.
    Supports multiple possible key names across versions.
    """
    if not isinstance(order_obj, dict):
        return None

    data = order_obj
    if isinstance(order_obj.get("data"), dict):
        data = order_obj["data"]

    # common candidates
    # qty
    fq = (
        data.get("filledSize")
        or data.get("cumFilledSize")
        or data.get("sizeFilled")
        or data.get("cumSuccessFillSize")
        or data.get("successFillSize")
        or data.get("cumFillSize")
    )

    # avg price or last match price
    ap = (
        data.get("avgPrice")
        or data.get("averagePrice")
        or data.get("fillAvgPrice")
    )

    # sometimes only cumulative value is available
    fv = (
        data.get("cumSuccessFillValue")
        or data.get("successFillValue")
        or data.get("cumFilledValue")
        or data.get("filledValue")
        or data.get("cumFillValue")
    )

    latest = (
        data.get("latestMatchFillPrice")
        or data.get("lastFillPrice")
        or data.get("fillPrice")
    )

    try:
        if fq is None:
            return None
        dq = Decimal(str(fq))
        if dq <= 0:
            return None

        if ap is not None:
            dp = Decimal(str(ap))
            if dp > 0:
                return {"filled_qty": dq, "avg_fill_price": dp, "source": "order_avgPrice"}

        if fv is not None:
            dv = Decimal(str(fv))
            if dv > 0:
                dp = (dv / dq)
                return {"filled_qty": dq, "avg_fill_price": dp, "source": "order_value/size"}

        if latest is not None:
            dp = Decimal(str(latest))
            if dp > 0:
                # last fill price (less ideal than avg, but still real trade price)
                return {"filled_qty": dq, "avg_fill_price": dp, "source": "order_latestFill"}
    except Exception:
        return None

    return None


def _try_parse_fill_from_fills_obj(fills_obj: Any) -> Optional[Dict[str, Any]]:
    """
    Parse avg fill price from fills list.
    """
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
        return []

    fills_list = _as_list(fills_obj)

    total_qty = Decimal("0")
    total_notional = Decimal("0")
    total_fee = Decimal("0")

    for f in fills_list:
        if not isinstance(f, dict):
            continue

        q = f.get("size") or f.get("qty") or f.get("filledSize") or f.get("fillSize")
        p = f.get("price") or f.get("fillPrice")
        fee = f.get("fee") or f.get("fillFee")

        if q is None or p is None:
            continue

        try:
            dq = Decimal(str(q))
            dp = Decimal(str(p))
        except Exception:
            continue

        if dq <= 0 or dp <= 0:
            continue

        total_qty += dq
        total_notional += dq * dp

        if fee is not None:
            try:
                total_fee += Decimal(str(fee))
            except Exception:
                pass

    if total_qty > 0:
        avg = (total_notional / total_qty)
        out = {"filled_qty": total_qty, "avg_fill_price": avg, "source": "fills_list"}
        if total_fee > 0:
            out["fee"] = total_fee
        return out

    return None


def get_fill_summary_fill_first(
    symbol: str,
    order_id: Optional[str],
    client_order_id: Optional[str],
    fast_wait_sec: float = 3.0,
    fast_poll: float = 0.20,
    slow_wait_sec: float = 12.0,
    slow_poll: float = 0.50,
) -> Dict[str, Any]:
    """
    Fill-First:
    - High-frequency polling in first 2~3s
    - Then slower polling as backup
    - NO worstPrice fallback here
    """
    client = get_client()
    t0 = time.time()

    def _attempt_once() -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        dbg: Dict[str, Any] = {"order_obj": None, "fills_obj": None, "errors": []}

        # 1) Try order-by-client-id / order detail (best for cum fill)
        order_obj = None
        order_candidates = [
            # most useful if available
            "get_order_by_client_order_id_v3",
            "get_order_by_client_id_v3",
            "get_order_by_clientId_v3",
            # generic
            "get_order_v3",
            "get_order_detail_v3",
            "get_order_by_id_v3",
            "get_order_v3_by_id",
        ]

        for name in order_candidates:
            if not hasattr(client, name):
                continue
            fn = getattr(client, name)
            try:
                # try both identifiers
                if client_order_id is not None:
                    order_obj = _safe_call(fn, clientOrderId=client_order_id, clientId=client_order_id, client_id=client_order_id)
                if order_obj is None and order_id is not None:
                    order_obj = _safe_call(fn, orderId=order_id, order_id=order_id, id=order_id)
                if order_obj is not None:
                    break
            except Exception as e:
                dbg["errors"].append(f"{name}:{repr(e)}")

        dbg["order_obj"] = order_obj
        parsed = _try_parse_fill_from_order_obj(order_obj)
        if parsed:
            return parsed, dbg

        # 2) Try fills endpoints (trade history)
        fills_obj = None
        fill_candidates = [
            "get_fills_v3",
            "get_order_fills_v3",
            "get_trade_fills_v3",
        ]
        for name in fill_candidates:
            if not hasattr(client, name):
                continue
            fn = getattr(client, name)
            try:
                fills_obj = _safe_call(fn, orderId=order_id, order_id=order_id, clientId=client_order_id, clientOrderId=client_order_id)
                if fills_obj is not None:
                    break
            except Exception as e:
                dbg["errors"].append(f"{name}:{repr(e)}")

        dbg["fills_obj"] = fills_obj
        parsed2 = _try_parse_fill_from_fills_obj(fills_obj)
        if parsed2:
            return parsed2, dbg

        return None, dbg

    # fast window (2~3s high frequency)
    while True:
        fill, dbg = _attempt_once()
        if fill:
            return {
                "symbol": symbol,
                "order_id": order_id,
                "client_order_id": client_order_id,
                **fill,
                "debug": dbg,
            }
        if time.time() - t0 >= fast_wait_sec:
            break
        time.sleep(fast_poll)

    # slow window (backup)
    t1 = time.time()
    while True:
        fill, dbg = _attempt_once()
        if fill:
            return {
                "symbol": symbol,
                "order_id": order_id,
                "client_order_id": client_order_id,
                **fill,
                "debug": dbg,
            }
        if time.time() - t1 >= slow_wait_sec:
            break
        time.sleep(slow_poll)

    raise FillUnavailableError(
        f"fill unavailable after {fast_wait_sec}s fast + {slow_wait_sec}s slow. "
        f"symbol={symbol} order_id={order_id} client_order_id={client_order_id}"
    )


def create_market_order_fill_first(
    symbol: str,
    side: str,
    size: NumberLike | None = None,
    size_usdt: NumberLike | None = None,
    reduce_only: bool = False,
    tv_signal_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    MARKET order (ApeX requires price field; we use worstPrice ONLY for order placement, never for baselines).
    Returns ONLY if real fills are obtained; otherwise raises FillUnavailableError.
    """
    client = get_client()
    side = side.upper()

    rules = _get_symbol_rules(symbol)
    decimals = rules["qty_decimals"]

    # Determine size_str (qty)
    used_budget_est = None
    price_for_order = None

    if size_usdt is not None:
        budget = Decimal(str(size_usdt))
        if budget <= 0:
            raise ValueError("size_usdt must be > 0")

        min_qty = rules["min_qty"]

        # worstPrice used for conservative qty estimation and order placement only
        ref_price_decimal = Decimal(get_market_price(symbol, side, str(min_qty)))
        theoretical_qty = budget / ref_price_decimal
        snapped_qty = _snap_quantity(symbol, theoretical_qty)

        price_for_order = get_market_price(symbol, side, str(snapped_qty))
        price_decimal = Decimal(price_for_order)

        size_str = format(snapped_qty, f".{decimals}f")

        used_budget_est = (snapped_qty * price_decimal).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )
    else:
        if size is None:
            raise ValueError("size or size_usdt must be provided")
        size_str = str(size)
        price_for_order = get_market_price(symbol, side, size_str)

    ts = int(time.time())
    apex_client_id = _random_client_id()

    # If you have a stable TV id / signal id, pass it for dedup at app layer
    if tv_signal_id:
        print(f"[apex_client] tv_signal_id={tv_signal_id} -> apex_clientId={apex_client_id}")

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "size": size_str,
        "price": str(price_for_order),
        "timestampSeconds": ts,
        "reduceOnly": reduce_only,
        "clientId": apex_client_id,
    }

    raw_order = client.create_order_v3(**params)
    order_id, client_order_id = _extract_order_ids(raw_order)

    # Fill-First: wait for real fills (2~3s high frequency)
    fill = get_fill_summary_fill_first(
        symbol=symbol,
        order_id=order_id,
        client_order_id=client_order_id,
        fast_wait_sec=float(os.getenv("FILL_FAST_WAIT_SEC", "3.0")),
        fast_poll=float(os.getenv("FILL_FAST_POLL_SEC", "0.20")),
        slow_wait_sec=float(os.getenv("FILL_SLOW_WAIT_SEC", "12.0")),
        slow_poll=float(os.getenv("FILL_SLOW_POLL_SEC", "0.50")),
    )

    # Real fill numbers
    filled_qty: Decimal = fill["filled_qty"]
    avg_fill_price: Decimal = fill["avg_fill_price"]

    used_budget_real = (filled_qty * avg_fill_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    return {
        "raw_order": raw_order,
        "order_id": order_id,
        "client_order_id": client_order_id,
        "apex_client_id": apex_client_id,
        "placed": {
            "symbol": symbol,
            "side": side,
            "size": size_str,
            "price_for_order": str(price_for_order),  # for placement only
            "reduce_only": reduce_only,
            "used_budget_est": str(used_budget_est) if used_budget_est is not None else None,
        },
        "fill": {
            "filled_qty": str(filled_qty),
            "avg_fill_price": str(avg_fill_price),
            "used_budget_real": str(used_budget_real),
            "source": fill.get("source"),
        },
        "fill_debug": fill.get("debug"),
    }


# ---------------------------
# Remote position query
# ---------------------------
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
