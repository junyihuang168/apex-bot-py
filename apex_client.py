import os
import time
import random
import inspect
import threading
import queue
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Dict, Any, Optional, Union, Tuple

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

# WS（v3 private）
try:
    from apexomni.constants import APEX_OMNI_WS_MAIN, APEX_OMNI_WS_TEST
    from apexomni.websocket_api import WebSocket
except Exception:
    APEX_OMNI_WS_MAIN = None
    APEX_OMNI_WS_TEST = None
    WebSocket = None


# -------------------------------------------------------------------
# 交易规则（默认：最小数量 0.01，步长 0.01，小数点后 2 位）
# -------------------------------------------------------------------
DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),
    "step_size": Decimal("0.01"),
    "qty_decimals": 2,
    # Price constraints (tick size / decimals). Used for triggerPrice & price snapping.
    "tick_size": Decimal("0.01"),
    "price_decimals": 2,
}

SYMBOL_RULES: Dict[str, Dict[str, Any]] = {
    # "BTC-USDT": {"min_qty": Decimal("0.001"), "step_size": Decimal("0.001"), "qty_decimals": 3},
}

_CLIENT: Optional[HttpPrivateSign] = None

NumberLike = Union[str, float, int]

# ----------------------------
# ✅ WS order updates cache + queue (NO fills subscription logic)
# ----------------------------
# We consume ONLY order update messages from the private WS stream.
# From cumulative fields (cumFilledSize/filledSize + avgPrice), we reconstruct delta fills.
_ORDER_Q: "queue.Queue[dict]" = queue.Queue(maxsize=20000)
_WS_STARTED = False
_WS_LOCK = threading.Lock()

# orderId -> tracker: {"cum_qty": Decimal, "avg": Decimal, "notional": Decimal, "status": str, "ts": float, "client_order_id": str|None, "symbol": str|None}
_WS_ORDER_STATE: Dict[str, Dict[str, Any]] = {}

# orderId -> clientOrderId (best-effort cache)
_ORDER_ID_TO_CLIENT_ID: Dict[str, str] = {}


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


def _get_ws_endpoint():
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)
    env_name = os.getenv("APEX_ENV", "").lower()
    if env_name in ("main", "mainnet", "prod", "production"):
        use_mainnet = True
    if use_mainnet:
        return APEX_OMNI_WS_MAIN
    return APEX_OMNI_WS_TEST


def _get_api_credentials():
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


def _random_client_id() -> str:
    return str(int(float(str(random.random())[2:])))


def _safe_call(fn, **kwargs):
    """
    Call function with only supported kwargs (robust across SDK versions).
    """
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        call_kwargs = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        return fn(**call_kwargs)
    except Exception:
        return fn(**{k: v for k, v in kwargs.items() if v is not None})


def _install_compat_shims(client: HttpPrivateSign):
    """
    DO NOT change business logic.
    Only add alias methods for SDK version differences.
    """
    if not hasattr(client, "get_account_v3") and hasattr(client, "accountV3"):
        client.get_account_v3 = getattr(client, "accountV3")  # type: ignore
    if not hasattr(client, "accountV3") and hasattr(client, "get_account_v3"):
        client.accountV3 = getattr(client, "get_account_v3")  # type: ignore

    if not hasattr(client, "configs_v3") and hasattr(client, "configsV3"):
        client.configs_v3 = getattr(client, "configsV3")  # type: ignore
    if not hasattr(client, "configsV3") and hasattr(client, "configs_v3"):
        client.configsV3 = getattr(client, "configs_v3")  # type: ignore

    if not hasattr(client, "create_order_v3") and hasattr(client, "createOrderV3"):
        client.create_order_v3 = getattr(client, "createOrderV3")  # type: ignore
    if not hasattr(client, "createOrderV3") and hasattr(client, "create_order_v3"):
        client.createOrderV3 = getattr(client, "create_order_v3")  # type: ignore

    if not hasattr(client, "cancel_order_v3") and hasattr(client, "cancelOrderV3"):
        client.cancel_order_v3 = getattr(client, "cancelOrderV3")  # type: ignore
    if not hasattr(client, "cancelOrderV3") and hasattr(client, "cancel_order_v3"):
        client.cancelOrderV3 = getattr(client, "cancel_order_v3")  # type: ignore

    if not hasattr(client, "cancel_order_by_client_id_v3") and hasattr(client, "cancelOrderByClientOrderIdV3"):
        client.cancel_order_by_client_id_v3 = getattr(client, "cancelOrderByClientOrderIdV3")  # type: ignore
    if not hasattr(client, "cancelOrderByClientOrderIdV3") and hasattr(client, "cancel_order_by_client_id_v3"):
        client.cancelOrderByClientOrderIdV3 = getattr(client, "cancel_order_by_client_id_v3")  # type: ignore


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

    _install_compat_shims(client)

    try:
        cfg = _safe_call(client.configs_v3)
        global _CONFIGS_V3
        _CONFIGS_V3 = cfg
        print("[apex_client] configs_v3 ok:", cfg)
    except Exception as e:
        print("[apex_client] WARNING configs_v3 error:", e)

    try:
        acc = _safe_call(client.get_account_v3)
        print("[apex_client] get_account_v3 ok:", acc)
    except Exception as e:
        print("[apex_client] WARNING get_account_v3 error:", e)

    _CLIENT = client
    return client


def get_account():
    client = get_client()
    return _safe_call(client.get_account_v3)



def _decimalize(x: Any) -> Decimal:
    """Best-effort conversion to Decimal without float artifacts."""
    if isinstance(x, Decimal):
        return x
    # ints are safe
    if isinstance(x, int):
        return Decimal(x)
    # strings / others
    try:
        return Decimal(str(x))
    except Exception as e:
        raise ValueError(f"cannot convert to Decimal: {x!r}") from e


def _infer_price_decimals_from_tick(tick: Decimal) -> int:
    """Infer decimals from tick size (e.g., 0.01 -> 2)."""
    t = tick.normalize()
    # For tick like 1E-2, exponent is -2
    exp = -t.as_tuple().exponent
    return max(0, exp)


def _get_tick_size(symbol: str) -> Tuple[Decimal, int]:
    """Return (tick_size, price_decimals). Defaults to symbol rules; may be improved by configs_v3 if present."""
    rules = _get_symbol_rules(symbol)
    tick = rules.get("tick_size", DEFAULT_SYMBOL_RULES["tick_size"])
    decs = rules.get("price_decimals", DEFAULT_SYMBOL_RULES["price_decimals"])

    # If configs_v3 cache exists, try to infer tick size for this symbol (best effort)
    global _CONFIGS_V3
    cfg = _CONFIGS_V3
    if cfg:
        sym_variants = {symbol.upper(), symbol.replace("-", "").upper(), symbol.replace("-", "_").upper()}
        try:
            # Walk nested dict/list
            stack = [cfg]
            while stack:
                cur = stack.pop()
                if isinstance(cur, dict):
                    # Match by common keys
                    v_sym = cur.get("symbol") or cur.get("symbolName") or cur.get("symbolId") or cur.get("id")
                    if isinstance(v_sym, str) and v_sym.upper() in sym_variants:
                        tk = cur.get("tickSize") or cur.get("tick_size") or cur.get("priceTick") or cur.get("price_tick")
                        if tk is not None:
                            tick = _decimalize(tk)
                            decs = _infer_price_decimals_from_tick(tick)
                            break
                    for v in cur.values():
                        if isinstance(v, (dict, list)):
                            stack.append(v)
                elif isinstance(cur, list):
                    for v in cur:
                        if isinstance(v, (dict, list)):
                            stack.append(v)
        except Exception:
            # Keep defaults if inference fails
            pass

    # Ensure sane
    tick = _decimalize(tick)
    if tick <= 0:
        tick = DEFAULT_SYMBOL_RULES["tick_size"]
    decs = int(decs) if decs is not None else _infer_price_decimals_from_tick(tick)
    return tick, decs


def _snap_price(symbol: str, price: Decimal, rounding) -> Decimal:
    """Snap a price to symbol tick size using Decimal math."""
    tick, decs = _get_tick_size(symbol)
    # steps = price / tick, then round to integer steps
    steps = (price / tick).to_integral_value(rounding=rounding)
    snapped = steps * tick
    quantum = Decimal("1").scaleb(-decs)
    return snapped.quantize(quantum, rounding=rounding)


def snap_price_for_order(symbol: str, side: str, order_type: str, px: Any) -> Decimal:
    """
    Snap triggerPrice/price to tick size with direction-safe rounding.
    - STOP_*: SELL -> down, BUY -> up
    - TAKE_PROFIT_*: SELL -> up, BUY -> down
    """
    side_u = str(side).upper()
    typ_u = str(order_type).upper()

    is_stop = "STOP" in typ_u and "TAKE_PROFIT" not in typ_u
    is_tp = "TAKE_PROFIT" in typ_u

    if is_stop:
        rounding = ROUND_DOWN if side_u == "SELL" else ROUND_UP
    elif is_tp:
        rounding = ROUND_UP if side_u == "SELL" else ROUND_DOWN
    else:
        rounding = ROUND_DOWN if side_u == "SELL" else ROUND_UP

    return _snap_price(symbol, _decimalize(px), rounding)


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


# ------------------------------------------------------------
# ✅ Private WS: account_info_stream_v3 -> fills push
# ------------------------------------------------------------

def start_private_ws():
    """
    Start ONE private WS connection (idempotent).

    We subscribe to account_info_stream_v3 and consume ONLY order updates (contents.orders).
    We do NOT depend on fills push.

    For each order update, we maintain a per-order tracker:
      - cumulative filled qty (cumFilledSize / filledSize)
      - average fill price (avgPrice / fillAvgPrice / averagePrice)
      - status

    From cumulative (qty, avg), we reconstruct delta fills:
      deltaQty = newCum - prevCum
      deltaPx  = (newAvg*newCum - prevAvg*prevCum) / deltaQty

    Delta events are pushed into _ORDER_Q for app.py to consume (protective OCO, etc.).
    """
    global _WS_STARTED
    with _WS_LOCK:
        if _WS_STARTED:
            return
        _WS_STARTED = True

    if WebSocket is None:
        print("[apex_client][WS] websocket_api not available in apexomni. Skipping WS.")
        return

    endpoint = _get_ws_endpoint()
    if not endpoint:
        print("[apex_client][WS] no WS endpoint constants available. Skipping WS.")
        return

    api_creds = _get_api_credentials()

    def _pick(d: dict, *keys):
        for k in keys:
            v = d.get(k)
            if v is not None:
                return v
        return None

    def _to_decimal(x) -> Optional[Decimal]:
        if x is None:
            return None
        try:
            d = Decimal(str(x))
            return d
        except Exception:
            return None

    def handle_account(message: dict):
        try:
            if not isinstance(message, dict):
                return

            contents = message.get("contents")
            if not isinstance(contents, dict):
                return

            # Prefer orders updates. SDKs differ: orders / order / orderUpdates.
            orders = (
                contents.get("orders")
                or contents.get("order")
                or contents.get("orderUpdates")
                or contents.get("ordersV3")
            )

            if isinstance(orders, dict):
                orders = [orders]

            if not isinstance(orders, list) or not orders:
                return

            for o in orders:
                if not isinstance(o, dict):
                    continue

                # Normalize order payload
                data = o.get("data") if isinstance(o.get("data"), dict) else o
                if not isinstance(data, dict):
                    continue

                order_id = _pick(data, "orderId", "id")
                if not order_id:
                    continue
                order_id = str(order_id)

                client_oid = _pick(data, "clientOrderId", "clientId", "client_id")
                if client_oid is not None:
                    client_oid = str(client_oid)
                    _ORDER_ID_TO_CLIENT_ID[order_id] = client_oid

                symbol = _pick(data, "symbol")
                if symbol is not None:
                    symbol = str(symbol).upper().strip()

                status = _pick(data, "status", "orderStatus", "state")
                status = str(status).upper().strip() if status is not None else ""

                cum_qty = _to_decimal(_pick(data, "cumFilledSize", "filledSize", "sizeFilled", "cumFilled", "executedQty"))
                avg_px = _to_decimal(_pick(data, "avgPrice", "fillAvgPrice", "averagePrice", "avgFillPrice"))

                # Update tracker even if only status changes.
                prev = _WS_ORDER_STATE.get(order_id) or {
                    "cum_qty": Decimal("0"),
                    "avg": None,
                    "notional": Decimal("0"),
                    "status": "",
                    "ts": 0.0,
                    "client_order_id": client_oid,
                    "symbol": symbol,
                }

                prev_cum = Decimal(str(prev.get("cum_qty") or "0"))
                prev_avg = prev.get("avg")
                prev_notional = Decimal(str(prev.get("notional") or "0"))

                # If cum/avg absent, only refresh status/timestamps.
                delta_qty = Decimal("0")
                delta_px: Optional[Decimal] = None

                if cum_qty is not None and cum_qty >= 0:
                    # notional reconstruction requires avg
                    if avg_px is not None and cum_qty > 0:
                        new_notional = (avg_px * cum_qty)
                        if prev_avg is not None and prev_cum > 0:
                            prev_notional = (Decimal(str(prev_avg)) * prev_cum)
                        else:
                            prev_notional = Decimal(str(prev_notional or "0"))

                        if cum_qty > prev_cum:
                            delta_qty = (cum_qty - prev_cum)
                            delta_notional = (new_notional - prev_notional)
                            if delta_qty > 0:
                                try:
                                    delta_px = (delta_notional / delta_qty)
                                except Exception:
                                    delta_px = None

                        prev["cum_qty"] = cum_qty
                        prev["avg"] = avg_px
                        prev["notional"] = new_notional
                    else:
                        # If avg is missing, still store cum_qty so we can later reconcile once avg arrives.
                        prev["cum_qty"] = cum_qty

                if status:
                    prev["status"] = status
                if client_oid:
                    prev["client_order_id"] = client_oid
                if symbol:
                    prev["symbol"] = symbol

                prev["ts"] = time.time()
                _WS_ORDER_STATE[order_id] = prev

                # Emit delta event only when we can compute a delta price.
                if delta_qty > 0 and delta_px is not None and (symbol or prev.get("symbol")):
                    evt = {
                        "type": "order_delta",
                        "order_id": order_id,
                        "client_order_id": client_oid or prev.get("client_order_id"),
                        "symbol": (symbol or prev.get("symbol")),
                        "status": prev.get("status"),
                        "cum_filled_qty": str(prev.get("cum_qty")),
                        "avg_fill_price": str(prev.get("avg")) if prev.get("avg") is not None else None,
                        "delta_qty": str(delta_qty),
                        "delta_price": str(delta_px),
                        "raw": data,
                        "ts": time.time(),
                    }
                    try:
                        _ORDER_Q.put_nowait(evt)
                    except Exception:
                        pass

        except Exception as e:
            print("[apex_client][WS] handle_account error:", e)

    def _run_forever():
        backoff = 1.0
        while True:
            try:
                ws_client = WebSocket(
                    endpoint=endpoint,
                    api_key_credentials=api_creds,
                )
                ws_client.account_info_stream_v3(handle_account)
                print("[apex_client][WS] subscribed account_info_stream_v3 (orders)")

                backoff = 1.0
                while True:
                    try:
                        if hasattr(ws_client, "ping"):
                            ws_client.ping()
                    except Exception:
                        raise
                    time.sleep(15)

            except Exception as e:
                print(f"[apex_client][WS] reconnecting after error: {e} (sleep {backoff}s)")
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    t = threading.Thread(target=_run_forever, daemon=True, name="apex-private-ws")
    t.start()
    print("[apex_client][WS] started account_info_stream_v3 (orders)")


def pop_order_event(timeout: float = 0.5) -> Optional[dict]:
    try:
        return _ORDER_Q.get(timeout=timeout)
    except Exception:
        return None


def _ws_order_summary(order_id: Optional[str], client_order_id: Optional[str]) -> Optional[Dict[str, Any]]:
    now = time.time()
    ttl = float(os.getenv("WS_ORDER_TTL_SEC", "30"))
    if order_id:
        st = _WS_ORDER_STATE.get(str(order_id))
        if st and (now - float(st.get("ts", 0.0)) <= ttl):
            cum = st.get("cum_qty")
            avg = st.get("avg")
            if cum is not None and avg is not None and Decimal(str(cum)) > 0 and Decimal(str(avg)) > 0:
                return {
                    "order_id": str(order_id),
                    "client_order_id": client_order_id or st.get("client_order_id"),
                    "filled_qty": Decimal(str(cum)),
                    "avg_fill_price": Decimal(str(avg)),
                    "status": str(st.get("status") or ""),
                }
    return None



def get_fill_summary(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    max_wait_sec: float = 12.0,
    poll_interval: float = 0.25,
) -> Dict[str, Any]:
    """
    ✅ Order-based fill summary (NO fills subscription).

    Priority:
      1) Private WS order tracker (_WS_ORDER_STATE) derived from order updates.
      2) REST order detail (filledSize/cumFilledSize + avgPrice) as fallback.

    This function NEVER uses market quote / worstPrice as execution price.
    """
    # 1) WS order summary
    ws_hit = _ws_order_summary(order_id, client_order_id)
    if ws_hit:
        return {
            "symbol": symbol,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "filled_qty": ws_hit["filled_qty"],
            "avg_fill_price": ws_hit["avg_fill_price"],
            "order_status": ws_hit.get("status") or "",
            "raw_order": None,
            "raw_source": "ws_order",
        }

    client = get_client()
    start = time.time()
    last_err: Optional[Exception] = None

    while True:
        # 2) REST order detail fallback (no fills endpoint)
        order_obj = None
        candidates = [
            "get_order_v3",
            "get_order_detail_v3",
            "get_order_by_id_v3",
            "get_order_v3_by_id",
        ]
        for name in candidates:
            if hasattr(client, name) and order_id:
                try:
                    fn = getattr(client, name)
                    try:
                        order_obj = fn(orderId=order_id)
                    except Exception:
                        order_obj = fn(order_id)
                    break
                except Exception as e:
                    last_err = e

        if isinstance(order_obj, dict):
            data = order_obj.get("data") if isinstance(order_obj.get("data"), dict) else order_obj
            if isinstance(data, dict):
                fq = data.get("filledSize") or data.get("cumFilledSize") or data.get("sizeFilled")
                ap = data.get("avgPrice") or data.get("averagePrice") or data.get("fillAvgPrice")
                st = data.get("status") or data.get("orderStatus") or data.get("state") or ""
                try:
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
                                "order_status": str(st).upper(),
                                "raw_order": order_obj,
                                "raw_source": "rest_order",
                            }
                except Exception as e:
                    last_err = e

        if time.time() - start >= max_wait_sec:
            raise RuntimeError(f"order fill summary unavailable, last_err={last_err!r}")

        # Re-check WS each loop in case WS arrives after first miss.
        ws_hit = _ws_order_summary(order_id, client_order_id)
        if ws_hit:
            return {
                "symbol": symbol,
                "order_id": order_id,
                "client_order_id": client_order_id,
                "filled_qty": ws_hit["filled_qty"],
                "avg_fill_price": ws_hit["avg_fill_price"],
                "order_status": ws_hit.get("status") or "",
                "raw_order": None,
                "raw_source": "ws_order",
            }

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
    apex_client_id = str(client_id) if client_id else _random_client_id()

    print(f"[apex_client] clientId={apex_client_id} (provided={bool(client_id)})")


    # Snap price as well (best-effort). Some gateways validate scale.
    try:
        px_dec = snap_price_for_order(symbol, side, "MARKET", price_str)
        price_str = format(px_dec, f".{rules.get('price_decimals', 2)}f")
    except Exception:
        # If snapping fails for any reason, fall back to original price_str.
        price_str = str(price_str)

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

    raw_order = _safe_call(client.create_order_v3, **params)
    print("[apex_client] order response:", raw_order)

    data = raw_order["data"] if isinstance(raw_order, dict) and "data" in raw_order else raw_order
    order_id, client_order_id = _extract_order_ids(raw_order)

    if order_id and client_order_id:
        _ORDER_ID_TO_CLIENT_ID[str(order_id)] = str(client_order_id)

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


def create_trigger_order(
    symbol: str,
    side: str,
    order_type: str,             # STOP_MARKET / TAKE_PROFIT_MARKET / STOP_LIMIT / TAKE_PROFIT_LIMIT
    size: NumberLike,
    trigger_price: NumberLike,
    price: Optional[NumberLike] = None,
    reduce_only: bool = True,
    client_id: Optional[str] = None,
    expiration_sec: Optional[int] = None,
) -> Dict[str, Any]:
    client = get_client()
    side = str(side).upper()
    order_type = str(order_type).upper()

    ts = int(time.time())
    apex_client_id = client_id or _random_client_id()

    size_str = str(size)
    # Snap triggerPrice to tick size to satisfy exchange precision rules.
    trigger_dec = snap_price_for_order(symbol, side, order_type, trigger_price)
    trigger_str = str(trigger_dec)

    # price is required by some SDK/builds even for *_MARKET; fallback to worstPrice
    if price is None:
        try:
            px = get_market_price(symbol, side, size_str)
        except Exception:
            px = trigger_str
        price = px

    # Snap price as well (best-effort). Even for *_MARKET some gateways validate scale.
    try:
        price_dec = snap_price_for_order(symbol, side, order_type, price)
        price = str(price_dec)
    except Exception:
        price = str(price)

    params = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "size": size_str,
        "price": str(price),
        "triggerPrice": trigger_str,
        "timestampSeconds": ts,
        "reduceOnly": reduce_only,
        "clientId": apex_client_id,
    }

    if expiration_sec is not None:
        params["expiration"] = int(expiration_sec)

    print("[apex_client] create_trigger_order params:", params)

    raw_order = _safe_call(client.create_order_v3, **params)
    print("[apex_client] trigger order response:", raw_order)

    order_id, client_order_id = _extract_order_ids(raw_order)
    if order_id and client_order_id:
        _ORDER_ID_TO_CLIENT_ID[str(order_id)] = str(client_order_id)

    return {
        "raw_order": raw_order,
        "order_id": order_id,
        "client_order_id": client_order_id,
        "sent": params,
    }


def cancel_order(order_id: str) -> Dict[str, Any]:
    client = get_client()
    if not order_id:
        return {"ok": False, "error": "missing order_id"}
    candidates = ["cancel_order_v3", "cancelOrderV3", "cancel_order", "cancelOrder"]
    last = None
    for name in candidates:
        if hasattr(client, name):
            fn = getattr(client, name)
            try:
                return _safe_call(fn, orderId=str(order_id))
            except Exception as e:
                last = e
    return {"ok": False, "error": f"cancel failed: {last!r}"}


def cancel_order_by_client_id(client_order_id: str) -> Dict[str, Any]:
    client = get_client()
    if not client_order_id:
        return {"ok": False, "error": "missing client_order_id"}
    candidates = ["cancel_order_by_client_id_v3", "cancelOrderByClientOrderIdV3"]
    last = None
    for name in candidates:
        if hasattr(client, name):
            fn = getattr(client, name)
            try:
                return _safe_call(fn, clientOrderId=str(client_order_id))
            except Exception as e:
                last = e
    return {"ok": False, "error": f"cancel by clientId failed: {last!r}"}


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
        data.get("positions") or data.get("openPositions") or data.get("position") or data.get("positionV3") or []
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
        side = str(p.get("side", "")).upper()
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
