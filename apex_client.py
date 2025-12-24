import os
import time
import random
import inspect
import threading
import queue
from collections import OrderedDict
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, Tuple, Union, Iterable

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

try:
    from apexomni.constants import APEX_OMNI_WS_MAIN, APEX_OMNI_WS_TEST
    from apexomni.websocket_api import WebSocket
except Exception:
    APEX_OMNI_WS_MAIN = None
    APEX_OMNI_WS_TEST = None
    WebSocket = None


# -----------------------------------------------------------------------------
# Symbol rules (fallback defaults; can be extended via SYMBOL_RULES dict).
# -----------------------------------------------------------------------------
DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),
    "step_size": Decimal("0.01"),
    "qty_decimals": 2,
    "tick_size": Decimal("0.01"),
    "price_decimals": 2,
}

SYMBOL_RULES: Dict[str, Dict[str, Any]] = {}

NumberLike = Union[str, int, float]

_CLIENT: Optional[HttpPrivateSign] = None


# -----------------------------------------------------------------------------
# WS + caches
# We monitor BOTH orders and fills. Fills are the source of truth for price.
# Orders are used for lifecycle/status and as a backup when fills are missing.
# -----------------------------------------------------------------------------

_WS_STARTED = False
_WS_LOCK = threading.Lock()

# Fill aggregation per orderId
_FILL_AGG: Dict[str, Dict[str, Any]] = {}

# (orderId, fillId) dedupe with TTL (OrderedDict as LRU)
_FILL_DEDUPE: "OrderedDict[str, float]" = OrderedDict()

# Order state cache (status/cumQty/avg)
_ORDER_STATE: Dict[str, Dict[str, Any]] = {}

# A small queue of raw fill events (optional; currently not consumed by app.py)
_FILL_Q: "queue.Queue[dict]" = queue.Queue(maxsize=20000)

# A small queue of order update events (optional)
_ORDER_Q: "queue.Queue[dict]" = queue.Queue(maxsize=20000)


# REST poller
_REST_POLL_STARTED = False
_REST_POLL_LOCK = threading.Lock()


def _env_bool(name: str, default: bool = False) -> bool:
    return str(os.getenv(name, str(default))).strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_base_and_network() -> Tuple[str, int]:
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)
    env_name = str(os.getenv("APEX_ENV", "")).lower()
    if env_name in {"main", "mainnet", "prod", "production"}:
        use_mainnet = True
    base_url = APEX_OMNI_HTTP_MAIN if use_mainnet else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_OMNI_MAIN_ARB if use_mainnet else NETWORKID_TEST
    return base_url, network_id


def _get_ws_endpoint() -> Optional[str]:
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)
    env_name = str(os.getenv("APEX_ENV", "")).lower()
    if env_name in {"main", "mainnet", "prod", "production"}:
        use_mainnet = True
    return APEX_OMNI_WS_MAIN if use_mainnet else APEX_OMNI_WS_TEST


def _get_api_credentials() -> Dict[str, str]:
    # Required by apexomni
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


def _safe_call(fn, **kwargs):
    """Call SDK function with only supported kwargs (cross-version compatible)."""
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        call_kwargs = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        return fn(**call_kwargs)
    except Exception:
        return fn(**{k: v for k, v in kwargs.items() if v is not None})


def _install_compat_shims(client: HttpPrivateSign) -> None:
    """Add method aliases for SDK naming differences (no behavior changes)."""
    aliases = {
        "configs_v3": ["configsV3"],
        "get_account_v3": ["accountV3"],
        "create_order_v3": ["createOrderV3"],
        "cancel_order_v3": ["cancelOrderV3"],
        "cancel_order_by_client_id_v3": ["cancelOrderByClientOrderIdV3"],
        "get_order_v3": ["getOrderV3", "get_order_detail_v3", "getOrderDetailV3"],
        "get_open_orders_v3": ["openOrdersV3", "open_orders_v3"],
        "get_positions_v3": ["positionsV3", "get_open_positions_v3", "open_positions_v3"],
    }
    for canonical, candidates in aliases.items():
        if hasattr(client, canonical):
            continue
        for cand in candidates:
            if hasattr(client, cand):
                setattr(client, canonical, getattr(client, cand))
                break


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
    _CLIENT = client
    return client


# -----------------------------------------------------------------------------
# Symbol helpers
# -----------------------------------------------------------------------------


def _get_symbol_rules(symbol: str) -> Dict[str, Any]:
    s = str(symbol).upper().strip()
    rules = SYMBOL_RULES.get(s)
    if rules:
        merged = dict(DEFAULT_SYMBOL_RULES)
        merged.update(rules)
        return merged
    return dict(DEFAULT_SYMBOL_RULES)


def _snap_quantity(symbol: str, qty: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    step = Decimal(str(rules["step_size"]))
    min_qty = Decimal(str(rules["min_qty"]))
    if qty <= 0:
        return Decimal("0")
    snapped = (qty // step) * step
    snapped = snapped.quantize(step, rounding=ROUND_DOWN)
    if snapped < min_qty:
        raise ValueError(f"budget too small: snapped {snapped} < minQty {min_qty}")
    return snapped


def _snap_price(symbol: str, price: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    tick = Decimal(str(rules.get("tick_size") or "0.01"))
    if price <= 0:
        return Decimal("0")
    snapped = (price // tick) * tick
    return snapped.quantize(tick, rounding=ROUND_DOWN)


# -----------------------------------------------------------------------------
# Market price helper
# Many Apex endpoints require a "price" even for MARKET orders (signature).
# This function returns a conservative price bound (worst price).
# -----------------------------------------------------------------------------


def get_market_price(symbol: str, side: str, size: NumberLike) -> str:
    """Return a numeric worstPrice quote (string) for MARKET signature.

    Some SDK endpoints may return nested dict payloads or non-numeric objects.
    This helper is strict: it only returns values that can be parsed as Decimal > 0.
    """
    client = get_client()

    def _as_num_str(v):
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        try:
            d = Decimal(s)
        except Exception:
            return None
        return s if d > 0 else None

    methods = [
        "get_worst_price",
        "get_worst_price_v3",
        "getWorstPrice",
        "getWorstPriceV3",
        "worst_price_v3",
    ]

    last_err = None
    for name in methods:
        if not hasattr(client, name):
            continue
        fn = getattr(client, name)
        try:
            res = _safe_call(fn, symbol=str(symbol).upper(), side=str(side).upper(), size=str(size))

            # dict payload
            if isinstance(res, dict):
                data = res.get("data") if res.get("data") is not None else res
                if isinstance(data, dict):
                    for k in ("worstPrice", "price", "worst_price"):
                        s = _as_num_str(data.get(k))
                        if s:
                            return s
                # If dict but no numeric in expected keys, keep trying other methods
                continue

            # raw scalar
            s = _as_num_str(res)
            if s:
                return s

        except Exception as e:
            last_err = e

    # Fallback: try mark/oracle/index price from positions; last resort "0".
    try:
        pos = get_open_position_for_symbol(symbol)
        if isinstance(pos, dict):
            mp = pos.get("markPrice") or pos.get("oraclePrice") or pos.get("indexPrice")
            s = _as_num_str(mp)
            if s:
                return s
    except Exception:
        pass

    if last_err:
        print(f"[apex_client] get_market_price fallback used (last_err={last_err})")
    return "0"


# -----------------------------------------------------------------------------
# Order placement
# -----------------------------------------------------------------------------


def _random_client_id() -> str:
    return str(int(float(str(random.random())[2:])))


def create_market_order(
    symbol: str,
    side: str,
    size: NumberLike,
    reduce_only: bool = False,
    client_id: Optional[str] = None,
) -> Dict[str, Any]:
    client = get_client()
    sym = str(symbol).upper().strip()
    side_u = str(side).upper().strip()
    qty = str(size)

    # Apex: market orders still need a signed price bound
    worst_price = get_market_price(sym, side_u, qty)

    client_id = client_id or _random_client_id()

    res = _safe_call(
        getattr(client, "create_order_v3"),
        symbol=sym,
        side=side_u,
        type="MARKET",
        size=qty,
        price=str(worst_price),
        reduceOnly=bool(reduce_only),
        clientOrderId=str(client_id),
    )

    order_id = None
    client_order_id = None
    if isinstance(res, dict):
        data = res.get("data") if isinstance(res.get("data"), dict) else res
        order_id = data.get("orderId") or data.get("id")
        client_order_id = data.get("clientOrderId") or data.get("clientId") or client_id
    else:
        # Some SDKs return raw object
        try:
            order_id = getattr(res, "orderId", None)
        except Exception:
            order_id = None
        client_order_id = client_id

    if order_id:
        register_order_for_tracking(order_id=str(order_id), client_order_id=str(client_order_id), symbol=sym, expected_qty=qty)

    return {
        "raw": res,
        "order_id": str(order_id) if order_id is not None else None,
        "client_order_id": str(client_order_id) if client_order_id is not None else str(client_id),
        "symbol": sym,
        "side": side_u,
        "size": qty,
        "worst_price": str(worst_price),
    }


def create_trigger_order(
    symbol: str,
    side: str,
    order_type: str,
    size: NumberLike,
    trigger_price: NumberLike,
    price: Optional[NumberLike] = None,
    reduce_only: bool = True,
    client_id: Optional[str] = None,
    trigger_price_type: Optional[str] = None,
    expiration_sec: Optional[int] = None,
) -> Dict[str, Any]:
    """Create a conditional (trigger) order (STOP/TP).

    Backward-compatible shim: some app.py versions import this symbol.
    """
    client = get_client()
    sym = str(symbol).upper().strip()
    side_u = str(side).upper().strip()
    otype = str(order_type).upper().strip()
    qty = str(size)

    client_id = client_id or _random_client_id()

    # snap trigger/price to tick-size best-effort
    try:
        trigger_str = str(_snap_price(sym, Decimal(str(trigger_price))))
    except Exception:
        trigger_str = str(trigger_price)

    if price is None:
        try:
            price = get_market_price(sym, side_u, qty)
        except Exception:
            price = trigger_str

    try:
        price_str = str(_snap_price(sym, Decimal(str(price))))
    except Exception:
        price_str = str(price)

    params: Dict[str, Any] = {
        "symbol": sym,
        "side": side_u,
        "type": otype,
        "size": qty,
        "triggerPrice": trigger_str,
        "price": price_str,
        "reduceOnly": bool(reduce_only),
        "clientOrderId": str(client_id),
    }

    if trigger_price_type:
        params["triggerPriceType"] = str(trigger_price_type)

    if expiration_sec is not None:
        params["expiration"] = int(expiration_sec)

    # Attempt multiple SDK method names
    method_names = ["create_trigger_order_v3", "createTriggerOrderV3", "create_conditional_order_v3", "createConditionalOrderV3"]
    last_err = None
    for name in method_names:
        if hasattr(client, name):
            try:
                res = _safe_call(getattr(client, name), **params)
                return {"raw": res, "client_order_id": str(client_id)}
            except Exception as e:
                last_err = e

    # Some SDKs reuse create_order_v3 for trigger orders with "timeInForce"/"triggerPrice"
    if hasattr(client, "create_order_v3"):
        try:
            res = _safe_call(getattr(client, "create_order_v3"), **params)
            return {"raw": res, "client_order_id": str(client_id)}
        except Exception as e:
            last_err = e

    raise RuntimeError(f"create_trigger_order failed (last_err={last_err})")


def cancel_order(order_id: Optional[str] = None, client_id: Optional[str] = None, symbol: Optional[str] = None) -> Dict[str, Any]:
    """Cancel by order_id or client_id (best effort)."""
    client = get_client()
    sym = str(symbol).upper().strip() if symbol else None
    last_err = None

    if order_id:
        # try cancel_order_v3
        if hasattr(client, "cancel_order_v3"):
            try:
                res = _safe_call(getattr(client, "cancel_order_v3"), orderId=str(order_id), symbol=sym)
                return {"raw": res}
            except Exception as e:
                last_err = e

    if client_id:
        if hasattr(client, "cancel_order_by_client_id_v3"):
            try:
                res = _safe_call(getattr(client, "cancel_order_by_client_id_v3"), clientOrderId=str(client_id), symbol=sym)
                return {"raw": res}
            except Exception as e:
                last_err = e

    raise RuntimeError(f"cancel_order failed (last_err={last_err})")


# -----------------------------------------------------------------------------
# Positions
# -----------------------------------------------------------------------------


def get_open_position_for_symbol(symbol: str) -> Optional[dict]:
    """Return a position dict for symbol if exists, else None."""
    client = get_client()
    sym = str(symbol).upper().strip()

    if hasattr(client, "get_positions_v3"):
        res = _safe_call(getattr(client, "get_positions_v3"))
        if isinstance(res, dict):
            data = res.get("data") if isinstance(res.get("data"), list) else res.get("data", [])
            if isinstance(data, list):
                for p in data:
                    if str(p.get("symbol", "")).upper() == sym and Decimal(str(p.get("size", "0"))) != 0:
                        return dict(p)
        elif isinstance(res, list):
            for p in res:
                if str(getattr(p, "symbol", "")).upper() == sym and Decimal(str(getattr(p, "size", "0"))) != 0:
                    return p.__dict__
    return None


# -----------------------------------------------------------------------------
# WS tracking + fill aggregation
# -----------------------------------------------------------------------------


def register_order_for_tracking(order_id: str, client_order_id: str, symbol: str, expected_qty: str) -> None:
    _ORDER_STATE[str(order_id)] = {
        "orderId": str(order_id),
        "clientOrderId": str(client_order_id),
        "symbol": str(symbol).upper(),
        "expectedQty": str(expected_qty),
        "status": "NEW",
        "cumQty": "0",
        "avgPrice": None,
        "updatedAt": time.time(),
    }


def _dedupe_key(order_id: str, fill_id: Optional[str]) -> str:
    return f"{order_id}:{fill_id or ''}"


def _dedupe_put(key: str, ttl_sec: int = 3600, max_size: int = 20000) -> bool:
    """Return True if new, False if already seen."""
    now = time.time()
    # purge expired
    while _FILL_DEDUPE:
        k, ts = next(iter(_FILL_DEDUPE.items()))
        if now - ts > ttl_sec:
            _FILL_DEDUPE.popitem(last=False)
        else:
            break

    if key in _FILL_DEDUPE:
        _FILL_DEDUPE.move_to_end(key)
        return False

    _FILL_DEDUPE[key] = now
    _FILL_DEDUPE.move_to_end(key)

    if len(_FILL_DEDUPE) > max_size:
        _FILL_DEDUPE.popitem(last=False)
    return True


def _agg_add_fill(order_id: str, price: Decimal, size: Decimal, fee: Optional[Decimal] = None) -> None:
    oid = str(order_id)
    a = _FILL_AGG.get(oid)
    if a is None:
        a = {"qty": Decimal("0"), "notional": Decimal("0"), "fee": Decimal("0")}
        _FILL_AGG[oid] = a
    a["qty"] += size
    a["notional"] += price * size
    if fee is not None:
        a["fee"] += fee


def _agg_get_avg(order_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    a = _FILL_AGG.get(str(order_id))
    if not a:
        return None, None, None
    qty = a["qty"]
    if qty <= 0:
        return None, "0", str(a.get("fee", "0"))
    avg = (a["notional"] / qty).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    return str(avg), str(qty), str(a.get("fee", "0"))


def pop_fill_event(timeout: float = 0.0) -> Optional[dict]:
    try:
        return _FILL_Q.get(timeout=timeout)
    except Exception:
        return None


def pop_order_event(timeout: float = 0.0) -> Optional[dict]:
    try:
        return _ORDER_Q.get(timeout=timeout)
    except Exception:
        return None


def _extract_iterable(obj: Any) -> Iterable[dict]:
    """Normalize payload content into iterable of dicts."""
    if obj is None:
        return []
    if isinstance(obj, dict):
        return [obj]
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    return []


def _handle_ws_message(msg: Any) -> None:
    """Parse WS msg and update caches (orders + fills)."""
    if msg is None:
        return
    # apexomni may deliver dict already or raw json string
    if isinstance(msg, str):
        try:
            import json
            msg = json.loads(msg)
        except Exception:
            return
    if not isinstance(msg, dict):
        return

    contents = msg.get("contents") or msg.get("data") or msg

    # Known shapes:
    #  - contents: { "orders": [...], "fills": [...] }
    #  - contents: { "orders": {...}, "fills": {...} }
    #  - contents: [ ... ] (rare)
    if isinstance(contents, list):
        # try to detect type by keys per item
        for it in contents:
            if not isinstance(it, dict):
                continue
            _handle_ws_message({"contents": it})
        return

    if not isinstance(contents, dict):
        return

    # Orders
    for key in ("orders", "order", "orderUpdates", "order_update"):
        if key in contents:
            for o in _extract_iterable(contents.get(key)):
                _handle_order_update(o)

    # Fills / Trades
    for key in ("fills", "fill", "trades", "trade", "fillUpdates", "tradeUpdates"):
        if key in contents:
            for f in _extract_iterable(contents.get(key)):
                _handle_fill_update(f)


def _handle_order_update(o: dict) -> None:
    try:
        oid = str(o.get("orderId") or o.get("id") or "").strip()
        if not oid:
            return
        st = str(o.get("status") or o.get("state") or "").upper()
        cum = o.get("cumQty") or o.get("cum_size") or o.get("filledSize") or o.get("filledQty") or "0"
        avg = o.get("avgPrice") or o.get("averagePrice") or o.get("avg_fill_price") or o.get("priceAvg")

        rec = _ORDER_STATE.get(oid) or {"orderId": oid}
        rec.update(
            {
                "status": st or rec.get("status"),
                "cumQty": str(cum),
                "avgPrice": str(avg) if avg is not None else rec.get("avgPrice"),
                "symbol": str(o.get("symbol") or rec.get("symbol") or "").upper(),
                "clientOrderId": str(o.get("clientOrderId") or o.get("clientId") or rec.get("clientOrderId") or ""),
                "updatedAt": time.time(),
                "raw": o,
            }
        )
        _ORDER_STATE[oid] = rec
        try:
            _ORDER_Q.put_nowait(o)
        except Exception:
            pass
    except Exception:
        return


def _handle_fill_update(f: dict) -> None:
    try:
        oid = str(f.get("orderId") or f.get("id") or "").strip()
        if not oid:
            # sometimes trade payload uses "order" nested
            od = f.get("order") if isinstance(f.get("order"), dict) else None
            if od:
                oid = str(od.get("orderId") or od.get("id") or "").strip()
        if not oid:
            return

        fill_id = str(f.get("fillId") or f.get("tradeId") or f.get("id") or "").strip() or None
        key = _dedupe_key(oid, fill_id)
        if not _dedupe_put(key):
            return

        px = f.get("price") or f.get("fillPrice") or f.get("tradePrice") or f.get("avgPrice")
        sz = f.get("size") or f.get("fillSize") or f.get("qty") or f.get("tradeSize") or f.get("filledSize")

        try:
            px_d = Decimal(str(px))
            sz_d = Decimal(str(sz))
        except Exception:
            return
        if px_d <= 0 or sz_d <= 0:
            return

        fee = f.get("fee") or f.get("commission") or f.get("tradingFee")
        fee_d = None
        if fee is not None:
            try:
                fee_d = Decimal(str(fee))
            except Exception:
                fee_d = None

        _agg_add_fill(order_id=oid, price=px_d, size=sz_d, fee=fee_d)

        try:
            _FILL_Q.put_nowait({"orderId": oid, "fillId": fill_id, "price": str(px_d), "size": str(sz_d), "fee": str(fee_d) if fee_d is not None else None, "raw": f})
        except Exception:
            pass
    except Exception:
        return


def start_private_ws() -> None:
    """Start private WS (orders + fills) in background thread (idempotent)."""
    global _WS_STARTED
    with _WS_LOCK:
        if _WS_STARTED:
            return
        _WS_STARTED = True

    if WebSocket is None:
        print("[apex_client][WS] WebSocket class not available; skipping WS.")
        return

    endpoint = _get_ws_endpoint()
    if not endpoint:
        print("[apex_client][WS] WS endpoint missing; skipping WS.")
        return

    api = _get_api_credentials()
    ws = WebSocket(endpoint)

    def _worker():
        while True:
            try:
                # login
                try:
                    _safe_call(getattr(ws, "login"), api_key=api["key"], secret=api["secret"], passphrase=api["passphrase"])
                except Exception:
                    try:
                        _safe_call(getattr(ws, "login"), **api)
                    except Exception:
                        pass

                # subscribe private streams
                # We try multiple subscribe APIs across versions.
                subscribed = False
                candidates = [
                    ("account_info_stream_v3", {"channel": "account_info_stream_v3"}),
                    ("subscribe", {"channel": "account_info_stream_v3"}),
                    ("subscribe_private", {"channel": "account_info_stream_v3"}),
                    ("subscribe_account_info_stream_v3", {}),
                ]
                for method, kwargs in candidates:
                    if hasattr(ws, method):
                        try:
                            _safe_call(getattr(ws, method), **kwargs)
                            subscribed = True
                            break
                        except Exception:
                            continue

                if subscribed:
                    print("[apex_client][WS] subscribed: account_info_stream_v3 (orders+fills)")
                else:
                    print("[apex_client][WS] subscribe failed; WS may not deliver orders/fills.")

                # message loop
                while True:
                    try:
                        # Different versions: ws.recv() / ws.receive() / ws.listen(callback)
                        if hasattr(ws, "recv"):
                            msg = ws.recv()
                            _handle_ws_message(msg)
                        elif hasattr(ws, "receive"):
                            msg = ws.receive()
                            _handle_ws_message(msg)
                        elif hasattr(ws, "listen"):
                            def _cb(m):
                                _handle_ws_message(m)
                            ws.listen(_cb)
                        else:
                            time.sleep(1.0)
                    except Exception:
                        # connection dropped -> break to reconnect
                        break

            except Exception as e:
                print(f"[apex_client][WS] loop error: {e}")

            # backoff reconnect
            time.sleep(1.0)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


# -----------------------------------------------------------------------------
# Fill summary API for app.py / pnl_store integration
# -----------------------------------------------------------------------------

def get_fill_summary(order_id: Optional[str] = None, client_order_id: Optional[str] = None, timeout_sec: float = 3.0) -> Dict[str, Any]:
    """Return best-effort fill summary for an order.

    Priority:
      1) WS fills aggregation (true execution)
      2) WS order state avg/cum (if present)
      3) REST fallback (fills/trades by orderId or order detail)
    """
    t0 = time.time()
    oid = str(order_id) if order_id else None

    # 1) If orderId provided, try agg immediately
    if oid:
        avg, qty, fee = _agg_get_avg(oid)
        if avg and qty:
            return {"source": "ws_fills", "order_id": oid, "avg_price": avg, "filled_qty": qty, "fee": fee}

    # If we only have client_order_id, map to oid by scanning state
    if not oid and client_order_id:
        cid = str(client_order_id)
        for k, v in _ORDER_STATE.items():
            if str(v.get("clientOrderId") or "") == cid:
                oid = str(k)
                break

    # 2) Wait briefly for WS fills to arrive
    while time.time() - t0 < timeout_sec:
        if oid:
            avg, qty, fee = _agg_get_avg(oid)
            if avg and qty:
                return {"source": "ws_fills", "order_id": oid, "avg_price": avg, "filled_qty": qty, "fee": fee}

        # Check order state avg/cum
        if oid and oid in _ORDER_STATE:
            st = _ORDER_STATE[oid]
            avgp = st.get("avgPrice")
            cum = st.get("cumQty")
            if avgp not in (None, "", "None") and cum not in (None, "", "None"):
                try:
                    if Decimal(str(cum)) > 0 and Decimal(str(avgp)) > 0:
                        return {"source": "ws_orders", "order_id": oid, "avg_price": str(avgp), "filled_qty": str(cum), "fee": None}
                except Exception:
                    pass

        time.sleep(0.05)

    # 3) REST fallback (best-effort)
    if oid:
        rest = _rest_fill_summary(oid)
        if rest:
            return rest

    return {"source": "none", "order_id": oid, "avg_price": None, "filled_qty": None, "fee": None}


def _rest_fill_summary(order_id: str) -> Optional[Dict[str, Any]]:
    """REST gap-fill. Tries to locate fills/trades or at least order detail."""
    client = get_client()
    oid = str(order_id)

    # Try likely fills endpoints (SDK naming differs)
    fill_methods = [
        "get_fills_v3",
        "fillsV3",
        "get_trades_v3",
        "tradesV3",
        "get_order_fills_v3",
        "getOrderFillsV3",
    ]

    def _as_list(res: Any) -> list:
        if res is None:
            return []
        if isinstance(res, dict):
            data = res.get("data")
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                # sometimes {data:{list:[...]}}
                for k in ("list", "items", "fills", "trades"):
                    v = data.get(k)
                    if isinstance(v, list):
                        return v
            return []
        if isinstance(res, list):
            return res
        return []

    for name in fill_methods:
        if hasattr(client, name):
            try:
                res = _safe_call(getattr(client, name), orderId=oid)
                items = _as_list(res)
                if items:
                    # weighted avg
                    qty = Decimal("0")
                    notional = Decimal("0")
                    fee = Decimal("0")
                    for it in items:
                        if not isinstance(it, dict):
                            continue
                        px = it.get("price") or it.get("fillPrice") or it.get("tradePrice")
                        sz = it.get("size") or it.get("fillSize") or it.get("qty") or it.get("tradeSize")
                        try:
                            px_d = Decimal(str(px))
                            sz_d = Decimal(str(sz))
                        except Exception:
                            continue
                        if px_d <= 0 or sz_d <= 0:
                            continue
                        qty += sz_d
                        notional += px_d * sz_d
                        try:
                            fee += Decimal(str(it.get("fee") or it.get("commission") or "0"))
                        except Exception:
                            pass
                    if qty > 0:
                        avg = (notional / qty).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
                        return {"source": "rest_fills", "order_id": oid, "avg_price": str(avg), "filled_qty": str(qty), "fee": str(fee)}
            except Exception:
                pass

    # Fall back to order detail if fills not available
    order_methods = ["get_order_v3", "getOrderV3", "get_order_detail_v3", "getOrderDetailV3"]
    for name in order_methods:
        if hasattr(client, name):
            try:
                res = _safe_call(getattr(client, name), orderId=oid)
                if isinstance(res, dict):
                    data = res.get("data") if isinstance(res.get("data"), dict) else res
                    avg = data.get("avgPrice") or data.get("averagePrice")
                    cum = data.get("cumQty") or data.get("filledQty") or data.get("filledSize")
                    if avg is not None and cum is not None:
                        try:
                            if Decimal(str(avg)) > 0 and Decimal(str(cum)) > 0:
                                return {"source": "rest_order", "order_id": oid, "avg_price": str(avg), "filled_qty": str(cum), "fee": None}
                        except Exception:
                            pass
            except Exception:
                pass

    return None


# -----------------------------------------------------------------------------
# REST order poller (backup path)
# -----------------------------------------------------------------------------

def start_order_rest_poller(poll_interval: float = 5.0) -> None:
    """Start a lightweight REST poller to refresh order status and gap-fill WS."""
    global _REST_POLL_STARTED
    with _REST_POLL_LOCK:
        if _REST_POLL_STARTED:
            return
        _REST_POLL_STARTED = True

    def _poll():
        while True:
            try:
                client = get_client()
                # Iterate tracked orders and refresh detail
                for oid, st in list(_ORDER_STATE.items()):
                    # only poll recent/in-flight orders
                    age = time.time() - float(st.get("updatedAt") or 0)
                    if age > 3600:
                        continue
                    try:
                        if hasattr(client, "get_order_v3"):
                            res = _safe_call(getattr(client, "get_order_v3"), orderId=str(oid))
                        else:
                            res = None
                        if isinstance(res, dict):
                            data = res.get("data") if isinstance(res.get("data"), dict) else res
                            _handle_order_update(data if isinstance(data, dict) else {})
                    except Exception:
                        pass

                    # gap-fill if order says filled but fills missing
                    try:
                        st2 = _ORDER_STATE.get(oid) or {}
                        status = str(st2.get("status") or "").upper()
                        cum = st2.get("cumQty")
                        if status in {"FILLED", "CLOSED"}:
                            avg, qty, _fee = _agg_get_avg(oid)
                            if (not qty) and (cum not in (None, "", "None")):
                                try:
                                    if Decimal(str(cum)) > 0:
                                        _rest_fill_summary(str(oid))  # will compute if possible
                                except Exception:
                                    pass
                    except Exception:
                        pass

            except Exception:
                pass
            time.sleep(max(1.0, float(poll_interval)))

    t = threading.Thread(target=_poll, daemon=True)
    t.start()
