import os
import time
import random
import inspect
import threading
import queue
from collections import OrderedDict
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, Tuple, Union, Iterable

import requests

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
    s = format_symbol(symbol)
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


def get_reference_price(symbol: str) -> Decimal:
    """Public ticker-based reference price (no auth).

    We prefer indexPrice (used by ApeX for market order bound rules), then markPrice, then lastPrice.
    The public ticker endpoint sometimes expects `crossSymbolName` without dashes (e.g., BTCUSDT),
    but we also try the dashed form as a fallback (e.g., BTC-USDT).
    """
    base_url, _ = _get_base_and_network()
    sym_dash = format_symbol(symbol)
    candidates = [sym_dash.replace("-", ""), sym_dash]

    last_err: Optional[Exception] = None
    for sym in candidates:
        try:
            url = f"{base_url}/api/v3/ticker"
            resp = requests.get(url, params={"symbol": sym}, timeout=8)
            resp.raise_for_status()
            j = resp.json()

            data = j.get("data")
            if isinstance(data, list) and data:
                item = data[0]
            elif isinstance(data, dict):
                item = data
            else:
                raise ValueError(f"unexpected ticker payload: keys={list(j.keys())}")

            for k in ("indexPrice", "markPrice", "lastPrice", "price"):
                v = item.get(k)
                if v is None or v == "":
                    continue
                return Decimal(str(v))

            raise ValueError(f"no numeric price in ticker: keys={list(item.keys())}")
        except Exception as e:
            last_err = e

    raise ValueError(f"ticker lookup failed for {sym_dash} (last_err={last_err})")

def get_market_price(symbol: str, side: str, size: str) -> str:
    """Price used only as a 'reference/worse' bound for signing market orders.

    Primary path: use public ticker index price + buffer (robust, no 'args invalid' issues).
    Fallback path: call private worst-price endpoint(s).
    """
    side_u = (side or "").upper()
    if side_u not in ("BUY", "SELL"):
        side_u = "BUY"

    try:
        ref = get_reference_price(symbol)
        buf = Decimal(os.getenv("SIG_PRICE_BUFFER_PCT", "0.02"))
        if buf < 0:
            buf = Decimal("0.02")
        price = ref * (Decimal("1") + buf) if side_u == "BUY" else ref * (Decimal("1") - buf)
        price = _snap_price(symbol, price)
        return str(price)
    except Exception as e:
        last_err = f"ticker fallback used (last_err={e})"

    # ---- Fallback: private worst-price ----
    try:
        client = get_client()
        methods = [
            getattr(client, "get_worst_price_v3", None),
            getattr(client, "get_worst_price", None),
            getattr(client, "getWorstPrice", None),
        ]
        for fn in methods:
            if not fn:
                continue
            try:
                r = fn(symbol=format_symbol(symbol), side=side_u, size=str(size))
                if isinstance(r, dict):
                    data = r.get("data") or r
                    for k in ("price", "worstPrice", "worst_price"):
                        if k in data and data[k]:
                            p = _snap_price(symbol, Decimal(str(data[k])))
                            return str(p)
                    last_err = f"no numeric price in response for {getattr(fn,'__name__','fn')}: {list(r.keys())}"
                else:
                    p = _snap_price(symbol, Decimal(str(r)))
                    return str(p)
            except Exception as ie:
                last_err = str(ie)
    except Exception as ce:
        last_err = str(ce)

    print(f"[apex_client] get_market_price fallback used (last_err={last_err})")
    return "0"

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
    sym = format_symbol(symbol)
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


# -----------------------------------------------------------------------------
# Positions
# -----------------------------------------------------------------------------


def create_trigger_order(
    symbol: str,
    side: str,
    qty: str,
    trigger_price: str,
    reduce_only: bool = True,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create an exchange-native Stop-Market (Trigger-Market) order.

    Notes:
    - For STOP_MARKET orders, ApeX still requires a `price` field for protection.
      We use `get_market_price()` as a best-effort bound; if that fails, we fall back
      to the trigger price itself.
    - `side` should be the execution side (SELL closes a LONG, BUY closes a SHORT).
    """
    client = get_client()
    sym = format_symbol(symbol)
    side_u = str(side).upper().strip()

    # Best-effort protective price (market-protection). If unavailable, use trigger price.
    try:
        protective_price = get_market_price(sym, side_u, qty)
    except Exception:
        protective_price = str(trigger_price)

    payload = {
        "symbol": sym,
        "side": side_u,
        "type": "STOP_MARKET",
        "size": str(qty),
        "price": str(protective_price),
        "triggerPrice": str(trigger_price),
        "reduceOnly": bool(reduce_only),
    }
    if client_order_id:
        payload["clientOrderId"] = str(client_order_id)

    register_order_for_tracking(payload.get("clientOrderId"), sym)
    return _safe_call(client.create_order_v3, **payload)

def get_open_position_for_symbol(symbol: str) -> Dict[str, Any]:
    client = get_client()
    sym = format_symbol(symbol)

    methods = [
        "get_positions_v3",
        "get_open_positions_v3",
        "positions_v3",
        "open_positions_v3",
    ]
    for name in methods:
        if not hasattr(client, name):
            continue
        try:
            res = _safe_call(getattr(client, name))
            data = res
            if isinstance(res, dict):
                data = res.get("data") if res.get("data") is not None else (res.get("positions") or res.get("list") or res)
            if isinstance(data, dict) and "positions" in data:
                data = data["positions"]
            if isinstance(data, list):
                for p in data:
                    if not isinstance(p, dict):
                        continue
                    if str(p.get("symbol") or p.get("market") or "").upper().strip() == sym:
                        return p
        except Exception:
            continue
    return {}


# -----------------------------------------------------------------------------
# WS parsing: orders + fills
# -----------------------------------------------------------------------------


def _walk_collect(root: Any, predicate) -> Iterable[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    stack = [root]
    while stack:
        x = stack.pop()
        if isinstance(x, dict):
            if predicate(x):
                out.append(x)
            for v in x.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(x, list):
            for v in x:
                if isinstance(v, (dict, list)):
                    stack.append(v)
    return out


def _pick(d: Dict[str, Any], *keys) -> Any:
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return None


def _to_dec(x: Any) -> Optional[Decimal]:
    if x is None:
        return None
    if isinstance(x, str) and x.strip() == "":
        return None
    try:
        return Decimal(str(x))
    except Exception:
        return None


def _parse_fill(x: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    d = x.get("data") if isinstance(x.get("data"), dict) else x
    if not isinstance(d, dict):
        return None

    order_id = _pick(d, "orderId", "order_id", "id")
    if order_id is None:
        return None

    fill_id = _pick(d, "fillId", "tradeId", "matchFillId", "id")
    price = _to_dec(_pick(d, "fillPrice", "matchFillPrice", "price", "tradePrice"))
    size = _to_dec(_pick(d, "fillSize", "matchFillSize", "size", "qty", "filledSize"))
    if price is None or size is None or price <= 0 or size <= 0:
        return None

    symbol = _pick(d, "symbol", "market")
    client_oid = _pick(d, "clientOrderId", "clientId", "client_id")
    fee = _to_dec(_pick(d, "fee", "commission", "tradeFee"))
    ts = _pick(d, "ts", "timestamp", "createdAt", "time")
    try:
        ts_f = float(ts) if ts is not None else time.time()
        # Some APIs use ms
        if ts_f > 10_000_000_000:
            ts_f = ts_f / 1000.0
    except Exception:
        ts_f = time.time()

    return {
        "order_id": str(order_id),
        "fill_id": str(fill_id) if fill_id is not None else f"{order_id}:{price}:{size}:{ts_f}",
        "symbol": format_symbol(symbol) if symbol is not None else None,
        "client_order_id": str(client_oid) if client_oid is not None else None,
        "price": price,
        "qty": size,
        "fee": fee,
        "ts": ts_f,
        "raw": d,
    }


def _parse_order_update(x: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    d = x.get("data") if isinstance(x.get("data"), dict) else x
    if not isinstance(d, dict):
        return None
    order_id = _pick(d, "orderId", "id")
    if order_id is None:
        return None

    symbol = _pick(d, "symbol", "market")
    client_oid = _pick(d, "clientOrderId", "clientId")
    status = str(_pick(d, "status", "orderStatus", "state") or "").upper().strip()
    cum_qty = _to_dec(_pick(
        d,
        "cumFilledSize", "filledSize", "sizeFilled", "executedQty",
        "cumSuccessFillSize", "cumMatchFillSize",
    ))
    avg_px = _to_dec(_pick(d, "avgPrice", "averagePrice", "fillAvgPrice", "avgFillPrice"))
    cum_val = _to_dec(_pick(d, "cumSuccessFillValue", "cumMatchFillValue", "cumFilledValue", "filledValue"))

    return {
        "order_id": str(order_id),
        "symbol": format_symbol(symbol) if symbol is not None else None,
        "client_order_id": str(client_oid) if client_oid is not None else None,
        "status": status,
        "cum_qty": cum_qty,
        "avg_px": avg_px,
        "cum_val": cum_val,
        "raw": d,
        "ts": time.time(),
    }


def _dedupe_key(order_id: str, fill_id: str) -> str:
    return f"{order_id}:{fill_id}"


def _dedupe_add(key: str, ts: float) -> bool:
    """Return True if new; False if duplicate."""
    ttl = float(os.getenv("FILL_DEDUPE_TTL_SEC", "3600"))
    max_items = int(os.getenv("FILL_DEDUPE_MAX", "50000"))

    # prune old
    while _FILL_DEDUPE:
        k0, t0 = next(iter(_FILL_DEDUPE.items()))
        if ts - t0 <= ttl and len(_FILL_DEDUPE) <= max_items:
            break
        _FILL_DEDUPE.popitem(last=False)

    if key in _FILL_DEDUPE:
        return False
    _FILL_DEDUPE[key] = ts
    return True


def _apply_fill(fill: Dict[str, Any]) -> None:
    oid = fill["order_id"]
    qty = Decimal(str(fill["qty"]))
    px = Decimal(str(fill["price"]))
    notional = qty * px
    now = float(fill.get("ts") or time.time())

    agg = _FILL_AGG.get(oid)
    if agg is None:
        agg = {
            "order_id": oid,
            "symbol": fill.get("symbol"),
            "client_order_id": fill.get("client_order_id"),
            "qty": Decimal("0"),
            "notional": Decimal("0"),
            "fee": Decimal("0"),
            "ts": now,
            "source": "ws_fills",
        }
        _FILL_AGG[oid] = agg

    agg["qty"] = Decimal(str(agg["qty"])) + qty
    agg["notional"] = Decimal(str(agg["notional"])) + notional
    if fill.get("fee") is not None:
        try:
            agg["fee"] = Decimal(str(agg["fee"])) + Decimal(str(fill["fee"]))
        except Exception:
            pass
    agg["ts"] = now
    if fill.get("symbol"):
        agg["symbol"] = fill.get("symbol")
    if fill.get("client_order_id"):
        agg["client_order_id"] = fill.get("client_order_id")

    # optional event queue
    try:
        _FILL_Q.put_nowait({"type": "fill", **fill})
    except Exception:
        pass


def register_order_for_tracking(
    order_id: str,
    client_order_id: Optional[str] = None,
    symbol: Optional[str] = None,
    expected_qty: Optional[str] = None,
    status: str = "PENDING",
) -> None:
    if not order_id:
        return
    st = _ORDER_STATE.get(str(order_id))
    if st is None:
        st = {
            "order_id": str(order_id),
            "client_order_id": client_order_id,
            "symbol": format_symbol(symbol) if symbol else None,
            "status": str(status).upper().strip(),
            "cum_qty": Decimal("0"),
            "avg_px": None,
            "expected_qty": _to_dec(expected_qty) if expected_qty is not None else None,
            "ts": time.time(),
        }
        _ORDER_STATE[str(order_id)] = st
    else:
        if client_order_id:
            st["client_order_id"] = client_order_id
        if symbol:
            st["symbol"] = format_symbol(symbol)
        if status:
            st["status"] = str(status).upper().strip()
        if expected_qty is not None:
            st["expected_qty"] = _to_dec(expected_qty)
        st["ts"] = time.time()


def start_private_ws() -> None:
    """Idempotent. Subscribe to private stream and parse BOTH orders and fills."""
    global _WS_STARTED
    with _WS_LOCK:
        if _WS_STARTED:
            return
        _WS_STARTED = True

    if WebSocket is None:
        print("[apex_client][WS] apexomni websocket_api unavailable; WS disabled")
        return

    endpoint = _get_ws_endpoint()
    if not endpoint:
        print("[apex_client][WS] WS endpoint unavailable; WS disabled")
        return

    api_creds = _get_api_credentials()

    def handle_account(message: Dict[str, Any]):
        try:
            if not isinstance(message, dict):
                return
            contents = message.get("contents")
            if not isinstance(contents, dict):
                return

            # ----- fills -----
            def is_fill(d: Dict[str, Any]) -> bool:
                # heuristics: must have orderId + (price) + (size)
                if not any(k in d for k in ("orderId", "order_id")):
                    return False
                if not any(k in d for k in ("fillPrice", "matchFillPrice", "price", "tradePrice")):
                    return False
                if not any(k in d for k in ("fillSize", "matchFillSize", "size", "qty", "filledSize")):
                    return False
                return True

            fill_candidates = []
            for k in ("fills", "trades", "matchFills", "fillsV3", "trade", "fill"):
                if k in contents:
                    fill_candidates.append(contents.get(k))
            if not fill_candidates:
                fill_candidates = [contents]

            for root in fill_candidates:
                for raw in _walk_collect(root, is_fill):
                    fill = _parse_fill(raw)
                    if not fill:
                        continue
                    key = _dedupe_key(fill["order_id"], fill["fill_id"])
                    if not _dedupe_add(key, float(fill["ts"])):
                        continue
                    _apply_fill(fill)

            # ----- orders -----
            def is_order(d: Dict[str, Any]) -> bool:
                return any(k in d for k in ("orderId", "clientOrderId", "status", "cumFilledSize", "avgPrice"))

            order_candidates = []
            for k in ("orders", "order", "orderUpdates", "ordersV3"):
                if k in contents:
                    order_candidates.append(contents.get(k))
            if not order_candidates:
                order_candidates = [contents]

            for root in order_candidates:
                for raw in _walk_collect(root, is_order):
                    upd = _parse_order_update(raw)
                    if not upd:
                        continue
                    oid = upd["order_id"]
                    st = _ORDER_STATE.get(oid) or {"order_id": oid}
                    st.update({
                        "client_order_id": upd.get("client_order_id") or st.get("client_order_id"),
                        "symbol": upd.get("symbol") or st.get("symbol"),
                        "status": upd.get("status") or st.get("status"),
                        "ts": time.time(),
                    })

                    # cumulative fields (best effort)
                    if upd.get("cum_qty") is not None:
                        st["cum_qty"] = Decimal(str(upd["cum_qty"]))
                    if upd.get("avg_px") is not None:
                        st["avg_px"] = Decimal(str(upd["avg_px"]))
                    if upd.get("expected_qty") is not None:
                        st["expected_qty"] = upd.get("expected_qty")
                    _ORDER_STATE[oid] = st

                    # If we have cumQty+avg but no fills, we can backfill agg (lower priority)
                    agg = _FILL_AGG.get(oid)
                    if (agg is None or Decimal(str(agg.get("qty") or "0")) <= 0) and st.get("cum_qty") and st.get("avg_px"):
                        cum_qty = Decimal(str(st.get("cum_qty") or "0"))
                        avg_px = Decimal(str(st.get("avg_px") or "0"))
                        if cum_qty > 0 and avg_px > 0:
                            _FILL_AGG[oid] = {
                                "order_id": oid,
                                "symbol": st.get("symbol"),
                                "client_order_id": st.get("client_order_id"),
                                "qty": cum_qty,
                                "notional": cum_qty * avg_px,
                                "fee": Decimal("0"),
                                "ts": time.time(),
                                "source": "ws_orders",
                            }

        except Exception as e:
            print("[apex_client][WS] handle_account error:", e)

    def _run_forever():
        backoff = 1.0
        while True:
            try:
                ws = WebSocket(endpoint=endpoint, api_key_credentials=api_creds)
                ws.account_info_stream_v3(handle_account)
                print("[apex_client][WS] subscribed: account_info_stream_v3 (orders+fills)")
                backoff = 1.0
                while True:
                    try:
                        if hasattr(ws, "ping"):
                            ws.ping()
                    except Exception:
                        raise
                    time.sleep(15)
            except Exception as e:
                print(f"[apex_client][WS] reconnect: {e} (sleep {backoff}s)")
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    threading.Thread(target=_run_forever, daemon=True, name="apex-private-ws").start()


def pop_fill_event(timeout: float = 0.5) -> Optional[dict]:
    try:
        return _FILL_Q.get(timeout=timeout)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# REST fallback: orders + fills/trades
# -----------------------------------------------------------------------------


def _rest_fetch_order(order_id: str) -> Optional[Dict[str, Any]]:
    client = get_client()
    if not order_id:
        return None
    for name in ("get_order_v3", "get_order_detail_v3", "get_order_by_id_v3", "getOrderV3", "getOrderDetailV3"):
        if not hasattr(client, name):
            continue
        fn = getattr(client, name)
        try:
            res = _safe_call(fn, orderId=str(order_id))
            return res if isinstance(res, dict) else {"data": res}
        except Exception:
            try:
                res = _safe_call(fn, id=str(order_id))
                return res if isinstance(res, dict) else {"data": res}
            except Exception:
                continue
    return None


def _rest_fetch_fills_by_order(order_id: str, symbol: Optional[str] = None) -> Optional[list]:
    """Best-effort REST fills/trades lookup by orderId across SDK versions."""
    client = get_client()
    oid = str(order_id)

    method_names = [
        "get_fills_v3",
        "fills_v3",
        "get_user_fills_v3",
        "get_trades_v3",
        "get_user_trades_v3",
        "trade_history_v3",
        "get_trade_history_v3",
        "get_fill_history_v3",
        "fill_history_v3",
    ]

    for name in method_names:
        if not hasattr(client, name):
            continue
        fn = getattr(client, name)
        try:
            res = _safe_call(fn, orderId=oid, symbol=(symbol or None))
        except Exception:
            try:
                res = _safe_call(fn, order_id=oid, symbol=(symbol or None))
            except Exception:
                continue

        if res is None:
            continue

        data = res
        if isinstance(res, dict):
            data = res.get("data") if res.get("data") is not None else (res.get("list") or res.get("fills") or res.get("trades") or res)
        if isinstance(data, dict) and "list" in data:
            data = data["list"]
        if isinstance(data, list):
            return data

    return None


def start_order_rest_poller(poll_interval: float = 5.0) -> None:
    """Background REST reconciliation to fill WS gaps (orders+fills)."""
    global _REST_POLL_STARTED
    with _REST_POLL_LOCK:
        if _REST_POLL_STARTED:
            return
        _REST_POLL_STARTED = True

    def _loop():
        gap_sec = float(os.getenv("REST_GAP_SEC", "3"))
        while True:
            try:
                now = time.time()
                # only poll recently-touched orders
                order_ids = list(_ORDER_STATE.keys())
                for oid in order_ids:
                    st = _ORDER_STATE.get(oid) or {}
                    if now - float(st.get("ts") or 0) < gap_sec:
                        continue

                    # If we already have fills and order is terminal, no need.
                    agg = _FILL_AGG.get(oid)
                    if agg and Decimal(str(agg.get("qty") or "0")) > 0 and str(st.get("status") or "").upper() in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}:
                        continue

                    # 1) refresh order detail
                    od = _rest_fetch_order(oid)
                    if isinstance(od, dict):
                        d = od.get("data") if isinstance(od.get("data"), dict) else od
                        if isinstance(d, dict):
                            upd = _parse_order_update(d)
                            if upd:
                                st.update({
                                    "status": upd.get("status") or st.get("status"),
                                    "symbol": upd.get("symbol") or st.get("symbol"),
                                    "client_order_id": upd.get("client_order_id") or st.get("client_order_id"),
                                    "ts": time.time(),
                                })
                                if upd.get("cum_qty") is not None:
                                    st["cum_qty"] = Decimal(str(upd.get("cum_qty")))
                                if upd.get("avg_px") is not None:
                                    st["avg_px"] = Decimal(str(upd.get("avg_px")))
                                _ORDER_STATE[oid] = st

                    # 2) fills fallback (truth source)
                    agg_qty = Decimal(str((agg or {}).get("qty") or "0"))
                    need_fills = (agg is None) or (agg_qty <= 0)
                    if need_fills:
                        fills = _rest_fetch_fills_by_order(oid, symbol=st.get("symbol"))
                        if isinstance(fills, list) and fills:
                            for raw in fills:
                                if not isinstance(raw, dict):
                                    continue
                                fill = _parse_fill(raw)
                                if not fill:
                                    continue
                                key = _dedupe_key(fill["order_id"], fill["fill_id"])
                                if not _dedupe_add(key, float(fill["ts"])):
                                    continue
                                fill["ts"] = float(fill.get("ts") or time.time())
                                fill["raw_source"] = "rest"
                                _apply_fill(fill)

                    # If REST still can't get fills, but order has cum+avg, backfill.
                    agg2 = _FILL_AGG.get(oid)
                    if (agg2 is None or Decimal(str(agg2.get("qty") or "0")) <= 0) and st.get("cum_qty") and st.get("avg_px"):
                        cum_qty = Decimal(str(st.get("cum_qty") or "0"))
                        avg_px = Decimal(str(st.get("avg_px") or "0"))
                        if cum_qty > 0 and avg_px > 0:
                            _FILL_AGG[oid] = {
                                "order_id": oid,
                                "symbol": st.get("symbol"),
                                "client_order_id": st.get("client_order_id"),
                                "qty": cum_qty,
                                "notional": cum_qty * avg_px,
                                "fee": Decimal("0"),
                                "ts": time.time(),
                                "source": "rest_order",
                            }

            except Exception as e:
                print("[apex_client][REST] poller error:", e)

            time.sleep(max(0.5, poll_interval))

    threading.Thread(target=_loop, daemon=True, name="apex-rest-poller").start()


# -----------------------------------------------------------------------------
# Public fill summary API used by app.py
# -----------------------------------------------------------------------------


def _agg_summary(order_id: Optional[str], client_order_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if order_id:
        agg = _FILL_AGG.get(str(order_id))
        if agg:
            qty = Decimal(str(agg.get("qty") or "0"))
            notional = Decimal(str(agg.get("notional") or "0"))
            if qty > 0 and notional > 0:
                return {
                    "order_id": str(order_id),
                    "client_order_id": agg.get("client_order_id") or client_order_id,
                    "filled_qty": qty,
                    "avg_fill_price": (notional / qty),
                    "fee": agg.get("fee"),
                    "source": agg.get("source"),
                }
    # clientOrderId fallback: scan a small subset (best effort)
    if client_order_id:
        cid = str(client_order_id)
        for oid, agg in list(_FILL_AGG.items())[-200:]:
            if str(agg.get("client_order_id") or "") == cid:
                qty = Decimal(str(agg.get("qty") or "0"))
                notional = Decimal(str(agg.get("notional") or "0"))
                if qty > 0 and notional > 0:
                    return {
                        "order_id": oid,
                        "client_order_id": cid,
                        "filled_qty": qty,
                        "avg_fill_price": (notional / qty),
                        "fee": agg.get("fee"),
                        "source": agg.get("source"),
                    }
    return None


def get_fill_summary(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    max_wait_sec: float = 25.0,
    poll_interval: float = 0.25,
) -> Dict[str, Any]:
    """Return authoritative fill summary.

    Priority:
      1) WS fills aggregation
      2) WS orders (cum+avg) (backup)
      3) REST fills/trades by orderId (gap fill)
      4) REST order detail (last resort)
    """
    start_private_ws()
    if _env_bool("ENABLE_REST_POLL", True):
        # Ensure poller is running; it's idempotent.
        try:
            start_order_rest_poller(poll_interval=float(os.getenv("REST_ORDER_POLL_INTERVAL", "5.0")))
        except Exception:
            pass

    t0 = time.time()
    last_source = None
    while True:
        summ = _agg_summary(order_id, client_order_id)
        if summ:
            last_source = summ.get("source")
            # If order is terminal or we've waited enough, return.
            st = _ORDER_STATE.get(str(summ.get("order_id"))) if summ.get("order_id") else None
            status = str((st or {}).get("status") or "").upper()
            if status in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}:
                return {
                    "symbol": format_symbol(symbol),
                    "order_id": str(summ["order_id"]),
                    "client_order_id": summ.get("client_order_id"),
                    "filled_qty": str(summ["filled_qty"]),
                    "avg_fill_price": str(summ["avg_fill_price"]),
                    "fee": str(summ.get("fee")) if summ.get("fee") is not None else None,
                    "source": str(summ.get("source") or ""),
                }
            # Non-terminal: still allow return if qty>0 and waited enough.
            if time.time() - t0 >= max_wait_sec:
                return {
                    "symbol": format_symbol(symbol),
                    "order_id": str(summ["order_id"]),
                    "client_order_id": summ.get("client_order_id"),
                    "filled_qty": str(summ["filled_qty"]),
                    "avg_fill_price": str(summ["avg_fill_price"]),
                    "fee": str(summ.get("fee")) if summ.get("fee") is not None else None,
                    "source": str(summ.get("source") or ""),
                }

        if time.time() - t0 >= max_wait_sec:
            break
        time.sleep(max(0.05, poll_interval))

    # Last resort: try REST order detail -> cum+avg
    if order_id:
        od = _rest_fetch_order(str(order_id))
        if isinstance(od, dict):
            d = od.get("data") if isinstance(od.get("data"), dict) else od
            if isinstance(d, dict):
                upd = _parse_order_update(d)
                if upd and upd.get("cum_qty") and upd.get("avg_px") and Decimal(str(upd["cum_qty"])) > 0 and Decimal(str(upd["avg_px"])) > 0:
                    return {
                        "symbol": format_symbol(symbol),
                        "order_id": str(order_id),
                        "client_order_id": client_order_id,
                        "filled_qty": str(upd["cum_qty"]),
                        "avg_fill_price": str(upd["avg_px"]),
                        "fee": None,
                        "source": "rest_order_last",
                    }

    raise RuntimeError(f"fill_summary timeout; last_source={last_source}")# ──────────────────────────────────────────────────────────────
# SYMBOL NORMALIZATION
# ──────────────────────────────────────────────────────────────

def format_symbol(symbol: str) -> str:
    """Normalize a contract symbol to ApeX private API format (e.g., 'BTC-USDT')."""
    s = str(symbol).upper().replace("/", "-").replace("_", "-").strip()
    if "-" in s:
        return s
    # Best-effort split for common quotes (fallback to raw if unknown)
    for quote in ("USDT", "USDC", "USD", "BTC", "ETH"):
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[:-len(quote)]}-{quote}"
    return s

def format_symbol_for_ticker(symbol: str) -> str:
    """Normalize to public ticker crossSymbolName (e.g., 'BTCUSDT')."""
    return re.sub(r"[^A-Z0-9]", "", format_symbol(symbol))


