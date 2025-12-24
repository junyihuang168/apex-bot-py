import os
import time
import random
import inspect
import threading
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, Optional, Tuple, List

import requests

from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_MAIN,
    NETWORKID_TEST,
)

# -----------------------------------------------------------------------------
# Notes
# -----------------------------------------------------------------------------
# This client module is designed to work across multiple apexomni SDK versions.
# It uses "best effort" method discovery + robust fallbacks.
#
# Plan-B change (Dec 2025):
# - DO NOT call /api/v1/get-worst-price (it can 409 and return non-JSON).
# - For sizing + MARKET price bound:
#     reference price priority: markPrice -> oraclePrice -> indexPrice -> last.
#     MARKET bound = ref_price +/- buffer_pct (env APEX_MARKET_PRICE_BUFFER_PCT, default 1.0).
# - Real execution price must still come from fills (WS + REST fallback) in app/pnl logic.
# -----------------------------------------------------------------------------


NumberLike = Any


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _sleep_jitter(base: float = 0.10, jitter: float = 0.10):
    time.sleep(base + random.random() * jitter)


def _random_client_id(prefix: str = "tv"):
    return f"{prefix}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"


def _to_decimal(x: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
    if x is None:
        return default
    try:
        return Decimal(str(x))
    except Exception:
        return default


def _safe_call(fn, *args, **kwargs):
    """Call SDK function robustly even if param names differ."""
    try:
        sig = inspect.signature(fn)
        accepted = set(sig.parameters.keys())
        filtered = {k: v for k, v in kwargs.items() if k in accepted}
        return fn(*args, **filtered)
    except Exception:
        # If signature fails or filtering breaks, try raw call
        return fn(*args, **kwargs)


def _get_base_and_network():
    is_main = str(os.getenv("APEX_IS_MAINNET", "1")).strip() in ("1", "true", "True", "YES", "yes")
    if is_main:
        return APEX_OMNI_HTTP_MAIN, NETWORKID_MAIN
    return APEX_OMNI_HTTP_TEST, NETWORKID_TEST


# -----------------------------------------------------------------------------
# Client factory
# -----------------------------------------------------------------------------
_CLIENT = None
_CLIENT_LOCK = threading.Lock()


def get_client():
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    with _CLIENT_LOCK:
        if _CLIENT is not None:
            return _CLIENT

        # apexomni client needs seeds / keys in env
        from apexomni.client import ApexClient  # type: ignore

        base_url, network_id = _get_base_and_network()

        # Seeds: may be empty; Apex will fill defaults in some cases.
        apex_zk_seeds = os.getenv("APEX_ZK_SEEDS", "").strip()
        apex_l2key_seeds = os.getenv("APEX_L2KEY_SEEDS", "").strip()
        l2key_seeds = os.getenv("L2KEY_SEEDS", "").strip()

        # Required identity
        stark_key = os.getenv("APEX_STARK_KEY", "").strip()
        stark_key_y = os.getenv("APEX_STARK_KEY_Y", "").strip()
        l2_key = os.getenv("APEX_L2_KEY", "").strip()
        l2_key_y = os.getenv("APEX_L2_KEY_Y", "").strip()
        user_addr = os.getenv("APEX_USER_ADDRESS", "").strip()

        # API key / passphrase (if your SDK requires it)
        api_key = os.getenv("APEX_API_KEY", "").strip()
        passphrase = os.getenv("APEX_PASSPHRASE", "").strip()

        _CLIENT = ApexClient(
            base_url=base_url,
            network_id=network_id,
            stark_key=stark_key,
            stark_key_y=stark_key_y,
            l2_key=l2_key,
            l2_key_y=l2_key_y,
            user_addr=user_addr,
            api_key=api_key,
            passphrase=passphrase,
            apex_zk_seeds=apex_zk_seeds,
            apex_l2key_seeds=apex_l2key_seeds,
            l2key_seeds=l2key_seeds,
        )
        return _CLIENT


# -----------------------------------------------------------------------------
# Symbol rules
# -----------------------------------------------------------------------------
# NOTE: You can extend this dict for hard-coded per-symbol rules if needed.
SYMBOL_RULES: Dict[str, Dict[str, Any]] = {}


def _get_symbol_rules(symbol: str) -> Dict[str, Any]:
    sym = str(symbol).upper().strip()

    # Hard-coded overrides
    if sym in SYMBOL_RULES:
        return SYMBOL_RULES[sym]

    # Reasonable defaults; can be overridden via env or SYMBOL_RULES dict
    return {
        "min_qty": Decimal(os.getenv("APEX_MIN_QTY_DEFAULT", "0.01")),
        "qty_step": Decimal(os.getenv("APEX_QTY_STEP_DEFAULT", "0.01")),
        "price_tick": Decimal(os.getenv("APEX_PRICE_TICK_DEFAULT", "0.01")),
    }


def _snap_quantity(symbol: str, qty: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    step = Decimal(str(rules["qty_step"]))
    if step <= 0:
        return qty
    return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step


def _snap_price(symbol: str, price: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    tick = Decimal(str(rules["price_tick"]))
    if tick <= 0:
        return price
    return (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick


# -----------------------------------------------------------------------------
# Positions / fills (REST)
# -----------------------------------------------------------------------------
def get_open_position_for_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    """Return a position dict if open, else None (best effort)."""
    client = get_client()
    sym = str(symbol).upper().strip()

    methods = ["get_positions_v3", "getPositionsV3", "get_positions", "getPositions"]
    last_err = None
    for name in methods:
        if not hasattr(client, name):
            continue
        fn = getattr(client, name)
        try:
            res = _safe_call(fn)
            if isinstance(res, dict):
                data = res.get("data") if res.get("data") is not None else res
                if isinstance(data, list):
                    for p in data:
                        if isinstance(p, dict) and str(p.get("symbol", "")).upper() == sym:
                            # some SDK returns only open positions, some returns all
                            size = _to_decimal(p.get("size") or p.get("positionSize") or p.get("qty"), Decimal("0"))
                            if size and size != 0:
                                return p
                if isinstance(data, dict):
                    # single position response
                    if str(data.get("symbol", "")).upper() == sym:
                        return data
            # If res is list
            if isinstance(res, list):
                for p in res:
                    if isinstance(p, dict) and str(p.get("symbol", "")).upper() == sym:
                        return p
        except Exception as e:
            last_err = e

    # last resort: None
    if last_err:
        pass
    return None


def get_fill_summary(symbol: str, client_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Best-effort fill summary; used by app's REST fallback."""
    client = get_client()
    sym = str(symbol).upper().strip()

    methods = ["get_fills_v3", "getFillsV3", "get_fills", "getFills", "fillsV3"]
    for name in methods:
        if not hasattr(client, name):
            continue
        fn = getattr(client, name)
        try:
            kwargs = {"symbol": sym}
            if client_id:
                kwargs["clientOrderId"] = str(client_id)
            res = _safe_call(fn, **kwargs)
            return res if isinstance(res, dict) else {"data": res}
        except Exception:
            continue
    return None


# -----------------------------------------------------------------------------
# Plan-B: Reference price (NO /get-worst-price)
# -----------------------------------------------------------------------------
def _parse_decimal_maybe(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        if not s:
            return None
        d = Decimal(s)
        return d if d > 0 else None
    except Exception:
        return None


def get_reference_price(symbol: str) -> Optional[Decimal]:
    """Return a stable reference price for sizing and MARKET price-bound.

    Priority (default):
      1) markPrice
      2) oraclePrice
      3) indexPrice
      4) last / ticker price

    This intentionally avoids the /get-worst-price endpoint (can 409 / non-JSON).
    """
    sym = str(symbol).upper().strip()

    # 1) If we already have a position object, it may contain mark/oracle/index.
    try:
        pos = get_open_position_for_symbol(sym)
        if isinstance(pos, dict):
            for k in ("markPrice", "oraclePrice", "indexPrice", "lastPrice", "price"):
                d = _parse_decimal_maybe(pos.get(k))
                if d:
                    return d
    except Exception:
        pass

    client = get_client()

    # 2) Try SDK market/ticker methods (names differ by version).
    sdk_methods = [
        ("get_ticker_v3", {"symbol": sym}),
        ("tickerV3", {"symbol": sym}),
        ("get_ticker", {"symbol": sym}),
        ("ticker", {"symbol": sym}),
        ("get_markets_v3", {}),
        ("marketsV3", {}),
        ("get_mark_price_v3", {"symbol": sym}),
        ("getMarkPriceV3", {"symbol": sym}),
        ("get_index_price_v3", {"symbol": sym}),
        ("getIndexPriceV3", {"symbol": sym}),
        ("get_oracle_price_v3", {"symbol": sym}),
        ("getOraclePriceV3", {"symbol": sym}),
    ]
    for name, kwargs in sdk_methods:
        if not hasattr(client, name):
            continue
        try:
            res = _safe_call(getattr(client, name), **kwargs)
            # Normalize typical shapes
            if isinstance(res, dict):
                data = res.get("data") if res.get("data") is not None else res
                # If the method returns a list of markets, pick matching symbol
                if isinstance(data, list):
                    for it in data:
                        if isinstance(it, dict) and str(it.get("symbol", "")).upper() == sym:
                            for k in ("markPrice", "oraclePrice", "indexPrice", "lastPrice", "price", "close"):
                                d = _parse_decimal_maybe(it.get(k))
                                if d:
                                    return d
                if isinstance(data, dict):
                    for k in ("markPrice", "oraclePrice", "indexPrice", "lastPrice", "price", "close"):
                        d = _parse_decimal_maybe(data.get(k))
                        if d:
                            return d
            else:
                d = _parse_decimal_maybe(res)
                if d:
                    return d
        except Exception:
            continue

    # 3) Raw REST fallback against Omni base URL (best-effort).
    # We try a small set of common endpoints; whichever returns JSON with a usable field wins.
    try:
        base_url, _network = _get_base_and_network()
        endpoints = [
            ("/api/v1/ticker", {"symbol": sym}),
            ("/api/v1/get-ticker", {"symbol": sym}),
            ("/api/v1/markets", {"symbol": sym}),
            ("/api/v1/market", {"symbol": sym}),
            ("/api/v1/mark-price", {"symbol": sym}),
            ("/api/v1/index-price", {"symbol": sym}),
            ("/api/v1/oracle-price", {"symbol": sym}),
        ]
        for path, params in endpoints:
            try:
                r = requests.get(base_url + path, params=params, timeout=5)
                if r.status_code != 200:
                    continue
                j = r.json()
                data = j.get("data") if isinstance(j, dict) and j.get("data") is not None else j
                # If list, pick symbol
                if isinstance(data, list):
                    for it in data:
                        if isinstance(it, dict) and str(it.get("symbol", "")).upper() == sym:
                            for k in ("markPrice", "oraclePrice", "indexPrice", "lastPrice", "price", "close"):
                                d = _parse_decimal_maybe(it.get(k))
                                if d:
                                    return d
                if isinstance(data, dict):
                    for k in ("markPrice", "oraclePrice", "indexPrice", "lastPrice", "price", "close"):
                        d = _parse_decimal_maybe(data.get(k))
                        if d:
                            return d
            except Exception:
                continue
    except Exception:
        pass

    return None


def get_market_price(symbol: str, side: str, size: NumberLike) -> str:
    """Return a numeric price bound (string) for MARKET signature.

    We DO NOT call /get-worst-price. Instead we compute a conservative bound from
    reference price + buffer.

    Env:
      APEX_MARKET_PRICE_BUFFER_PCT (default 1.0)
        BUY  -> ref * (1 + buffer%)
        SELL -> ref * (1 - buffer%)
    """
    sym = str(symbol).upper().strip()
    side_u = str(side).upper().strip()

    ref = get_reference_price(sym)
    if ref is None or ref <= 0:
        return "0"

    try:
        buf_pct = Decimal(str(os.getenv("APEX_MARKET_PRICE_BUFFER_PCT", "1.0")))
    except Exception:
        buf_pct = Decimal("1.0")

    if buf_pct < 0:
        buf_pct = Decimal("0")

    if side_u == "BUY":
        px = ref * (Decimal("1") + (buf_pct / Decimal("100")))
    else:
        px = ref * (Decimal("1") - (buf_pct / Decimal("100")))

    try:
        px = _snap_price(sym, px)
    except Exception:
        pass

    return str(px)


# -----------------------------------------------------------------------------
# Order placement
# -----------------------------------------------------------------------------
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

    methods = ["create_order_v3", "createOrderV3", "create_order", "createOrder"]
    last_err = None
    for name in methods:
        if not hasattr(client, name):
            continue
        fn = getattr(client, name)
        try:
            res = _safe_call(
                fn,
                symbol=sym,
                side=side_u,
                type="MARKET",
                size=qty,
                price=str(worst_price),
                reduceOnly=bool(reduce_only),
                clientOrderId=str(client_id),
            )
            return res if isinstance(res, dict) else {"data": res}
        except Exception as e:
            last_err = e

    raise RuntimeError(f"create_market_order failed: symbol={sym} side={side_u} qty={qty} err={last_err}")


def cancel_order(symbol: str, order_id: str) -> Dict[str, Any]:
    client = get_client()
    sym = str(symbol).upper().strip()
    oid = str(order_id)

    methods = ["cancel_order_v3", "cancelOrderV3", "cancel_order", "cancelOrder"]
    last_err = None
    for name in methods:
        if not hasattr(client, name):
            continue
        fn = getattr(client, name)
        try:
            res = _safe_call(fn, symbol=sym, orderId=oid)
            return res if isinstance(res, dict) else {"data": res}
        except Exception as e:
            last_err = e
    raise RuntimeError(f"cancel_order failed: symbol={sym} order_id={oid} err={last_err}")


# -----------------------------------------------------------------------------
# WS (best effort)
# -----------------------------------------------------------------------------
_WS_THREAD = None
_WS_STOP = threading.Event()

_FILL_QUEUE: List[Dict[str, Any]] = []
_FILL_LOCK = threading.Lock()


def _push_fill_event(evt: Dict[str, Any]):
    with _FILL_LOCK:
        _FILL_QUEUE.append(evt)


def pop_fill_event() -> Optional[Dict[str, Any]]:
    with _FILL_LOCK:
        if not _FILL_QUEUE:
            return None
        return _FILL_QUEUE.pop(0)


def start_private_ws():
    """Start private WS stream for account events (orders + fills)."""
    global _WS_THREAD
    if _WS_THREAD and _WS_THREAD.is_alive():
        return

    _WS_STOP.clear()

    def _run():
        client = get_client()

        # Many SDK versions expose different WS connectors.
        # We'll attempt method discovery.
        ws_methods = [
            "start_account_info_stream_v3",
            "startAccountInfoStreamV3",
            "start_private_stream_v3",
            "startPrivateStreamV3",
            "start_private_ws",
            "startPrivateWs",
        ]

        for name in ws_methods:
            if not hasattr(client, name):
                continue
            fn = getattr(client, name)
            try:
                # Most versions accept a callback
                def on_message(msg):
                    # We accept both order/fill messages here and let app decide.
                    if isinstance(msg, dict):
                        _push_fill_event(msg)
                    else:
                        _push_fill_event({"raw": msg})

                _safe_call(fn, callback=on_message)
                # Keep alive loop if function returns quickly
                while not _WS_STOP.is_set():
                    _sleep_jitter(0.5, 0.5)
                return
            except Exception:
                continue

        # If SDK doesn't support, just idle.
        while not _WS_STOP.is_set():
            _sleep_jitter(0.5, 0.5)

    _WS_THREAD = threading.Thread(target=_run, daemon=True)
    _WS_THREAD.start()


def stop_private_ws():
    _WS_STOP.set()


# -----------------------------------------------------------------------------
# REST order poller (backup)
# -----------------------------------------------------------------------------
_REST_POLL_THREAD = None
_REST_POLL_STOP = threading.Event()

_ORDER_QUEUE: List[Dict[str, Any]] = []
_ORDER_LOCK = threading.Lock()


def _push_order_event(evt: Dict[str, Any]):
    with _ORDER_LOCK:
        _ORDER_QUEUE.append(evt)


def pop_order_event() -> Optional[Dict[str, Any]]:
    with _ORDER_LOCK:
        if not _ORDER_QUEUE:
            return None
        return _ORDER_QUEUE.pop(0)


def _fetch_recent_orders(symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
    client = get_client()
    methods = ["get_orders_v3", "getOrdersV3", "get_orders", "getOrders", "ordersV3"]
    for name in methods:
        if not hasattr(client, name):
            continue
        fn = getattr(client, name)
        try:
            kwargs = {}
            if symbol:
                kwargs["symbol"] = str(symbol).upper().strip()
            res = _safe_call(fn, **kwargs)
            return res if isinstance(res, dict) else {"data": res}
        except Exception:
            continue
    return None


def start_order_rest_poller():
    """Backup REST poller for recent orders + fills (if WS misses)."""
    global _REST_POLL_THREAD
    if _REST_POLL_THREAD and _REST_POLL_THREAD.is_alive():
        return

    _REST_POLL_STOP.clear()

    interval = float(os.getenv("ORDER_REST_POLL_INTERVAL", "5.0"))

    def _run():
        while not _REST_POLL_STOP.is_set():
            try:
                res = _fetch_recent_orders()
                if res is not None:
                    _push_order_event({"type": "orders_poll", "payload": res})
            except Exception:
                pass
            _sleep_jitter(interval, 0.5)

    _REST_POLL_THREAD = threading.Thread(target=_run, daemon=True)
    _REST_POLL_THREAD.start()


def stop_order_rest_poller():
    _REST_POLL_STOP.set()
