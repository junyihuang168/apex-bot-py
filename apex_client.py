import os
import time
import json
import random
import inspect
import re
import threading
import queue
from collections import OrderedDict
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, Tuple, Union, Iterable, Set, List

import requests

# Public WS client (top-of-book)
try:
    import websocket  # websocket-client
except Exception:
    websocket = None

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


# -----------------------------------------------------------------------------
# Public WS (L1 / top-of-book)
#
# Purpose:
# - Provide best bid / best ask (BBO) for bot-side risk checks.
# - Keep a long-lived connection with ping/pong, reconnect, and resubscribe.
# - Subscribe to: orderBook25.H.{symbol}
#
# Notes:
# - This does NOT replace any private/fill logic; it is an additional market-data feed.
# - We intentionally keep only a small in-memory book (25 levels) per topic.
# -----------------------------------------------------------------------------

_PUB_WS_STARTED = False
_PUB_WS_LOCK = threading.Lock()

_PUB_WS_THREAD: Optional[threading.Thread] = None
_PUB_WS_APP: Any = None

_PUB_WS_CONNECTED = False
_PUB_WS_CONN_TS = 0.0
_PUB_WS_LAST_SUB_TS = 0.0
_PUB_WS_ACTIVE_TOPICS: Set[str] = set()

_PUB_WS_SEND_Q: "queue.Queue[dict]" = queue.Queue(maxsize=5000)
_PUB_WS_DESIRED_TOPICS: Set[str] = set()
_PUB_WS_TOPICS_LOCK = threading.Lock()

_PUB_LAST_MSG_TS = 0.0
_PUB_LAST_PONG_TS = 0.0

_BOOKS_BY_TOPIC: Dict[str, Dict[str, Any]] = {}

_L1_LOCK = threading.Lock()
_L1_CACHE: Dict[str, Dict[str, Any]] = {}


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


def _get_public_ws_endpoint() -> str:
    """Public market-data WS endpoint.

    The ApeX Omni public quote WS is separate from the private WS endpoint used by the SDK.
    We allow overriding via env for maximum compatibility.
    """
    override = str(os.getenv("APEX_PUBLIC_WS_URL", "")).strip()
    if override:
        return override

    use_mainnet = _env_bool("APEX_USE_MAINNET", False)
    env_name = str(os.getenv("APEX_ENV", "")).lower()
    if env_name in {"main", "mainnet", "prod", "production"}:
        use_mainnet = True

    # Defaults based on ApeX Omni docs/examples.
    # If these ever change, set APEX_PUBLIC_WS_URL explicitly.
    return (
        "wss://quote.omni.apex.exchange/realtime_public"
        if use_mainnet
        else "wss://quote-qa.omni.apex.exchange/realtime_public"
    )


def _get_api_credentials() -> Dict[str, str]:
    # Required by apexomni
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


def _safe_call(fn, **kwargs):
    """
    Call an SDK function in a cross-version compatible way.

    This wrapper solves two common ApeX SDK drift issues:
      1) Unexpected keyword argument 'X'  -> we auto-prune X and retry.
      2) Signature-filtering can accidentally drop required parameters on some wrapped methods.
         If we see "missing required positional arguments" after a filtered attempt, we retry
         with the original (unfiltered) kwargs and prune unsupported keywords instead.
    """
    call_kwargs = {k: v for k, v in kwargs.items() if v is not None}

    def _call_with_prune(f, payload: Dict[str, Any]):
        p = dict(payload)
        last = None
        for _ in range(12):
            try:
                return f(**p)
            except TypeError as e:
                last = e
                msg = str(e)

                # Only prune on "unexpected keyword argument".
                m = re.search(r"unexpected keyword argument ['\"]([^'\"]+)['\"]", msg)
                if m:
                    bad = m.group(1)
                    if bad in p:
                        p.pop(bad, None)
                        continue
                raise
        if last:
            raise last
        raise RuntimeError("_safe_call: call failed")

    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

        if has_var_kw:
            return _call_with_prune(fn, call_kwargs)

        allowed = set(params.keys())
        filtered = {k: v for k, v in call_kwargs.items() if k in allowed}

        try:
            return _call_with_prune(fn, filtered)
        except TypeError as e:
            msg = str(e)
            # Critical: if filtering caused missing required args, retry unfiltered.
            if ("missing" in msg) and (("required positional argument" in msg) or ("required positional arguments" in msg)):
                return _call_with_prune(fn, call_kwargs)
            raise

    except (ValueError, TypeError):
        # Signature not available (C-extensions / dynamic wrappers): prune on error.
        return _call_with_prune(fn, call_kwargs)


def _extract_data_dict(res: Any) -> Optional[Dict[str, Any]]:
    """
    Normalize common SDK responses to a dict payload.
    Handles:
      - {"data": {...}} / {"data": [..]}
      - {...} direct dict
      - object with .data
    """
    if res is None:
        return None

    if isinstance(res, dict):
        d = res.get("data")
        if isinstance(d, dict):
            return d
        if isinstance(d, list) and d and isinstance(d[0], dict):
            return d[0]
        return res

    # object with attribute .data
    try:
        d = getattr(res, "data", None)
        if isinstance(d, dict):
            return d
        if isinstance(d, list) and d and isinstance(d[0], dict):
            return d[0]
    except Exception:
        pass

    return None


def _install_compat_shims(client: HttpPrivateSign) -> None:
    """
    Add method aliases for SDK naming differences (no behavior changes).

    IMPORTANT:
    Do NOT alias `accountV3` to a callable method. In some SDK builds, `accountV3`
    is expected to be a DICT cache (used internally like: self.accountV3.get(...)).
    If we set it to a function, SDK will crash with:
        'function' object has no attribute 'get'
    """
    aliases = {
        "configs_v3": ["configsV3"],
        "create_order_v3": ["createOrderV3"],
        "cancel_order_v3": ["cancelOrderV3"],
        "cancel_order_by_client_id_v3": ["cancelOrderByClientOrderIdV3"],
        "get_order_v3": ["getOrderV3", "get_order_detail_v3", "getOrderDetailV3"],
        "get_open_orders_v3": ["openOrdersV3", "open_orders_v3"],
        "get_positions_v3": ["positionsV3", "get_open_positions_v3", "open_positions_v3"],
        # account getter (callable) — keep it as a method, not as accountV3 attribute:
        "get_account_v3": ["getAccountV3", "get_account", "getAccount", "account_v3"],
    }

    # 1) candidate -> canonical
    for canonical, candidates in aliases.items():
        if hasattr(client, canonical):
            continue
        for cand in candidates:
            if hasattr(client, cand):
                try:
                    setattr(client, canonical, getattr(client, cand))
                except Exception:
                    pass
                break

    # 2) canonical -> candidate (safe direction)
    for canonical, candidates in aliases.items():
        if not hasattr(client, canonical):
            continue
        for cand in candidates:
            if not hasattr(client, cand):
                try:
                    setattr(client, cand, getattr(client, canonical))
                except Exception:
                    pass


def _ensure_account_v3_cache(client: HttpPrivateSign) -> None:
    """
    Ensure client.accountV3 is a DICT cache if the SDK expects it.
    This prevents: 'function' object has no attribute 'get'
    """
    try:
        cur = getattr(client, "accountV3", None)
        if isinstance(cur, dict):
            return

        # If accountV3 exists but is callable (method), preserve it elsewhere to avoid losing access.
        if callable(cur) and not hasattr(client, "_accountV3_callable"):
            try:
                setattr(client, "_accountV3_callable", cur)
            except Exception:
                pass

        # Try to fetch account info via a callable getter (best-effort).
        call_candidates = []

        if hasattr(client, "get_account_v3") and callable(getattr(client, "get_account_v3")):
            call_candidates.append(getattr(client, "get_account_v3"))

        if hasattr(client, "_accountV3_callable") and callable(getattr(client, "_accountV3_callable")):
            call_candidates.append(getattr(client, "_accountV3_callable"))

        # Some builds expose getAccountV3 / accountV3 as callable getters:
        for n in ("getAccountV3", "accountV3"):
            if hasattr(client, n) and callable(getattr(client, n)):
                call_candidates.append(getattr(client, n))

        for fn in call_candidates:
            try:
                res = _safe_call(fn)
                d = _extract_data_dict(res)
                if isinstance(d, dict) and d:
                    # Install dict cache at client.accountV3 for SDK internals.
                    try:
                        setattr(client, "accountV3", d)
                    except Exception:
                        pass
                    return
            except Exception:
                continue

        # If we cannot fetch, last-resort set empty dict to avoid attribute crash.
        try:
            setattr(client, "accountV3", {})
        except Exception:
            pass

    except Exception:
        # Do not crash get_client on best-effort cache setup.
        pass


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

    # Install shims first so snake/camel are both available where safe.
    _install_compat_shims(client)

    # Best-effort: initialize v3 configs (some SDK builds rely on it for v3 helpers)
    try:
        if hasattr(client, "configs_v3") and callable(getattr(client, "configs_v3")):
            _safe_call(getattr(client, "configs_v3"))
        elif hasattr(client, "configsV3") and callable(getattr(client, "configsV3")):
            _safe_call(getattr(client, "configsV3"))
    except Exception as e:
        print(f"[apex_client][WARN] configs_v3 init failed (continuing): {e}")

    # CRITICAL: ensure accountV3 cache is a dict (SDK internal usage)
    _ensure_account_v3_cache(client)

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
    """Public ticker-based reference price (no auth)."""
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

            if not isinstance(item, dict):
                raise ValueError("ticker item is not dict")

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
    """Price used only as a 'reference/worse' bound for signing market orders."""
    side_u = (side or "").upper()
    if side_u not in ("BUY", "SELL"):
        side_u = "BUY"

    last_err = None

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
                    if isinstance(data, dict):
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


def _create_order_v3_compat(
    client: HttpPrivateSign,
    *,
    symbol: str,
    side: str,
    order_type: str,
    size: str,
    price: str,
    reduce_only: bool,
    client_id: Optional[str],
) -> Any:
    """
    SDK parameter names differ across apexomni versions.

    NOTE:
    We also ensure accountV3 cache is dict before sending order (some SDK builds require it).
    """
    _ensure_account_v3_cache(client)

    base = {
        "symbol": symbol,
        "side": side,
    }

    type_variants = [
        {"type": order_type},
        {"orderType": order_type},
        {"order_type": order_type},
    ]
    size_variants = [
        {"size": size},
        {"qty": size},
        {"quantity": size},
    ]
    price_variants = [
        {"price": price},
        {"limitPrice": price},
        {"worstPrice": price},
        {"worst_price": price},
    ]
    tif_variants = [
        {"timeInForce": "IOC"},
        {"time_in_force": "IOC"},
        {"tif": "IOC"},
        {},
    ]
    reduce_variants = [
        {"reduceOnly": bool(reduce_only)},
        {"reduce_only": bool(reduce_only)},
        {},
    ]

    client_variants = []
    if client_id:
        client_variants = [
            {"clientId": str(client_id)},
            {"clientOrderId": str(client_id)},
            {"client_id": str(client_id)},
        ]
    else:
        client_variants = [{}]

    def _call_positional_with_prune(f, args, kw_payload: Dict[str, Any]):
        p = dict(kw_payload)
        last = None
        for _ in range(12):
            try:
                return f(*args, **p)
            except TypeError as e:
                last = e
                msg = str(e)
                m = re.search(r"unexpected keyword argument ['\"]([^'\"]+)['\"]", msg)
                if m:
                    bad = m.group(1)
                    if bad in p:
                        p.pop(bad, None)
                        continue
                raise
        if last:
            raise last
        raise RuntimeError("_call_positional_with_prune failed")

    def _try_positional(f, payload: Dict[str, Any]):
        """Call create_order_v3 with positional fallbacks.

        ApeX SDK drifts here are especially painful. We've observed real deployments where
        `create_order_v3()` is defined like either:
          - create_order_v3(side, type, size, ...)
          - create_order_v3(symbol, side, type, size, ...)
          - create_order_v3(type, size, ... , symbol=?, side=? as kwargs)

        This helper tries a broader positional matrix (while leaving any remaining fields
        in kwargs) and prunes unexpected kwargs along the way.
        """

        sym = payload.get("symbol", symbol)
        sd = payload.get("side", side)
        ty = payload.get("type") or payload.get("orderType") or payload.get("order_type")
        sz = payload.get("size") or payload.get("qty") or payload.get("quantity")
        px = payload.get("price") or payload.get("limitPrice") or payload.get("worstPrice") or payload.get("worst_price")

        if ty is None or sz is None:
            raise TypeError("positional fallback missing type/size")

        SYM_KEYS = ("symbol",)
        SIDE_KEYS = ("side",)
        TYPE_KEYS = ("type", "orderType", "order_type")
        SIZE_KEYS = ("size", "qty", "quantity")
        PRICE_KEYS = ("price", "limitPrice", "worstPrice", "worst_price")

        def _kw_without(*remove_keys: str) -> Dict[str, Any]:
            kwp = dict(payload)
            for k in remove_keys:
                kwp.pop(k, None)
            return kwp

        # Patterns are (args, keys_to_remove_from_kwargs)
        patterns = []

        # Some builds require only (type, size) positionally.
        patterns.append(([ty, sz], TYPE_KEYS + SIZE_KEYS))
        if px is not None:
            patterns.append(([ty, sz, px], TYPE_KEYS + SIZE_KEYS + PRICE_KEYS))

        # Many builds use (side, type, size, ...)
        patterns.append(([sd, ty, sz], SIDE_KEYS + TYPE_KEYS + SIZE_KEYS))
        if px is not None:
            patterns.append(([sd, ty, sz, px], SIDE_KEYS + TYPE_KEYS + SIZE_KEYS + PRICE_KEYS))

        # Some builds include symbol explicitly.
        patterns.append(([sym, sd, ty, sz], SYM_KEYS + SIDE_KEYS + TYPE_KEYS + SIZE_KEYS))
        if px is not None:
            patterns.append(([sym, sd, ty, sz, px], SYM_KEYS + SIDE_KEYS + TYPE_KEYS + SIZE_KEYS + PRICE_KEYS))

        # A few builds use (symbol, type, size, ...)
        patterns.append(([sym, ty, sz], SYM_KEYS + TYPE_KEYS + SIZE_KEYS))
        if px is not None:
            patterns.append(([sym, ty, sz, px], SYM_KEYS + TYPE_KEYS + SIZE_KEYS + PRICE_KEYS))

        last_e = None
        for args, remove in patterns:
            try:
                return _call_positional_with_prune(f, args, _kw_without(*remove))
            except Exception as e:
                last_e = e
                continue
        if last_e:
            raise last_e
        raise RuntimeError("positional patterns all failed")

    last_exc = None
    fn = getattr(client, "create_order_v3")

    for t in type_variants:
        for s in size_variants:
            for p in price_variants:
                for tif in tif_variants:
                    for r in reduce_variants:
                        for c in client_variants:
                            payload = {}
                            payload.update(base)
                            payload.update(t)
                            payload.update(s)
                            payload.update(p)
                            payload.update(tif)
                            payload.update(r)
                            payload.update(c)
                            try:
                                return _safe_call(fn, **payload)
                            except TypeError as e:
                                last_exc = e
                                msg = str(e)
                                if ("missing" in msg and (("required positional argument" in msg) or ("required positional arguments" in msg))) or (
                                    "unexpected keyword argument" in msg and ("'type'" in msg or "'size'" in msg)
                                ):
                                    try:
                                        return _try_positional(fn, payload)
                                    except Exception as e2:
                                        last_exc = e2
                                        continue
                                continue
                            except Exception as e:
                                last_exc = e
                                continue

    if last_exc:
        raise last_exc
    raise RuntimeError("create_order_v3 failed with all compatible parameter variants")


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

    worst_price = get_market_price(sym, side_u, qty)

    client_id = client_id or _random_client_id()

    raw_res = _create_order_v3_compat(
        client,
        symbol=sym,
        side=side_u,
        order_type="MARKET",
        size=qty,
        price=str(worst_price),
        reduce_only=bool(reduce_only),
        client_id=str(client_id) if client_id else None,
    )

    # Robust parse
    data = _extract_data_dict(raw_res)
    order_id = None
    client_order_id = None

    if isinstance(data, dict):
        order_id = data.get("orderId") or data.get("id")
        client_order_id = data.get("clientOrderId") or data.get("clientId") or client_id
    else:
        # object response
        try:
            order_id = getattr(raw_res, "orderId", None) or getattr(raw_res, "id", None)
        except Exception:
            order_id = None
        client_order_id = client_id

    if order_id:
        register_order_for_tracking(
            order_id=str(order_id),
            client_order_id=str(client_order_id) if client_order_id is not None else None,
            symbol=sym,
            expected_qty=qty,
        )

    return {
        "raw": raw_res,
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
    client = get_client()
    sym = format_symbol(symbol)
    side_u = str(side).upper().strip()

    try:
        protective_price = get_market_price(sym, side_u, qty)
    except Exception:
        protective_price = str(trigger_price)

    base = {
        "symbol": sym,
        "side": side_u,
    }

    type_variants = [
        {"type": "STOP_MARKET"},
        {"orderType": "STOP_MARKET"},
    ]
    size_variants = [
        {"size": str(qty)},
        {"qty": str(qty)},
    ]
    price_variants = [
        {"price": str(protective_price)},
        {"limitPrice": str(protective_price)},
    ]
    trigger_variants = [
        {"triggerPrice": str(trigger_price)},
        {"trigger_price": str(trigger_price)},
    ]
    reduce_variants = [
        {"reduceOnly": bool(reduce_only)},
        {"reduce_only": bool(reduce_only)},
    ]
    client_variants = [{}]
    if client_order_id:
        client_variants = [
            {"clientOrderId": str(client_order_id)},
            {"clientId": str(client_order_id)},
            {"client_id": str(client_order_id)},
        ]

    last_exc: Optional[Exception] = None
    for t in type_variants:
        for s in size_variants:
            for p in price_variants:
                for trig in trigger_variants:
                    for r in reduce_variants:
                        for c in client_variants:
                            payload: Dict[str, Any] = {}
                            payload.update(base)
                            payload.update(t)
                            payload.update(s)
                            payload.update(p)
                            payload.update(trig)
                            payload.update(r)
                            payload.update(c)
                            try:
                                res = _safe_call(getattr(client, "create_order_v3"), **payload)
                                data = _extract_data_dict(res)
                                if isinstance(data, dict):
                                    oid = data.get("orderId") or data.get("id")
                                    if oid:
                                        register_order_for_tracking(str(oid), str(client_order_id or ""), sym)
                                return res
                            except Exception as e:
                                # Some SDK builds require positional args (notably: type + size). Try a broader
                                # positional fallback before giving up.
                                last_exc = e

                                try:
                                    if isinstance(e, TypeError) and ("missing" in str(e)) and ("required positional argument" in str(e)):
                                        fn = getattr(client, "create_order_v3")

                                        ty = payload.get("type") or payload.get("orderType") or payload.get("order_type")
                                        sz = payload.get("size") or payload.get("qty") or payload.get("quantity")
                                        px = payload.get("price") or payload.get("limitPrice") or payload.get("worstPrice") or payload.get("worst_price")

                                        if ty is None or sz is None:
                                            raise e

                                        TYPE_KEYS = ("type", "orderType", "order_type")
                                        SIZE_KEYS = ("size", "qty", "quantity")
                                        PRICE_KEYS = ("price", "limitPrice", "worstPrice", "worst_price")
                                        SIDE_KEYS = ("side",)
                                        SYM_KEYS = ("symbol",)

                                        def _call_positional_with_prune(f, args, kw_payload: Dict[str, Any]):
                                            p = dict(kw_payload)
                                            last = None
                                            for _ in range(12):
                                                try:
                                                    return f(*args, **p)
                                                except TypeError as te:
                                                    last = te
                                                    msg = str(te)
                                                    m = re.search(r"unexpected keyword argument ['\"]([^'\"]+)['\"]", msg)
                                                    if m:
                                                        bad = m.group(1)
                                                        p.pop(bad, None)
                                                        continue
                                                    raise
                                            if last:
                                                raise last
                                            raise RuntimeError("positional prune failed")

                                        def _kw_without(remove_keys: Tuple[str, ...]):
                                            kw = dict(payload)
                                            for k in remove_keys:
                                                kw.pop(k, None)
                                            return kw

                                        patterns = [
                                            ([ty, sz], TYPE_KEYS + SIZE_KEYS),
                                            ([ty, sz, px] if px is not None else None, TYPE_KEYS + SIZE_KEYS + PRICE_KEYS),
                                            ([side_u, ty, sz], SIDE_KEYS + TYPE_KEYS + SIZE_KEYS),
                                            ([side_u, ty, sz, px] if px is not None else None, SIDE_KEYS + TYPE_KEYS + SIZE_KEYS + PRICE_KEYS),
                                            ([sym, side_u, ty, sz], SYM_KEYS + SIDE_KEYS + TYPE_KEYS + SIZE_KEYS),
                                            ([sym, side_u, ty, sz, px] if px is not None else None, SYM_KEYS + SIDE_KEYS + TYPE_KEYS + SIZE_KEYS + PRICE_KEYS),
                                            ([sym, ty, sz], SYM_KEYS + TYPE_KEYS + SIZE_KEYS),
                                            ([sym, ty, sz, px] if px is not None else None, SYM_KEYS + TYPE_KEYS + SIZE_KEYS + PRICE_KEYS),
                                        ]

                                        for args, remove in patterns:
                                            if args is None:
                                                continue
                                            try:
                                                res2 = _call_positional_with_prune(fn, args, _kw_without(remove))
                                                data2 = _extract_data_dict(res2)
                                                if isinstance(data2, dict):
                                                    oid2 = data2.get("orderId") or data2.get("id")
                                                    if oid2:
                                                        register_order_for_tracking(str(oid2), str(client_order_id or ""), sym)
                                                return res2
                                            except Exception as e2:
                                                last_exc = e2
                                                continue
                                except Exception:
                                    # Keep the original exception semantics.
                                    pass

                                continue

    if last_exc:
        raise last_exc
    raise RuntimeError("create_trigger_order: create_order_v3 failed with all compatible parameter variants")


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

            def is_fill(d: Dict[str, Any]) -> bool:
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

                    if upd.get("cum_qty") is not None:
                        st["cum_qty"] = Decimal(str(upd["cum_qty"]))
                    if upd.get("avg_px") is not None:
                        st["avg_px"] = Decimal(str(upd["avg_px"]))
                    if upd.get("expected_qty") is not None:
                        st["expected_qty"] = upd.get("expected_qty")
                    _ORDER_STATE[oid] = st

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
                order_ids = list(_ORDER_STATE.keys())
                for oid in order_ids:
                    st = _ORDER_STATE.get(oid) or {}
                    if now - float(st.get("ts") or 0) < gap_sec:
                        continue

                    agg = _FILL_AGG.get(oid)
                    if agg and Decimal(str(agg.get("qty") or "0")) > 0 and str(st.get("status") or "").upper() in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}:
                        continue

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
    start_private_ws()
    if _env_bool("ENABLE_REST_POLL", True):
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

    raise RuntimeError(f"fill_summary timeout; last_source={last_source}")


# ──────────────────────────────────────────────────────────────
# SYMBOL NORMALIZATION
# ──────────────────────────────────────────────────────────────

def format_symbol(symbol: str) -> str:
    """Normalize a contract symbol to ApeX private API format (e.g., 'BTC-USDT')."""
    s = str(symbol).upper().replace("/", "-").replace("_", "-").strip()
    if "-" in s:
        return s
    for quote in ("USDT", "USDC", "USD", "BTC", "ETH"):
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[:-len(quote)]}-{quote}"
    return s


def format_symbol_for_ticker(symbol: str) -> str:
    """Normalize to public ticker crossSymbolName (e.g., 'BTCUSDT')."""
    return re.sub(r"[^A-Z0-9]", "", format_symbol(symbol))


# ──────────────────────────────────────────────────────────────
# PUBLIC WS (L1) HELPERS
# ──────────────────────────────────────────────────────────────

_KNOWN_QUOTES: Tuple[str, ...] = ("USDT", "USDC", "USD", "BTC", "ETH")


def _canon_symbol_from_ws_symbol(raw: str) -> str:
    """Convert WS topic symbol into canonical 'BASE-QUOTE' if possible."""
    s = str(raw or "").upper().replace("/", "-").replace("_", "-").strip()
    if "-" in s:
        return format_symbol(s)
    for q in _KNOWN_QUOTES:
        if s.endswith(q) and len(s) > len(q):
            return f"{s[:-len(q)]}-{q}"
    return s


def _topics_for_symbol(symbol: str, limit: int = 25, speed: str = "H") -> List[str]:
    """Return one or more candidate topics for a symbol.

    We subscribe to the canonical dash symbol and (optionally) the no-dash variant to
    tolerate exchange/topic format drift.
    """
    lim = 25 if int(limit) != 200 else 200
    spd = str(speed or "H").upper()
    if spd not in {"H", "M"}:
        spd = "H"

    sym_dash = format_symbol(symbol)
    sym_nodash = format_symbol_for_ticker(symbol)

    topics = [f"orderBook{lim}.{spd}.{sym_dash}"]

    # Default on: subscribe nodash too for compatibility.
    if _env_bool("PUBLIC_WS_SUBSCRIBE_NODASH", True) and sym_nodash and sym_nodash != sym_dash:
        topics.append(f"orderBook{lim}.{spd}.{sym_nodash}")
    return topics


def get_l1_bid_ask(symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal], float]:
    """Return (best_bid, best_ask, ts) from public WS cache."""
    sym = format_symbol(symbol)
    with _L1_LOCK:
        row = _L1_CACHE.get(sym)
        if not row:
            return None, None, 0.0
        return row.get("bid"), row.get("ask"), float(row.get("ts") or 0.0)


def ensure_public_depth_subscription(symbol: str, limit: int = 25, speed: str = "H") -> None:
    """Ensure the public WS is running and subscribed to orderBook topics for this symbol."""
    start_public_ws()
    topics = _topics_for_symbol(symbol, limit=limit, speed=speed)
    new_topics: List[str] = []
    with _PUB_WS_TOPICS_LOCK:
        for t in topics:
            if t not in _PUB_WS_DESIRED_TOPICS:
                _PUB_WS_DESIRED_TOPICS.add(t)
                new_topics.append(t)

    # If WS is already connected, request subscribe immediately.
    if new_topics and _env_bool("PUBLIC_WS_LOG_SUBS", False):
        try:
            print(f"[apex_client][PUBWS] want topics for {format_symbol(symbol)}: {new_topics}")
        except Exception:
            pass
    for t in new_topics:
        try:
            _PUB_WS_SEND_Q.put_nowait({"op": "subscribe", "args": [t]})
        except Exception:
            pass


def start_public_ws() -> None:
    """Idempotent starter for the public market-data WS."""
    global _PUB_WS_STARTED, _PUB_WS_THREAD
    with _PUB_WS_LOCK:
        if _PUB_WS_STARTED:
            return
        _PUB_WS_STARTED = True

    if websocket is None:
        print("[apex_client][PUBWS] websocket-client unavailable; public WS disabled")
        return

    url = _get_public_ws_endpoint()

    # Tunables
    ping_interval = float(os.getenv("PUBLIC_WS_PING_SEC", "15"))
    pong_timeout = float(os.getenv("PUBLIC_WS_PONG_TIMEOUT_SEC", "10"))
    idle_reconnect = float(os.getenv("PUBLIC_WS_IDLE_RECONNECT_SEC", "45"))
    resubscribe_sec = float(os.getenv("PUBLIC_WS_RESUBSCRIBE_SEC", "60"))
    idle_close_without_topics = _env_bool("PUBLIC_WS_IDLE_CLOSE_WITHOUT_TOPICS", False)

    def _parse_levels(val: Any) -> List[Tuple[Decimal, Decimal]]:
        out: List[Tuple[Decimal, Decimal]] = []
        if not val:
            return out
        if isinstance(val, dict):
            # Some feeds use {price: size}
            for pk, sv in val.items():
                try:
                    p = Decimal(str(pk))
                    s = Decimal(str(sv))
                    out.append((p, s))
                except Exception:
                    continue
            return out
        if isinstance(val, list):
            for row in val:
                if not isinstance(row, (list, tuple)) or len(row) < 2:
                    continue
                try:
                    p = Decimal(str(row[0]))
                    s = Decimal(str(row[1]))
                    out.append((p, s))
                except Exception:
                    continue
        return out

    def _update_book(topic: str, payload: Dict[str, Any]) -> None:
        # Extract symbol from topic: orderBook25.H.BTC-USDT
        parts = str(topic).split(".")
        ws_sym = parts[-1] if parts else ""
        canon = _canon_symbol_from_ws_symbol(ws_sym)

        book = _BOOKS_BY_TOPIC.get(topic)
        if book is None:
            book = {"bids": {}, "asks": {}, "u": None}
            _BOOKS_BY_TOPIC[topic] = book

        # Determine whether snapshot or delta
        mtype = str(payload.get("type") or payload.get("action") or "").lower()
        is_snapshot = mtype in {"snapshot", "partial", "init"} or payload.get("snapshot") is True

        # Locate data container
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload

        # Update id (optional)
        u = data.get("u") if isinstance(data, dict) else None
        if u is None and isinstance(payload.get("u"), (int, str)):
            u = payload.get("u")
        try:
            u_int = int(u) if u is not None and str(u).strip() != "" else None
        except Exception:
            u_int = None

        # Basic monotonic check: if u regresses, ignore this update.
        prev_u = book.get("u")
        if u_int is not None and isinstance(prev_u, int) and u_int <= prev_u:
            return

        bids_val = None
        asks_val = None
        if isinstance(data, dict):
            # Common variants: bids/asks, b/a
            bids_val = data.get("bids") if "bids" in data else data.get("b")
            asks_val = data.get("asks") if "asks" in data else data.get("a")

        bids = _parse_levels(bids_val)
        asks = _parse_levels(asks_val)

        # Snapshot replaces; delta applies
        if is_snapshot:
            book["bids"] = {}
            book["asks"] = {}

        # Apply updates
        for p, s in bids:
            if s <= 0:
                book["bids"].pop(p, None)
            else:
                book["bids"][p] = s
        for p, s in asks:
            if s <= 0:
                book["asks"].pop(p, None)
            else:
                book["asks"][p] = s

        if u_int is not None:
            book["u"] = u_int

        # Compute BBO
        try:
            best_bid = max(book["bids"].keys()) if book["bids"] else None
            best_ask = min(book["asks"].keys()) if book["asks"] else None
        except Exception:
            best_bid, best_ask = None, None

        if best_bid is None or best_ask is None:
            # Don't overwrite cache with empties.
            return

        now = time.time()

        with _L1_LOCK:
            _L1_CACHE[canon] = {
                "bid": best_bid,
                "ask": best_ask,
                "ts": now,
                "topic": topic,
                "u": book.get("u"),
            }

    def _run_forever() -> None:
        global _PUB_WS_APP, _PUB_LAST_MSG_TS, _PUB_LAST_PONG_TS
        backoff = 1.0

        def _on_open(wsapp):
            global _PUB_WS_CONNECTED, _PUB_WS_CONN_TS, _PUB_WS_ACTIVE_TOPICS, _PUB_WS_LAST_SUB_TS
            nonlocal backoff
            print(f"[apex_client][PUBWS] connected: {url}")
            _PUB_WS_CONNECTED = True
            _PUB_WS_CONN_TS = time.time()
            _PUB_WS_ACTIVE_TOPICS = set()
            backoff = 1.0
            # Subscribe desired topics
            with _PUB_WS_TOPICS_LOCK:
                topics = list(_PUB_WS_DESIRED_TOPICS)
            if topics:
                try:
                    wsapp.send(json.dumps({"op": "subscribe", "args": topics}))
                    _PUB_WS_ACTIVE_TOPICS.update(set(topics))
                    _PUB_WS_LAST_SUB_TS = time.time()
                    print(f"[apex_client][PUBWS] subscribed {len(topics)} topics")
                except Exception as e:
                    print("[apex_client][PUBWS] subscribe on_open failed:", e)

        def _on_message(wsapp, message: str):
            nonlocal backoff
            try:
                _PUB_LAST_MSG_TS = time.time()
                msg = json.loads(message) if isinstance(message, str) else message
                if not isinstance(msg, dict):
                    return

                op = str(msg.get("op") or "").lower()
                if op == "pong":
                    _PUB_LAST_PONG_TS = time.time()
                    return

                topic = msg.get("topic") or msg.get("stream") or msg.get("channel")
                if topic:
                    _update_book(str(topic), msg)
            except Exception:
                # ignore parse errors (do not kill connection)
                return

        def _on_error(wsapp, error):
            print("[apex_client][PUBWS] error:", error)

        def _on_close(wsapp, status_code, msg):
            print(f"[apex_client][PUBWS] closed: code={status_code} msg={msg}")

        while True:
            try:
                _PUB_LAST_MSG_TS = time.time()
                _PUB_LAST_PONG_TS = time.time()
                _PUB_WS_APP = websocket.WebSocketApp(
                    url,
                    on_open=_on_open,
                    on_message=_on_message,
                    on_error=_on_error,
                    on_close=_on_close,
                )

                # Sender + heartbeat loop
                def _heartbeat_and_sender():
                    while True:
                        # Flush outbound subscribe commands
                        try:
                            cmd = _PUB_WS_SEND_Q.get(timeout=0.5)
                            if cmd and isinstance(cmd, dict):
                                try:
                                    _PUB_WS_APP.send(json.dumps(cmd))
                                except Exception:
                                    return
                        except Exception:
                            pass

                        now = time.time()

                        # Periodic re-subscribe (keeps topics alive across transient WS issues)
                        if resubscribe_sec > 0:
                            try:
                                with _PUB_WS_TOPICS_LOCK:
                                    topics_now = list(_PUB_WS_DESIRED_TOPICS)
                                if topics_now and (time.time() - _PUB_WS_LAST_SUB_TS) >= resubscribe_sec:
                                    try:
                                        _PUB_WS_APP.send(json.dumps({"op": "subscribe", "args": topics_now}))
                                        _PUB_WS_ACTIVE_TOPICS.update(set(topics_now))
                                        _PUB_WS_LAST_SUB_TS = time.time()
                                    except Exception:
                                        return
                            except Exception:
                                pass


                        # Ping
                        if ping_interval > 0 and now - _PUB_LAST_PONG_TS >= ping_interval:
                            try:
                                _PUB_WS_APP.send(json.dumps({"op": "ping"}))
                            except Exception:
                                return

                        # Pong timeout
                        if pong_timeout > 0 and now - _PUB_LAST_PONG_TS > (ping_interval + pong_timeout):
                            try:
                                _PUB_WS_APP.close()
                            except Exception:
                                pass
                            return

                        # Idle reconnect (socket might be "open" but not receiving)
                        with _PUB_WS_TOPICS_LOCK:
                            has_topics = bool(_PUB_WS_DESIRED_TOPICS)
                        if idle_close_without_topics and (not has_topics) and idle_reconnect > 0 and now - _PUB_LAST_MSG_TS > idle_reconnect:
                            try:
                                _PUB_WS_APP.close()
                            except Exception:
                                pass
                            return
                        if idle_reconnect > 0 and has_topics and now - _PUB_LAST_MSG_TS > idle_reconnect:
                            try:
                                _PUB_WS_APP.close()
                            except Exception:
                                pass
                            return

                threading.Thread(target=_heartbeat_and_sender, daemon=True, name="apex-public-ws-heartbeat").start()
                _PUB_WS_APP.run_forever(ping_interval=0, ping_timeout=None)

            except Exception as e:
                print(f"[apex_client][PUBWS] reconnect: {e} (sleep {backoff}s)")
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
                continue

            # run_forever returned -> reconnect
            print(f"[apex_client][PUBWS] reconnecting (sleep {backoff}s)")
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)

    _PUB_WS_THREAD = threading.Thread(target=_run_forever, daemon=True, name="apex-public-ws")
    _PUB_WS_THREAD.start()
