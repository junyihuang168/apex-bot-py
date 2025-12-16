import os
import time
import json
import random
import inspect
import threading
import hmac
import base64
import hashlib
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Union, Tuple, Callable, List

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

# -----------------------------
# Trading rules (qty snapping)
# -----------------------------
DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),
    "step_size": Decimal("0.01"),
    "qty_decimals": 2,
}

SYMBOL_RULES: Dict[str, Dict[str, Any]] = {
    # "BTC-USDT": {"min_qty": Decimal("0.001"), "step_size": Decimal("0.001"), "qty_decimals": 3},
}

_CLIENT: Optional[HttpPrivateSign] = None

NumberLike = Union[str, float, int]

# -----------------------------
# WS (fills) runtime
# -----------------------------
_WS_ENABLE = os.getenv("APEX_WS_ENABLE", "1").lower() in ("1", "true", "yes", "y", "on")
_WS_THREAD: Optional[threading.Thread] = None
_WS_LOCK = threading.Lock()

# orderId -> list[fills]
_FILLS_BY_ORDER: Dict[str, List[dict]] = {}
_FILLS_CV = threading.Condition()

# optional callback: fn(fill_dict) -> None
_ON_FILL: Optional[Callable[[dict], None]] = None


def set_on_fill_callback(fn: Optional[Callable[[dict], None]]):
    global _ON_FILL
    _ON_FILL = fn


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
    return base_url, network_id, use_mainnet


def _get_api_credentials():
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


def _random_client_id() -> str:
    # keep it numeric-only to avoid any strict validation
    return str(int(float(str(random.random())[2:])))


def _safe_call(fn, **kwargs):
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        call_kwargs = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        return fn(**call_kwargs)
    except Exception:
        return fn(**{k: v for k, v in kwargs.items() if v is not None})


def _install_compat_shims(client: HttpPrivateSign):
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

    # cancel order (best-effort)
    if not hasattr(client, "cancel_order_v3") and hasattr(client, "cancelOrderV3"):
        client.cancel_order_v3 = getattr(client, "cancelOrderV3")  # type: ignore
    if not hasattr(client, "cancelOrderV3") and hasattr(client, "cancel_order_v3"):
        client.cancelOrderV3 = getattr(client, "cancel_order_v3")  # type: ignore


def get_client() -> HttpPrivateSign:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    base_url, network_id, _ = _get_base_and_network()
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
        print("[apex_client] configs_v3 ok:", "OK" if cfg else cfg)
    except Exception as e:
        print("[apex_client] WARNING configs_v3 error:", e)

    try:
        acc = _safe_call(client.get_account_v3)
        print("[apex_client] get_account_v3 ok:", "OK" if acc else acc)
    except Exception as e:
        print("[apex_client] WARNING get_account_v3 error:", e)

    _CLIENT = client

    # start WS fills listener
    if _WS_ENABLE:
        start_ws_fills()

    return client


def get_account():
    client = get_client()
    return _safe_call(client.get_account_v3)


def get_market_price(symbol: str, side: str, size: str) -> str:
    base_url, network_id, _ = _get_base_and_network()
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
# WS: Private realtime fills (方案 A)
# ------------------------------------------------------------
def _ws_urls(use_mainnet: bool) -> str:
    # allow override
    if use_mainnet:
        return os.getenv("APEX_WS_PRIVATE_MAIN", "wss://quote.omni.apex.exchange/realtime_private")
    return os.getenv("APEX_WS_PRIVATE_TEST", "wss://qa-quote.omni.apex.exchange/realtime_private")


def _ws_sign(timestamp_ms: str, secret: str) -> str:
    # WS auth message: timestamp + 'GET' + '/ws/accounts' (官方示例) 
    prehash = f"{timestamp_ms}GET/ws/accounts".encode("utf-8")
    key = base64.b64decode(secret)
    digest = hmac.new(key, prehash, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def start_ws_fills():
    global _WS_THREAD
    with _WS_LOCK:
        if _WS_THREAD and _WS_THREAD.is_alive():
            return

        t = threading.Thread(target=_ws_loop, daemon=True)
        t.start()
        _WS_THREAD = t
        print("[apex_client||WS] started account_info_stream_v3 (custom ws loop)")


def _ws_loop():
    base_url, _, use_mainnet = _get_base_and_network()
    _ = base_url  # keep for logs
    ws_url = _ws_urls(use_mainnet)

    # Some deployments require query params; keep minimal and robust:
    # wss://.../realtime_private?v=2&timestamp=...
    # We'll include v=2 and timestamp for parity with docs. 
    now_ms = str(int(time.time() * 1000))
    if "?" in ws_url:
        full_url = f"{ws_url}&v=2&timestamp={now_ms}"
    else:
        full_url = f"{ws_url}?v=2&timestamp={now_ms}"

    creds = _get_api_credentials()
    api_key = creds["key"]
    passphrase = creds["passphrase"]
    secret = creds["secret"]

    import websocket  # websocket-client

    def on_open(ws):
        try:
            ts = str(int(time.time() * 1000))
            sig = _ws_sign(ts, secret)
            login_args = json.dumps({
                "key": api_key,
                "passphrase": passphrase,
                "timestamp": ts,
                "signature": sig,
            })
            ws.send(json.dumps({"op": "login", "args": [login_args]}))
            ws.send(json.dumps({"op": "subscribe", "args": ["ws_zk_accounts_v3"]}))
            print("[WS-FILLS] login+subscribe sent")
        except Exception as e:
            print("[WS-FILLS] on_open error:", e)

    def on_message(ws, message: str):
        try:
            obj = json.loads(message)
        except Exception:
            return

        # Expect push message containing fills list. 
        # In practice, topic may be `ws_zk_accounts_v3` with `contents` holding fills.
        topic = obj.get("topic") or obj.get("channel") or ""
        contents = obj.get("contents") if isinstance(obj, dict) else None

        if topic != "ws_zk_accounts_v3":
            return
        if not isinstance(contents, dict):
            return

        fills = contents.get("fills")
        if not isinstance(fills, list) or not fills:
            return

        for f in fills:
            if not isinstance(f, dict):
                continue

            order_id = str(f.get("orderId") or "")
            if not order_id:
                continue

            with _FILLS_CV:
                _FILLS_BY_ORDER.setdefault(order_id, []).append(f)
                _FILLS_CV.notify_all()

            if _ON_FILL:
                try:
                    _ON_FILL(f)
                except Exception as e:
                    print("[WS-FILLS] on_fill callback error:", e)

    def on_error(ws, error):
        print("[WS-FILLS] error:", error)

    def on_close(ws, code, msg):
        print(f"[WS-FILLS] closed code={code} msg={msg!r}")

    while True:
        try:
            ws_app = websocket.WebSocketApp(
                full_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("[WS-FILLS] run_forever exception:", e)

        time.sleep(2.0)  # reconnect backoff


def wait_fills_for_order(order_id: str, timeout_sec: float = 5.0, settle_ms: int = 250) -> Dict[str, Any]:
    """
    Wait WS fills for a given orderId, then aggregate avg price and qty.
    """
    order_id = str(order_id or "")
    if not order_id:
        raise ValueError("wait_fills_for_order requires order_id")

    deadline = time.time() + float(timeout_sec)
    first_seen_ts = None

    while True:
        with _FILLS_CV:
            fills = list(_FILLS_BY_ORDER.get(order_id, []))
            now = time.time()

            if fills and first_seen_ts is None:
                first_seen_ts = now

            if fills and first_seen_ts is not None:
                # settle window to capture multiple partial fills
                if (now - first_seen_ts) * 1000.0 >= float(settle_ms):
                    break

            remain = deadline - now
            if remain <= 0:
                break

            _FILLS_CV.wait(timeout=min(0.5, remain))

    fills = list(_FILLS_BY_ORDER.get(order_id, []))
    if not fills:
        raise RuntimeError(f"WS fills timeout: order_id={order_id}")

    total_qty = Decimal("0")
    total_notional = Decimal("0")

    for f in fills:
        q = f.get("size")
        p = f.get("price")
        if q is None or p is None:
            continue
        dq = Decimal(str(q))
        dp = Decimal(str(p))
        if dq <= 0 or dp <= 0:
            continue
        total_qty += dq
        total_notional += dq * dp

    if total_qty <= 0:
        raise RuntimeError(f"WS fills invalid qty: order_id={order_id} fills={fills[:3]}")

    avg = total_notional / total_qty
    return {
        "order_id": order_id,
        "filled_qty": total_qty,
        "avg_fill_price": avg,
        "raw_fills": fills,
    }


# ------------------------------------------------------------
# Order APIs
# ------------------------------------------------------------
def create_order_v3_generic(
    symbol: str,
    side: str,
    order_type: str,
    size: str,
    price: str,
    reduce_only: bool,
    trigger_price: Optional[str] = None,
    client_id: Optional[str] = None,
) -> Dict[str, Any]:
    client = get_client()
    side = side.upper()
    ts = int(time.time())
    apex_client_id = client_id or _random_client_id()

    params = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "size": str(size),
        "price": str(price),
        "timestampSeconds": ts,
        "reduceOnly": bool(reduce_only),

        # compat: some SDKs expect clientId, some expect clientOrderId
        "clientId": apex_client_id,
        "clientOrderId": apex_client_id,
    }
    if trigger_price is not None:
        params["triggerPrice"] = str(trigger_price)

    raw_order = _safe_call(client.create_order_v3, **params)
    order_id, client_order_id = _extract_order_ids(raw_order)

    return {
        "raw_order": raw_order,
        "order_id": order_id,
        "client_order_id": client_order_id or apex_client_id,
        "apex_client_id": apex_client_id,
        "computed": {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "size": str(size),
            "price": str(price),
            "triggerPrice": str(trigger_price) if trigger_price is not None else None,
            "reduceOnly": bool(reduce_only),
        }
    }


def create_market_order(
    symbol: str,
    side: str,
    size: NumberLike | None = None,
    size_usdt: NumberLike | None = None,
    reduce_only: bool = False,
    client_id: Optional[str] = None,
) -> Dict[str, Any]:
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
        used_budget = (snapped_qty * price_decimal).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        print(f"[apex_client] budget={budget} USDT -> qty={size_str}, used≈{used_budget} USDT (price {price_str})")
    else:
        if size is None:
            raise ValueError("size or size_usdt must be provided")
        size_str = str(size)
        price_str = get_market_price(symbol, side, size_str)

    return create_order_v3_generic(
        symbol=symbol,
        side=side,
        order_type="MARKET",
        size=size_str,
        price=price_str,
        reduce_only=reduce_only,
        trigger_price=None,
        client_id=client_id,  # keep numeric if you pass one
    )


def cancel_order(order_id: str) -> bool:
    client = get_client()
    if not order_id:
        return False
    try:
        if hasattr(client, "cancel_order_v3"):
            _safe_call(client.cancel_order_v3, orderId=str(order_id))
            return True
    except Exception as e:
        print("[apex_client] cancel_order error:", e)
    return False


def place_fixed_tpsl_orders(
    symbol: str,
    direction: str,
    qty: Decimal,
    entry_price: Decimal,
    sl_pct: Decimal,
    tp_pct: Decimal,
) -> Dict[str, Any]:
    """
    Place STOP_MARKET + TAKE_PROFIT_MARKET reduceOnly orders with triggerPrice.
    Uses a conservative 'price buffer' to satisfy "market order price worse than index" constraints. 
    """
    symbol = str(symbol).upper().strip()
    direction = str(direction).upper().strip()
    q = Decimal(str(qty))
    ep = Decimal(str(entry_price))

    if q <= 0 or ep <= 0:
        raise ValueError("qty and entry_price must be > 0")

    buffer_pct = Decimal(os.getenv("TPSL_PRICE_BUFFER_PCT", "0.10"))  # 10% default

    sl_mult = (Decimal("1") - (sl_pct / Decimal("100"))) if direction == "LONG" else (Decimal("1") + (sl_pct / Decimal("100")))
    tp_mult = (Decimal("1") + (tp_pct / Decimal("100"))) if direction == "LONG" else (Decimal("1") - (tp_pct / Decimal("100")))

    sl_trigger = (ep * sl_mult)
    tp_trigger = (ep * tp_mult)

    if direction == "LONG":
        exit_side = "SELL"
        sl_price = sl_trigger * (Decimal("1") - buffer_pct)  # worse for SELL = lower
        tp_price = tp_trigger * (Decimal("1") - buffer_pct)
    else:
        exit_side = "BUY"
        sl_price = sl_trigger * (Decimal("1") + buffer_pct)  # worse for BUY = higher
        tp_price = tp_trigger * (Decimal("1") + buffer_pct)

    # string formatting: keep as plain str (ApeX accepts decimal strings)
    size_str = str(q)

    sl = create_order_v3_generic(
        symbol=symbol,
        side=exit_side,
        order_type="STOP_MARKET",
        size=size_str,
        price=str(sl_price),
        reduce_only=True,
        trigger_price=str(sl_trigger),
        client_id=None,
    )
    tp = create_order_v3_generic(
        symbol=symbol,
        side=exit_side,
        order_type="TAKE_PROFIT_MARKET",
        size=size_str,
        price=str(tp_price),
        reduce_only=True,
        trigger_price=str(tp_trigger),
        client_id=None,
    )

    return {"sl": sl, "tp": tp, "sl_trigger": str(sl_trigger), "tp_trigger": str(tp_trigger)}
