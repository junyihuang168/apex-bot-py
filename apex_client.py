import os
import time
import random
import inspect
import threading
import queue
from decimal import Decimal, ROUND_DOWN
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
}

SYMBOL_RULES: Dict[str, Dict[str, Any]] = {
    # "BTC-USDT": {"min_qty": Decimal("0.001"), "step_size": Decimal("0.001"), "qty_decimals": 3},
}

_CLIENT: Optional[HttpPrivateSign] = None

NumberLike = Union[str, float, int]

# ----------------------------
# ✅ WS fills cache + queue
# ----------------------------
_FILL_Q: "queue.Queue[dict]" = queue.Queue(maxsize=20000)
_WS_STARTED = False
_WS_LOCK = threading.Lock()

# orderId -> {"qty": Decimal, "notional": Decimal, "avg": Decimal, "ts": float}
_WS_FILL_AGG: Dict[str, Dict[str, Any]] = {}
# orderId -> clientOrderId
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
    启动一次即可：
    - 订阅 ws_zk_accounts_v3
    - 将 fills 推入队列 _FILL_Q
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

    def handle_account(message: dict):
        # message expected dict; best-effort
        try:
            contents = message.get("contents") if isinstance(message, dict) else None
            if not isinstance(contents, dict):
                return
            fills = contents.get("fills")
            if not isinstance(fills, list) or not fills:
                return

            for f in fills:
                if not isinstance(f, dict):
                    continue

                order_id = str(f.get("orderId") or "")
                px = f.get("price")
                sz = f.get("size")
                fid = f.get("id")

                if not order_id or px is None or sz is None:
                    continue

                try:
                    dq = Decimal(str(sz))
                    dp = Decimal(str(px))
                    if dq <= 0 or dp <= 0:
                        continue
                except Exception:
                    continue

                # aggregate avg by order_id
                agg = _WS_FILL_AGG.get(order_id) or {"qty": Decimal("0"), "notional": Decimal("0"), "avg": None, "ts": 0.0}
                agg["qty"] = Decimal(str(agg["qty"])) + dq
                agg["notional"] = Decimal(str(agg["notional"])) + dq * dp
                agg["avg"] = (agg["notional"] / agg["qty"]) if agg["qty"] > 0 else None
                agg["ts"] = time.time()
                _WS_FILL_AGG[order_id] = agg

                # push event (non-blocking)
                evt = dict(f)
                evt["_ts_recv"] = time.time()
                try:
                    _FILL_Q.put_nowait(evt)
                except Exception:
                    pass

        except Exception as e:
            print("[apex_client][WS] handle_account error:", e)

    def _run():
        try:
            ws_client = WebSocket(
                endpoint=endpoint,
                api_key_credentials=api_creds,
            )
            ws_client.account_info_stream_v3(handle_account)
            # apexomni websocket may run internal thread; keep alive just in case
            while True:
                time.sleep(30)
        except Exception as e:
            print("[apex_client][WS] thread crashed:", e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    print("[apex_client][WS] started account_info_stream_v3")


def pop_fill_event(timeout: float = 0.5) -> Optional[dict]:
    try:
        return _FILL_Q.get(timeout=timeout)
    except Exception:
        return None


def _ws_fill_summary(order_id: Optional[str], client_order_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    用 WS 聚合的 fills 给 entry “秒级拿均价”加速。
    """
    now = time.time()
    if order_id:
        agg = _WS_FILL_AGG.get(str(order_id))
        if agg and (now - float(agg.get("ts", 0.0)) <= float(os.getenv("WS_FILL_TTL_SEC", "10"))):
            avg = agg.get("avg")
            qty = agg.get("qty")
            if avg and qty and Decimal(str(qty)) > 0:
                return {
                    "order_id": str(order_id),
                    "client_order_id": client_order_id,
                    "filled_qty": Decimal(str(qty)),
                    "avg_fill_price": Decimal(str(avg)),
                }
    return None


# ------------------------------------------------------------
# ✅ 真实成交价格获取：优先 WS，再 fallback 轮询 REST（保留你原风格）
# ------------------------------------------------------------
def get_fill_summary(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    max_wait_sec: float = 2.0,
    poll_interval: float = 0.25,
) -> Dict[str, Any]:
    # 先尝试 WS
    ws_hit = _ws_fill_summary(order_id, client_order_id)
    if ws_hit:
        return {
            "symbol": symbol,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "filled_qty": ws_hit["filled_qty"],
            "avg_fill_price": ws_hit["avg_fill_price"],
            "raw_order": None,
            "raw_fills": {"source": "ws"},
        }

    client = get_client()
    start = time.time()

    while True:
        last_err = None

        candidates = [
            "get_order_v3",
            "get_order_detail_v3",
            "get_order_by_id_v3",
            "get_order_v3_by_id",
        ]

        order_obj = None
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

        fills_obj = None
        fill_candidates = [
            "get_fills_v3",
            "get_order_fills_v3",
            "get_trade_fills_v3",
        ]
        for name in fill_candidates:
            if hasattr(client, name):
                try:
                    fn = getattr(client, name)
                    if order_id:
                        try:
                            fills_obj = fn(orderId=order_id)
                        except Exception:
                            fills_obj = fn(order_id)
                    elif client_order_id:
                        try:
                            fills_obj = fn(clientId=client_order_id)
                        except Exception:
                            fills_obj = fn(client_order_id)
                    else:
                        continue
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
                if isinstance(data, dict) and "fills" in data and isinstance(data["fills"], list):
                    return data["fills"]
            return []

        fills_list = _as_list(fills_obj)

        total_qty = Decimal("0")
        total_notional = Decimal("0")

        for f in fills_list:
            if not isinstance(f, dict):
                continue
            q = f.get("size") or f.get("qty") or f.get("filledSize") or f.get("fillSize")
            p = f.get("price") or f.get("fillPrice")
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

        if isinstance(order_obj, dict):
            data = order_obj.get("data") if isinstance(order_obj.get("data"), dict) else order_obj
            if isinstance(data, dict):
                fq = data.get("filledSize") or data.get("cumFilledSize") or data.get("sizeFilled")
                ap = data.get("avgPrice") or data.get("averagePrice") or data.get("fillAvgPrice")
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
                                "raw_order": order_obj,
                                "raw_fills": fills_obj,
                            }
                except Exception as e:
                    last_err = e

        if time.time() - start >= max_wait_sec:
            raise RuntimeError(f"fill summary unavailable, last_err={last_err!r}")

        time.sleep(poll_interval)


# ------------------------------------------------------------
# ✅ Create Orders
# ------------------------------------------------------------
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
    trigger_str = str(trigger_price)

    # price is required by some SDK/builds even for *_MARKET; fallback to worstPrice
    if price is None:
        try:
            px = get_market_price(symbol, side, size_str)
        except Exception:
            px = trigger_str
        price = px

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


# ------------------------------------------------------------
# ✅ 远程查仓兜底（保持你原逻辑）
# ------------------------------------------------------------
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
