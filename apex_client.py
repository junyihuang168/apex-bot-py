import os
import time
import random
import inspect
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Union, Tuple, Callable

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

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
_PUBLIC_CLIENT: Optional[Any] = None


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


def _call_compat(fn: Callable, **kwargs):
    """
    用 inspect.signature 做参数兼容（比 __code__.co_varnames 稳定）
    """
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        filtered = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        return fn(**filtered)
    except Exception:
        # 兜底：如果签名不可用，直接尽力用 kwargs 调
        return fn(**{k: v for k, v in kwargs.items() if v is not None})


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
        print("[apex_client] configs_v3 ok:", cfg)
    except Exception as e:
        print("[apex_client] WARNING configs_v3 error:", e)

    try:
        acc = client.get_account_v3()
        print("[apex_client] get_account_v3 ok:", acc)
    except Exception as e:
        print("[apex_client] WARNING get_account_v3 error:", e)

    _CLIENT = client
    return client


def get_account():
    client = get_client()
    return client.get_account_v3()


# ---------------------------
# Public price (mark/last) – 用于风控/触发判断，更贴近图表
# ---------------------------
def get_public_client():
    """
    尽量初始化 public client（不同 apexomni 版本类名可能不同）
    """
    global _PUBLIC_CLIENT
    if _PUBLIC_CLIENT is not None:
        return _PUBLIC_CLIENT

    base_url, network_id = _get_base_and_network()

    candidates = [
        ("apexomni.http_public_v3", "HttpPublic_v3"),
        ("apexomni.http_public_v2", "HttpPublic_v2"),
        ("apexomni.http_public", "HttpPublic"),
    ]

    last_err = None
    for mod_name, cls_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=[cls_name])
            cls = getattr(mod, cls_name)
            _PUBLIC_CLIENT = cls(base_url, network_id=network_id)
            print(f"[apex_client] public client ok: {mod_name}.{cls_name}")
            return _PUBLIC_CLIENT
        except Exception as e:
            last_err = e

    raise RuntimeError(f"public client unavailable: {last_err!r}")


def _pick_price_from_ticker(obj: Any) -> Optional[str]:
    """
    解析各种 ticker 返回结构，尽量取 mark/last/index
    """
    if obj is None:
        return None

    if isinstance(obj, dict):
        d = obj.get("data") if isinstance(obj.get("data"), (dict, list)) else obj

        # data 可能是 list（多 symbol）
        if isinstance(d, list) and d:
            # 找第一个有价格的
            for it in d:
                if isinstance(it, dict):
                    for k in ("markPrice", "lastPrice", "indexPrice", "price", "close"):
                        v = it.get(k)
                        if v is not None:
                            return str(v)

        if isinstance(d, dict):
            for k in ("markPrice", "lastPrice", "indexPrice", "price", "close"):
                v = d.get(k)
                if v is not None:
                    return str(v)

    return None


def get_ticker_price(symbol: str, prefer: str = "mark") -> str:
    """
    风控/触发用：mark/last/index 价优先（贴近图表）
    prefer: "mark" / "last" / "index"
    """
    pub = get_public_client()

    # 可能的方法名
    methods = [
        "get_ticker_v3",
        "get_ticker",
        "ticker_v3",
        "get_mark_price_v3",
        "get_index_price_v3",
    ]

    last_err = None
    for name in methods:
        if hasattr(pub, name):
            try:
                fn = getattr(pub, name)

                # 尽量传 symbol
                res = None
                try:
                    res = _call_compat(fn, symbol=symbol)
                except Exception:
                    # 有些方法可能叫 market / instrument
                    res = _call_compat(fn, market=symbol)

                px = _pick_price_from_ticker(res)
                if px:
                    return px
            except Exception as e:
                last_err = e

    raise RuntimeError(f"ticker price unavailable for {symbol}, last_err={last_err!r}")


# ---------------------------
# Worst executable price – 用于下单保护（不用于记账默认，不用于风控触发）
# ---------------------------
def get_worst_price(symbol: str, side: str, size: str) -> str:
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


NumberLike = Union[str, float, int]


def _extract_order_ids(raw_order: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    尽量从 Apex 返回中拿 orderId / clientOrderId
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


def _as_fills_list(x: Any):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        data = x.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # 常见：data.fills
            fills = data.get("fills")
            if isinstance(fills, list):
                return fills
    return []


def get_fill_summary(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    max_wait_sec: float = 3.0,
    poll_interval_fast: float = 0.15,
    poll_interval_slow: float = 0.35,
    fast_window_sec: float = 2.2,
) -> Dict[str, Any]:
    """
    下单后：前 2–3 秒高频拉 fills。
    - 优先解析 fills 的逐笔，计算 avg_fill_price
    - 再退而求其次从 order_detail 里找 avgPrice/filledSize
    """
    client = get_client()
    start = time.time()
    last_err = None

    # 可能的候选方法名（不同版本 SDK 可能不一样）
    order_candidates = [
        "get_order_v3",
        "get_order_detail_v3",
        "get_order_by_id_v3",
        "get_order_v3_by_id",
        "get_order",
    ]

    fill_candidates = [
        "get_fills_v3",
        "get_order_fills_v3",
        "get_trade_fills_v3",
        "get_fills",
    ]

    while True:
        order_obj = None
        fills_obj = None

        # 1) order detail
        if order_id:
            for name in order_candidates:
                if hasattr(client, name):
                    try:
                        fn = getattr(client, name)
                        # 兼容参数
                        order_obj = None
                        try:
                            order_obj = _call_compat(fn, orderId=order_id, order_id=order_id, id=order_id, symbol=symbol)
                        except Exception:
                            # 有些 SDK 可能只接受位置参数
                            order_obj = fn(order_id)
                        break
                    except Exception as e:
                        last_err = e

        # 2) fills
        for name in fill_candidates:
            if hasattr(client, name):
                try:
                    fn = getattr(client, name)
                    fills_obj = None
                    # 优先 orderId
                    if order_id:
                        try:
                            fills_obj = _call_compat(fn, orderId=order_id, order_id=order_id, id=order_id, symbol=symbol)
                        except Exception:
                            fills_obj = fn(order_id)
                    # 再试 clientId
                    if fills_obj is None and client_order_id:
                        fills_obj = _call_compat(fn, clientId=client_order_id, client_id=client_order_id, symbol=symbol)

                    if fills_obj is not None:
                        break
                except Exception as e:
                    last_err = e

        fills_list = _as_fills_list(fills_obj)

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

        # 3) fallback: 从 order detail 找 filledSize/avgPrice
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

        elapsed = time.time() - start
        if elapsed >= max_wait_sec:
            raise RuntimeError(f"fill summary unavailable, last_err={last_err!r}")

        # 前 2.x 秒高频，之后降频
        interval = poll_interval_fast if elapsed <= fast_window_sec else poll_interval_slow
        time.sleep(interval)


def create_market_order(
    symbol: str,
    side: str,
    size: NumberLike | None = None,
    size_usdt: NumberLike | None = None,
    reduce_only: bool = False,
    client_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    MARKET 市价单。
    注意：
    - 下单 price 默认仍使用 worstPrice 作为“保护性 price”（防滑点/风控）
    - 真实成交价/触发判断由上层用 get_fill_summary / get_ticker_price 来做
    """
    client = get_client()
    side = side.upper()

    rules = _get_symbol_rules(symbol)
    decimals = rules["qty_decimals"]

    # 订单保护价模式：默认 worst（更稳，不容易被交易所拒单）
    order_price_mode = os.getenv("APEX_ORDER_PRICE_MODE", "worst").lower().strip()  # worst / last

    if size_usdt is not None:
        budget = Decimal(str(size_usdt))
        if budget <= 0:
            raise ValueError("size_usdt must be > 0")

        # 用 ticker 估算 qty（不走 worstPrice 做估值），如果 public 不可用才兜底 worst
        ref_px = None
        try:
            ref_px = Decimal(get_ticker_price(symbol, prefer="last"))
        except Exception as e:
            print("[apex_client] ticker price unavailable for sizing, fallback worst:", e)

        min_qty = rules["min_qty"]
        if ref_px is None or ref_px <= 0:
            ref_px = Decimal(get_worst_price(symbol, side, str(min_qty)))

        theoretical_qty = budget / ref_px
        snapped_qty = _snap_quantity(symbol, theoretical_qty)

        # 下单 price
        if order_price_mode == "last":
            try:
                price_str = get_ticker_price(symbol, prefer="last")
            except Exception:
                price_str = get_worst_price(symbol, side, str(snapped_qty))
        else:
            price_str = get_worst_price(symbol, side, str(snapped_qty))

        price_decimal = Decimal(str(price_str))
        size_str = format(snapped_qty, f".{decimals}f")

        used_budget = (snapped_qty * price_decimal).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        print(
            f"[apex_client] budget={budget} USDT -> qty={size_str}, "
            f"used≈{used_budget} USDT (order_price={price_str}, mode={order_price_mode})"
        )
    else:
        if size is None:
            raise ValueError("size or size_usdt must be provided")
        size_str = str(size)

        if order_price_mode == "last":
            try:
                price_str = get_ticker_price(symbol, prefer="last")
            except Exception:
                price_str = get_worst_price(symbol, side, size_str)
        else:
            price_str = get_worst_price(symbol, side, size_str)

        price_decimal = Decimal(str(price_str))
        used_budget = (price_decimal * Decimal(size_str)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

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
            "order_price": str(price_str),        # 下单保护价
            "used_budget": str(used_budget),
            "reduce_only": reduce_only,
            "order_price_mode": order_price_mode,
        },
    }


# ✅ 远程查仓函数（追加在后面）
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
