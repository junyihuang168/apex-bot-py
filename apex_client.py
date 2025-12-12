# apex_client.py
import os
import time
import random
from decimal import Decimal, ROUND_DOWN

from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_MAIN,
    NETWORKID_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,  # keep
)

# NOTE:
# - 官网说明私有接口签名依赖 api_key_credentials['secret']，而不是 api_key=... 这种参数。
# - 你的报错：Http__init__() got unexpected keyword argument 'api_key'
#   说明你当前 SDK 版本不接受 api_key 这个命名；应传 api_key_credentials dict。:contentReference[oaicite:2]{index=2}

try:
    from apexomni.http_private_sign import HttpPrivateSign
except Exception:
    HttpPrivateSign = None

try:
    from apexomni.http_public_v3 import HttpPublic_v3
except Exception:
    HttpPublic_v3 = None

# -----------------------------
# Symbol rules (fallback)
# -----------------------------
DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),
    "step_size": Decimal("0.01"),
    "qty_dp": 2,
}

SYMBOL_RULES = {
    # "ZEC-USDT": {"min_qty": Decimal("0.01"), "step_size": Decimal("0.01"), "qty_dp": 2},
}


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def _host() -> str:
    use_main = _env_bool("APEX_USE_MAINNET", True)
    return APEX_OMNI_HTTP_MAIN if use_main else APEX_OMNI_HTTP_TEST


def _network_id() -> int:
    use_main = _env_bool("APEX_USE_MAINNET", True)
    return NETWORKID_MAIN if use_main else NETWORKID_TEST


def _chain_id() -> int:
    # ApeX Omni mainnet chainId in docs examples: NETWORKID_OMNI_MAIN_ARB :contentReference[oaicite:3]{index=3}
    use_main = _env_bool("APEX_USE_MAINNET", True)
    return NETWORKID_OMNI_MAIN_ARB if use_main else NETWORKID_TEST


_PRIVATE = None
_PUBLIC = None


def _mk_private_client():
    global _PRIVATE
    if _PRIVATE is not None:
        return _PRIVATE

    if HttpPrivateSign is None:
        raise RuntimeError("apexomni.http_private_sign.HttpPrivateSign import failed. Check requirements / SDK version.")

    api_key_credentials = {
        "key": os.getenv("APEX_API_KEY", "").strip(),
        "secret": os.getenv("APEX_API_SECRET", "").strip(),
        "passphrase": os.getenv("APEX_API_PASSPHRASE", "").strip(),
    }
    if not api_key_credentials["key"] or not api_key_credentials["secret"] or not api_key_credentials["passphrase"]:
        raise RuntimeError("Missing APEX_API_KEY / APEX_API_SECRET / APEX_API_PASSPHRASE env vars.")

    seeds = (os.getenv("APEX_ZK_SEEDS") or "").strip()
    l2key = (os.getenv("APEX_L2KEY_SEEDS") or "").strip()

    # 关键修复：不要用 api_key=... 这种写法；用 api_key_credentials=dict（官网签名示例就是这么用的）:contentReference[oaicite:4]{index=4}
    _PRIVATE = HttpPrivateSign(
        _host(),
        network_id=_network_id(),
        api_key_credentials=api_key_credentials,
        seeds=seeds if seeds else None,
        l2Key=l2key if l2key else None,
        chainId=_chain_id(),
    )
    return _PRIVATE


def _mk_public_client():
    global _PUBLIC
    if _PUBLIC is not None:
        return _PUBLIC
    if HttpPublic_v3 is None:
        # public client optional; if missing you can still run by relying on order response prices
        _PUBLIC = None
        return _PUBLIC
    _PUBLIC = HttpPublic_v3(_host(), network_id=_network_id())
    return _PUBLIC


def random_client_id() -> str:
    # docs show random client id approach :contentReference[oaicite:5]{index=5}
    return str(int(float(str(random.random())[2:])))


def _get_symbol_rules(symbol: str):
    r = SYMBOL_RULES.get(symbol)
    if r:
        return r
    return DEFAULT_SYMBOL_RULES


def _snap_quantity(qty: Decimal, symbol: str) -> Decimal:
    rules = _get_symbol_rules(symbol)
    step = rules["step_size"]
    minq = rules["min_qty"]
    if qty < minq:
        qty = minq
    # floor to step
    steps = (qty / step).quantize(Decimal("1"), rounding=ROUND_DOWN)
    return (steps * step).quantize(step)


def get_ticker_price(symbol: str, max_retry: int = 5, sleep_s: float = 0.25) -> Decimal | None:
    pub = _mk_public_client()
    if pub is None:
        return None
    last_err = None
    for _ in range(max_retry):
        try:
            # SDK naming can vary; try common ones
            for fn_name in ("ticker_v3", "get_ticker_v3", "ticker", "get_ticker"):
                fn = getattr(pub, fn_name, None)
                if fn:
                    res = fn(symbol=symbol)
                    data = res.get("data") if isinstance(res, dict) else None
                    if isinstance(data, dict):
                        px = data.get("price") or data.get("lastPrice") or data.get("indexPrice")
                        if px:
                            return Decimal(str(px))
            time.sleep(sleep_s)
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)
    return None


def create_market_order(symbol: str, side: str, size: Decimal, reduce_only: bool, client_id: str | None = None):
    """
    size: base qty (NOT USDT budget), already snapped.
    side: BUY or SELL
    """
    c = _mk_private_client()
    cid = client_id or random_client_id()

    # Try v3 create order
    for fn_name in ("create_order_v3", "create_order", "create_market_order"):
        fn = getattr(c, fn_name, None)
        if not fn:
            continue
        res = fn(
            symbol=symbol,
            side=side,
            type="MARKET",
            size=str(size),
            clientId=cid,
            reduceOnly=bool(reduce_only),
        )
        return res, cid

    raise RuntimeError("No create order method found on SDK client (expected create_order_v3/create_order/...)")



def get_order_v3(order_id: str | None = None, client_id: str | None = None):
    c = _mk_private_client()

    # 关键修复：一律用 keyword args，避免你现在这种 positional 触发 “takes 1 positional but 2 were given”
    fn = getattr(c, "get_order_v3", None) or getattr(c, "get_order", None)
    if not fn:
        raise RuntimeError("No get_order_v3/get_order method found on SDK client.")

    kwargs = {}
    if order_id:
        kwargs["orderId"] = order_id
    if client_id:
        kwargs["clientOrderId"] = client_id

    return fn(**kwargs)


def get_fill_summary(order_id: str, client_id: str | None = None) -> dict:
    """
    Returns:
      {
        "status": "...",
        "filled_size": Decimal,
        "avg_price": Decimal,
      }
    """
    res = get_order_v3(order_id=order_id, client_id=client_id)
    data = res.get("data") if isinstance(res, dict) else None
    if not isinstance(data, dict):
        return {"status": "UNKNOWN", "filled_size": Decimal("0"), "avg_price": Decimal("0")}

    status = str(data.get("status") or "UNKNOWN")
    filled = Decimal(str(data.get("cumMatchFillSize") or "0"))
    avg = Decimal(str(data.get("averagePrice") or "0"))

    # Some responses may expose latestMatchFillPrice as a fallback
    if avg == 0:
        lpx = data.get("latestMatchFillPrice") or data.get("lastFillPrice")
        if lpx:
            avg = Decimal(str(lpx))

    return {"status": status, "filled_size": filled, "avg_price": avg}


def wait_for_fill(order_id: str, client_id: str | None, fast_wait_s: float = 3.0, slow_wait_s: float = 60.0):
    """
    Fast poll for a few seconds, then slow poll up to 60s.
    """
    t0 = time.time()
    # fast
    while time.time() - t0 < fast_wait_s:
        fs = get_fill_summary(order_id, client_id)
        if fs["filled_size"] > 0 and fs["avg_price"] > 0:
            return fs
        time.sleep(0.2)

    # slow
    t1 = time.time()
    while time.time() - t1 < slow_wait_s:
        fs = get_fill_summary(order_id, client_id)
        if fs["filled_size"] > 0 and fs["avg_price"] > 0:
            return fs
        time.sleep(1.0)

    return {"status": "TIMEOUT", "filled_size": Decimal("0"), "avg_price": Decimal("0")}


def get_account_positions():
    c = _mk_private_client()
    fn = getattr(c, "get_account_v3", None) or getattr(c, "get_account", None)
    if not fn:
        raise RuntimeError("No get_account_v3/get_account method found on SDK client.")
    res = fn()
    data = res.get("data") if isinstance(res, dict) else None
    if not isinstance(data, dict):
        return []
    positions = data.get("positions") or []
    if isinstance(positions, list):
        return positions
    return []


def get_open_position_for_symbol(symbol: str):
    positions = get_account_positions()
    for p in positions:
        if str(p.get("symbol")) != symbol:
            continue
        size = Decimal(str(p.get("size") or "0"))
        side = str(p.get("side") or "")
        if size > 0 and side in ("LONG", "SHORT"):
            return {"symbol": symbol, "side": side, "size": size, "raw": p}
    return None


def map_position_side_to_exit_order_side(pos_side: str) -> str:
    return "SELL" if pos_side == "LONG" else "BUY"
