import os
import time
import random
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Union, Tuple

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import APEX_OMNI_HTTP_MAIN, APEX_OMNI_HTTP_TEST, NETWORKID_OMNI_MAIN_ARB, NETWORKID_TEST

DEFAULT_SYMBOL_RULES = {"min_qty": Decimal("0.01"), "step_size": Decimal("0.01"), "qty_decimals": 2}
SYMBOL_RULES: Dict[str, Dict[str, Any]] = {}
_CLIENT: Optional[HttpPrivateSign] = None
NumberLike = Union[str, float, int]

def _get_symbol_rules(symbol: str) -> Dict[str, Any]:
    return {**DEFAULT_SYMBOL_RULES, **SYMBOL_RULES.get(symbol.upper(), {})}

def _snap_quantity(symbol: str, theoretical_qty: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    snapped = (theoretical_qty // rules["step_size"]) * rules["step_size"]
    snapped = snapped.quantize(Decimal("1").scaleb(-rules["qty_decimals"]), rounding=ROUND_DOWN)
    if snapped < rules["min_qty"]: raise ValueError(f"qty {snapped} < min {rules['min_qty']}")
    return snapped

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
    if _CLIENT is not None: return _CLIENT
    use_main = os.getenv("APEX_ENV", "").lower() in ("main", "prod")
    client = HttpPrivateSign(
        APEX_OMNI_HTTP_MAIN if use_main else APEX_OMNI_HTTP_TEST,
        network_id=NETWORKID_OMNI_MAIN_ARB if use_main else NETWORKID_TEST,
        zk_seeds=os.environ["APEX_ZK_SEEDS"],
        zk_l2Key=os.getenv("APEX_L2KEY_SEEDS") or "",
        api_key_credentials=_get_api_credentials(),
    )
    try:
        client.configs_v3()
        # print("[apex_client] configs_v3 ok") # 注释以防刷屏
    except Exception as e:
        print("[apex_client] config init error:", e)
    _CLIENT = client
    return client

def get_market_price(symbol: str, side: str, size: str) -> str:
    from apexomni.http_private_v3 import HttpPrivate_v3
    c = get_client()
    use_main = os.getenv("APEX_ENV", "").lower() in ("main", "prod")
    v3 = HttpPrivate_v3(
        APEX_OMNI_HTTP_MAIN if use_main else APEX_OMNI_HTTP_TEST,
        network_id=NETWORKID_OMNI_MAIN_ARB if use_main else NETWORKID_TEST,
        api_key_credentials=_get_api_credentials()
    )
    res = v3.get_worst_price_v3(symbol=symbol, size=str(size), side=side.upper())
    price = None
    if isinstance(res, dict):
        price = res.get("worstPrice")
        if not price and "data" in res: price = res["data"].get("worstPrice")
    if not price: raise RuntimeError(f"get_market_price failed: {res}")
    return str(price)

def _extract_order_ids(raw: Any) -> Tuple[Optional[str], Optional[str]]:
    oid, cid = None, None
    if isinstance(raw, dict):
        oid = raw.get("orderId") or raw.get("id")
        cid = raw.get("clientOrderId") or raw.get("clientId")
        if "data" in raw:
            oid = oid or raw["data"].get("orderId") or raw["data"].get("id")
            cid = cid or raw["data"].get("clientOrderId") or raw["data"].get("clientId")
    return (str(oid) if oid else None, str(cid) if cid else None)

# ✅ 仓库逻辑：真实成交回写 (Real Executable Price)
def get_fill_summary(symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None, max_wait_sec: float = 2.0, poll_interval: float = 0.25) -> Dict[str, Any]:
    client = get_client()
    start = time.time()
    while True:
        fills = []
        try:
            if hasattr(client, "get_fills_v3"):
                res = client.get_fills_v3(orderId=order_id) if order_id else (client.get_fills_v3(clientId=client_order_id) if client_order_id else [])
                fills = res.get("data", []) if isinstance(res, dict) else (res if isinstance(res, list) else [])
        except: pass
        
        t_qty, t_notional = Decimal("0"), Decimal("0")
        for f in fills:
            q = Decimal(str(f.get("size") or f.get("filledSize") or 0))
            p = Decimal(str(f.get("price") or f.get("fillPrice") or 0))
            if q > 0 and p > 0:
                t_qty += q
                t_notional += q * p
        
        if t_qty > 0:
            return {"symbol": symbol, "filled_qty": t_qty, "avg_fill_price": t_notional / t_qty}
            
        try:
            if hasattr(client, "get_order_v3") and order_id:
                o = client.get_order_v3(orderId=order_id)
                d = o.get("data", {}) if isinstance(o, dict) else o
                fq = Decimal(str(d.get("filledSize") or 0))
                ap = Decimal(str(d.get("avgPrice") or 0))
                if fq > 0 and ap > 0:
                     return {"symbol": symbol, "filled_qty": fq, "avg_fill_price": ap}
        except: pass

        if time.time() - start > max_wait_sec: raise RuntimeError("fill summary timeout")
        time.sleep(poll_interval)

# ✅ 仓库逻辑：远程查仓兜底
def get_open_position_for_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    client = get_client()
    try:
        acc = client.get_account_v3()
        positions = acc.get("data", {}).get("positions", [])
        norm = symbol.upper().replace("-", "")
        for p in positions:
            if p.get("symbol", "").upper().replace("-", "") == norm:
                sz = Decimal(str(p.get("size", 0)))
                if sz > 0: return {"side": p["side"], "size": sz, "entryPrice": Decimal(str(p["entryPrice"]))}
    except: pass
    return None

def map_position_side_to_exit_order_side(side: str) -> str:
    return "SELL" if side.upper() == "LONG" else "BUY"

def create_market_order(symbol: str, side: str, size: str, reduce_only: bool = False, client_id: str = None) -> Dict[str, Any]:
    client = get_client()
    price = get_market_price(symbol, side, size)
    params = {
        "symbol": symbol, "side": side.upper(), "type": "MARKET",
        "size": str(size), "price": price, "reduceOnly": reduce_only,
        "clientId": client_id or _random_client_id(), "timestampSeconds": int(time.time())
    }
    print(f"[apex_client] create_market_order: {symbol} {side} {size}")
    raw = client.create_order_v3(**params)
    oid, cid = _extract_order_ids(raw)
    used = (Decimal(size) * Decimal(price)).quantize(Decimal("0.01"))
    return {"data": raw.get("data", raw), "raw_order": raw, "order_id": oid, "client_order_id": cid, "computed": {"price": price, "used_budget": str(used)}}
