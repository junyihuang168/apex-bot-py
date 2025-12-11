import os
import time
import random
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Union, Tuple

# ✅ 尝试引入 Public 客户端，如果不存在也不报错，后面有处理
try:
    from apexomni.http_public import HttpPublic
except ImportError:
    HttpPublic = None

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.http_private_v3 import HttpPrivate_v3
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

# -------------------------------------------------------------------
# 交易规则
# -------------------------------------------------------------------
DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),
    "step_size": Decimal("0.01"),
    "qty_decimals": 2,
}

SYMBOL_RULES: Dict[str, Dict[str, Any]] = {}

_CLIENT: Optional[HttpPrivateSign] = None


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
        # 尝试调用一下 configs_v3 测试连接，如果这个也没有就忽略
        if hasattr(client, 'configs_v3'):
            client.configs_v3()
    except Exception as e:
        print("[apex_client] WARNING configs_v3 error:", e)
    _CLIENT = client
    return client


def get_account():
    client = get_client()
    return client.get_account_v3()


def get_market_price(symbol: str, side: str, size: str) -> str:
    """
    获取市价单预估的最差成交价 (Worst Price)。
    """
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()
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
    return str(price)


def get_ticker_price(symbol: str) -> str:
    """
    【修复版 v3】获取风控监控价格 (三级容错机制)。
    """
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()
    
    # 准备好 Private 客户端 (用来做 Plan B 和 C)
    private_client = HttpPrivate_v3(base_url, network_id=network_id, api_key_credentials=api_creds)
    
    # 准备 Public 客户端 (用来做 Plan A)，如果 SDK 支持的话
    public_client = None
    if HttpPublic:
        try:
            public_client = HttpPublic(base_url)
        except Exception:
            pass

    # --- 方案 A: 尝试通过 Ticker 接口获取 ---
    # 尝试 public 和 private 两个客户端，尝试 ticker_v3 和 get_tickers_v3 两个名字
    clients_to_try = [c for c in [public_client, private_client] if c]
    method_names = ['ticker_v3', 'get_tickers_v3', 'get_ticker_v3']
    
    for client in clients_to_try:
        for method_name in method_names:
            if hasattr(client, method_name):
                try:
                    fn = getattr(client, method_name)
                    res = fn(symbol=symbol)
                    
                    # 解析返回值
                    data = res.get("data", res) if isinstance(res, dict) else res
                    item = data[0] if isinstance(data, list) and data else data
                    
                    if isinstance(item, dict):
                        price = item.get("oraclePrice") or item.get("lastPrice") or item.get("price")
                        if price:
                            return str(price)
                except Exception:
                    pass # 失败就继续试下一个

    # --- 方案 B: 尝试通过 Depth (深度图) 获取 ---
    # 大部分 SDK 只要能交易，就一定有 depth/orderbook 接口
    depth_method_names = ['depth_v3', 'get_depth_v3', 'get_orderbook_v3']
    for client in clients_to_try:
        for method_name in depth_method_names:
            if hasattr(client, method_name):
                try:
                    fn = getattr(client, method_name)
                    res = fn(symbol=symbol, limit=5)
                    
                    data = res.get("data", res) if isinstance(res, dict) else res
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    
                    if bids and asks:
                        # bids[0][0] 买一价, asks[0][0] 卖一价
                        bid1 = float(bids[0][0])
                        ask1 = float(asks[0][0])
                        mid_price = (bid1 + ask1) / 2
                        return str(mid_price)
                except Exception:
                    pass

    # --- 方案 C: 最后的保底 (Worst Price 平均值) ---
    # 如果上面全部失败，绝不能让程序报错崩溃，否则锁盈就彻底失效了。
    # 我们知道 get_market_price 是好的 (因为你能下单)。
    # 虽然 Worst Price 含滑点，但用来做止损监控总比不监控好。
    print(f"[apex_client] WARNING: Ticker/Depth failed, using worst_price fallback for {symbol}")
    try:
        # 获取买卖两个方向的最差价，取平均，抵消部分滑点影响
        p_buy = Decimal(get_market_price(symbol, "BUY", "0.01"))
        p_sell = Decimal(get_market_price(symbol, "SELL", "0.01"))
        avg = (p_buy + p_sell) / 2
        return str(avg)
    except Exception as e:
        # 如果连这个都挂了，那我也没办法了
        raise RuntimeError(f"CRITICAL: All price fetch methods failed for {symbol}: {e}")


NumberLike = Union[str, float, int]


def _extract_order_ids(raw_order: Any) -> Tuple[Optional[str], Optional[str]]:
    order_id = None
    client_order_id = None
    def _pick(d: dict, *keys):
        for k in keys:
            v = d.get(k)
            if v is not None: return v
        return None
    if isinstance(raw_order, dict):
        order_id = _pick(raw_order, "orderId", "id")
        client_order_id = _pick(raw_order, "clientOrderId", "clientId")
        data = raw_order.get("data")
        if isinstance(data, dict):
            order_id = order_id or _pick(data, "orderId", "id")
            client_order_id = client_order_id or _pick(data, "clientOrderId", "clientId")
    return (str(order_id) if order_id else None, str(client_order_id) if client_order_id else None)


def get_fill_summary(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    max_wait_sec: float = 2.0,
    poll_interval: float = 0.25,
) -> Dict[str, Any]:
    client = get_client()
    start = time.time()
    while True:
        last_err = None
        candidates = ["get_order_v3", "get_order_detail_v3", "get_order_by_id_v3", "get_order_v3_by_id"]
        order_obj = None
        for name in candidates:
            if hasattr(client, name) and order_id:
                try:
                    fn = getattr(client, name)
                    order_obj = fn(orderId=order_id) if "orderId" in fn.__code__.co_varnames else fn(order_id)
                    break
                except Exception as e:
                    last_err = e

        fills_obj = None
        fill_candidates = ["get_fills_v3", "get_order_fills_v3", "get_trade_fills_v3"]
        for name in fill_candidates:
            if hasattr(client, name):
                try:
                    fn = getattr(client, name)
                    if order_id and "orderId" in fn.__code__.co_varnames:
                        fills_obj = fn(orderId=order_id)
                    elif client_order_id and "clientId" in fn.__code__.co_varnames:
                        fills_obj = fn(clientId=client_order_id)
                    elif order_id:
                        fills_obj = fn(order_id)
                    else:
                        continue
                    break
                except Exception as e:
                    last_err = e

        def _as_list(x):
            if x is None: return []
            if isinstance(x, list): return x
            if isinstance(x, dict):
                data = x.get("data")
                if isinstance(data, list): return data
                if isinstance(data, dict) and "fills" in data and isinstance(data["fills"], list):
                    return data["fills"]
            return []

        fills_list = _as_list(fills_obj)
        total_qty = Decimal("0")
        total_notional = Decimal("0")
        for f in fills_list:
            if not isinstance(f, dict): continue
            q = f.get("size") or f.get("qty") or f.get("filledSize") or f.get("fillSize")
            p = f.get("price") or f.get("fillPrice")
            if q is None or p is None: continue
            dq = Decimal(str(q))
            dp = Decimal(str(p))
            if dq <= 0 or dp <= 0: continue
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
        if budget <= 0: raise ValueError("size_usdt must be > 0")
        min_qty = rules["min_qty"]
        ref_price_decimal = Decimal(get_market_price(symbol, side, str(min_qty)))
        theoretical_qty = budget / ref_price_decimal
        snapped_qty = _snap_quantity(symbol, theoretical_qty)
        price_str = get_market_price(symbol, side, str(snapped_qty))
        price_decimal = Decimal(price_str)
        size_str = format(snapped_qty, f".{decimals}f")
        used_budget = (snapped_qty * price_decimal).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        print(f"[apex_client] budget={budget} USDT -> qty={size_str}, used≈{used_budget} USDT")
    else:
        if size is None: raise ValueError("size or size_usdt must be provided")
        size_str = str(size)
        price_str = get_market_price(symbol, side, size_str)
        price_decimal = Decimal(price_str)
        used_budget = (price_decimal * Decimal(size_str)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    ts = int(time.time())
    apex_client_id = _random_client_id()
    if client_id: print(f"[apex_client] tv_client_id={client_id} -> apex_clientId={apex_client_id}")
    else: print(f"[apex_client] apex_clientId={apex_client_id}")

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
            "price": price_str,
            "used_budget": str(used_budget),
            "reduce_only": reduce_only,
        },
    }
