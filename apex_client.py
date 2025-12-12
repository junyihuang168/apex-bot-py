import os
import time
import random
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, List, Tuple

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

# 这些类名在不同版本的 apexomni SDK 可能略有差异：
# 我做了“延迟导入 + 兼容分支”，避免你再遇到签名不匹配/类不存在直接崩。
try:
    from apexomni.http_private_v3 import HttpPrivate_v3
except Exception:
    HttpPrivate_v3 = None  # type: ignore


# -------------------------------------------------------------------
# 交易规则（默认：最小数量 0.01，步长 0.01，小数点后 2 位）
# 如某些币不同，可在 SYMBOL_RULES 里覆盖
# -------------------------------------------------------------------
DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),
    "step_size": Decimal("0.01"),
    "qty_dp": 2,
}

SYMBOL_RULES = {
    # "ZEC-USDT": {"min_qty": Decimal("0.01"), "step_size": Decimal("0.01"), "qty_dp": 2},
}


def _env_bool(name: str, default: bool = False) -> bool:
    v = str(os.getenv(name, "")).strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def _get_symbol_rules(symbol: str) -> Dict[str, Decimal]:
    r = SYMBOL_RULES.get(symbol, None)
    if not r:
        r = DEFAULT_SYMBOL_RULES
    return {
        "min_qty": Decimal(str(r["min_qty"])),
        "step_size": Decimal(str(r["step_size"])),
        "qty_dp": int(r["qty_dp"]),
    }


def _snap_quantity(symbol: str, qty: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    step = rules["step_size"]
    if step <= 0:
        return qty

    snapped = (qty / step).to_integral_value(rounding=ROUND_DOWN) * step

    # 再按小数位截断（保险）
    dp = rules["qty_dp"]
    fmt = Decimal("1") / (Decimal("10") ** dp)
    snapped = (snapped // fmt) * fmt

    if snapped < rules["min_qty"]:
        return Decimal("0")
    return snapped


def _mk_private_client() -> Any:
    api_key = os.getenv("APEX_API_KEY", "")
    api_secret = os.getenv("APEX_API_SECRET", "")
    api_passphrase = os.getenv("APEX_API_PASSPHRASE", "")
    zk_seeds = os.getenv("APEX_ZK_SEEDS", "") or os.getenv("APEX_ZKSEEDS", "")
    l2key_seeds = os.getenv("APEX_L2KEY_SEEDS", "")

    use_main = _env_bool("APEX_USE_MAINNET", True) or (str(os.getenv("APEX_ENV", "")).lower() == "main")
    base_url = APEX_OMNI_HTTP_MAIN if use_main else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_OMNI_MAIN_ARB if use_main else NETWORKID_TEST

    if HttpPrivate_v3 is None:
        raise RuntimeError("apexomni HttpPrivate_v3 not available in your environment")

    signer = HttpPrivateSign(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        zk_seeds=zk_seeds,
        l2key_seeds=l2key_seeds,
    )

    # 不同 SDK 版本的构造器参数可能不同，这里做兼容尝试
    try:
        return HttpPrivate_v3(base_url, network_id, signer)
    except TypeError:
        try:
            return HttpPrivate_v3(base_url, network_id=network_id, signer=signer)
        except TypeError:
            return HttpPrivate_v3(host=base_url, network_id=network_id, signer=signer)


_PRIVATE = None


def _client() -> Any:
    global _PRIVATE
    if _PRIVATE is None:
        _PRIVATE = _mk_private_client()
    return _PRIVATE


# -------------------------------------------------------------------
# 下单：仍然需要 worstPrice 作为“限价保护参数”（很多交易所/SDK 的 market 实现会要求 price）
# 但：成交价（记账、风控）严格用 fills/avgFill，不再用 worstPrice 回填。
# -------------------------------------------------------------------
def get_worst_price(symbol: str, side: str, size: str) -> str:
    c = _client()
    side_u = side.upper()
    try:
        r = c.get_worst_price_v3(symbol=symbol, side=side_u, size=size)
    except TypeError:
        r = c.get_worst_price_v3(symbol, side_u, size)

    data = (r or {}).get("data", r) or {}
    px = data.get("worstPrice") or data.get("price") or data.get("worst_price")
    if px is None:
        raise RuntimeError(f"worst price unavailable for {symbol} {side_u} size={size}")
    return str(px)


def get_mark_price(symbol: str) -> str:
    """
    用 (worstBuy + worstSell)/2 做近似 mark/mid，避免你现在的“pnl 被系统性低估”
    导致 0.15% 明明到了却不抬锁盈。
    """
    rules = _get_symbol_rules(symbol)
    s = str(rules["min_qty"])
    wb = Decimal(get_worst_price(symbol, "BUY", s))
    ws = Decimal(get_worst_price(symbol, "SELL", s))
    mid = (wb + ws) / Decimal("2")
    return str(mid)


def create_market_order(
    symbol: str,
    side: str,
    size: str,
    reduce_only: bool,
    client_id: Optional[str] = None,
) -> Dict[str, Any]:
    c = _client()
    side_u = side.upper()

    # 市价单这里仍给 price=worstPrice（用于保护/兼容 SDK）
    px = get_worst_price(symbol, side_u, size)

    params = {
        "symbol": symbol,
        "side": side_u,
        "type": "MARKET",
        "size": size,
        "price": px,
        "reduceOnly": bool(reduce_only),
    }
    if client_id:
        params["clientId"] = client_id

    # 兼容不同 SDK 方法名
    try:
        resp = c.create_order_v3(**params)
    except Exception:
        resp = c.create_order_v3(params)

    # 统一输出结构，方便 app.py 解析
    out: Dict[str, Any] = {"raw": resp, "computed": {"worst_price": px}}

    data = (resp or {}).get("data", resp) or {}
    order_id = data.get("id") or data.get("orderId") or data.get("order_id")
    client_order_id = data.get("clientOrderId") or data.get("client_order_id") or data.get("clientId")

    out["order_id"] = str(order_id) if order_id is not None else None
    out["client_order_id"] = str(client_order_id) if client_order_id is not None else None
    out["data"] = data
    return out


# -------------------------------------------------------------------
# 订单查询（兼容 get_order_v3 签名差异）
# -------------------------------------------------------------------
def _get_order_detail_v3(order_id: Optional[str], client_order_id: Optional[str]) -> Dict[str, Any]:
    c = _client()

    # 尽量用 order_id
    if order_id:
        for call in (
            lambda: c.get_order_v3(order_id),
            lambda: c.get_order_v3(id=order_id),
            lambda: c.get_order_v3(orderId=order_id),
            lambda: c.get_order_v3(order_id=order_id),
        ):
            try:
                r = call()
                return (r or {}).get("data", r) or {}
            except TypeError:
                continue
            except Exception:
                continue

    # 再用 client_order_id
    if client_order_id:
        for call in (
            lambda: c.get_order_v3(client_order_id),
            lambda: c.get_order_v3(clientOrderId=client_order_id),
            lambda: c.get_order_v3(client_order_id=client_order_id),
        ):
            try:
                r = call()
                return (r or {}).get("data", r) or {}
            except TypeError:
                continue
            except Exception:
                continue

    return {}


def _extract_fill_list(resp: Any) -> List[Dict[str, Any]]:
    data = (resp or {}).get("data", resp) or {}
    if isinstance(data, dict):
        items = data.get("fills") or data.get("list") or data.get("data") or data.get("rows")
        if isinstance(items, list):
            return items
    if isinstance(data, list):
        return data
    return []


def _fetch_fills_v3(symbol: str, order_id: Optional[str], client_order_id: Optional[str], begin_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    c = _client()

    # 不同 SDK 版本方法名差异很大：这里做“多路尝试”
    candidates = []

    # 常见：fills_v3 / get_fills_v3
    if hasattr(c, "fills_v3"):
        candidates.append(lambda: c.fills_v3(symbol=symbol, begin=begin_ms, end=end_ms, orderId=order_id, clientOrderId=client_order_id))
        candidates.append(lambda: c.fills_v3(symbol=symbol, begin=begin_ms, end=end_ms))
    if hasattr(c, "get_fills_v3"):
        candidates.append(lambda: c.get_fills_v3(symbol=symbol, begin=begin_ms, end=end_ms, orderId=order_id, clientOrderId=client_order_id))
        candidates.append(lambda: c.get_fills_v3(symbol=symbol, begin=begin_ms, end=end_ms))

    # 有些版本叫 get_my_fills_v3 / my_fills_v3
    if hasattr(c, "get_my_fills_v3"):
        candidates.append(lambda: c.get_my_fills_v3(symbol=symbol, begin=begin_ms, end=end_ms, orderId=order_id, clientOrderId=client_order_id))
    if hasattr(c, "my_fills_v3"):
        candidates.append(lambda: c.my_fills_v3(symbol=symbol, begin=begin_ms, end=end_ms, orderId=order_id, clientOrderId=client_order_id))

    last_err = None
    for fn in candidates:
        try:
            r = fn()
            fills = _extract_fill_list(r)
            if fills:
                # 如传了 order_id/client_order_id，再做一次过滤（防止返回了别的成交）
                if order_id or client_order_id:
                    filtered = []
                    for f in fills:
                        oid = str(f.get("orderId") or f.get("order_id") or f.get("id") or "")
                        coid = str(f.get("clientOrderId") or f.get("client_order_id") or "")
                        if order_id and oid and oid == str(order_id):
                            filtered.append(f)
                        elif client_order_id and coid and coid == str(client_order_id):
                            filtered.append(f)
                    if filtered:
                        return filtered
                return fills
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise last_err
    return []


def get_fill_summary(
    symbol: str,
    order_id: Optional[str],
    client_order_id: Optional[str],
    max_wait_sec: float = 30.0,
    poll_interval: float = 0.5,
    fast_window_sec: float = 3.0,
    fast_poll_interval: float = 0.2,
) -> Dict[str, Any]:
    """
    - 前 fast_window_sec 秒：高频尝试（你要求的 2–3 秒）
    - 之后：降频直到 max_wait_sec
    - 订单 PENDING 时 fills 可能暂不可见：会持续轮询
    """
    start = time.time()
    start_ms = int(time.time() * 1000) - 60_000  # 往前 60s 缓冲，避免时钟偏差
    last_err = None

    while True:
        elapsed = time.time() - start
        if elapsed > max_wait_sec:
            break

        now_ms = int(time.time() * 1000) + 5_000

        try:
            fills = _fetch_fills_v3(symbol, order_id, client_order_id, start_ms, now_ms)
            if fills:
                total_qty = Decimal("0")
                total_notional = Decimal("0")

                for f in fills:
                    px = f.get("price") or f.get("fillPrice") or f.get("avgPrice") or f.get("matchPrice")
                    sz = f.get("size") or f.get("qty") or f.get("fillSize") or f.get("matchSize")
                    if px is None or sz is None:
                        continue
                    pxd = Decimal(str(px))
                    szd = Decimal(str(sz))
                    if szd <= 0:
                        continue
                    total_qty += szd
                    total_notional += pxd * szd

                if total_qty > 0:
                    avg = (total_notional / total_qty)
                    return {
                        "filled_qty": str(total_qty),
                        "avg_fill_price": str(avg),
                        "fills_count": len(fills),
                    }
        except Exception as e:
            last_err = e

        # 兜底：有些 SDK 订单详情里会带 averagePrice/cumMatchFillSize
        try:
            od = _get_order_detail_v3(order_id, client_order_id)
            avgp = od.get("averagePrice") or od.get("avgPrice") or od.get("avg_fill_price")
            cumq = od.get("cumMatchFillSize") or od.get("cumMatchFillSizeValue") or od.get("cumMatchFillSizeQty") or od.get("filledSize") or od.get("cumFilledSize")
            if avgp not in (None, "", "0", 0) and cumq not in (None, "", "0", 0):
                avgd = Decimal(str(avgp))
                cumd = Decimal(str(cumq))
                if avgd > 0 and cumd > 0:
                    return {
                        "filled_qty": str(cumd),
                        "avg_fill_price": str(avgd),
                        "fills_count": 0,
                    }
        except Exception as e:
            last_err = e

        # sleep
        if elapsed <= fast_window_sec:
            time.sleep(fast_poll_interval)
        else:
            time.sleep(poll_interval)

    raise RuntimeError(f"fill summary unavailable, last_err={repr(last_err)}")


# -------------------------------------------------------------------
# 查仓（用于 app.py 的远程兜底）
# -------------------------------------------------------------------
def get_open_position_for_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    c = _client()
    # 兼容：positions_v3 / get_positions_v3
    resp = None
    try:
        if hasattr(c, "positions_v3"):
            resp = c.positions_v3()
        elif hasattr(c, "get_positions_v3"):
            resp = c.get_positions_v3()
    except Exception:
        resp = None

    data = (resp or {}).get("data", resp) or {}
    positions = data.get("positions") or data.get("list") or data.get("data") or data.get("rows") or data
    if not isinstance(positions, list):
        return None

    for p in positions:
        sym = p.get("symbol")
        if sym != symbol:
            continue
        side = (p.get("side") or p.get("positionSide") or "").upper()
        size = p.get("size") or p.get("positionSize") or p.get("qty") or 0
        entry = p.get("entryPrice") or p.get("avgEntryPrice") or p.get("entry_price") or 0
        try:
            sized = Decimal(str(size))
            entryd = Decimal(str(entry))
        except Exception:
            continue

        if sized > 0 and side in ("LONG", "SHORT"):
            return {"symbol": symbol, "side": side, "size": sized, "entryPrice": entryd}

    return None


def map_position_side_to_exit_order_side(direction: str) -> str:
    d = direction.upper()
    return "SELL" if d == "LONG" else "BUY"
