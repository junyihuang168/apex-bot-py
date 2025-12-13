import os
import time
import threading
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, Optional, Tuple, List

import requests

# ---- SDK imports (兼容 apexomni / apexpro 两种命名) ----
try:
    from apexomni.constants import (
        APEX_OMNI_HTTP_MAIN,
        APEX_OMNI_HTTP_TEST,
        NETWORKID_OMNI_MAIN_ARB,
    )
    from apexomni.http_public import HttpPublic
    from apexomni.http_private_v3 import HttpPrivateSign
except Exception:  # pragma: no cover
    from apexpro.constants import (  # type: ignore
        APEX_OMNI_HTTP_MAIN,
        APEX_OMNI_HTTP_TEST,
        NETWORKID_OMNI_MAIN_ARB,
    )
    from apexpro.http_public import HttpPublic  # type: ignore
    from apexpro.http_private_v3 import HttpPrivateSign  # type: ignore


# -----------------------------
# Helpers
# -----------------------------
def _d(x: Any) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return Decimal("0")


class ApexClient:
    """
    Public:  HttpPublic(APEX_OMNI_HTTP_MAIN/TEST)  + depth_v3/ticker_v3/configs_v3 ...
    Private: HttpPrivateSign(APEX_OMNI_HTTP_MAIN/TEST, network_id=..., api_key_credentials=..., zk_seeds=..., l2key_seeds=...)
    (按官方 V3 示例/命名) :contentReference[oaicite:4]{index=4}
    """

    def __init__(self) -> None:
        use_main = os.getenv("APEX_USE_MAINNET", "true").lower() == "true"
        self.http_host = APEX_OMNI_HTTP_MAIN if use_main else APEX_OMNI_HTTP_TEST
        self.network_id = NETWORKID_OMNI_MAIN_ARB  # mainnet omni arb

        # creds
        key = os.getenv("APEX_API_KEY", "").strip()
        secret = os.getenv("APEX_API_SECRET", "").strip()
        passphrase = os.getenv("APEX_API_PASSPHRASE", "").strip()

        # zk seeds (ApeX v3 私有端一般需要)
        zk_seeds = os.getenv("APEX_ZK_SEEDS", "").strip()
        l2key_seeds = os.getenv("APEX_L2KEY_SEEDS", "").strip()

        if not key or not secret or not passphrase:
            raise RuntimeError("Missing APEX_API_KEY / APEX_API_SECRET / APEX_API_PASSPHRASE")

        self.public = HttpPublic(self.http_host)

        # Private signer client
        # 注意：不同版本 SDK 参数名可能略有差异；这里按官方 V3 示例写法 :contentReference[oaicite:5]{index=5}
        self.private = HttpPrivateSign(
            self.http_host,
            network_id=self.network_id,
            api_key_credentials={"key": key, "secret": secret, "passphrase": passphrase},
            zk_seeds=zk_seeds if zk_seeds else None,
            l2key_seeds=l2key_seeds if l2key_seeds else None,
        )

        self._cfg_lock = threading.Lock()
        self._cfg_cached: Optional[Dict[str, Any]] = None
        self._symbol_rules: Dict[str, Dict[str, Decimal]] = {}
        self._default_rules: Dict[str, Decimal] = {}

        self._ensure_configs_and_account()

    # -----------------------------
    # Init/cache
    # -----------------------------
    def _ensure_configs_and_account(self) -> None:
        with self._cfg_lock:
            if self._cfg_cached is not None:
                return
            cfg = self.private.configs_v3()
            _ = self.private.get_account_v3()
            self._cfg_cached = cfg
            self._build_symbol_rules_from_configs(cfg)

    def _build_symbol_rules_from_configs(self, cfg: Dict[str, Any]) -> None:
        default_min_qty = _d(os.getenv("DEFAULT_MIN_QTY", "0.01"))
        default_step = _d(os.getenv("DEFAULT_STEP_SIZE", "0.01"))
        default_tick = _d(os.getenv("DEFAULT_TICK_SIZE", "0.01"))
        self._default_rules = {"min_qty": default_min_qty, "step_size": default_step, "tick_size": default_tick}

        rules: Dict[str, Dict[str, Decimal]] = {}

        def _walk(obj: Any) -> None:
            if isinstance(obj, dict):
                # configs 里可能是 token/symbol 结构，也可能是 market 列表
                if ("symbol" in obj) and ("stepSize" in obj or "minSize" in obj or "tickSize" in obj):
                    sym = str(obj.get("symbol"))
                    step = _d(obj.get("stepSize") or obj.get("step") or default_step)
                    minq = _d(obj.get("minSize") or obj.get("minQty") or default_min_qty)
                    tick = _d(obj.get("tickSize") or obj.get("tick") or default_tick)
                    rules[sym] = {"min_qty": minq, "step_size": step, "tick_size": tick}
                for v in obj.values():
                    _walk(v)
            elif isinstance(obj, list):
                for it in obj:
                    _walk(it)

        _walk(cfg)
        self._symbol_rules = rules

    def get_symbol_rules(self, symbol: str) -> Dict[str, Decimal]:
        self._ensure_configs_and_account()
        return self._symbol_rules.get(symbol, dict(self._default_rules))

    def snap_qty(self, symbol: str, qty: Decimal) -> Decimal:
        r = self.get_symbol_rules(symbol)
        step = r["step_size"]
        minq = r["min_qty"]
        if qty < minq:
            return Decimal("0")
        snapped = (qty / step).to_integral_value(rounding=ROUND_DOWN) * step
        if snapped < minq:
            return Decimal("0")
        return snapped

    def snap_price(self, symbol: str, price: Decimal) -> Decimal:
        r = self.get_symbol_rules(symbol)
        tick = r["tick_size"]
        if tick <= 0:
            return price
        return (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick

    # -----------------------------
    # Public price helpers
    # -----------------------------
    def get_best_bid_ask(self, symbol: str, limit: int = 5) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        try:
            depth = self.public.depth_v3(symbol=symbol, limit=limit)
            data = depth.get("data") or depth
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            best_bid = _d(bids[0][0]) if bids and len(bids[0]) >= 1 else None
            best_ask = _d(asks[0][0]) if asks and len(asks[0]) >= 1 else None
            if best_bid is not None and best_bid <= 0:
                best_bid = None
            if best_ask is not None and best_ask <= 0:
                best_ask = None
            return best_bid, best_ask
        except Exception:
            return None, None

    def get_last_price(self, symbol: str) -> Optional[Decimal]:
        try:
            ticker = self.public.ticker_v3(symbol=symbol)
            data = ticker.get("data") or ticker
            for k in ("lastPrice", "last", "price", "markPrice"):
                if k in data and _d(data[k]) > 0:
                    return _d(data[k])
            return None
        except Exception:
            return None

    def price_for_market_order(self, symbol: str, side: str) -> Decimal:
        """
        MARKET 单依然要带 price：用盘口/last 推一个可成交的价格（轻微滑点）。
        """
        slippage_bps = _d(os.getenv("SLIPPAGE_BPS", "10"))  # 10 bps = 0.10%
        slip = slippage_bps / Decimal("10000")

        bid, ask = self.get_best_bid_ask(symbol)
        last = self.get_last_price(symbol)

        if side.upper() == "BUY":
            base = ask or last or bid
            if base is None:
                raise RuntimeError("ticker/depth unavailable")
            px = base * (Decimal("1") + slip)
        else:
            base = bid or last or ask
            if base is None:
                raise RuntimeError("ticker/depth unavailable")
            px = base * (Decimal("1") - slip)

        px = self.snap_price(symbol, _d(px))
        if px <= 0:
            px = _d(base or "0")
        return px

    # -----------------------------
    # Account / positions (从 get_account_v3 解析 positions)
    # -----------------------------
    def get_account(self) -> Dict[str, Any]:
        self._ensure_configs_and_account()
        return self.private.get_account_v3()

    def get_open_position_size(self, symbol: str, direction: str) -> Decimal:
        """
        direction: LONG/SHORT
        从 get_account_v3 返回的 positions 列表里找 size。
        """
        acc = self.get_account()
        data = acc.get("data") or acc
        positions = data.get("positions") or []
        for p in positions:
            if str(p.get("symbol")) == symbol and str(p.get("side")).upper() == direction.upper():
                return _d(p.get("size") or "0")
        return Decimal("0")

    # -----------------------------
    # Orders / fills
    # -----------------------------
    def create_market_order(
        self,
        symbol: str,
        side: str,
        size: Decimal,
        reduce_only: bool,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._ensure_configs_and_account()

        size = self.snap_qty(symbol, size)
        if size <= 0:
            raise RuntimeError(f"size too small after snap: {size}")

        price = self.price_for_market_order(symbol, side)
        ts_sec = int(time.time())  # 文档示例使用 timestampSeconds 

        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "size": str(size),
            "price": str(price),
            "timestampSeconds": ts_sec,
            "reduceOnly": bool(reduce_only),
        }
        if client_order_id:
            params["clientOrderId"] = client_order_id

        return self.private.create_order_v3(**params)

    def get_order(self, order_id: str) -> Dict[str, Any]:
        self._ensure_configs_and_account()
        # 注意：用 keyword，避免 “positional argument” 的 TypeError
        return self.private.get_order_v3(id=order_id)

    def fills(self, symbol: Optional[str] = None, side: Optional[str] = None, limit: int = 100, page: int = 0) -> Dict[str, Any]:
        self._ensure_configs_and_account()
        kwargs: Dict[str, Any] = {"limit": limit, "page": page}
        if symbol:
            kwargs["symbol"] = symbol
        if side:
            kwargs["side"] = side
        return self.private.fills_v3(**kwargs)

    def get_fill_summary(
        self,
        symbol: str,
        order_id: str,
        max_wait_sec: float = 3.0,
        poll_interval: float = 0.2,
    ) -> Dict[str, Any]:
        """
        优先轮询 get_order_v3 里的 averagePrice / latestMatchFillPrice / cumMatchFillSize；
        不行再 fallback fills_v3 尝试匹配 orderId。 
        """
        t0 = time.time()
        last_err: Optional[str] = None

        while True:
            # 1) try order
            try:
                od = self.get_order(order_id)
                data = od.get("data") or od

                filled_qty = _d(data.get("cumMatchFillSize") or data.get("cumSuccessFillSize") or "0")
                last_px = _d(data.get("latestMatchFillPrice") or "0")
                avg_px = _d(data.get("averagePrice") or data.get("avgPrice") or "0")
                status = str(data.get("status") or "")

                if filled_qty > 0:
                    px = avg_px if avg_px > 0 else last_px
                    return {
                        "ok": True,
                        "order_id": order_id,
                        "filled_qty": str(filled_qty),
                        "avg_price": str(px if px > 0 else last_px),
                        "status": status,
                        "source": "order",
                    }
            except Exception as e:
                last_err = f"get_order_v3 failed: {e}"

            # 2) fallback fills
            try:
                fr = self.fills(symbol=symbol, limit=100, page=0)
                data = fr.get("data") or fr
                orders = data.get("orders") or data.get("fills") or []

                matches = [o for o in orders if str(o.get("orderId") or o.get("id")) == str(order_id)]
                if matches:
                    qty_sum = Decimal("0")
                    value_sum = Decimal("0")
                    for f in matches:
                        q = _d(f.get("size") or f.get("cumMatchFillSize") or "0")
                        p = _d(f.get("price") or f.get("latestMatchFillPrice") or "0")
                        if q > 0 and p > 0:
                            qty_sum += q
                            value_sum += q * p
                    if qty_sum > 0:
                        avg = (value_sum / qty_sum) if value_sum > 0 else Decimal("0")
                        return {
                            "ok": True,
                            "order_id": order_id,
                            "filled_qty": str(qty_sum),
                            "avg_price": str(avg),
                            "status": "FILLED_OR_PARTIAL",
                            "source": "fills",
                        }
            except Exception as e:
                last_err = (last_err or "") + f" | fills_v3 failed: {e}"

            if time.time() - t0 >= max_wait_sec:
                return {
                    "ok": False,
                    "order_id": order_id,
                    "filled_qty": "0",
                    "avg_price": "0",
                    "status": "UNKNOWN",
                    "last_err": last_err,
                }

            time.sleep(poll_interval)


_client: Optional[ApexClient] = None


def get_client() -> ApexClient:
    global _client
    if _client is None:
        _client = ApexClient()
    return _client
