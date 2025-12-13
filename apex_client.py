import os
import time
import threading
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, Optional, Tuple

from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

from apexomni.http_public import HttpPublic
from apexomni.http_private_sign import HttpPrivateSign


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
    - Public:  HttpPublic(base_url)
    - Private: HttpPrivateSign(http_host, network_id=..., zk_seeds=..., zk_l2Key=..., api_key_credentials={...})
      (official create_order_v3 example uses HttpPrivateSign) :contentReference[oaicite:6]{index=6}
    """
    def __init__(self) -> None:
        self.base_url = os.getenv("APEX_BASE_URL", "https://omni.apex.exchange").strip()

        use_main = os.getenv("APEX_USE_MAINNET", "true").lower() == "true"
        self.http_host = APEX_OMNI_HTTP_MAIN if use_main else APEX_OMNI_HTTP_TEST
        self.network_id = NETWORKID_OMNI_MAIN_ARB if use_main else NETWORKID_TEST

        key = os.getenv("APEX_API_KEY", "").strip()
        secret = os.getenv("APEX_API_SECRET", "").strip()
        passphrase = os.getenv("APEX_API_PASSPHRASE", "").strip()

        zk_seeds = os.getenv("APEX_ZK_SEEDS", "").strip()
        zk_l2key = os.getenv("APEX_L2KEY_SEEDS", "").strip()  # 你面板里现在是空的，必须补上

        if not key or not secret or not passphrase:
            raise RuntimeError("Missing APEX_API_KEY / APEX_API_SECRET / APEX_API_PASSPHRASE")

        if not zk_seeds or not zk_l2key:
            raise RuntimeError("Missing APEX_ZK_SEEDS / APEX_L2KEY_SEEDS (required for signed private endpoints)")

        self.public = HttpPublic(self.base_url)
        self.private = HttpPrivateSign(
            self.http_host,
            network_id=self.network_id,
            zk_seeds=zk_seeds,
            zk_l2Key=zk_l2key,
            api_key_credentials={"key": key, "secret": secret, "passphrase": passphrase},
        )

        self._init_lock = threading.Lock()
        self._symbol_rules: Dict[str, Dict[str, Decimal]] = {}
        self._default_rules = {
            "min_qty": _d(os.getenv("DEFAULT_MIN_QTY", "0.01")),
            "step_size": _d(os.getenv("DEFAULT_STEP_SIZE", "0.01")),
            "tick_size": _d(os.getenv("DEFAULT_TICK_SIZE", "0.01")),
        }

        # warm-up (official flow calls configs/account before trading) :contentReference[oaicite:7]{index=7}
        self._ensure_configs_and_account()

    def _ensure_configs_and_account(self) -> None:
        with self._init_lock:
            if self._symbol_rules:
                return
            cfg = self.private.configs_v3()
            _ = self.private.get_account_v3()
            self._build_symbol_rules_from_configs(cfg)

    def _build_symbol_rules_from_configs(self, cfg: Dict[str, Any]) -> None:
        rules: Dict[str, Dict[str, Decimal]] = {}

        def _walk(obj: Any) -> None:
            if isinstance(obj, dict):
                if "symbol" in obj and ("stepSize" in obj or "minSize" in obj or "tickSize" in obj):
                    sym = str(obj.get("symbol"))
                    step = _d(obj.get("stepSize") or self._default_rules["step_size"])
                    minq = _d(obj.get("minSize") or self._default_rules["min_qty"])
                    tick = _d(obj.get("tickSize") or self._default_rules["tick_size"])
                    rules[sym] = {"min_qty": minq, "step_size": step, "tick_size": tick}
                for v in obj.values():
                    _walk(v)
            elif isinstance(obj, list):
                for it in obj:
                    _walk(it)

        _walk(cfg)
        self._symbol_rules = rules or {}

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
        return snapped if snapped >= minq else Decimal("0")

    def snap_price(self, symbol: str, price: Decimal) -> Decimal:
        r = self.get_symbol_rules(symbol)
        tick = r["tick_size"]
        if tick <= 0:
            return price
        return (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick

    # -----------------------------
    # Public price (depth_v3 schema uses a/b) 
    # -----------------------------
    def get_best_bid_ask(self, symbol: str, limit: int = 5) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        try:
            depth = self.public.depth_v3(symbol=symbol, limit=limit)
            data = depth.get("data") if isinstance(depth, dict) else None
            if not isinstance(data, dict):
                return None, None
            asks = data.get("a") or []
            bids = data.get("b") or []
            best_ask = _d(asks[0][0]) if asks and len(asks[0]) >= 1 else None
            best_bid = _d(bids[0][0]) if bids and len(bids[0]) >= 1 else None
            if best_ask is not None and best_ask <= 0:
                best_ask = None
            if best_bid is not None and best_bid <= 0:
                best_bid = None
            return best_bid, best_ask
        except Exception:
            return None, None

    def get_last_price(self, symbol: str) -> Optional[Decimal]:
        try:
            t = self.public.ticker_v3(symbol=symbol)
            data = t.get("data") if isinstance(t, dict) else None
            if isinstance(data, list) and data:
                row = data[0]
                for k in ("lastPrice", "last", "price", "markPrice"):
                    if k in row and _d(row[k]) > 0:
                        return _d(row[k])
            return None
        except Exception:
            return None

    def price_for_market_order(self, symbol: str, side: str) -> Decimal:
        """
        Apex create_order_v3 requires a price even for MARKET; use depth/ticker + small slippage.
        """
        slippage_bps = _d(os.getenv("SLIPPAGE_BPS", "10"))  # 10 bps = 0.10%
        slippage = slippage_bps / Decimal("10000")

        bid, ask = self.get_best_bid_ask(symbol)
        last = self.get_last_price(symbol)

        if side.upper() == "BUY":
            base = ask or last or bid
            if base is None:
                raise RuntimeError("ticker/depth unavailable")
            px = base * (Decimal("1") + slippage)
        else:
            base = bid or last or ask
            if base is None:
                raise RuntimeError("ticker/depth unavailable")
            px = base * (Decimal("1") - slippage)

        px = self.snap_price(symbol, _d(px))
        if px <= 0:
            px = _d(base or "0")
        return px

    # -----------------------------
    # Orders / fills (v3)
    # -----------------------------
    def create_market_order(
        self,
        symbol: str,
        side: str,                 # BUY/SELL
        size: Decimal,             # base asset qty
        reduce_only: bool,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._ensure_configs_and_account()

        size = self.snap_qty(symbol, size)
        if size <= 0:
            raise RuntimeError(f"size too small after snap: {size}")

        price = self.price_for_market_order(symbol, side)
        ts_sec = int(time.time())

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

        return self.private.create_order_v3(**params)  # official method name :contentReference[oaicite:9]{index=9}

    def get_order(self, order_id: str) -> Dict[str, Any]:
        return self.private.get_order_v3(id=order_id)

    def fills(self, symbol: Optional[str] = None, limit: int = 100, page: int = 0) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {"limit": limit, "page": page}
        if symbol:
            kwargs["symbol"] = symbol
        return self.private.fills_v3(**kwargs)

    def get_fill_summary(
        self,
        symbol: str,
        order_id: str,
        max_wait_sec: float = 3.0,
        poll_interval: float = 0.2,
    ) -> Dict[str, Any]:
        """
        先 get_order_v3(id=...) 轮询 averagePrice/latestMatchFillPrice/cumMatchFillSize；
        不行再用 fills_v3(symbol=...) 尝试按 orderId 匹配。
        （官方确实提供 fills 接口）:contentReference[oaicite:10]{index=10}
        """
        t0 = time.time()
        last_err: Optional[str] = None

        while True:
            try:
                od = self.get_order(order_id)
                data = od.get("data") or od
                filled_qty = _d(data.get("cumMatchFillSize") or data.get("cumSuccessFillSize") or "0")
                last_fill_px = _d(data.get("latestMatchFillPrice") or "0")
                avg_px = _d(data.get("averagePrice") or data.get("avgPrice") or "0")
                status = str(data.get("status") or "")

                if filled_qty > 0:
                    px = avg_px if avg_px > 0 else last_fill_px
                    return {
                        "ok": True,
                        "order_id": order_id,
                        "filled_qty": str(filled_qty),
                        "avg_price": str(px if px > 0 else last_fill_px),
                        "status": status,
                        "source": "order",
                    }
            except Exception as e:
                last_err = f"get_order_v3 failed: {e}"

            try:
                fr = self.fills(symbol=symbol, limit=100, page=0)
                data = fr.get("data") if isinstance(fr, dict) else None
                orders = []
                if isinstance(data, dict) and isinstance(data.get("orders"), list):
                    orders = data.get("orders", [])
                elif isinstance(fr.get("orders"), list):
                    orders = fr.get("orders", [])

                match = [o for o in orders if str(o.get("orderId") or o.get("id")) == str(order_id)]
                if match:
                    qty_sum = Decimal("0")
                    value_sum = Decimal("0")
                    for f in match:
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
