import os
import time
import json
import math
import threading
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, Optional, Tuple

import requests

from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

# v3 clients (per official docs)
from apexomni.http_private_v3 import HttpPrivate_v3
from apexomni.http_public_v3 import HttpPublic_v3


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


def _now_ms() -> int:
    return int(time.time() * 1000)


class ApexClient:
    """
    - Private client: HttpPrivate_v3(..., api_key_credentials={key,secret,passphrase})
    - Public client:  HttpPublic_v3(base_url)
    """
    def __init__(self) -> None:
        self.base_url = os.getenv("APEX_BASE_URL", "https://omni.apex.exchange")
        use_main = os.getenv("APEX_USE_MAINNET", "true").lower() == "true"
        self.http_host = APEX_OMNI_HTTP_MAIN if use_main else APEX_OMNI_HTTP_TEST
        self.network_id = NETWORKID_OMNI_MAIN_ARB if use_main else NETWORKID_TEST

        key = os.getenv("APEX_API_KEY", "").strip()
        secret = os.getenv("APEX_API_SECRET", "").strip()
        passphrase = os.getenv("APEX_API_PASSPHRASE", "").strip()

        if not key or not secret or not passphrase:
            raise RuntimeError("Missing APEX_API_KEY / APEX_API_SECRET / APEX_API_PASSPHRASE")

        self.private = HttpPrivate_v3(
            self.http_host,
            network_id=self.network_id,
            api_key_credentials={"key": key, "secret": secret, "passphrase": passphrase},
        )
        self.public = HttpPublic_v3(self.base_url)

        # cache
        self._configs_lock = threading.Lock()
        self._configs_cache: Optional[Dict[str, Any]] = None
        self._symbol_rules: Dict[str, Dict[str, Decimal]] = {}

        # one-time init (per docs)
        self._ensure_configs_and_account()

    # -----------------------------
    # Init/cache
    # -----------------------------
    def _ensure_configs_and_account(self) -> None:
        """
        Official docs show calling configs_v3() and get_account_v3() before using private endpoints. :contentReference[oaicite:6]{index=6}
        """
        with self._configs_lock:
            if self._configs_cache is not None:
                return
            cfg = self.private.configs_v3()
            _ = self.private.get_account_v3()
            self._configs_cache = cfg
            self._build_symbol_rules_from_configs(cfg)

    def _build_symbol_rules_from_configs(self, cfg: Dict[str, Any]) -> None:
        """
        Try to extract stepSize/minSize/tickSize from configs response.
        If not found, keep defaults.
        """
        # Safe defaults
        default_min_qty = Decimal(os.getenv("DEFAULT_MIN_QTY", "0.01"))
        default_step = Decimal(os.getenv("DEFAULT_STEP_SIZE", "0.01"))
        default_tick = Decimal(os.getenv("DEFAULT_TICK_SIZE", "0.01"))

        rules: Dict[str, Dict[str, Decimal]] = {}

        # The configs schema is large; try common locations.
        # We avoid printing the whole configs to logs to prevent massive output.
        def _walk(obj: Any) -> None:
            if isinstance(obj, dict):
                # possible symbol item
                if "symbol" in obj and ("stepSize" in obj or "minSize" in obj or "tickSize" in obj):
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

        # keep defaults fallback
        self._symbol_rules = rules
        self._default_rules = {"min_qty": default_min_qty, "step_size": default_step, "tick_size": default_tick}

    def get_symbol_rules(self, symbol: str) -> Dict[str, Decimal]:
        self._ensure_configs_and_account()
        return self._symbol_rules.get(symbol, dict(self._default_rules))

    def snap_qty(self, symbol: str, qty: Decimal) -> Decimal:
        r = self.get_symbol_rules(symbol)
        step = r["step_size"]
        minq = r["min_qty"]
        if qty < minq:
            return Decimal("0")
        # floor to step
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
    # Public price helpers (NO worst-price)
    # -----------------------------
    def get_best_bid_ask(self, symbol: str, limit: int = 5) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Use public depth endpoint. :contentReference[oaicite:7]{index=7}
        """
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
        """
        Use public ticker endpoint. :contentReference[oaicite:8]{index=8}
        """
        try:
            ticker = self.public.ticker_v3(symbol=symbol)
            data = ticker.get("data") or ticker
            # common fields
            for k in ("lastPrice", "last", "price", "markPrice"):
                if k in data and _d(data[k]) > 0:
                    return _d(data[k])
            return None
        except Exception:
            return None

    def get_mark_price(self, symbol: str, direction: str) -> Optional[Decimal]:
        """
        direction: "LONG" or "SHORT"
        - LONG mark: best_bid (what you can sell at)
        - SHORT mark: best_ask (what you can buy back at)
        """
        bid, ask = self.get_best_bid_ask(symbol)
        if direction.upper() == "LONG":
            return bid or self.get_last_price(symbol)
        return ask or self.get_last_price(symbol)

    def price_for_market_order(self, symbol: str, side: str) -> Decimal:
        """
        We still need to send a price for MARKET orders; we derive it from depth/ticker (no worst-price).
        Add small slippage to ensure it crosses.
        """
        slippage_bps = _d(os.getenv("SLIPPAGE_BPS", "10"))  # 10 bps = 0.10%
        slippage = slippage_bps / Decimal("10000")

        bid, ask = self.get_best_bid_ask(symbol)
        last = self.get_last_price(symbol)

        if side.upper() == "BUY":
            base = ask or last or bid
            if base is None:
                raise RuntimeError("ticker price unavailable")
            px = base * (Decimal("1") + slippage)
        else:
            base = bid or last or ask
            if base is None:
                raise RuntimeError("ticker price unavailable")
            px = base * (Decimal("1") - slippage)

        px = self.snap_price(symbol, _d(px))
        if px <= 0:
            px = _d(base or "0")
        return px

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
        """
        Create MARKET order via private v3.
        """
        self._ensure_configs_and_account()

        size = self.snap_qty(symbol, size)
        if size <= 0:
            raise RuntimeError(f"size too small after snap: {size}")

        price = self.price_for_market_order(symbol, side)

        # NOTE: per docs, timestampSeconds is required for create order; pass int seconds
        ts_sec = int(time.time())
        params = {
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
        # per docs: GET /v3/order :contentReference[oaicite:9]{index=9}
        return self.private.get_order_v3(id=order_id)

    def fills(
        self,
        symbol: Optional[str] = None,
        side: Optional[str] = None,
        limit: int = 100,
        page: int = 0,
    ) -> Dict[str, Any]:
        self._ensure_configs_and_account()
        # per docs: GET /v3/fills => fills_v3(...) :contentReference[oaicite:10]{index=10}
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
        High-frequency in first ~2-3s:
        - Poll get_order_v3(id=...) to read cumMatchFillSize / latestMatchFillPrice / averagePrice if available.
        - If still empty, fall back to fills_v3(symbol=..., side=...) and try to match by orderId (best-effort).
        """
        t0 = time.time()
        last_err: Optional[str] = None

        while True:
            try:
                od = self.get_order(order_id)
                data = od.get("data") or od
                # some responses wrap as {"data":{"..."}}
                # common fill fields in docs: latestMatchFillPrice, cumMatchFillSize :contentReference[oaicite:11]{index=11}
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

            # fallback: query fills feed and try match orderId
            try:
                fr = self.fills(symbol=symbol, limit=100, page=0)
                orders = (fr.get("orders") or fr.get("data", {}).get("orders") or [])
                # docs show fills response includes orderId field :contentReference[oaicite:12]{index=12}
                match = [o for o in orders if str(o.get("orderId") or o.get("id")) == str(order_id)]
                if match:
                    # aggregate
                    qty_sum = Decimal("0")
                    value_sum = Decimal("0")
                    for f in match:
                        q = _d(f.get("cumMatchFillSize") or f.get("size") or "0")
                        p = _d(f.get("latestMatchFillPrice") or f.get("price") or "0")
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


# singleton
_client: Optional[ApexClient] = None


def get_client() -> ApexClient:
    global _client
    if _client is None:
        _client = ApexClient()
    return _client
