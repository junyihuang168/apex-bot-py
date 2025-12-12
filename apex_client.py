import os
import time
import threading
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, Optional, Tuple, List

# Apex Omni SDK (official)
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

from apexomni.http_private_v3 import HttpPrivate_v3
from apexomni.http_private_sign import HttpPrivateSign
from apexomni.http_public import HttpPublic


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
    Official V3 usage notes:
    - Public:  HttpPublic(APEX_OMNI_HTTP_MAIN / APEX_OMNI_HTTP_TEST)
    - Private: HttpPrivate_v3(..., api_key_credentials={key,secret,passphrase})
    - Create order (v3): HttpPrivateSign(..., zk_seeds, zk_l2Key, api_key_credentials=...)
      (create_order_v3 requires zk signature per official docs)
    """

    def __init__(self) -> None:
        use_main = os.getenv("APEX_USE_MAINNET", "true").lower() == "true"
        self.http_host = APEX_OMNI_HTTP_MAIN if use_main else APEX_OMNI_HTTP_TEST
        self.network_id = NETWORKID_OMNI_MAIN_ARB if use_main else NETWORKID_TEST

        # API key creds
        key = os.getenv("APEX_API_KEY", "").strip()
        secret = os.getenv("APEX_API_SECRET", "").strip()
        passphrase = os.getenv("APEX_API_PASSPHRASE", "").strip()
        if not key or not secret or not passphrase:
            raise RuntimeError("Missing APEX_API_KEY / APEX_API_SECRET / APEX_API_PASSPHRASE")

        self._api_key_credentials = {"key": key, "secret": secret, "passphrase": passphrase}

        # ZK creds for signing (required for create_order_v3 in V3)
        self.zk_seeds = (os.getenv("APEX_ZK_SEEDS", "") or "").strip()
        # 你环境里叫 APEX_L2KEY_SEEDS（截图里就是这个）
        self.zk_l2key = (os.getenv("APEX_L2KEY_SEEDS", "") or os.getenv("APEX_ZK_L2KEY", "") or "").strip()

        # Clients
        self.public = HttpPublic(self.http_host)
        self.private_v3 = HttpPrivate_v3(
            self.http_host,
            network_id=self.network_id,
            api_key_credentials=self._api_key_credentials,
        )

        self.private_sign: Optional[HttpPrivateSign] = None
        if self.zk_seeds and self.zk_l2key:
            self.private_sign = HttpPrivateSign(
                self.http_host,
                network_id=self.network_id,
                zk_seeds=self.zk_seeds,
                zk_l2Key=self.zk_l2key,
                api_key_credentials=self._api_key_credentials,
            )

        # cache
        self._configs_lock = threading.Lock()
        self._configs_cache: Optional[Dict[str, Any]] = None
        self._symbol_rules: Dict[str, Dict[str, Decimal]] = {}
        self._default_rules = {
            "min_qty": Decimal(os.getenv("DEFAULT_MIN_QTY", "0.01")),
            "step_size": Decimal(os.getenv("DEFAULT_STEP_SIZE", "0.01")),
            "tick_size": Decimal(os.getenv("DEFAULT_TICK_SIZE", "0.01")),
        }

        # init
        self._ensure_configs_and_account()

    # -----------------------------
    # Symbol format compatibility
    # -----------------------------
    @staticmethod
    def _public_symbol(symbol: str) -> str:
        # 官方 public demo 常用 BTCUSDT / ETHUSDT；你的交易 symbol 是 BTC-USDT / ZEC-USDT
        return symbol.replace("-", "").replace("_", "")

    # -----------------------------
    # Init/cache
    # -----------------------------
    def _ensure_configs_and_account(self) -> None:
        """
        Per official docs, user should call configs_v3() and get_account_v3()
        before using private endpoints and to obtain symbol configurations.
        """
        with self._configs_lock:
            if self._configs_cache is not None:
                return

            # configs from PUBLIC is enough for rules
            cfg = self.public.configs_v3()
            # also warm up private account call (helps avoid first-call surprises)
            _ = self.private_v3.get_account_v3()

            self._configs_cache = cfg
            self._build_symbol_rules_from_configs(cfg)

    def _build_symbol_rules_from_configs(self, cfg: Dict[str, Any]) -> None:
        rules: Dict[str, Dict[str, Decimal]] = {}

        def _walk(obj: Any) -> None:
            if isinstance(obj, dict):
                # configs contains many places; catch anything that looks like symbol rules
                if "symbol" in obj and ("stepSize" in obj or "minSize" in obj or "tickSize" in obj):
                    sym = str(obj.get("symbol"))
                    step = _d(obj.get("stepSize") or obj.get("step") or self._default_rules["step_size"])
                    minq = _d(obj.get("minSize") or obj.get("minQty") or self._default_rules["min_qty"])
                    tick = _d(obj.get("tickSize") or obj.get("tick") or self._default_rules["tick_size"])
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
        """
        Use public depth_v3. Try both 'ZEC-USDT' and 'ZECUSDT' formats.
        """
        sym_candidates = [symbol, self._public_symbol(symbol)]
        for sym in sym_candidates:
            try:
                depth = self.public.depth_v3(symbol=sym, limit=limit)
                data = depth.get("data") if isinstance(depth, dict) else None
                d = data if isinstance(data, dict) else (depth if isinstance(depth, dict) else {})
                bids = d.get("bids") or []
                asks = d.get("asks") or []
                best_bid = _d(bids[0][0]) if bids and len(bids[0]) >= 1 else None
                best_ask = _d(asks[0][0]) if asks and len(asks[0]) >= 1 else None
                if best_bid is not None and best_bid <= 0:
                    best_bid = None
                if best_ask is not None and best_ask <= 0:
                    best_ask = None
                if best_bid or best_ask:
                    return best_bid, best_ask
            except Exception:
                continue
        return None, None

    def get_last_price(self, symbol: str) -> Optional[Decimal]:
        """
        Use public ticker_v3. Try both symbol formats.
        """
        sym_candidates = [symbol, self._public_symbol(symbol)]
        for sym in sym_candidates:
            try:
                ticker = self.public.ticker_v3(symbol=sym)
                data = ticker.get("data") if isinstance(ticker, dict) else None
                d = data if isinstance(data, dict) else (ticker if isinstance(ticker, dict) else {})
                for k in ("lastPrice", "last", "price", "markPrice"):
                    if k in d and _d(d[k]) > 0:
                        return _d(d[k])
            except Exception:
                continue
        return None

    def price_for_market_order(self, symbol: str, side: str) -> Decimal:
        """
        APEX market order still needs a price; we derive it from depth/ticker
        and add slippage to ensure it crosses.
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
    # Orders / fills (V3)
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
        V3 create order requires zk signature in official docs -> HttpPrivateSign.create_order_v3
        """
        self._ensure_configs_and_account()

        size = self.snap_qty(symbol, size)
        if size <= 0:
            raise RuntimeError(f"size too small after snap: {size}")

        if self.private_sign is None:
            raise RuntimeError(
                "Missing APEX_ZK_SEEDS / APEX_L2KEY_SEEDS. "
                "create_order_v3 requires zk signature (HttpPrivateSign)."
            )

        price = self.price_for_market_order(symbol, side)
        ts = time.time()  # official demo uses time.time() (float) for timestampSeconds

        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "size": str(size),
            "price": str(price),
            "timestampSeconds": ts,
            "reduceOnly": bool(reduce_only),
        }
        if client_order_id:
            params["clientOrderId"] = client_order_id

        return self.private_sign.create_order_v3(**params)

    def get_order(self, order_id: str) -> Dict[str, Any]:
        self._ensure_configs_and_account()
        return self.private_v3.get_order_v3(id=order_id)

    def fills(
        self,
        symbol: Optional[str] = None,
        side: Optional[str] = None,
        limit: int = 100,
        page: int = 0,
    ) -> Dict[str, Any]:
        self._ensure_configs_and_account()
        kwargs: Dict[str, Any] = {"limit": limit, "page": page}

        if symbol:
            kwargs["symbol"] = symbol
        if side:
            kwargs["side"] = side

        token = os.getenv("APEX_TOKEN", "USDT").strip()
        if token:
            kwargs["token"] = token

        return self.private_v3.fills_v3(**kwargs)

    @staticmethod
    def _extract_fill_rows(resp: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        def walk(o: Any) -> None:
            if isinstance(o, dict):
                looks_like_fill = (
                    any(k in o for k in ("orderId", "clientOrderId", "id"))
                    and any(k in o for k in ("price", "latestMatchFillPrice", "fillPrice"))
                    and any(k in o for k in ("size", "fillSize", "cumMatchFillSize", "cumSuccessFillSize"))
                )
                if looks_like_fill:
                    rows.append(o)
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for it in o:
                    walk(it)

        walk(resp)
        return rows

    def get_fill_summary(
        self,
        symbol: str,
        order_id: str,
        max_wait_sec: float = 8.0,
        poll_interval: float = 0.25,
    ) -> Dict[str, Any]:
        """
        Best effort:
        1) Poll get_order_v3(id=...) for cumMatchFillSize / averagePrice / latestMatchFillPrice
        2) If still empty, query fills_v3(...) and match orderId
        """
        t0 = time.time()
        last_err: Optional[str] = None

        while True:
            # 1) order endpoint
            try:
                od = self.get_order(order_id)
                data = od.get("data") if isinstance(od, dict) else None
                d = data if isinstance(data, dict) else (od if isinstance(od, dict) else {})

                filled_qty = _d(d.get("cumMatchFillSize") or d.get("cumSuccessFillSize") or "0")
                last_fill_px = _d(d.get("latestMatchFillPrice") or "0")
                avg_px = _d(d.get("averagePrice") or d.get("avgPrice") or "0")
                status = str(d.get("status") or "")

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

                # 如果已经到终态但还没 qty，直接给出状态（避免死等）
                if status.upper() in {"CANCELED", "REJECTED", "EXPIRED"}:
                    return {
                        "ok": False,
                        "order_id": order_id,
                        "filled_qty": "0",
                        "avg_price": "0",
                        "status": status,
                        "last_err": "order terminal without fills",
                    }

            except Exception as e:
                last_err = f"get_order_v3 failed: {e}"

            # 2) fills endpoint fallback
            try:
                fr = self.fills(symbol=symbol, limit=100, page=0)
                rows = self._extract_fill_rows(fr)
                matched = []
                for r in rows:
                    oid = str(r.get("orderId") or r.get("id") or "")
                    if oid == str(order_id):
                        matched.append(r)

                if matched:
                    qty_sum = Decimal("0")
                    val_sum = Decimal("0")
                    for f in matched:
                        q = _d(f.get("size") or f.get("fillSize") or f.get("cumMatchFillSize") or f.get("cumSuccessFillSize") or "0")
                        p = _d(f.get("price") or f.get("fillPrice") or f.get("latestMatchFillPrice") or "0")
                        if q > 0 and p > 0:
                            qty_sum += q
                            val_sum += q * p
                    if qty_sum > 0:
                        avg = (val_sum / qty_sum) if val_sum > 0 else Decimal("0")
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
