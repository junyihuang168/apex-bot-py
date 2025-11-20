# apex_client.py
# Thin wrapper around apexomni HttpPrivateSign for Apex Omni V3.
# Uses configs_v3() and get_account_v3() as required by the new SDK.

import os
import time
import logging
from decimal import Decimal, ROUND_DOWN

from apexomni.http_private_v3 import HttpPrivateSign
from apexomni.constants import APEX_OMNI_HTTP_TEST, NETWORKID_TEST

log = logging.getLogger("apex_client")
log.setLevel(logging.INFO)


def _get_env(name: str, default: str | None = None) -> str | None:
    """Small helper to read environment variables and log when missing."""
    value = os.getenv(name, default)
    if value is None or value == "":
        log.warning("[apex_client] ENV %s is not set", name)
    return value


class ApexClient:
    """
    Simple client that:
    1. Initializes HttpPrivateSign with zk_seeds + l2Key + api keys
    2. Calls configs_v3() and get_account_v3() once
    3. Provides create_market_order() which can take size in USDT
    """

    def __init__(self) -> None:
        # --- Load credentials from environment ---
        api_key = _get_env("APEX_API_KEY")
        api_secret = _get_env("APEX_API_SECRET")
        api_passphrase = _get_env("APEX_API_PASSPHRASE")

        zk_seeds = _get_env("APEX_ZK_SEEDS")
        l2_key = _get_env("APEX_L2KEY_SEEDS", "")

        if not (api_key and api_secret and api_passphrase and zk_seeds):
            log.warning(
                "[apex_client] Some Apex credentials are missing. "
                "Trading requests may fail."
            )

        # --- Init HttpPrivateSign client (Testnet endpoint + network id) ---
        # If you want mainnet later, just change APEX_OMNI_HTTP_TEST / NETWORKID_TEST.
        self.client = HttpPrivateSign(
            APEX_OMNI_HTTP_TEST,
            network_id=NETWORKID_TEST,
            zk_seeds=zk_seeds,
            zk_l2Key=l2_key,
            api_key_credentials={
                "key": api_key,
                "secret": api_secret,
                "passphrase": api_passphrase,
            },
        )

        # --- Required by the SDK: configs_v3 + get_account_v3 ---
        # DO NOT use the old `configV3` name. It does not exist.
        log.info("[apex_client] Fetching configs_v3() and account_v3() ...")
        self.configs = self.client.configs_v3()
        self.account = self.client.get_account_v3()
        log.info("[apex_client] configs_v3 and account_v3 loaded successfully")

    # ------------------------------------------------------------------
    # Helper to convert from USDT notionals to base-asset size
    # ------------------------------------------------------------------
    @staticmethod
    def _calc_size_from_usdt(symbol: str, size_usdt: float, price: float) -> str:
        """
        Convert a USDT notional amount to base asset size (string),
        flooring to 6 decimal places to be safe for most symbols.

        Example: size_usdt = 10, price = 683.21 -> size ~ 0.01464...
        """
        if price <= 0 or size_usdt <= 0:
            raise ValueError("price and size_usdt must be positive")

        raw = Decimal(str(size_usdt)) / Decimal(str(price))
        # Floor to 6 decimal places to avoid "too many decimal" issues
        size = raw.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
        return format(size, "f")

    # ------------------------------------------------------------------
    # Public API used by app.py
    # ------------------------------------------------------------------
    def create_market_order(
        self,
        symbol: str,
        side: str,
        sizeUSDT: float | int | str | None = None,
        price: float | int | str | None = None,
        reduce_only: bool = False,
        client_id: str | None = None,
        signal_type: str | None = None,
        **_,
    ):
        """
        Create a MARKET order on Apex Omni.

        Parameters (most common from webhook/app.py):
            symbol      - e.g. "ZEC-USDT"
            side        - "BUY" or "SELL"
            sizeUSDT    - notional in USDT (we convert to coin size)
            price       - reference / worst price from TV (required for MARKET)
            reduce_only - whether this is a reduce-only (exit) order
            client_id   - optional client id (for logging)
            signal_type - optional string like "entry"/"exit" (for logging)

        Any extra keyword arguments are accepted via **_ so that
        changes in app.py/webhook won't break this function.
        """

        # --- Normalize inputs ---
        if isinstance(sizeUSDT, str):
            size_usdt_val = float(sizeUSDT)
        else:
            size_usdt_val = float(sizeUSDT) if sizeUSDT is not None else 0.0

        if isinstance(price, str):
            price_val = float(price) if price else 0.0
        else:
            price_val = float(price) if price is not None else 0.0

        if size_usdt_val <= 0:
            raise ValueError("sizeUSDT must be > 0")

        if price_val <= 0:
            raise ValueError("price must be > 0 for MARKET orders")

        # --- Convert USDT notional -> base size ---
        size_str = self._calc_size_from_usdt(symbol, size_usdt_val, price_val)

        log.info(
            "[apex_client] worst price for %s %s sizeUSDT=%.4f: %.8f (signal_type=%s)",
            symbol,
            side,
            size_usdt_val,
            price_val,
            signal_type,
        )

        # For MARKET orders Apex still expects a "price" (protective worst price)
        order_params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "size": size_str,
            "timestampSeconds": int(time.time()),
            "price": str(price_val),
            "reduce_only": bool(reduce_only),
        }

        # Optional client id, if your webhook sends it
        if client_id:
            # Only add if provided – safe even if Apex ignores it
            order_params["clientId"] = str(client_id)

        log.info("[apex_client] create_market_order params: %s", order_params)

        # --- Send order to Apex Omni ---
        resp = self.client.create_order_v3(**order_params)
        log.info("[apex_client] create_order_v3 response: %s", resp)
        return resp


# Optional module-level singleton, so app.py can do:
#   from apex_client import apex_client
# and call apex_client.create_market_order(...)
try:
    apex_client = ApexClient()
except Exception as e:
    # We log the error instead of crashing import; app.py can still
    # decide how to handle missing client.
    log.exception("[apex_client] Failed to initialize ApexClient: %s", e)
    apex_client = None
