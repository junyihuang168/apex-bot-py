import os
import time
from typing import Any, Dict, Iterable, Optional

from flask import Flask, request, jsonify

from apex_client import make_client

app = Flask(__name__)


# ----------------- 小工具函数 -----------------
def str_to_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def normalize_symbol(sym: Optional[str]) -> Optional[str]:
    """把 TV 里的 BTCUSDT 之类，转成 Apex 需要的 BTC-USDT"""
    if not sym:
        return sym
    s = sym.upper()
    if "-" in s:
        return s
    if s.endswith("USDT") and len(s) > 4:
        base = s[:-4]
        return f"{base}-USDT"
    return s


def _find_account_id_in_obj(obj: Any) -> Optional[str]:
    """在任意嵌套的 dict/list 里递归寻找 accountId 字段"""
    target_keys = {"accountId", "account_id", "accountid", "accountID"}

    def _search(o: Any) -> Optional[str]:
        if isinstance(o, dict):
            for k, v in o.items():
                if k in target_keys and isinstance(v, (str, int)):
                    return str(v)
                res = _search(v)
                if res is not None:
                    return res
        elif isinstance(o, list):
            for item in o:
                res = _search(item)
                if res is not None:
                    return res
        return None

    return _search(obj)


def ensure_account_id(client: Any, account_raw: Any) -> Optional[str]:
    """
    尝试从 client 和 get_account_v3() 的返回里找出 accountId，
    找到后设置到 client.accountId，返回字符串，否则返回 None。
    """
    print("Account raw passed into ensure_account_id:", account_raw)

    # 1) 客户端上本来就有？
    for attr in ("accountId", "account_id", "accountid"):
        if hasattr(client, attr):
            val = getattr(client, attr)
            if val:
                setattr(client, "accountId", str(val))
                print(f"Found existing client.{attr} =", val)
                return str(val)

    # 2) 如果是自定义对象，先尝试转成 dict
    if account_raw is not None and not isinstance(account_raw, (dict, list)):
        if hasattr(account_raw, "__dict__"):
            account_raw = account_raw.__dict__

    # 3) 递归在返回结果里找 accountId
    account_id = None
    if account_raw is not None:
        account_id = _find_account_id_in_obj(account_raw)

    if account_id:
        setattr(client, "accountId", str(account_id))
        print("Set client.accountId from account_raw:", account_id)
        return str(account_id)

    print("Still unable to determine accountId.")
    return None


# ----------------- 基本路由 -----------------
@app.route("/")
def root():
    return "ok", 200


@app.route("/health")
def health():
    return "ok", 200


@app.route("/test")
def test():
    """手动测试：/test 用浏览器打开看 configs / account / accountId"""
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /test:", e)
        return jsonify({"status": "error", "where": "make_client", "error": str(e)}), 500

    try:
        configs = client.configs_v3()
        account_raw = client.get_account_v3()
        print("configs_v3/get_account_v3 ok in /test")
        print("Raw get_account_v3() in /test:", account_raw)
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /test:", e)
        return (
            jsonify({"status": "error", "where": "configs_or_account", "error": str(e)}),
            500,
        )

    account_id = ensure_account_id(client, account_raw)

    return (
        jsonify(
            {
                "status": "ok",
                "account_id": account_id,
                "account_raw": account_raw,
                "configs": configs,
            }
        ),
        200,
    )


# ----------------- TradingView Webhook 下单 -----------------
@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(force=True, silent=True) or {}
    print("📩 Incoming webhook:", payload)

    # 校验 secret
    expected_secret = os.getenv("WEBHOOK_SECRET")
    if not expected_secret:
        print("⚠️ WEBHOOK_SECRET not set in env")
    incoming_secret = payload.get("secret")
    if expected_secret and incoming_secret != expected_secret:
        print("❌ Invalid WEBHOOK secret:", incoming_secret)
        return "forbidden", 403

    # 是否真的下单
    enable_live_raw = os.getenv("ENABLE_LIVE_TRADING", "false")
    print("ENABLE_LIVE_TRADING raw =", repr(enable_live_raw))
    enable_live = str_to_bool(enable_live_raw, False)
    print("ENABLE_LIVE_TRADING normalized =", enable_live)

    # 解析 TV 传进来的字段
    tv_symbol = payload.get("symbol")          # 例如 BTCUSDT / ZECUSDT
    side = (payload.get("side") or "").upper() # BUY / SELL
    order_type = (payload.get("order_type") or "market").upper()  # MARKET / LIMIT
    position_size = payload.get("position_size")  # 建议在 TV 里就填字符串
    signal_type = payload.get("signal_type", "entry")  # entry / exit 等

    norm_symbol = normalize_symbol(tv_symbol)
    print(f"✅ Normalized symbol: {tv_symbol} -> {norm_symbol}")

    # 创建 Apex 客户端
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /webhook:", e)
        return "error", 500

    # 先拉 configs & account，再从返回里解析 accountId
    try:
        configs = client.configs_v3()
        account_raw = client.get_account_v3()
        print("configs_v3/get_account_v3 ok in /webhook")
        print("Raw get_account_v3() in /webhook:", account_raw)
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /webhook:", e)
        return "error", 500

    account_id = ensure_account_id(client, account_raw)
    if not account_id:
        print("⚠️ Could not determine accountId; create_order_v3 may still fail.")

    # 如果只是纸上谈兵，就不真正下单
    if not enable_live:
        print("🚫 ENABLE_LIVE_TRADING is false; skip real order, only log.")
        return "ok (paper)", 200

    # Apex 要求所有数字用字符串
    size_str = str(position_size)
    now_ts = int(time.time())

    try:
        order = client.create_order_v3(
            symbol=norm_symbol,
            side=side,
            type=order_type,
            size=size_str,
            timestampSeconds=now_ts,
        )
        print("✅ create_order_v3 ok in /webhook:", order)
        return "ok", 200
    except Exception as e:
        print("❌ create_order_v3 failed in /webhook:", e)
        return "error", 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
