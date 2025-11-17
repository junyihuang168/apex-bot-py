# app.py
import os
import time
from collections.abc import Mapping

from flask import Flask, jsonify, request

from apex_client import make_client

app = Flask(__name__)


# --------------------------------------------------
# 小工具函数
# --------------------------------------------------
def normalize_symbol(sym: str) -> str:
    """
    把 TradingView 的 BTCUSDT / ZECUSDT 之类，
    转成 Apex 需要的 BTC-USDT / ZEC-USDT。
    """
    if not sym:
        return sym
    sym = sym.upper()
    if "-" in sym:
        return sym
    if len(sym) > 4:
        base = sym[:-4]
        quote = sym[-4:]
        return f"{base}-{quote}"
    return sym


def _extract_account_id(obj):
    """
    递归地从任意 dict/list 里尽量找出 accountId。
    """
    visited = set()

    def _inner(value):
        if isinstance(value, Mapping):
            obj_id = id(value)
            if obj_id in visited:
                return None
            visited.add(obj_id)

            for key in ("accountId", "account_id", "zkAccountId", "zk_account_id", "id"):
                if key in value:
                    v = value[key]
                    if isinstance(v, (str, int)) and str(v).strip():
                        return str(v)

            for v in value.values():
                result = _inner(v)
                if result:
                    return result

        elif isinstance(value, list):
            for item in value:
                result = _inner(item)
                if result:
                    return result

        return None

    return _inner(obj)


def ensure_account_ready(client):
    """
    调用 configs_v3 / get_account_v3，尽量找出 accountId。

    返回 (account_id, account_raw, configs_raw)
    """
    configs = None
    account = None

    try:
        configs = client.configs_v3()
    except Exception as e:
        print("⚠ configs_v3 failed in helper:", e)

    try:
        account = client.get_account_v3()
    except Exception as e:
        print("⚠ get_account_v3 failed in helper:", e)

    print("configs_v3/get_account_v3 ok in helper (可能有空值)")
    print("Raw get_account_v3() in helper:", account)

    # 尽量把账户信息挂在 client 上，兼容不同 SDK 版本
    try:
        if isinstance(account, dict):
            setattr(client, "accountV3", account)
            if getattr(client, "account", None) is None:
                setattr(client, "account", account)
    except Exception as e:
        print("⚠ Unable to attach account data on client:", e)

    account_id = _extract_account_id(account)

    if not account_id and isinstance(configs, Mapping):
        account_id = _extract_account_id(configs)

    if not account_id:
        # 最后再从 client 的属性里找一圈
        try:
            for attr_name in dir(client):
                if "account" not in attr_name.lower():
                    continue
                value = getattr(client, attr_name)
                if isinstance(value, Mapping):
                    candidate = _extract_account_id(value)
                    if candidate:
                        account_id = candidate
                        print(f"Found accountId on client.{attr_name}: {account_id}")
                        break
        except Exception as e:
            print("⚠ Error scanning client attributes for accountId:", e)

    print("AccountId resolved in helper:", account_id)
    return account_id, account, configs


# --------------------------------------------------
# 路由：健康检查
# --------------------------------------------------
@app.route("/")
def health():
    return "ok", 200


# --------------------------------------------------
# 路由：手动测试下单（会真下一个很小的 TEST 单）
# --------------------------------------------------
@app.route("/test")
def test():
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /test:", e)
        return jsonify({"status": "error", "where": "make_client", "error": str(e)}), 500

    account_id, account, configs = ensure_account_ready(client)

    if not account_id:
        print("❌ Unable to determine accountId in /test")
        return jsonify(
            {
                "status": "error",
                "where": "account",
                "error": "Unable to determine accountId from get_account_v3",
                "account_raw": account,
            }
        ), 500

    now_ts = int(time.time())
    try:
        order = client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=now_ts,
            price="60000",
        )
        print("✅ create_order_v3 ok in /test:", order)
        return jsonify(
            {
                "status": "ok",
                "configs": configs,
                "account": account,
                "accountId": account_id,
                "order": order,
            }
        ), 200
    except Exception as e:
        print("❌ create_order_v3 failed in /test:", e)
        return jsonify(
            {"status": "error", "where": "create_order_v3", "error": str(e)}
        ), 500


# --------------------------------------------------
# 路由：TradingView Webhook 下单
# --------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("📩 Incoming webhook:", data)

    # 校验 secret
    expected_secret = os.getenv("WEBHOOK_SECRET")
    if expected_secret:
        if data.get("secret") != expected_secret:
            print("❌ Invalid WEBHOOK secret")
            return (
                jsonify({"status": "error", "where": "auth", "error": "invalid secret"}),
                403,
            )

    enable_raw = os.getenv("ENABLE_LIVE_TRADING", "false")
    enable_live = enable_raw.lower() == "true"
    print("ENABLE_LIVE_TRADING raw =", enable_raw)
    print("ENABLE_LIVE_TRADING normalized =", enable_live)

    symbol_raw = data.get("symbol") or data.get("ticker")
    side_raw = (data.get("side") or "").upper()
    order_type_raw = (data.get("order_type") or "market").upper()
    position_size = str(data.get("position_size") or "0")
    leverage = data.get("leverage")
    signal_type = data.get("signal_type") or "entry"

    symbol = normalize_symbol(symbol_raw)
    print("✅ Normalized symbol:", symbol_raw, "->", symbol)

    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /webhook:", e)
        return jsonify({"status": "error", "where": "make_client", "error": str(e)}), 500

    account_id, account, configs = ensure_account_ready(client)

    if not account_id:
        print("❌ Unable to determine accountId in /webhook，停止下单")
        return jsonify(
            {
                "status": "error",
                "where": "account",
                "error": "Unable to determine accountId from get_account_v3; please double-check Omni zk seeds (APEX_ZK_SEEDS) and l2Key (APEX_L2KEY_SEEDS).",
                "account_raw": account,
            }
        ), 500

    if not enable_live:
        print("🧪 ENABLE_LIVE_TRADING=false，只做模拟打印，不真实下单")
        return jsonify(
            {
                "status": "ok",
                "mode": "dry-run",
                "symbol": symbol,
                "side": side_raw,
                "position_size": position_size,
                "signal_type": signal_type,
                "accountId": account_id,
            }
        ), 200

    # 真正下单
    side = side_raw or "BUY"
    order_type = "MARKET" if order_type_raw == "MARKET" else order_type_raw
    size = position_size
    ts = int(time.time())

    try:
        order = client.create_order_v3(
            symbol=symbol,
            side=side,
            type=order_type,
            size=size,
            timestampSeconds=ts,
            price="0",  # 市价单，价格字段会被忽略
        )
        print("✅ create_order_v3 ok in /webhook:", order)
        return jsonify(
            {
                "status": "ok",
                "accountId": account_id,
                "order": order,
            }
        ), 200
    except Exception as e:
        print("❌ create_order_v3 failed in /webhook:", e)
        return jsonify(
            {"status": "error", "where": "create_order_v3", "error": str(e)}
        ), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
