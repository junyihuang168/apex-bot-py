# app.py
# Flask Web 服务，提供 / 、/test 、/webhook 三个路由

import os
import time
from typing import Any

from flask import Flask, jsonify, request

from apex_client import make_client

app = Flask(__name__)


# -------------------------
# 工具函数
# -------------------------


def normalize_symbol(sym: str) -> str:
    """把 TradingView 传过来的 BTCUSDT / BTCUSD 变成 BTC-USDT / BTC-USD"""
    if not sym:
        return sym
    sym = sym.upper()
    if "-" in sym:
        return sym
    if len(sym) >= 6:
        base = sym[:-4]
        quote = sym[-4:]
        return f"{base}-{quote}"
    return sym


def _extract_account_id_any(data: Any):
    """在任意嵌套的 dict/list 里递归查找 accountId / id / positionId"""
    seen = set()

    def _walk(obj):
        if id(obj) in seen:
            return None
        seen.add(id(obj))

        if isinstance(obj, dict):
            # 先直接检查常见字段
            for k in ("accountId", "account_id", "id", "positionId"):
                if k in obj and obj[k]:
                    return obj[k]
            for v in obj.values():
                res = _walk(v)
                if res is not None:
                    return res
        elif isinstance(obj, (list, tuple)):
            for v in obj:
                res = _walk(v)
                if res is not None:
                    return res
        return None

    return _walk(data)


def attach_account_id(client, account_data):
    """
    从 account_data 或 client 上找 accountId，
    找到后设置到 client.accountId，返回 accountId，找不到返回 None
    """
    account_id = None

    # 1) 先从 get_account_v3 的返回里找
    if account_data:
        account_id = _extract_account_id_any(account_data)

    # 2) 找不到的话，在 client 的所有 *account* 属性里再搜一次
    if not account_id:
        print("Unable to find accountId in account_data, scanning client attributes...")
        for name in dir(client):
            if "account" not in name.lower():
                continue
            try:
                value = getattr(client, name)
            except Exception:
                continue

            # 只对 dict 或带 __dict__ 的对象做检查
            if isinstance(value, dict):
                candidate = _extract_account_id_any(value)
            elif hasattr(value, "__dict__"):
                candidate = _extract_account_id_any(value.__dict__)
            else:
                candidate = None

            if candidate:
                account_id = candidate
                print(f"Found accountId={account_id!r} in client.{name}")
                break

    if account_id:
        try:
            setattr(client, "accountId", account_id)
            print("Set client.accountId =", account_id)
        except Exception as e:
            print("Failed to set client.accountId:", e)
    else:
        print("Still unable to determine accountId.")

    return account_id


# -------------------------
# 路由
# -------------------------


@app.route("/")
def health():
    # DO 的健康检查用
    return "ok", 200


@app.route("/test")
def test():
    """
    手动测试：不下单，只是返回 configs_v3 / get_account_v3 / accountId
    你可以在浏览器里直接打开 https://你的-app-url/test 看 JSON
    """
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /test:", e)
        return (
            jsonify({"status": "error", "where": "make_client", "error": str(e)}),
            500,
        )

    try:
        configs = client.configs_v3()
        account = client.get_account_v3()
        print("configs_v3/get_account_v3 ok in /test")
        print("Account data in /test:", account)
        account_id = attach_account_id(client, account)
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /test:", e)
        return (
            jsonify({"status": "error", "where": "configs_or_account", "error": str(e)}),
            500,
        )

    return (
        jsonify(
            {
                "status": "ok",
                "account_id": account_id,
                "configs": configs,
                "account": account,
            }
        ),
        200,
    )


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("📩 Incoming webhook:", data)

    # 1) 校验 TradingView secret
    expected_secret = os.getenv("WEBHOOK_SECRET")
    if expected_secret and data.get("secret") != expected_secret:
        print("❌ Invalid webhook secret")
        return jsonify({"status": "error", "where": "secret", "error": "Invalid secret"}), 403

    # 2) 是否开启实盘
    live_raw = os.getenv("ENABLE_LIVE_TRADING", "false")
    enable_live = live_raw.lower() == "true"
    print("ENABLE_LIVE_TRADING raw =", repr(live_raw))
    print("ENABLE_LIVE_TRADING normalized =", enable_live)

    symbol_raw = data.get("symbol")
    symbol = normalize_symbol(symbol_raw)
    side = str(data.get("side", "")).upper()  # "BUY" / "SELL"
    order_type = str(data.get("order_type", "market")).upper()  # "MARKET" / "LIMIT"
    signal_type = str(data.get("signal_type", "entry")).lower()  # "entry" / "exit"
    size_str = str(data.get("position_size", "0"))

    print(f"Normalized symbol: {symbol_raw} -> {symbol}")

    try:
        size = float(size_str)
    except Exception:
        size = 0.0

    if not enable_live:
        print("⚠️ LIVE TRADING DISABLED, skip placing order")
        return jsonify({"status": "ok", "live_trading": False}), 200

    # 3) 创建 client
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /webhook:", e)
        return (
            jsonify({"status": "error", "where": "make_client", "error": str(e)}),
            500,
        )

    # 4) 先调用 configs_v3 / get_account_v3，并尝试拿到 accountId
    try:
        configs = client.configs_v3()
        account = client.get_account_v3()
        print("configs_v3/get_account_v3 ok in /webhook")
        print("Account data in /webhook:", account)
        account_id = attach_account_id(client, account)
        if not account_id:
            # 找不到 accountId，就不要再让 SDK 抛 “No accountId provided” 的异常了
            print(
                "❌ Unable to determine accountId from get_account_v3() response, raw:",
                account,
            )
            return (
                jsonify(
                    {
                        "status": "error",
                        "where": "get_account_v3",
                        "error": "Unable to determine accountId from account data",
                    }
                ),
                500,
            )
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /webhook:", e)
        return (
            jsonify({"status": "error", "where": "configs_or_account", "error": str(e)}),
            500,
        )

    # 5) 组装下单参数
    current_time = int(time.time())
    order_kwargs = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "size": str(size),
        "timestampSeconds": current_time,
    }

    # LIMIT 单的话可以在 TV 里传 price，我们就跟着用
    if order_type == "LIMIT" and "price" in data:
        order_kwargs["price"] = str(data["price"])

    # signal_type 暂时只是打印一下，可以以后扩展
    print("signal_type:", signal_type)

    # 6) 真正下单
    try:
        print("📤 Sending create_order_v3 with params:", order_kwargs)
        order = client.create_order_v3(**order_kwargs)
        print("✅ create_order_v3 ok in /webhook:", order)
        return jsonify({"status": "ok", "account_id": account_id, "order": order}), 200
    except Exception as e:
        print("❌ create_order_v3 failed in /webhook:", e)
        return (
            jsonify({"status": "error", "where": "create_order_v3", "error": str(e)}),
            500,
        )


if __name__ == "__main__":
    # 本地调试用
    app.run(host="0.0.0.0", port=8080)
