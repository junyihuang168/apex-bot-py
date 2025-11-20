import os
import time
from decimal import Decimal, ROUND_DOWN

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.http_private_v3 import HttpPrivate_v3
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)


# ---------------------------
# 小工具
# ---------------------------

def _env_bool(name: str, default: bool = False) -> bool:
    """环境变量字符串 -> bool."""
    value = os.getenv(name, str(default))
    return value.lower() in ("1", "true", "yes", "y", "on")


def _get_base_and_network():
    """根据环境变量决定 mainnet / testnet."""
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)

    env_name = os.getenv("APEX_ENV", "").lower()
    if env_name in ("main", "mainnet", "prod", "production"):
        use_mainnet = True

    base_url = APEX_OMNI_HTTP_MAIN if use_mainnet else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_OMNI_MAIN_ARB if use_mainnet else NETWORKID_TEST
    return base_url, network_id


def _get_api_credentials():
    """从环境变量拿 API key 三件套."""
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


# ---------------------------
# client 单例
# ---------------------------

_sign_client: HttpPrivateSign | None = None
_http_v3_client: HttpPrivate_v3 | None = None


def _get_sign_client() -> HttpPrivateSign:
    global _sign_client
    if _sign_client is None:
        base_url, network_id = _get_base_and_network()
        api_creds = _get_api_credentials()
        zk_seeds = os.environ["APEX_ZK_SEEDS"]
        zk_l2 = os.getenv("APEX_L2KEY_SEEDS") or None

        _sign_client = HttpPrivateSign(
            base_url,
            network_id=network_id,
            zk_seeds=zk_seeds,
            zk_l2Key=zk_l2,
            api_key_credentials=api_creds,
        )
    return _sign_client


def _get_http_v3_client() -> HttpPrivate_v3:
    global _http_v3_client
    if _http_v3_client is None:
        base_url, network_id = _get_base_and_network()
        api_creds = _get_api_credentials()
        _http_v3_client = HttpPrivate_v3(
            base_url,
            network_id=network_id,
            api_key_credentials=api_creds,
        )
    return _http_v3_client


# ---------------------------
# 通用查询函数
# ---------------------------

def get_account():
    """查询账号信息（positions 等），方便 reduce_only 逻辑用。"""
    client = _get_sign_client()
    return client.get_account_v3()


def get_worst_price(symbol: str, side: str, size: str = "1") -> str:
    """
    用官方 GET /v3/get-worst-price 查盘口价。
    只需要一个大概 size（1 就够），主要是拿 price。
    """
    side = side.upper()
    client = _get_http_v3_client()
    res = client.get_worst_price_v3(symbol=symbol, size=str(size), side=side)

    price = None
    if isinstance(res, dict):
        if "worstPrice" in res:
            price = res["worstPrice"]
        elif "data" in res and isinstance(res["data"], dict):
            price = res["data"].get("worstPrice")

    if price is None:
        raise RuntimeError(f"[apex_client] get_worst_price_v3 返回异常: {res}")

    price_str = str(price)
    print(f"[apex_client] worst price for {symbol} {side} size={size}: {price_str}")
    return price_str


def _decimal_floor(value: float, step: float) -> float:
    """
    把 value 按 step 向下取整（保证是 step 的整数倍，同时保证 <= 原值）
    """
    d = Decimal(str(value))
    s = Decimal(str(step))
    if s <= 0:
        return float(d)

    scaled = (d / s).quantize(Decimal("1"), rounding=ROUND_DOWN)
    floored = scaled * s
    return float(floored)


def _get_position_size(symbol: str) -> float:
    """
    当前 symbol 的持仓绝对数量（不管多空），没有就返回 0.
    """
    try:
        acc = get_account()
    except Exception as e:
        print("[apex_client] get_account_v3 失败:", e)
        return 0.0

    positions = acc.get("positions") or []
    for p in positions:
        if p.get("symbol") == symbol:
            try:
                return abs(float(p.get("size", "0")))
            except Exception:
                return 0.0
    return 0.0


# ---------------------------
# 下单核心：方案 B（按 USDT 量自动换算成币）
# ---------------------------

def create_market_order(
    symbol: str,
    side: str,
    usdt_size: float,
    reduce_only: bool = False,
):
    """
    根据 USDT 金额自动换算成币数量，再按官方 Demo 调用 create_order_v3。

    参数：
      - symbol: 例如 'ZEC-USDT'
      - side:   'BUY' / 'SELL'
      - usdt_size: 预算多少 USDT（来自 TradingView 的 size 字段）
      - reduce_only: True 表示“只平仓不反向开新仓”，这里通过当前持仓 size 做截断实现
    """
    side = side.upper()
    usdt_size = float(usdt_size)

    if usdt_size <= 0:
        raise ValueError("usdt_size 必须 > 0")

    # 1) 先拿 worst price
    price_str = get_worst_price(symbol, side, size="1")
    price = float(price_str)

    # 2) 预算 USDT 换算成“理论上的币数量”
    raw_qty = usdt_size / price

    # 3) reduce_only 的话，不允许下单数量超过当前持仓
    if reduce_only:
        pos_size = _get_position_size(symbol)
        if pos_size <= 0:
            print(f"[apex_client] reduce_only=True 但 {symbol} 没有持仓，跳过下单。")
            return {"code": 0, "msg": "NO_POSITION"}
        raw_qty = min(raw_qty, pos_size)

    # 先用 6 位小数做一个初始数量
    qty = float(f"{raw_qty:.6f}")

    client = _get_sign_client()

    def _send(size_float: float):
        # 转成字符串，去掉多余 0
        size_str = f"{size_float:.6f}".rstrip("0").rstrip(".")
        ts = int(time.time())

        print(
            "[apex_client] create_market_order params:",
            {
                "symbol": symbol,
                "side": side,
                "type": "MARKET",
                "size": size_str,
                "price": price_str,
                "reduce_only": reduce_only,
                "timestampSeconds": ts,
            },
        )

        # ⚠️ 这里严格对齐官方 Demo：只能传这几个参数
        order = client.create_order_v3(
            symbol=symbol,
            side=side,
            type="MARKET",
            size=size_str,
            timestampSeconds=ts,
            price=price_str,
        )

        print("[apex_client] order response:", order)
        return order

    # 4) 第一次尝试
    order = _send(qty)

    # 5) 如果因为小数精度 / stepSize 报错，再按 stepSize 向下取整一次重试
    try:
        if isinstance(order, dict) and order.get("key") == "INVALID_DECIMAL_SCALE_PARAM":
            detail = order.get("detail") or {}
            step = detail.get("stepSize") or detail.get("step_size")
            if step is not None:
                step_f = float(step)
                adj_qty = _decimal_floor(raw_qty, step_f)
                if adj_qty <= 0:
                    raise RuntimeError(
                        f"size {raw_qty} 太小，按 stepSize={step_f} 处理后为 0"
                    )
                print(f"[apex_client] 因 stepSize 重试: stepSize={step_f}, size={adj_qty}")
                order = _send(adj_qty)
    except Exception as e:
        print("[apex_client] 重试逻辑出错（可以忽略，看看上面的返回）:", e)

    return order
