import os
import time
import random
from decimal import Decimal, ROUND_DOWN

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

# -------------------------------------------------------------------
# 交易规则（默认：最小数量 0.01，步长 0.01，小数点后 2 位）
# 如果以后某个币不一样，可以在 SYMBOL_RULES 里单独覆盖
# -------------------------------------------------------------------

DEFAULT_SYMBOL_RULES = {
    "min_qty": Decimal("0.01"),      # 最小数量
    "step_size": Decimal("0.01"),    # 每档步长
    "qty_decimals": 2,               # 小数位数
}

SYMBOL_RULES: dict[str, dict] = {
    # 举例：如果以后 BTC-USDT 规则不同，可以这样写：
    # "BTC-USDT": {
    #     "min_qty": Decimal("0.001"),
    #     "step_size": Decimal("0.001"),
    #     "qty_decimals": 3,
    # },
}


def _get_symbol_rules(symbol: str) -> dict:
    """返回某个交易对的撮合规则（没有就用默认）"""
    s = symbol.upper()
    rules = SYMBOL_RULES.get(s, {})
    merged = {**DEFAULT_SYMBOL_RULES, **rules}
    return merged


def _snap_quantity(symbol: str, theoretical_qty: Decimal) -> Decimal:
    """
    把理论数量对齐到交易所允许的网格：
    - 向下取整到 step_size 的整数倍
    - 再限制为 qty_decimals 位小数
    - 如果小于 min_qty，就报错（预算太小）
    """
    rules = _get_symbol_rules(symbol)
    step = rules["step_size"]
    min_qty = rules["min_qty"]
    decimals = rules["qty_decimals"]

    if theoretical_qty <= 0:
        raise ValueError("calculated quantity must be > 0")

    # 向下取整到 step 的整数倍
    steps = (theoretical_qty // step)  # Decimal 的整除
    snapped = steps * step

    # 限制小数位数
    quantum = Decimal("1").scaleb(-decimals)  # 等价于 10^-decimals，比如 0.01
    snapped = snapped.quantize(quantum, rounding=ROUND_DOWN)

    if snapped < min_qty:
        raise ValueError(
            f"budget too small: snapped quantity {snapped} < minQty {min_qty}"
        )

    return snapped


# ---------------------------
# 内部小工具函数
# ---------------------------

def _env_bool(name: str, default: bool = False) -> bool:
    """把环境变量字符串转成布尔值."""
    value = os.getenv(name, str(default))
    return value.lower() in ("1", "true", "yes", "y", "on")


def _get_base_and_network():
    """根据环境变量决定用 mainnet 还是 testnet。"""
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)

    # 兼容你额外加的 APEX_ENV=main
    env_name = os.getenv("APEX_ENV", "").lower()
    if env_name in ("main", "mainnet", "prod", "production"):
        use_mainnet = True

    base_url = APEX_OMNI_HTTP_MAIN if use_mainnet else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_OMNI_MAIN_ARB if use_mainnet else NETWORKID_TEST
    return base_url, network_id


def _get_api_credentials():
    """从环境变量里拿 API key / secret / passphrase。"""
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


def _random_client_id() -> str:
    """
    生成 ApeX 官方风格的 clientId：纯数字字符串，避免 ORDER_INVALID_CLIENT_ORDER_ID。
    """
    return str(int(float(str(random.random())[2:])))


# ---------------------------
# 创建 ApeX 客户端
# ---------------------------

def get_client() -> HttpPrivateSign:
    """
    返回带 zk 签名的 HttpPrivateSign 客户端，
    用于真正的下单（create_order_v3）。

    这里模仿官方 demo：
        1) new HttpPrivateSign(...)
        2) client.configs_v3()
        3) client.get_account_v3()
    """
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()

    zk_seeds = os.environ["APEX_ZK_SEEDS"]
    zk_l2 = os.getenv("APEX_L2KEY_SEEDS") or None

    client = HttpPrivateSign(
        base_url,
        network_id=network_id,
        zk_seeds=zk_seeds,
        zk_l2Key=zk_l2,
        api_key_credentials=api_creds,
    )

    # ① 先拉 configs_v3，初始化 client.configV3
    try:
        cfg = client.configs_v3()
        print("[apex_client] configs_v3 ok:", cfg)
    except Exception as e:
        print("[apex_client] WARNING configs_v3 error:", e)

    # ② 再拉 account_v3，初始化 client.accountV3
    try:
        acc = client.get_account_v3()
        print("[apex_client] get_account_v3 ok:", acc)
    except Exception as e:
        print("[apex_client] WARNING get_account_v3 error:", e)

    return client


# ---------------------------
# 对外工具函数
# ---------------------------

def get_account():
    """查询账户信息，方便你在本地或日志里调试。"""
    client = get_client()
    return client.get_account_v3()


def get_market_price(symbol: str, side: str, size: str) -> str:
    """
    使用官方推荐的 GET /v3/get-worst-price 来获取市价单应当使用的价格。
    - symbol: 例如 'BNB-USDT'
    - side: 'BUY' 或 'SELL'
    - size: 下单数量（字符串或数字均可）

    返回: 字符串形式的价格（比如 '532.15'）
    """
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()

    # 官方示例用的是 HttpPrivate_v3（只用 API key，不需要 zk）
    from apexomni.http_private_v3 import HttpPrivate_v3

    http_v3_client = HttpPrivate_v3(
        base_url,
        network_id=network_id,
        api_key_credentials=api_creds,
    )

    side = side.upper()
    size_str = str(size)

    res = http_v3_client.get_worst_price_v3(
        symbol=symbol,
        size=size_str,
        side=side,
    )

    price = None
    if isinstance(res, dict):
        if "worstPrice" in res:
            price = res["worstPrice"]
        elif "data" in res and isinstance(res["data"], dict) and "worstPrice" in res["data"]:
            price = res["data"]["worstPrice"]

    if price is None:
        raise RuntimeError(f"[apex_client] get_worst_price_v3 返回异常: {res}")

    price_str = str(price)
    print(f"[apex_client] worst price for {symbol} {side} size={size_str}: {price_str}")
    return price_str


def create_market_order(
    symbol: str,
    side: str,
    size: str | float | int | None = None,
    size_usdt: str | float | int | None = None,
    reduce_only: bool = False,
    client_id: str | None = None,
):
    """
    创建 ApeX 的 MARKET 市价单。

    两种用法：
    1) 旧用法：直接传币的数量：
        create_market_order(symbol, side, size="0.05", ...)
       → 会直接用 size 作为下单数量。

    2) 新用法（推荐）：传 USDT 金额，自动撮合数量：
        create_market_order(symbol, side, size_usdt="10", ...)
       → 会根据市价 + 交易规则，把 10 USDT
         换算为合法的数量（如 0.01 ZEC），再下单。
    """
    client = get_client()

    # 再保险：如果某些版本没有提前创建属性，就在这里兜底一下
    if not hasattr(client, "configV3"):
        try:
            client.configs_v3()
        except Exception as e:
            print("[apex_client] fallback configs_v3 error:", e)

    if not hasattr(client, "accountV3"):
        try:
            client.get_account_v3()
        except Exception as e:
            print("[apex_client] fallback get_account_v3 error:", e)

    side = side.upper()

    # -----------------------------
    # 分支 A：用 USDT 预算自动撮合
    # -----------------------------
    if size_usdt is not None:
        budget = Decimal(str(size_usdt))
        if budget <= 0:
            raise ValueError("size_usdt must be > 0")

        rules = _get_symbol_rules(symbol)
        min_qty = rules["min_qty"]
        decimals = rules["qty_decimals"]

        # 先用最小数量去问一次 worst price，当作当前市价参考
        ref_price_decimal = Decimal(
            get_market_price(symbol, side, str(min_qty))
        )

        # 理论数量 = 预算 / 价格
        theoretical_qty = budget / ref_price_decimal

        # 对齐到交易所允许的网格（0.01, 0.02, ...）
        snapped_qty = _snap_quantity(symbol, theoretical_qty)

        # 为了更准确，再用真正的下单数量问一次 worst price
        price_str = get_market_price(symbol, side, str(snapped_qty))
        price_decimal = Decimal(price_str)

        # 格式化成固定小数位（例如 0.01）
        size_str = format(snapped_qty, f".{decimals}f")

        used_budget = (snapped_qty * price_decimal).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )

        print(
            f"[apex_client] budget={budget} USDT -> qty={size_str} {symbol.split('-')[0]}, "
            f"used≈{used_budget} USDT (price {price_str})"
        )

    # -----------------------------
    # 分支 B：旧逻辑，直接用 size 作为数量
    # -----------------------------
    else:
        if size is None:
            raise ValueError("size or size_usdt must be provided")

        size_str = str(size)
        price_str = get_market_price(symbol, side, size_str)

    ts = int(time.time())

    # ApeX 使用纯数字 clientId，我们这里总是重新生成一个合法的
    apex_client_id = _random_client_id()
    tv_client_id = client_id
    if tv_client_id:
        print(
            f"[apex_client] tv_client_id={tv_client_id} -> apex_clientId={apex_client_id}"
        )
    else:
        print(f"[apex_client] apex_clientId={apex_client_id} (no tv_client_id)")

    print(
        "[apex_client] create_market_order params:",
        {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "size": size_str,
            "price": price_str,
            "reduceOnly": reduce_only,
            "clientId": apex_client_id,
            "timestampSeconds": ts,
        },
    )

    # 关键：参数名要和 SDK 的 create_order_v3 定义一致
    order = client.create_order_v3(
        symbol=symbol,
        side=side,
        type="MARKET",
        size=size_str,
        price=price_str,
        timestampSeconds=ts,
        reduceOnly=reduce_only,   # ✅ 驼峰写法
        clientId=apex_client_id,  # ✅ 使用合法的纯数字 ID
    )

    print("[apex_client] order response:", order)
    return order


# ---------------------------------------------------------
# 新增：市价入场 + 直接挂 TP(+1.9%) / SL(-0.6%) 的封装
# ---------------------------------------------------------

def create_market_order_with_tpsl(
    symbol: str,
    side: str,
    size_usdt: str | float | int,
    tp_pct: float | str | Decimal = 0.019,  # +1.9%
    sl_pct: float | str | Decimal = 0.006,  # -0.6%
    client_id: str | None = None,
):
    """
    市价开仓 + 同一请求里挂 position TP/SL：
    - 对于多单(BUY)：
        TP 价格 = 开仓价 * (1 + tp_pct)
        SL 价格 = 开仓价 * (1 - sl_pct)
    - 对于空单(SELL)：
        TP 价格 = 开仓价 * (1 - tp_pct)
        SL 价格 = 开仓价 * (1 + sl_pct)

    注意：
    1）这是 position 级别的 TP/SL，会跟仓位绑定；
    2）本函数仅支持用 USDT 金额撮合（size_usdt），
       你原来的 create_market_order 仍然可以直接用。
    """
    client = get_client()

    # 再保险：兜底初始化 config/account
    if not hasattr(client, "configV3"):
        try:
            client.configs_v3()
        except Exception as e:
            print("[apex_client] fallback configs_v3 error:", e)

    if not hasattr(client, "accountV3"):
        try:
            client.get_account_v3()
        except Exception as e:
            print("[apex_client] fallback get_account_v3 error:", e)

    side = side.upper()
    budget = Decimal(str(size_usdt))
    if budget <= 0:
        raise ValueError("size_usdt must be > 0")

    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]
    decimals = rules["qty_decimals"]

    # 1) 先用最小数量问一次价格，得到参考价
    ref_price_decimal = Decimal(
        get_market_price(symbol, side, str(min_qty))
    )

    theoretical_qty = budget / ref_price_decimal
    snapped_qty = _snap_quantity(symbol, theoretical_qty)

    # 2) 用真实数量再问一次价格作为开仓价
    price_str = get_market_price(symbol, side, str(snapped_qty))
    entry_price = Decimal(price_str)

    size_str = format(snapped_qty, f".{decimals}f")
    used_budget = (snapped_qty * entry_price).quantize(
        Decimal("0.01"), rounding=ROUND_DOWN
    )

    print(
        f"[apex_client] (TP/SL) budget={budget} USDT -> qty={size_str} {symbol.split('-')[0]}, "
        f"used≈{used_budget} USDT (entry {price_str})"
    )

    tp_pct_dec = Decimal(str(tp_pct))
    sl_pct_dec = Decimal(str(sl_pct))

    if side == "BUY":
        # 多单：价格涨 +1.9% 止盈，下跌 -0.6% 止损
        tp_trigger = (entry_price * (Decimal("1") + tp_pct_dec)).quantize(
            Decimal("0.1"), rounding=ROUND_DOWN
        )
        sl_trigger = (entry_price * (Decimal("1") - sl_pct_dec)).quantize(
            Decimal("0.1"), rounding=ROUND_DOWN
        )
        tp_side = "SELL"
        sl_side = "SELL"
    else:
        # 空单：价格跌 -1.9% 止盈，上涨 +0.6% 止损
        tp_trigger = (entry_price * (Decimal("1") - tp_pct_dec)).quantize(
            Decimal("0.1"), rounding=ROUND_DOWN
        )
        sl_trigger = (entry_price * (Decimal("1") + sl_pct_dec)).quantize(
            Decimal("0.1"), rounding=ROUND_DOWN
        )
        tp_side = "BUY"
        sl_side = "BUY"

    ts = int(time.time())
    apex_client_id = _random_client_id()
    tv_client_id = client_id
    if tv_client_id:
        print(
            f"[apex_client] (TP/SL) tv_client_id={tv_client_id} -> apex_clientId={apex_client_id}"
        )
    else:
        print(f"[apex_client] (TP/SL) apex_clientId={apex_client_id} (no tv_client_id)")

    # ★ 这里使用 create_order_v3 的 TP/SL 参数（按官方文档）
    #   isOpenTpslOrder + isSetOpenTp/isSetOpenSl 会在开仓时帮你挂好仓位 TP/SL
    print(
        "[apex_client] create_market_order_with_tpsl params:",
        {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "size": size_str,
            "price": price_str,
            "tpTriggerPrice": str(tp_trigger),
            "slTriggerPrice": str(sl_trigger),
        },
    )

    order = client.create_order_v3(
        symbol=symbol,
        side=side,
        type="MARKET",
        size=size_str,
        price=price_str,
        timestampSeconds=ts,
        reduceOnly=False,
        clientId=apex_client_id,

        # ====== 关键：一并设置 Position TP/SL ======
        isOpenTpslOrder=True,

        # SL：市价止损（STOP_MARKET）
        isSetOpenSl=True,
        slSide=sl_side,
        slSize=size_str,
        slType="STOP_MARKET",
        slTriggerPrice=str(sl_trigger),
        slPrice=str(sl_trigger),

        # TP：市价止盈（TAKE_PROFIT_MARKET）
        isSetOpenTp=True,
        tpSide=tp_side,
        tpSize=size_str,
        tpType="TAKE_PROFIT_MARKET",
        tpTriggerPrice=str(tp_trigger),
        tpPrice=str(tp_trigger),
    )

    print("[apex_client] (TP/SL) order response:", order)

    return {
        "order": order,
        "computed": {
            "size": size_str,
            "entry_price": str(entry_price),
            "tp_trigger": str(tp_trigger),
            "sl_trigger": str(sl_trigger),
        },
    }
