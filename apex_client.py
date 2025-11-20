# apex_client.py
import os
import time
import math

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.http_private_v3 import HttpPrivate_v3
from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_TEST,
)

# ---------------------------
# 小工具：环境变量 & 网络选择
# ---------------------------

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, str(default))
    return v.lower() in ("1", "true", "yes", "y", "on")


def _get_base_and_network():
    """根据环境变量决定走 testnet 还是 mainnet。"""
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)

    # 兼容你额外加的 APEX_ENV
    env_name = os.getenv("APEX_ENV", "").lower()
    if env_name in ("main", "mainnet", "prod", "production"):
        use_mainnet = True

    base_url = APEX_OMNI_HTTP_MAIN if use_mainnet else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_OMNI_MAIN_ARB if use_mainnet else NETWORKID_TEST
    return base_url, network_id


def _get_api_credentials():
    """从环境变量拿 apiKey / secret / passphrase。"""
    return {
        "key": os.environ["APEX_API_KEY"],
        "secret": os.environ["APEX_API_SECRET"],
        "passphrase": os.environ["APEX_API_PASSPHRASE"],
    }


# ---------------------------
# 构造客户端
# ---------------------------

def get_client() -> HttpPrivateSign:
    """
    返回带 zk 签名的 HttpPrivateSign 客户端，
    用于真正的 create_order_v3 下单。
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
    return client


def _get_http_v3_client() -> HttpPrivate_v3:
    """
    只需要 apiKey 的 v3 客户端，用来调 get_worst_price_v3 / configs_v3 等。
    """
    base_url, network_id = _get_base_and_network()
    api_creds = _get_api_credentials()
    return HttpPrivate_v3(
        base_url,
        network_id=network_id,
        api_key_credentials=api_creds,
    )


# ---------------------------
# 配置 & 规则
# ---------------------------

def _load_symbol_rules(symbol: str):
    """
    从 configs_v3 里读取该合约的:
    - stepSize (每一档的最小数量)
    - minSize  (最小下单数量)
    如果没找到，就给一个保底值，避免崩溃。
    """
    http_v3 = _get_http_v3_client()
    cfg = http_v3.configs_v3()

    step_size = None
    min_size = None

    if isinstance(cfg, dict):
        # 常见结构：cfg["perpetualContract"] 是 list 或 dict
        candidates = []
        for key in ("perpetualContract", "perpetualContracts", "contracts", "contract"):
            if key in cfg:
                candidates.append(cfg[key])

        for block in candidates:
            markets = []
            if isinstance(block, list):
                markets = block
            elif isinstance(block, dict):
                # 有的结构是 {symbol: {...}}
                if "symbols" in block and isinstance(block["symbols"], list):
                    markets = block["symbols"]
                else:
                    markets = list(block.values())

            for m in markets:
                if not isinstance(m, dict):
                    continue
                sym = m.get("symbol") or m.get("market") or m.get("name")
                if sym == symbol:
                    # 字段名在不同版本里可能不一样，这里多试几个
                    step_size_str = (
                        m.get("stepSize")
                        or m.get("sizeStep")
                        or m.get("size_step")
                        or m.get("step_size")
                    )
                    min_size_str = (
                        m.get("minSize")
                        or m.get("minOrderSize")
                        or m.get("min_size")
                        or m.get("minOrderAmt")
                    )

                    if step_size_str is not None:
                        step_size = float(step_size_str)
                    if min_size_str is not None:
                        min_size = float(min_size_str)
                    break

    # 保底：如果取不到，就默认 0.001 / 0.001
    if step_size is None or step_size <= 0:
        step_size = 0.001
    if min_size is None or min_size <= 0:
        min_size = step_size

    return step_size, min_size


def _decimals_from_step(step: float) -> int:
    s = f"{step:.16f}".rstrip("0").rstrip(".")
    if "." not in s:
        return 0
    return len(s.split(".")[1])


def _quantize(value: float, step: float) -> float:
    """
    把数量 value 向下取整到 step 的整数倍，
    保证不会超过预算。
    """
    if step <= 0:
        return value
    steps = math.floor(value / step + 1e-12)
    return steps * step


def _format_size(size: float, step: float) -> str:
    """按 step 的小数位数把 size 变成字符串。"""
    decimals = _decimals_from_step(step)
    fmt = "{:0." + str(decimals) + "f}"
    return fmt.format(size)


# ---------------------------
# 账户 & 行情
# ---------------------------

def get_account():
    """查询账户信息，在本地或日志里调试用。"""
    client = get_client()
    return client.get_account_v3()


def _get_worst_price(symbol: str, side: str, size: float) -> float:
    """
    调官方 get_worst_price_v3 拿「盘口最差成交价」，
    size 用的是「币的数量」。
    """
    http_v3 = _get_http_v3_client()

    side = side.upper()
    size_str = str(size)

    res = http_v3.get_worst_price_v3(
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

    price_f = float(price)
    print(f"[apex_client] worst price for {symbol} {side} size={size}: {price_f}")
    return price_f


# ---------------------------
# 金额下单（USDT → 币数量）
# ---------------------------

def create_market_order(
    symbol: str,
    side: str,
    # 兼容旧版本：你可以传 size=10，也可以传 usdt_size=10
    size: float | None = None,
    usdt_size: float | None = None,
    reduce_only: bool = False,
    client_id: str | None = None,
):
    """
    方案 B：你输入的是「USDT 金额」，代码自动撮合成「币的数量」再下单。

    参数
    ----
    symbol:     'ZEC-USDT' 这种
    side:       'BUY' / 'SELL'
    size / usdt_size:  你想用多少 USDT 下单（比如 10）
    reduce_only: True 用在平仓单；False 用在开仓单
    client_id:  TradingView 传进来的 client_id（可选）
    """
    # ------- 兼容处理 -------
    if usdt_size is None:
        if size is None:
            raise ValueError("[apex_client] 必须提供 size 或 usdt_size 其中一个")
        usdt_size = float(size)
    else:
        usdt_size = float(usdt_size)

    client = get_client()

    # 这个调用会在 HttpPrivateSign 里初始化 configV3，避免之前的 AttributeError
    client.configs_v3()

    step_size, min_size = _load_symbol_rules(symbol)

    side = side.upper()

    if usdt_size <= 0:
        raise ValueError("[apex_client] usdt_size 必须 > 0")

    # 1) 先用最小 size 估一下价格
    probe_size = step_size
    probe_price = _get_worst_price(symbol, side, probe_size)

    # 2) 用估价计算「理论数量」
    raw_size = usdt_size / probe_price

    # 3) 向下取整到 stepSize 的整数倍，保证不会超预算
    size_float = _quantize(raw_size, step_size)

    if size_float < min_size:
        raise RuntimeError(
            f"[apex_client] 预算太小：按 {usdt_size} USDT 换算后 size={size_float}, "
            f"但 minSize={min_size}"
        )

    size_str = _format_size(size_float, step_size)

    # 4) 用真正要下的 size 再拿一次 worstPrice，当作市价单 price
    worst_price = _get_worst_price(symbol, side, size_float)
    price_str = f"{worst_price:.8f}".rstrip("0").rstrip(".")

    ts = int(time.time())

    if client_id is None:
        safe_symbol = symbol.replace("/", "-")
        client_id = f"tv-{safe_symbol}-{ts}"

    print(
        "[apex_client] create_market_order params:",
        {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "usdt_size": float(usdt_size),
            "size": size_str,
            "price": price_str,
            "reduce_only": bool(reduce_only),
            "clientOrderId": client_id,
            "timestampSeconds": ts,
        },
    )

    # 5) 真正下单：按照官方 demo 的 create_order_v3 风格
    order = client.create_order_v3(
        symbol=symbol,
        side=side,
        type="MARKET",
        size=size_str,
        timestampSeconds=ts,
        price=price_str,
        # 注意：新版 SDK 用 reduceOnly / clientOrderId（驼峰写法）
        reduceOnly=reduce_only,
        clientOrderId=client_id,
    )

    print("[apex_client] order response:", order)
    return order
