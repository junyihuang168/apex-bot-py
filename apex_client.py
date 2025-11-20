# apex_client.py
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

# ---------------------------------------------------------------------
# 一些小工具
# ---------------------------------------------------------------------


def _env_bool(name: str, default: bool = False) -> bool:
    """把环境变量字符串转成布尔值."""
    value = os.getenv(name, str(default))
    return value.lower() in ("1", "true", "yes", "y", "on")


def _get_base_and_network():
    """
    根据环境变量决定用 mainnet 还是 testnet。

    - APEX_USE_MAINNET=true  或
    - APEX_ENV=main / mainnet / prod / production
    都会走主网，否则走 testnet。
    """
    use_mainnet = _env_bool("APEX_USE_MAINNET", False)

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


# ---------------------------------------------------------------------
# 全局客户端（带缓存）
# ---------------------------------------------------------------------

_sign_client: HttpPrivateSign | None = None
_http_v3_client: HttpPrivate_v3 | None = None


def _get_sign_client() -> HttpPrivateSign:
    """
    返回带 zk 签名的 HttpPrivateSign 客户端，
    并且**预热一次 get_account_v3()**，保证有 accountV3。
    """
    global _sign_client
    if _sign_client is None:
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

        # ★ 关键：预热 accountV3，避免 AttributeError: accountV3
        try:
            _ = client.get_account_v3()
            print("[apex_client] warmup get_account_v3() ok")
        except Exception as e:  # noqa: BLE001
            # 即使失败也不致命，最多影响第一笔单
            print("[apex_client] warmup get_account_v3() failed:", e)

        _sign_client = client

    return _sign_client


def _get_http_v3_client() -> HttpPrivate_v3:
    """不需要 zk 的 HTTP v3 客户端（用来查 worst price 等公共接口）。"""
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


# ---------------------------------------------------------------------
# 对外暴露的一些小函数
# ---------------------------------------------------------------------


def get_account():
    """查询账户信息，方便在本地 / 日志里调试。"""
    client = _get_sign_client()
    return client.get_account_v3()


def _extract_worst_price(res) -> float:
    """
    从 get_worst_price_v3 的返回里抽出 worstPrice。
    官方有时候会包一层 data，所以做个兜底解析。
    """
    price = None
    if isinstance(res, dict):
        if "worstPrice" in res:
            price = res["worstPrice"]
        elif "data" in res and isinstance(res["data"], dict) and "worstPrice" in res["data"]:
            price = res["data"]["worstPrice"]

    if price is None:
        raise RuntimeError(f"[apex_client] get_worst_price_v3 返回异常: {res}")

    return float(price)


def get_market_price(symbol: str, side: str, usdt_size: float) -> float:
    """
    方案 B 辅助函数：获取当前市价。

    这里为了简单，直接用 size=1 调 get_worst_price_v3，
    对于你这种小仓位下单影响可以忽略。
    """
    http = _get_http_v3_client()
    side_up = side.upper()

    # size 传 1 就行，我们只要一个「每 1 个币」的大概价格
    res = http.get_worst_price_v3(symbol=symbol, size="1", side=side_up)
    price = _extract_worst_price(res)

    print(
        f"[apex_client] worst price for {symbol} {side_up} "
        f"sizeUSDT={usdt_size}: {price}"
    )
    return price


# ---------------------------------------------------------------------
# 自动撮合 USDT 金额 -> 币数量，并自动处理 stepSize 的 create_market_order
# ---------------------------------------------------------------------


def _submit_order(
    client: HttpPrivateSign,
    *,
    symbol: str,
    side: str,
    size_str: str,
    price_str: str,
    reduce_only: bool,
) -> dict:
    """
    真正调 create_order_v3 的地方。
    注意：
      - 这里只传官方肯定支持的参数，避免 TypeError。
      - reduce_only -> reduceOnly （驼峰）传给 SDK。
    """
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

    order = client.create_order_v3(
        symbol=symbol,
        side=side,
        type="MARKET",
        size=size_str,
        price=price_str,
        timestampSeconds=ts,
        reduceOnly=reduce_only,
    )

    print("[apex_client] order response:", order)
    return order


def create_market_order(
    symbol: str,
    side: str,
    usdt_size: float,
    reduce_only: bool = False,
):
    """
    方案 B：按照「USDT 金额」自动换算成币的数量再下 MARKET 单。

    - symbol: 'ZEC-USDT' 之类
    - side: 'BUY' / 'SELL'
    - usdt_size: 你在 TV / Pine 里输入的 USDT 金额（例如 10）
    - reduce_only: True 表示只减仓，用于平仓信号
    """
    side_up = side.upper()
    usdt_f = float(usdt_size)
    if usdt_f <= 0:
        raise ValueError(f"usdt_size 必须 > 0，当前为 {usdt_size}")

    client = _get_sign_client()

    # 1) 先拿一个市价（每 1 个币大概多少钱）
    price = get_market_price(symbol, side_up, usdt_f)

    # 2) 用 USDT 金额除以价格，得到大概的币数量
    #    用 Decimal 处理小数，并向下取整，避免金额超出。
    qty_est = Decimal(str(usdt_f)) / Decimal(str(price))

    # 先粗略保留 6 位小数再向下取整
    qty_est = qty_est.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
    if qty_est <= 0:
        raise ValueError(f"根据 USDT 金额 {usdt_size} 算出来的数量太小: {qty_est}")

    size_str = format(qty_est.normalize(), "f")
    price_str = str(price)

    # 3) 第一次尝试下单
    resp = _submit_order(
        client,
        symbol=symbol,
        side=side_up,
        size_str=size_str,
        price_str=price_str,
        reduce_only=reduce_only,
    )

    # 4) 如果返回的是 stepSize 不合法（INVALID_DECIMAL_SCALE_PARAM），
    #    读出 stepSize，自动向下取整 1 次再重新下单。
    try:
        if (
            isinstance(resp, dict)
            and resp.get("code") == 3
            and isinstance(resp.get("msg"), str)
            and "INVALID_DECIMAL_SCALE_PARAM" in resp.get("msg", "")
        ):
            detail = resp.get("detail") or {}
            step_size_str = detail.get("stepSize")
            if step_size_str:
                step = Decimal(step_size_str)
                size_dec = Decimal(size_str)

                # 按 stepSize 向下取整
                # new_size = floor(size_dec / step) * step
                new_size_units = (size_dec / step).to_integral_value(
                    rounding=ROUND_DOWN
                )
                new_size_dec = new_size_units * step

                if new_size_dec <= 0:
                    print(
                        "[apex_client] stepSize 调整后 size<=0，"
                        "不再重试下单，新 size:",
                        new_size_dec,
                    )
                    return resp

                new_size_str = format(new_size_dec.normalize(), "f")
                print(
                    "[apex_client] INVALID_DECIMAL_SCALE_PARAM，"
                    f"根据 stepSize={step_size_str} 把 size 从 {size_str} 调整为 {new_size_str}，"
                    "准备重新下单……"
                )

                # 用新的 size 再试一次
                resp2 = _submit_order(
                    client,
                    symbol=symbol,
                    side=side_up,
                    size_str=new_size_str,
                    price_str=price_str,
                    reduce_only=reduce_only,
                )
                return resp2
    except Exception as e:  # noqa: BLE001
        # 这里任何解析失败都不致命，直接把第一次返回的结果给出去
        print("[apex_client] 处理 INVALID_DECIMAL_SCALE_PARAM 时出错:", e)

    return resp
