import os
import time

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_TEST,
    APEX_OMNI_HTTP_MAIN,
    NETWORKID_TEST,
    NETWORKID_MAIN,
)


def _str2bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def make_client() -> HttpPrivateSign:
    """从环境变量创建 HttpPrivateSign 客户端"""

    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")

    # zk 签名材料
    seeds = os.getenv("APEX_ZK_SEEDS")
    # 官方文档说 l2Key 可以为空字符串，所以这里允许为空
    l2key = os.getenv("APEX_L2KEY_SEEDS", "")

    print("Loaded env variables in make_client():")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("SEEDS:", bool(seeds))
    print("L2KEY:", bool(l2key))

    # seeds 是必须的，否则 create_order_v3 根本没法签名
    if not all([key, secret, passphrase, seeds]):
        raise RuntimeError(
            "Missing one or more required APEX_* environment variables "
            "(APEX_API_KEY / APEX_API_SECRET / APEX_API_PASSPHRASE / APEX_ZK_SEEDS)"
        )

    use_mainnet = _str2bool(os.getenv("APEX_USE_MAINNET", "false"), default=False)
    if use_mainnet:
        endpoint = APEX_OMNI_HTTP_MAIN
        network_id = NETWORKID_MAIN
    else:
        endpoint = APEX_OMNI_HTTP_TEST
        network_id = NETWORKID_TEST

    print("Using endpoint:", endpoint)
    print("Using network_id:", network_id)

    client = HttpPrivateSign(
        endpoint,
        network_id=network_id,
        zk_seeds=seeds,
        zk_l2Key=l2key,
        api_key_credentials={"key": key, "secret": secret, "passphrase": passphrase},
    )

    return client


# 仅本地手动测试时用：在 DO 上不会执行
if __name__ == "__main__":
    print("Running apex_client.py self-test...")
    c = make_client()
    cfg = c.configs_v3()
    acc = c.get_account_v3()
    print("configs_v3:", cfg)
    print("account_v3:", acc)

    now = int(time.time())
    order_res = c.create_order_v3(
        symbol="BTC-USDT",
        side="SELL",
        type="MARKET",
        size="0.001",
        timestampSeconds=now,
        price="60000",
    )
    print("test order:", order_res)
