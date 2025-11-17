import os
import time

# ✅ 按官方文档的用法：从 http_private_v3 导入 HttpPrivateSign
from apexomni.http_private_v3 import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_TEST,
    APEX_OMNI_HTTP_MAIN,
    NETWORKID_TEST,
    NETWORKID_OMNI_MAIN_ARB,
)


def make_client():
    """创建 HttpPrivateSign 客户端（支持 Test / Mainnet 切换）"""

    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    seeds = os.getenv("APEX_ZK_SEEDS")
    l2key = os.getenv("APEX_L2KEY_SEEDS", "")

    print("Loaded env variables in make_client():")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("L2KEY:", bool(l2key))

    if not all([key, secret, passphrase, seeds]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    # APEX_USE_MAINNET = "true" 时走主网，否则走 TEST
    use_mainnet = os.getenv("APEX_USE_MAINNET", "false").lower() == "true"

    endpoint = APEX_OMNI_HTTP_MAIN if use_mainnet else APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_OMNI_MAIN_ARB if use_mainnet else NETWORKID_TEST

    print("Using endpoint:", endpoint)
    print("Using network_id:", network_id)

    client = HttpPrivateSign(
        endpoint,
        network_id=network_id,
        zk_seeds=seeds,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    return client


def self_test():
    """本地 /test 用：拉 configs + account + 发一笔小测试单"""
    client = make_client()

    configs = client.configs_v3()
    account = client.get_account_v3()
    print("Configs:", configs)
    print("Account:", account)

    now = int(time.time())
    order = client.create_order_v3(
        symbol="BTC-USDT",
        side="SELL",
        type="MARKET",
        size="0.001",
        timestampSeconds=now,
        price="60000",
    )
    print("Test order result:", order)


if __name__ == "__main__":
    self_test()
