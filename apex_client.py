import os
import time
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign   # ✅ 关键：按 GitHub 写

print("Hello, Apex Omni (testnet)")

def make_client():
    key        = os.getenv("APEX_API_KEY")
    secret     = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    l2key      = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables:")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("L2KEY:", bool(l2key))

    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,
        network_id=NETWORKID_TEST,
        zk_seeds=None,       # 让 SDK 自己处理 seeds
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )
    return client

if __name__ == "__main__":
    client = make_client()

    # 1) 配置
    configs = client.configs_v3()
    print("configs_v3:", configs)

    # 2) 账户
    account = client.get_account_v3()
    print("get_account_v3:", account)

    # 3) 测试单（TEST 网）
    now = int(time.time())
    order = client.create_order_v3(
        symbol="BTC-USDT",
        side="SELL",
        type="MARKET",
        size="0.001",
        timestampSeconds=now,
        price="60000",
    )
    print("create_order_v3 result:", order)
