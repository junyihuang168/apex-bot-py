import os
import time

from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign


def make_client():
    """创建带补丁的 HttpPrivateSign 客户端"""
    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    l2key = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables in apex_client.py:")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("L2KEY:", bool(l2key))

    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,
        network_id=NETWORKID_TEST,
        zk_seeds=None,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    # --- 关键补丁：SDK 里 create_order_v3 用的是 self.accountv3 ---
    if hasattr(client, "account_v3") and not hasattr(client, "accountv3"):
        client.accountv3 = client.account_v3
        print("✅ Patched client.accountv3 from client.account_v3")

    return client


def main():
    client = make_client()

    # 配置和账户测试
    configs = client.configs_v3()
    print("configs_v3 ok:", configs)

    account = client.get_account_v3()
    print("get_account_v3 ok:", account)

    # 下单测试（TEST 网络，小仓位）
    current_time = int(time.time())
    try:
        order = client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=current_time,
            price="60000",
        )
        print("✅ create_order_v3 ok:", order)
    except Exception as e:
        print("❌ create_order_v3 failed in apex_client.py:", e)


if __name__ == "__main__":
    main()
