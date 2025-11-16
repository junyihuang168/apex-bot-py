import os
import time
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

def make_client():
    key        = os.getenv("APEX_API_KEY")
    secret     = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    l2key      = os.getenv("APEX_L2KEY_SEEDS")

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
        zk_seeds=None,       # 让 SDK 自己根据 l2key 推导
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    # ---- 兼容补丁：给 SDK 补上 accountv3 属性 ----
    if not hasattr(client, "accountv3"):
        for cand in ("accountV3", "account_v3", "account", "accountClient"):
            if hasattr(client, cand):
                setattr(client, "accountv3", getattr(client, cand))
                print(f"Patched client.accountv3 from client.{cand}")
                break

    print("Client attributes containing 'account':",
          [a for a in dir(client) if "account" in a.lower()])

    return client


if __name__ == "__main__":
    print("Hello, Apex omni (test client)")

    client = make_client()

    # 先看配置 & 账户
    configs = client.configs_v3()
    account = client.get_account_v3()
    print("configs_v3 ok:", configs)
    print("get_account_v3 ok:", account)

    # 然后测一笔小单（测试网）
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
        print("create_order_v3 ok:", order)
    except Exception as e:
        print("❌ create_order_v3 failed in apex_client.py:", e)
