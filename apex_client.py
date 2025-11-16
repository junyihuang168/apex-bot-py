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
    print("API_KEY:",    bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:",       bool(passphrase))
    print("L2KEY:",      bool(l2key))

    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,       # 目前仍然是 TEST 网络
        network_id=NETWORKID_TEST,
        zk_seeds=None,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    # 打印一下有哪些 account 相关的属性
    acct_attrs = [a for a in dir(client) if "account" in a.lower()]
    print("Client attributes containing 'account':", acct_attrs)

    return client


if __name__ == "__main__":
    print("Hello, Apex omni (test client)")

    client = make_client()

    # 1) 读取 configs / account，看 API 是否正常
    configs = client.configs_v3()
    print("configs_v3 ok:", configs)

    account = client.get_account_v3()
    print("get_account_v3 ok:", account)

    # 2) 不再用 client.create_order_v3（它有 bug）
    #    直接从 account 客户端上下单
    account_client = getattr(client, "accountv3", None) or getattr(client, "account", None)
    if account_client is None:
        raise RuntimeError("No account client found on HttpPrivateSign instance")

    print("Using account_client type in apex_client.py:", type(account_client))

    current_time = int(time.time())
    try:
        order = account_client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=current_time,
            price="60000",
        )
        print("create_order_v3 ok (via account_client):", order)
    except Exception as e:
        print("❌ create_order_v3 failed in apex_client.py (via account_client):", e)
