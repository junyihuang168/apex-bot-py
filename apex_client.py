import os
import time

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST


def make_client():
    """
    从环境变量创建 HttpPrivateSign 客户端。
    依赖的环境变量：
      - APEX_API_KEY
      - APEX_API_SECRET
      - APEX_API_PASSPHRASE
      - APEX_L2KEY_SEEDS
    """
    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    l2key = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables in make_client():")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("L2KEY:", bool(l2key))

    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    # 创建 HttpPrivateSign 客户端（目前仍然是 TEST 网）
    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,
        network_id=NETWORKID_TEST,
        # 让 SDK 自己根据 l2key 推导 zk_seeds
        zk_seeds=None,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    # 打印一下有哪些 account 相关的属性（调试用）
    acct_attrs = [a for a in dir(client) if "account" in a.lower()]
    print("Client attributes containing 'account':", acct_attrs)

    return client


if __name__ == "__main__":
    print("=== apex_client.py self test ===")

    client = make_client()

    # 1) 读取 configs / account，看 API 是否正常
    configs = client.configs_v3()
    print("Configs:", configs)

    account = client.get_account_v3()
    print("Account:", account)

    # 2) 测一笔小单（TEST 网）
    current_time = int(time.time())
    try:
        order = client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=current_time,
            price="60000",  # 市价单时一般会被忽略
        )
        print("Test order result:", order)
    except Exception as e:
        print("✗ create_order_v3 failed in apex_client.py:", e)
