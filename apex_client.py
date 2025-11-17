# apex_client.py
# 负责创建 Apex Omni HttpPrivateSign 客户端

import os

from apexomni.http_private_v3 import HttpPrivateSign
from apexomni.constants import (
    APEX_OMNI_HTTP_TEST,
    APEX_OMNI_HTTP_MAIN,
    NETWORKID_OMNI_TEST_BNB,
    NETWORKID_OMNI_MAIN_ARB,
)


def make_client():
    """从环境变量中创建 HttpPrivateSign 客户端"""

    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")

    # ZK 种子：用 Omni Key Seed
    seeds = os.getenv("APEX_ZK_SEEDS", "")

    # L2Key 可以留空（SDK 里是可选的）
    l2key = os.getenv("APEX_L2KEY_SEEDS", "")

    print("Loaded env variables in apex_client.py:")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("SEEDS(optional):", bool(seeds))
    print("L2KEY(optional):", bool(l2key))

    # 只把 API key 三个当作必填
    if not all([key, secret, passphrase]):
        raise RuntimeError("Missing one or more mandatory APEX_API_* environment variables")

    use_mainnet = os.getenv("APEX_USE_MAINNET", "false").lower() == "true"
    if use_mainnet:
        endpoint = APEX_OMNI_HTTP_MAIN
        network_id = NETWORKID_OMNI_MAIN_ARB
    else:
        endpoint = APEX_OMNI_HTTP_TEST
        network_id = NETWORKID_OMNI_TEST_BNB

    print("Using endpoint:", endpoint)
    print("Using network_id:", network_id)

    client = HttpPrivateSign(
        endpoint,
        network_id=network_id,
        zk_seeds=seeds or None,
        zk_l2Key=l2key or None,
        api_key_credentials={"key": key, "secret": secret, "passphrase": passphrase},
    )

    # 打印一下含有 account 的属性名，方便排查
    acct_attrs = [a for a in dir(client) if "account" in a.lower()]
    print("Client attributes containing 'account':", acct_attrs)

    return client


# 本文件直接运行时做个简单自检（在 DO 上不会用到）
if __name__ == "__main__":
    from pprint import pprint

    c = make_client()
    cfg = c.configs_v3()
    acc = c.get_account_v3()
    print("configs_v3:")
    pprint(cfg)
    print("account_v3:")
    pprint(acc)
