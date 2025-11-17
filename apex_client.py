import os
import time

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import APEX_OMNI_HTTP_TEST, NETWORKID_TEST

# 主网常量如果当前版本没有，就忽略
try:
    from apexomni.constants import APEX_OMNI_HTTP_MAIN, NETWORKID_OMNI_MAIN_ARB
except ImportError:
    APEX_OMNI_HTTP_MAIN = None
    NETWORKID_OMNI_MAIN_ARB = None


def make_client():
    """
    从环境变量创建 HttpPrivateSign 客户端。

    必须有：
      - APEX_API_KEY
      - APEX_API_SECRET
      - APEX_API_PASSPHRASE
      - APEX_L2KEY_SEEDS

    可选：
      - APEX_ZK_SEEDS   （没有也没关系，SDK 会根据 l2key 推导）
      - APEX_USE_MAINNET = "true" 时走主网，否则 TEST 网
    """
    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    seeds = os.getenv("APEX_ZK_SEEDS")          # 可选
    l2key = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables in make_client():")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("SEEDS(optional):", bool(seeds))
    print("L2KEY:", bool(l2key))

    # 只强制检查 key / secret / passphrase / l2key
    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more mandatory APEX_* environment variables")

    use_mainnet = os.getenv("APEX_USE_MAINNET", "false").lower() == "true"
    if use_mainnet and APEX_OMNI_HTTP_MAIN and NETWORKID_OMNI_MAIN_ARB:
        endpoint = APEX_OMNI_HTTP_MAIN
        network_id = NETWORKID_OMNI_MAIN_ARB
    else:
        endpoint = APEX_OMNI_HTTP_TEST
        network_id = NETWORKID_TEST

    print("Using endpoint:", endpoint)
    print("Using network_id:", network_id)

    client = HttpPrivateSign(
        endpoint,
        network_id=network_id,
        zk_seeds=seeds,      # 可以是 None
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    # ---------- 兼容补丁：补上 accountV3 / account_v3 / accountv3 ----------
    acct_obj = None
    source_name = None

    for name in ("accountV3", "account_v3", "account"):
        attr = getattr(client, name, None)
        if attr is None:
            continue
        source_name = name

        # 可能是一个方法，尝试调用一次拿到真正的对象
        if callable(attr):
            try:
                acct_obj = attr()
            except TypeError:
                acct_obj = attr
        else:
            acct_obj = attr

        if acct_obj is not None:
            break

    if acct_obj is not None:
        for alias in ("accountV3", "account_v3", "accountv3"):
            if not hasattr(client, alias):
                setattr(client, alias, acct_obj)
                print(f"Patched client.{alias} from client.{source_name}")
    else:
        print("Warning: could not determine account client; create_order_v3 may fail.")

    acct_attrs = [a for a in dir(client) if "account" in a.lower()]
    print("Client attributes containing 'account':", acct_attrs)

    return client


def self_test():
    """本地 /test 用：简单自测一下"""
    client = make_client()

    configs = client.configs_v3()
    account = client.get_account_v3()
    print("Configs:", configs)
    print("Account:", account)

    now = int(time.time())
    try:
        order = client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=now,
            price="60000",
        )
        print("Test order result:", order)
    except Exception as e:
        print("✗ create_order_v3 failed in apex_client.py:", e)


if __name__ == "__main__":
    self_test()
