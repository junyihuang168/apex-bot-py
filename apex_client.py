import os
import time

# 用之前证明可用的导入方式
from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import APEX_OMNI_HTTP_TEST, NETWORKID_TEST

# 主网常量可能在当前版本没有，所以用 try/except 包一层
try:
    from apexomni.constants import APEX_OMNI_HTTP_MAIN, NETWORKID_OMNI_MAIN_ARB
except ImportError:
    APEX_OMNI_HTTP_MAIN = None
    NETWORKID_OMNI_MAIN_ARB = None


def make_client():
    """
    从环境变量创建 HttpPrivateSign 客户端。
    依赖环境变量：
      - APEX_API_KEY
      - APEX_API_SECRET
      - APEX_API_PASSPHRASE
      - APEX_ZK_SEEDS
      - APEX_L2KEY_SEEDS
      - APEX_USE_MAINNET   (可选，"true" 则走主网，否则 TEST)
    """
    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    seeds = os.getenv("APEX_ZK_SEEDS")
    l2key = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables in make_client():")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("SEEDS:", bool(seeds))
    print("L2KEY:", bool(l2key))

    if not all([key, secret, passphrase, seeds]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    use_mainnet = os.getenv("APEX_USE_MAINNET", "false").lower() == "true"
    if use_mainnet:
        if APEX_OMNI_HTTP_MAIN is None or NETWORKID_OMNI_MAIN_ARB is None:
            raise RuntimeError("This apexomni version has no mainnet constants")
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
        zk_seeds=seeds,
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

    # 尝试从现有的属性里找一个真正的「账户客户端」对象
    for name in ("accountV3", "account_v3", "account"):
        attr = getattr(client, name, None)
        if attr is None:
            continue
        source_name = name

        # 可能是一个方法，需要调用一次拿到真正的 client 对象
        if callable(attr):
            try:
                acct_obj = attr()   # account()
            except TypeError:
                # 如果调用失败，就直接当成对象用
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
    """本地调试用：/test 时可以调用"""
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
