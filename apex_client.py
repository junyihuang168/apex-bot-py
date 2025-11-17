import os
import time

from apexomni.http_private_sign import HttpPrivateSign
from apexomni.constants import APEX_OMNI_HTTP_TEST, NETWORKID_TEST

# 主网常量，用 try/except 包一下，防止当前 SDK 里没有这些常量时导入失败
try:
    from apexomni.constants import APEX_OMNI_HTTP_MAIN, NETWORKID_OMNI_MAIN_ARB
except ImportError:
    APEX_OMNI_HTTP_MAIN = None
    NETWORKID_OMNI_MAIN_ARB = None


def make_client() -> HttpPrivateSign:
    """
    从环境变量创建 HttpPrivateSign 客户端。

    必须有：
      - APEX_API_KEY
      - APEX_API_SECRET
      - APEX_API_PASSPHRASE

    可选：
      - APEX_ZK_SEEDS（Omni Key Seed）
      - APEX_L2KEY_SEEDS（没有就算了，先不强制）
      - APEX_USE_MAINNET = 'true' 时走主网，否则 TEST 网
    """
    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    seeds = os.getenv("APEX_ZK_SEEDS")        # 可选，但最好有
    l2key = os.getenv("APEX_L2KEY_SEEDS")     # 可选

    print("Loaded env variables in make_client():")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("SEEDS(optional):", bool(seeds))
    print("L2KEY(optional):", bool(l2key))

    # ✅ 这里只要求 key / secret / pass，l2key 不再是必填
    if not all([key, secret, passphrase]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

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
        zk_seeds=seeds,     # 可以是 None
        zk_l2Key=l2key,     # 也可以是 None
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    # -----------------------------
    # 兼容补丁：补上 accountv3 / accountV3 / account_v3
    # -----------------------------
    acct_obj = None
    src_name = None

    for name in ("accountV3", "account_v3", "account", "accountClient"):
        attr = getattr(client, name, None)
        if attr is None:
            continue
        src_name = name

        # 既可能是对象，也可能是函数，这里都尝试一下
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
        for alias in ("accountv3", "accountV3", "account_v3"):
            if not hasattr(client, alias):
                setattr(client, alias, acct_obj)
                print(f"Patched client.{alias} from client.{src_name}")
    else:
        print("Warning: could not determine account client on HttpPrivateSign")

    acct_attrs = [a for a in dir(client) if "account" in a.lower()]
    print("Client attributes containing 'account':", acct_attrs)

    return client


# 仅本地调试用，DO 上不会进这里
if __name__ == "__main__":
    c = make_client()
    cfg = c.configs_v3()
    acc = c.get_account_v3()
    print("Configs:", cfg)
    print("Account:", acc)

    now = int(time.time())
    try:
        order = c.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=now,
            price="60000",
        )
        print("Test order result:", order)
    except Exception as e:
        print("create_order_v3 failed in apex_client self-test:", e)
