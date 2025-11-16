import os
import time
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_v3 import HttpPrivateSign  # ✅ 用 V3 版本

print("Hello, Apex Omni (client test)")

def make_client():
    # ---- 读取环境变量（在 DO 或本地都一样）----
    key        = os.getenv("APEX_API_KEY")
    secret     = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    seeds      = os.getenv("APEX_ZK_SEEDS")    # 建议你在 DO 里新建这个变量
    l2key      = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables:")
    print("API_KEY :", bool(key))
    print("API_SECRET :", bool(secret))
    print("PASS :", bool(passphrase))
    print("SEEDS :", bool(seeds))
    print("L2KEY :", bool(l2key))

    if not all([key, secret, passphrase]):
        raise RuntimeError("Missing one or more APEX_API_* environment variables")

    if not seeds or not l2key:
        print("⚠️ WARNING: zk_seeds 或 l2Key 没有设置，create_order_v3 可能会失败")

    # ---- 创建 HttpPrivateSign V3 client ----
    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,
        network_id=NETWORKID_TEST,
        zk_seeds=seeds,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    # 官方示例里建议下单前先跑这两个
    configs = client.configs_v3()
    account = client.get_account_v3()
    print("configs_v3 ok")
    print("get_account_v3 ok")

    return client

if __name__ == "__main__":
    client = make_client()

    current_time = time.time()
    # 这里是测试单：BTC-USDT，卖 0.001，MARKET 单
    create_order_res = client.create_order_v3(
        symbol="BTC-USDT",
        side="SELL",
        type="MARKET",
        size="0.001",
        timestampSeconds=current_time,
        price="60000",
    )

    print("Order result:", create_order_res)
