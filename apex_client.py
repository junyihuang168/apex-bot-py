import os
import time

from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

print("Hello, Apex Omni (apex_client.py)")

# 读取 DO 环境变量
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

# TESTNET 客户端
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

# 拉取配置与账户余额
configs = client.configs_v3()
accountData = client.get_account_v3()

print("Configs:", configs)
print("Account Data:", accountData)

# 下单测试（测试网极小仓位）
currentTime = int(time.time())
order = client.create_order_v3(
    symbol="BTC-USDT",
    side="SELL",
    type="MARKET",
    size="0.001",
    timestampSeconds=currentTime,
    price="60000",
)

print("Order result:", order)
