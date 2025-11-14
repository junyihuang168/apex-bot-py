import os
import time
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.client import HttpPrivateSign

print("Hello, Apex omni")

# 读取 DO 环境变量
key        = os.getenv("APEX_API_KEY")
secret     = os.getenv("APEX_API_SECRET")
passphrase = os.getenv("APEX_API_PASSPHRASE")
l2key      = os.getenv("APEX_L2KEY_SEEDS")

# 检查环境变量是否真的有值（开发阶段很重要）
print("Loaded env variables:")
print("API_KEY:", bool(key))
print("API_SECRET:", bool(secret))
print("PASS:", bool(passphrase))
print("L2KEY:", bool(l2key))

client = HttpPrivateSign(
    APEX_OMNI_HTTP_TEST,
    network_id=NETWORKID_TEST,

    # 让 SDK 自动生成 zk_seeds（不要自己填）
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

# 下单测试
currentTime = time.time()
order = client.create_order_v3(
    symbol="BTC-USDT",
    side="SELL",
    type="MARKET",
    size="0.001",
    timestampSeconds=currentTime,
    price="60000",
)

print("Order result:", order)
