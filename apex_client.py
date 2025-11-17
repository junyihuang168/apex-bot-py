import os
import time

from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

print("Hello, Apex omni (test client)")

# --------------------------------------------------
# 读取环境变量（和 DO 的 app-level 环境变量一致）
# --------------------------------------------------
key        = os.getenv("APEX_API_KEY")
secret     = os.getenv("APEX_API_SECRET")
passphrase = os.getenv("APEX_API_PASSPHRASE")
l2key      = os.getenv("APEX_L2KEY_SEEDS")

print("Loaded env variables:")
print("  API_KEY   :", bool(key))
print("  API_SECRET:", bool(secret))
print("  PASS      :", bool(passphrase))
print("  L2KEY     :", bool(l2key))

if not all([key, secret, passphrase, l2key]):
    raise RuntimeError("Missing one or more APEX_* environment variables")

# --------------------------------------------------
# 创建 HttpPrivateSign 客户端（官方 Demo 写法）
# --------------------------------------------------
client = HttpPrivateSign(
    APEX_OMNI_HTTP_TEST,          # 先用 TEST 网络
    network_id=NETWORKID_TEST,
    zk_seeds=None,                # 有 l2Key 的情况下可以置为 None
    zk_l2Key=l2key,
    api_key_credentials={
        "key": key,
        "secret": secret,
        "passphrase": passphrase,
    },
)

# --------------------------------------------------
# 拉取配置 & 账户信息
# --------------------------------------------------
configs = client.configs_v3()
account = client.get_account_v3()
print("Configs:", configs)
print("Account:", account)

# --------------------------------------------------
# 下一个很小的测试单（TEST 网）
# --------------------------------------------------
current_time = int(time.time())
order = client.create_order_v3(
    symbol="BTC-USDT",
    side="SELL",
    type="MARKET",
    size="0.001",
    timestampSeconds=current_time,
    price="60000",   # 市价单这里一般会被忽略
)

print("Test order result:", order)
