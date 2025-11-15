import os
import time
import decimal

from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

print("Hello, Apex omni")

# --------------------------------------------------
# 从环境变量读取在 DO 里配置好的 Key
# --------------------------------------------------
key        = os.getenv("APEX_API_KEY")
secret     = os.getenv("APEX_API_SECRET")
passphrase = os.getenv("APEX_API_PASSPHRASE")
l2key      = os.getenv("APEX_L2KEY_SEEDS")

# 简单检查一下有没有读到（True 就代表有值）
print("Loaded env variables:")
print("API_KEY:",    bool(key))
print("API_SECRET:", bool(secret))
print("PASS:",       bool(passphrase))
print("L2KEY:",      bool(l2key))

# --------------------------------------------------
# 初始化 Client
# --------------------------------------------------
client = HttpPrivateSign(
    APEX_OMNI_HTTP_TEST,
    network_id=NETWORKID_TEST,

    # 让 SDK 自动处理 zk_seeds，不自己写
    zk_seeds=None,
    zk_l2Key=l2key,

    api_key_credentials={
        "key": key,
        "secret": secret,
        "passphrase": passphrase,
    },
)

# --------------------------------------------------
# 读取账户 / 配置信息
# --------------------------------------------------
configs = client.configs_v3()
accountData = client.get_account_v3()
print("Configs:", configs)
print("Account:", accountData)

# --------------------------------------------------
# Sample 1: MARKET 市价单
# --------------------------------------------------
currentTime = time.time()
createOrderRes = client.create_order_v3(
    symbol="BTC-USDT",
    side="SELL",
    type="MARKET",
    size="0.001",
    timestampSeconds=currentTime,
    price="60000",  # MARKET 单这里的 price 其实不会真正用到
)
print("Market Order:", createOrderRes)

# --------------------------------------------------
# Sample 2: LIMIT + TP/SL 单
# --------------------------------------------------
slippage = decimal.Decimal("0.1")  # 10% 滑点示例

slPrice = decimal.Decimal("58000") * (decimal.Decimal("1") + slippage)
tpPrice = decimal.Decimal("79000") * (decimal.Decimal("1") - slippage)

createOrderRes = client.create_order_v3(
    symbol="BTC-USDT",
    side="BUY",
    type="LIMIT",
    size="0.01",
    price="65000",

    # 开启 TP / SL
    isOpenTpslOrder=True,

    # SL 参数
    isSetOpenSl=True,
    slPrice=str(slPrice),
    slSide="SELL",
    slSize="0.01",
    slTriggerPrice="58000",

    # TP 参数
    isSetOpenTp=True,
    tpPrice=str(tpPrice),
    tpSide="SELL",
    tpSize="0.01",
    tpTriggerPrice="79000",
)

print("TP/SL Order:", createOrderRes)

print("end, apexomni")
