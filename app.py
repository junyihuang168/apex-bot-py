import time
import decimal

from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

print("Hello, Apex omni")

# -----------------------------
# 你的注册 V3 得到的 Key
# -----------------------------
key = "your-apiKey-key"
secret = "your-apiKey-secret"
passphrase = "your-apiKey-passphrase"

seeds = "your-zk-seeds"
l2Key = "your-l2Key-seeds"

# -----------------------------
# 初始化 Client（测试网）
# -----------------------------
client = HttpPrivateSign(
    APEX_OMNI_HTTP_TEST,
    network_id=NETWORKID_TEST,
    zk_seeds=seeds,
    zk_l2Key=l2Key,
    api_key_credentials={
        "key": key,
        "secret": secret,
        "passphrase": passphrase
    }
)

# 读取账户信息
configs = client.configs_v3()
accountData = client.get_account_v3()
print("Account:", accountData)

# -----------------------------
# Sample 1: MARKET 市价单
# -----------------------------
currentTime = time.time()
createOrderRes = client.create_order_v3(
    symbol="BTC-USDT",
    side="SELL",
    type="MARKET",
    size="0.001",
    timestampSeconds=currentTime,
    price="60000"   # MARKET 订单也需要随便传个 price
)
print("Market Order:", createOrderRes)

# -----------------------------
# Sample 2: LIMIT + TP/SL 单
# -----------------------------
slippage = decimal.Decimal("-0.1")
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
    slPrice=slPrice,
    slSide="SELL",
    slSize="0.01",
    slTriggerPrice="58000",

    # TP 参数
    isSetOpenTp=True,
    tpPrice=tpPrice,
    tpSide="SELL",
    tpSize="0.01",
    tpTriggerPrice="79000",
)

print("TP/SL Order:", createOrderRes)

print("end, apexomni")
