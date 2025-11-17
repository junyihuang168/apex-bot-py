# apex_client.py
import os

from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign as _HttpPrivateSign


class PatchedHttpPrivateSign(_HttpPrivateSign):
    """
    小封装：修补官方 HttpPrivateSign 在 create_order_v3 里
    直接访问 self.accountV3 可能不存在的问题。
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 确保属性一定存在，避免 AttributeError
        self.accountV3 = getattr(self, "accountV3", None)

    def get_account_v3(self, *args, **kwargs):
        """
        调用原始 get_account_v3，并把结果保存到 self.accountV3 上，
        方便 create_order_v3 使用。
        """
        data = super().get_account_v3(*args, **kwargs)

        candidate = data
        # 某些版本 SDK 会把账户信息放到 self.account
        try:
            account_attr = getattr(self, "account", None)
        except Exception:
            account_attr = None

        if isinstance(account_attr, dict) and account_attr:
            candidate = account_attr

        if isinstance(candidate, dict):
            self.accountV3 = candidate

        return data


def make_client() -> PatchedHttpPrivateSign:
    """
    从环境变量读取配置，创建 Apex Omni HTTP 客户端。

    必需：
        APEX_API_KEY
        APEX_API_SECRET
        APEX_API_PASSPHRASE

    可选：
        APEX_ZK_SEEDS       - Omni Key Seed（zk seeds）
        APEX_L2KEY_SEEDS    - l2Key
        APEX_USE_MAINNET    - 目前仍然只连 TEST 网络，true 也会打印提示
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
    print("SEEDS(optional):", bool(seeds))
    print("L2KEY(optional):", bool(l2key))

    if not all([key, secret, passphrase]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    # 空字符串统一成 None，避免 SDK 误认为有值
    seeds = seeds or None
    l2key = l2key or None

    use_mainnet_raw = os.getenv("APEX_USE_MAINNET", "false")
    use_mainnet = use_mainnet_raw.lower() == "true"

    # 目前我们只连 TEST 网络；即使设置了 true，也打印个提示，避免误上真网
    endpoint = APEX_OMNI_HTTP_TEST
    network_id = NETWORKID_TEST

    if use_mainnet:
        print("APEX_USE_MAINNET=true，但当前代码仍使用 TEST 网络端点（安全起见）。")

    print("Using endpoint:", endpoint)
    print("Using network_id:", network_id)

    client = PatchedHttpPrivateSign(
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

    return client
