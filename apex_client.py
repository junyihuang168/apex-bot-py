# apex_client.py
# 负责创建 Apex Omni HttpPrivateSign 客户端

import os

# -----------------------------
# 尝试多种路径导入 HttpPrivateSign
# -----------------------------
HttpPrivateSign = None
_import_errors = []

try:
    # 新版 SDK 示例写法
    from apexomni.http_private_v3 import HttpPrivateSign as _HttpPrivateSign

    HttpPrivateSign = _HttpPrivateSign
    print("[apex_client] Using HttpPrivateSign from apexomni.http_private_v3")
except Exception as e:
    _import_errors.append(f"apexomni.http_private_v3: {e}")
    try:
        # 文档里常见的旧写法
        from apexomni.http_private_sign import HttpPrivateSign as _HttpPrivateSign

        HttpPrivateSign = _HttpPrivateSign
        print("[apex_client] Using HttpPrivateSign from apexomni.http_private_sign")
    except Exception as e2:
        _import_errors.append(f"apexomni.http_private_sign: {e2}")
        try:
            # 最老的一种写法：直接从 apexomni 根导入
            from apexomni import HttpPrivateSign as _HttpPrivateSign

            HttpPrivateSign = _HttpPrivateSign
            print("[apex_client] Using HttpPrivateSign from apexomni (root)")
        except Exception as e3:
            _import_errors.append(f"apexomni: {e3}")

if HttpPrivateSign is None:
    # 所有路径都失败，就给出清晰一点的错误信息
    raise ImportError(
        "Could not import HttpPrivateSign from apexomni.\n"
        "Tried:\n"
        "  - apexomni.http_private_v3.HttpPrivateSign\n"
        "  - apexomni.http_private_sign.HttpPrivateSign\n"
        "  - apexomni.HttpPrivateSign\n"
        f"Errors: {_import_errors}"
    )

# 常量：测试网 / 主网 endpoint + network_id
from apexomni.constants import (
    APEX_OMNI_HTTP_TEST,
    APEX_OMNI_HTTP_MAIN,
    NETWORKID_OMNI_TEST_BNB,
    NETWORKID_OMNI_MAIN_ARB,
)


def make_client():
    """从环境变量中创建 HttpPrivateSign 客户端"""

    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")

    # ZK seeds：用 Omni Key Seed；可以先留空调试
    seeds = os.getenv("APEX_ZK_SEEDS", "")
    # L2Key 可以为空字符串
    l2key = os.getenv("APEX_L2KEY_SEEDS", "")

    print("Loaded env variables in make_client():")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("SEEDS(optional):", bool(seeds))
    print("L2KEY(optional):", bool(l2key))

    # 只把 API 三件套当必填
    if not all([key, secret, passphrase]):
        raise RuntimeError("Missing one or more mandatory APEX_API_* environment variables")

    use_mainnet = os.getenv("APEX_USE_MAINNET", "false").lower() == "true"
    if use_mainnet:
        endpoint = APEX_OMNI_HTTP_MAIN
        network_id = NETWORKID_OMNI_MAIN_ARB
    else:
        endpoint = APEX_OMNI_HTTP_TEST
        network_id = NETWORKID_OMNI_TEST_BNB

    print("Using endpoint:", endpoint)
    print("Using network_id:", network_id)

    client = HttpPrivateSign(
        endpoint,
        network_id=network_id,
        zk_seeds=seeds or None,
        zk_l2Key=l2key or None,
        api_key_credentials={"key": key, "secret": secret, "passphrase": passphrase},
    )

    # 打印一下含有 account 的属性名（方便后面 /webhook 排查）
    acct_attrs = [a for a in dir(client) if "account" in a.lower()]
    print("Client attributes containing 'account':", acct_attrs)

    return client


# 本文件直接运行时做一个简单自检（本地调试用）
if __name__ == "__main__":
    from pprint import pprint

    c = make_client()
    cfg = c.configs_v3()
    acc = c.get_account_v3()
    print("configs_v3:")
    pprint(cfg)
    print("account_v3:")
    pprint(acc)
