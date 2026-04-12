# ============================================================
# AUTH.PY — Kotak Neo Login  (reused from algo_v3 pattern)
# ============================================================

import pyotp
import logging
import time
import os
import config
from neo_api_client import NeoAPI

logger = logging.getLogger(__name__)

TOTP_SECRET_KEY = os.getenv("TOTP_SECRET_KEY", "")


def generate_totp() -> str:
    if not TOTP_SECRET_KEY or TOTP_SECRET_KEY == "YOUR_TOTP_SECRET_KEY":
        return None
    try:
        key     = TOTP_SECRET_KEY.upper().strip().replace(" ", "")
        padding = (8 - len(key) % 8) % 8
        key     = key + "=" * padding
        return pyotp.TOTP(key).now()
    except Exception as e:
        print(f"TOTP auto-generation failed: {e}")
        return None


def get_kotak_session() -> NeoAPI:
    print("\n" + "=" * 50)
    print("  Connecting to Kotak Neo API  [Gap Algo]")
    print("=" * 50)

    client = NeoAPI(
        consumer_key = config.KOTAK_CONSUMER_KEY,
        environment  = config.KOTAK_ENVIRONMENT,
        access_token = None,
        neo_fin_key  = None,
    )

    totp_code = generate_totp()
    if totp_code:
        print(f"Auto-generated TOTP: {totp_code}")
    else:
        print("Enter your Kotak TOTP from Google Authenticator:")
        totp_code = input("  6-digit TOTP: ").strip()

    # TOTP login
    try:
        login_resp = client.totp_login(
            mobilenumber=config.KOTAK_MOBILE_NUMBER,
            ucc=config.KOTAK_UCC,
            totp=totp_code,
        )
    except Exception:
        try:
            login_resp = client.totp_login(
                mobile_number=config.KOTAK_MOBILE_NUMBER,
                ucc=config.KOTAK_UCC,
                totp=totp_code,
            )
        except Exception as e2:
            raise Exception(f"TOTP Login failed: {e2}")

    if not login_resp:
        raise Exception("TOTP Login returned empty response.")
    if isinstance(login_resp, dict) and login_resp.get("error"):
        for attempt in range(3):
            totp_code = input(f"  Enter TOTP manually (attempt {attempt+1}/3): ").strip()
            try:
                login_resp = client.totp_login(
                    mobilenumber=config.KOTAK_MOBILE_NUMBER,
                    ucc=config.KOTAK_UCC,
                    totp=totp_code,
                )
                if isinstance(login_resp, dict) and not login_resp.get("error"):
                    break
            except Exception:
                pass
            time.sleep(5)

    print("TOTP Login: SUCCESS")

    # MPIN validation
    try:
        client.totp_validate(mpin=config.KOTAK_MPIN)
    except Exception:
        try:
            client.totp_validate(mpin=config.KOTAK_MPIN, pan=config.KOTAK_UCC)
        except Exception as e:
            raise Exception(f"MPIN validation failed: {e}")

    print("MPIN Validated: SUCCESS")
    print("Kotak Neo Authentication Complete!\n")
    return client
