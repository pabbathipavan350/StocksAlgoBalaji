# ============================================================
# AUTH.PY — Kotak Neo Login
# ============================================================
# Fully CI/CD safe — never hangs on input().
# TOTP is auto-generated from TOTP_SECRET_KEY in .env
# If key is missing in CI, fails fast with a clear message.
# ============================================================

import pyotp
import logging
import time
import os
import sys
import config
from neo_api_client import NeoAPI

logger = logging.getLogger(__name__)

TOTP_SECRET_KEY = os.getenv("TOTP_SECRET_KEY", "")


def _is_interactive() -> bool:
    """True only when running in a real interactive terminal."""
    try:
        return sys.stdin.isatty()
    except Exception:
        return False


def generate_totp() -> str:
    """Auto-generate TOTP from secret key. Returns None on failure."""
    key = TOTP_SECRET_KEY.strip()
    if not key or key in ("YOUR_TOTP_SECRET_KEY", ""):
        return None
    try:
        key     = key.upper().replace(" ", "")
        padding = (8 - len(key) % 8) % 8
        key     = key + "=" * padding
        return pyotp.TOTP(key).now()
    except Exception as e:
        logger.error(f"TOTP generation failed: {e}")
        return None


def _get_totp() -> str:
    """
    Get TOTP code:
    - Auto-generates from TOTP_SECRET_KEY if configured
    - Interactive terminal: prompts user if key missing
    - CI/non-interactive: raises immediately (no hang)
    """
    code = generate_totp()
    if code:
        print(f"Auto-generated TOTP: {code}")
        return code

    if _is_interactive():
        print("\n⚠️  TOTP_SECRET_KEY not set in .env")
        print("Enter your 6-digit TOTP from Google Authenticator:")
        return input("  TOTP: ").strip()
    else:
        raise RuntimeError(
            "\n❌ TOTP_SECRET_KEY not configured!\n"
            "Add it to your .env file (the base32 key from Google Authenticator setup).\n"
            "Example: TOTP_SECRET_KEY = JBSWY3DPEHPK3PXP"
        )


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

    totp_code = _get_totp()

    # ── TOTP Login ────────────────────────────────────────
    login_resp = None
    for fn_kwargs in [
        {"mobilenumber": config.KOTAK_MOBILE_NUMBER, "ucc": config.KOTAK_UCC, "totp": totp_code},
        {"mobile_number": config.KOTAK_MOBILE_NUMBER, "ucc": config.KOTAK_UCC, "totp": totp_code},
    ]:
        try:
            login_resp = client.totp_login(**fn_kwargs)
            if login_resp:
                break
        except Exception:
            continue

    if not login_resp:
        raise Exception("TOTP Login failed — check credentials in .env")

    # If API returned an error, retry once with fresh TOTP (clock skew)
    if isinstance(login_resp, dict) and login_resp.get("error"):
        print("⚠️  TOTP rejected — waiting 30s and retrying with fresh code...")
        time.sleep(30)
        totp_code  = generate_totp()
        if not totp_code:
            raise Exception("TOTP retry failed — TOTP_SECRET_KEY may be wrong")
        try:
            login_resp = client.totp_login(
                mobilenumber=config.KOTAK_MOBILE_NUMBER,
                ucc=config.KOTAK_UCC,
                totp=totp_code,
            )
        except Exception as e:
            raise Exception(f"TOTP Login retry failed: {e}")

    print("TOTP Login: SUCCESS")

    # ── MPIN Validation ───────────────────────────────────
    validated = False
    for fn_kwargs in [
        {"mpin": config.KOTAK_MPIN},
        {"mpin": config.KOTAK_MPIN, "pan": config.KOTAK_UCC},
    ]:
        try:
            resp = client.totp_validate(**fn_kwargs)
            if resp is not None and not (isinstance(resp, dict) and resp.get("error")):
                validated = True
                break
        except Exception:
            continue

    if not validated:
        raise Exception("MPIN validation failed — check KOTAK_MPIN in .env")

    print("MPIN Validated: SUCCESS")
    print("Kotak Neo Authentication Complete!\n")
    return client
