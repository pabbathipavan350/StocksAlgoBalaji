# ============================================================
# SESSION_MANAGER.PY — Kotak Session Keepalive
# ============================================================
# Reused from algo_v3 — pings every 25 mins to keep session alive.
# Auto-relogs if session expires.
# ============================================================

import threading
import time
import datetime
import logging

logger = logging.getLogger(__name__)


class SessionManager:

    def __init__(self, client, auth_fn):
        self.client       = client
        self.auth_fn      = auth_fn
        self.is_running   = True
        self.last_ping    = datetime.datetime.now()
        self.ping_ok      = True
        self._thread      = None
        self.on_reconnect = None

    def start(self):
        self._thread = threading.Thread(
            target=self._keepalive_loop, daemon=True, name="SessionKeepalive")
        self._thread.start()
        print("[Session] Keepalive started — pinging every 25 mins")

    def stop(self):
        self.is_running = False

    def _keepalive_loop(self):
        PING_INTERVAL = 25 * 60
        while self.is_running:
            time.sleep(PING_INTERVAL)
            if not self.is_running:
                break
            t = datetime.datetime.now().time()
            if t < datetime.time(9, 0) or t > datetime.time(15, 35):
                continue
            self._ping()

    def _ping(self):
        try:
            self.client.limits(segment="ALL", exchange="NSE", product="ALL")
            self.last_ping = datetime.datetime.now()
            self.ping_ok   = True
            print(f"  [Session] Keepalive ping OK ({self.last_ping.strftime('%H:%M')})")
        except Exception as e:
            self.ping_ok = False
            logger.warning(f"Session ping failed: {e}")
            self._relogin()

    def _relogin(self):
        for attempt in range(3):
            try:
                new_client = self.auth_fn()
                if new_client:
                    self.client  = new_client
                    self.ping_ok = True
                    print("  [Session] ✅ Re-login successful")
                    if self.on_reconnect:
                        self.on_reconnect(new_client)
                    return
            except Exception as e:
                logger.error(f"Re-login attempt {attempt+1} failed: {e}")
                time.sleep(10)
        print("  [Session] ❌ Re-login failed after 3 attempts")

    def get_client(self):
        return self.client

    @property
    def is_healthy(self):
        return self.ping_ok
