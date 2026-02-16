import requests
import time
import math
import threading
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import os

# ===================== ENV =====================

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

PORT = int(os.environ.get("PORT", "10000"))
ENV  = os.environ.get("ENV", "render")

# ===================== CONFIG =====================

BASE_URL = "https://www.deribit.com/api/v2"

CURRENCIES = ["BTC", "ETH"]   # SOL УБРАН

CHECK_INTERVAL = 600  # 10 min

EVENT_NAME = "deribit_vbi_snapshot"

# --- VBI params ---
IV_SLOPE_STRONG = 6.0
IV_SLOPE_MEDIUM = 3.0

SKEW_NEUTRAL_BAND = 0.05
SKEW_EXTREME_BAND = 0.15

PATTERN_WINDOW = 3
NEAR_IV_STABILITY = 0.02

# ===================== TIME =====================

def now_ts_ms():
    return int(time.time() * 1000)

def now_iso_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ===================== SUPABASE =====================

def _sanitize_for_json(value):
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_for_json(v) for v in value]
    return value

def send_to_db(event, payload):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return

    try:
        safe_payload = _sanitize_for_json(payload)
        requests.post(
            f"{SUPABASE_URL}/rest/v1/logs",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal"
            },
            json={
                "ts": safe_payload["ts_unix_ms"],
                "event": event,
                "symbol": safe_payload["symbol"],
                "data": safe_payload
            },
            timeout=5
        )
    except Exception:
        pass  # stdout намеренно не используем

# ===================== HTTP (HEALTH) =====================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *_):
        pass

def run_http_server():
    HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever()

# ===================== DERIBIT API =====================

session = requests.Session()

def get_json(method, params=None, retries=3):
    url = f"{BASE_URL}/{method}"

    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=15)
            j = r.json()
            if "error" in j:
                raise Exception(j["error"])
            return j["result"]
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(1.5)

def get_index_price(currency):
    return get_json(
        "public/get_index_price",
        {"index_name": f"{currency.lower()}_usd"}
    )["index_price"]

def get_options(currency):
    return get_json(
        "public/get_instruments",
        {
            "currency": currency,
            "kind": "option",
            "expired": "false"
        }
    )

def get_book(instr):
    d = get_json(
        "public/get_book_summary_by_instrument",
        {"instrument_name": instr}
    )
    return d[0] if d else None

# ===================== OPTION HELPERS =====================

def pick_three_expiries(options):
    exps = sorted({o["expiration_timestamp"] for o in options})
    return exps[:3] if len(exps) >= 3 else (None, None, None)

def atm_option(options, exp, opt_type, spot):
    cands = [
        o for o in options
        if o["expiration_timestamp"] == exp and o["option_type"] == opt_type
    ]
    return min(cands, key=lambda o: abs(o["strike"] - spot)) if cands else None

# ===================== STATE =====================

iv_slope_hist = {s: deque(maxlen=PATTERN_WINDOW) for s in CURRENCIES}
near_iv_hist  = {s: deque(maxlen=PATTERN_WINDOW) for s in CURRENCIES}

# ===================== VBI CORE =====================

def compute_vbi(currency):
    options = get_options(currency)
    spot = get_index_price(currency)

    exp1, exp2, exp3 = pick_three_expiries(options)
    if not exp1 or not exp2 or not exp3:
        return None

    n_call = atm_option(options, exp1, "call", spot)
    n_put  = atm_option(options, exp1, "put", spot)
    m_call = atm_option(options, exp2, "call", spot)
    f_call = atm_option(options, exp3, "call", spot)

    if not n_call or not n_put or not m_call or not f_call:
        return None

    nc = get_book(n_call["instrument_name"])
    np = get_book(n_put["instrument_name"])
    mc = get_book(m_call["instrument_name"])
    fc = get_book(f_call["instrument_name"])

    if not nc or not np or not mc or not fc:
        return None

    near_iv = nc["mark_iv"]
    mid_iv  = mc["mark_iv"]
    far_iv  = fc["mark_iv"]

    slope_1 = mid_iv - near_iv
    slope_2 = far_iv - mid_iv
    iv_slope = slope_1
    curvature = slope_2 - slope_1

    skew = np["mark_iv"] / nc["mark_iv"] if nc["mark_iv"] else None

    # ===== SCORE =====
    score = 0

    if iv_slope > IV_SLOPE_MEDIUM:
        score += 40
    if iv_slope > IV_SLOPE_STRONG:
        score += 20

    iv_slope_hist[currency].append(iv_slope)
    near_iv_hist[currency].append(near_iv)

    if len(near_iv_hist[currency]) >= PATTERN_WINDOW:
        if near_iv > sum(near_iv_hist[currency]) / len(near_iv_hist[currency]):
            score += 20

    if skew is not None:
        if abs(skew - 1.0) < SKEW_NEUTRAL_BAND:
            score += 10
        if abs(skew - 1.0) > SKEW_EXTREME_BAND:
            score -= 10

    score = max(0, min(score, 100))

    # ===== STATE =====
    if score < 30:
        vbi_state = "COLD"
    elif score <= 60:
        vbi_state = "WARM"
    else:
        vbi_state = "HOT"

    # ===== PATTERN =====
    vbi_pattern = "NEUTRAL"

    if len(iv_slope_hist[currency]) >= 3:
        s = iv_slope_hist[currency]
        n = near_iv_hist[currency]

        pre_break_core = (
            s[0] < s[1] < s[2] and
            max(n) - min(n) < NEAR_IV_STABILITY * near_iv and
            abs(skew - 1.0) < SKEW_NEUTRAL_BAND
        )

        if pre_break_core and curvature > 0:
            vbi_pattern = "PRE_BREAK"

        if iv_slope < -2.0 and curvature < 0 and near_iv < n[-2]:
            vbi_pattern = "POST_EVENT"

    return {
        "ts_unix_ms": now_ts_ms(),
        "ts_iso_utc": now_iso_utc(),

        "symbol": currency,

        "vbi_state": vbi_state,
        "vbi_score": score,

        "near_iv": round(near_iv, 2),
        "mid_iv": round(mid_iv, 2),
        "far_iv": round(far_iv, 2),

        "iv_slope": round(iv_slope, 2),
        "curvature": round(curvature, 2),
        "skew": round(skew, 3) if skew else None,

        "vbi_pattern": vbi_pattern
    }

# ===================== MAIN =====================

def main():
    threading.Thread(target=run_http_server, daemon=True).start()

    while True:
        for c in CURRENCIES:
            try:
                out = compute_vbi(c)
                if out:
                    send_to_db(EVENT_NAME, out)
            except Exception as e:
                send_to_db(
                    "deribit_vbi_error",
                    {
                        "ts_unix_ms": now_ts_ms(),
                        "symbol": c,
                        "error": str(e)
                    }
                )

        # HEARTBEAT — ВСЕГДА
        send_to_db(
            "deribit_vbi_heartbeat",
            {
                "ts_unix_ms": now_ts_ms(),
                "symbol": "SYSTEM",
                "status": "alive"
            }
        )

        time.sleep(CHECK_INTERVAL)

