import requests
import time
import math
import threading
import random
import logging
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import os

# ===================== ENV =====================

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

TELEGRAM_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
TELEGRAM_CHAT_ID  = os.environ.get("TG_CHAT_ID")

PORT = int(os.environ.get("PORT", "10000"))
ENV  = os.environ.get("ENV", "render")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("deribit_vbi")

# ===================== CONFIG =====================

BASE_URL = "https://www.deribit.com/api/v2"

CURRENCIES = ["BTC", "ETH"]

degraded_cycles = {s: 0 for s in CURRENCIES}
degraded_alert_sent = {s: False for s in CURRENCIES}

CHECK_INTERVAL = 600  # 10 min

EVENT_NAME = "deribit_vbi_snapshot"

# --- VBI params ---
IV_SLOPE_STRONG = 6.0
IV_SLOPE_MEDIUM = 3.0

SKEW_NEUTRAL_BAND = 0.05
SKEW_EXTREME_BAND = 0.15

PATTERN_WINDOW = 3
NEAR_IV_STABILITY = 0.02

# --- ROLLING MATURITY WINDOWS (DAYS) ---
NEAR_DTE_RANGE = (3, 14)
MID_DTE_RANGE  = (14, 45)
FAR_DTE_RANGE  = (45, 120)

# ===================== TIME =====================

def now_ts_ms():
    return int(time.time() * 1000)

def now_iso_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def dte_days(exp_ts):
    return (exp_ts - now_ts_ms()) / (1000 * 60 * 60 * 24)

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
        logger.warning("Supabase ENV not set, skipping send")
        return

    try:
        safe_payload = _sanitize_for_json(payload)
        r = requests.post(
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
        r.raise_for_status()
        logger.info(f"Supabase OK → event={event} symbol={safe_payload['symbol']}")
    except Exception:
        logger.exception("Supabase insert failed")

# ===================== HTTP (HEALTH) =====================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()

    def log_message(self, *_):
        return

def run_http_server():
    logger.info(f"HTTP health server listening on {PORT}")
    HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever()

# ===================== DERIBIT API =====================

session = requests.Session()

def get_json(method, params=None, retries=3):
    url = f"{BASE_URL}/{method}"

    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=15)
            r.raise_for_status()
            j = r.json()
            if "error" in j:
                raise Exception(j["error"])
            return j["result"]
        except Exception as e:
            if attempt == retries - 1:
                raise
            logger.warning(f"Deribit retry {attempt+1} for {method}: {e}")
            time.sleep(1.5 * (2 ** attempt) + random.uniform(0, 0.5))

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

def pick_rolling_expiries(options):
    buckets = {"near": [], "mid": [], "far": []}

    for o in options:
        dte = dte_days(o["expiration_timestamp"])
        if NEAR_DTE_RANGE[0] <= dte <= NEAR_DTE_RANGE[1]:
            buckets["near"].append(o["expiration_timestamp"])
        elif MID_DTE_RANGE[0] <= dte <= MID_DTE_RANGE[1]:
            buckets["mid"].append(o["expiration_timestamp"])
        elif FAR_DTE_RANGE[0] <= dte <= FAR_DTE_RANGE[1]:
            buckets["far"].append(o["expiration_timestamp"])

    def pick(ts_list, target):
        if not ts_list:
            return None
        mid = sum(target) / 2
        return min(ts_list, key=lambda ts: abs(dte_days(ts) - mid))

    return (
        pick(buckets["near"], NEAR_DTE_RANGE),
        pick(buckets["mid"],  MID_DTE_RANGE),
        pick(buckets["far"],  FAR_DTE_RANGE),
    )

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

def degraded(currency, reason):
    logger.warning(f"{currency}: DEGRADED ({reason})")
    return {
        "ts_unix_ms": now_ts_ms(),
        "ts_iso_utc": now_iso_utc(),
        "symbol": currency,
        "status": "degraded",
        "reason": reason,
        "vbi_state": "DEGRADED",
        "vbi_score": None
    }

def compute_vbi(currency):
    logger.info(f"{currency}: compute start")

    try:
        options = get_options(currency)
        spot = get_index_price(currency)

        exp1, exp2, exp3 = pick_rolling_expiries(options)
        if not exp1 or not exp2 or not exp3:
            return degraded(currency, "no_expiries")

        n_call = atm_option(options, exp1, "call", spot)
        n_put  = atm_option(options, exp1, "put", spot)
        m_call = atm_option(options, exp2, "call", spot)
        f_call = atm_option(options, exp3, "call", spot)

        if not n_call or not n_put or not m_call or not f_call:
            return degraded(currency, "no_atm")

        nc = get_book(n_call["instrument_name"])
        np = get_book(n_put["instrument_name"])
        mc = get_book(m_call["instrument_name"])
        fc = get_book(f_call["instrument_name"])

        if not nc or not np or not mc or not fc:
            return degraded(currency, "no_book")

        near_iv = nc["mark_iv"]
        mid_iv  = mc["mark_iv"]
        far_iv  = fc["mark_iv"]

        if near_iv is None or mid_iv is None or far_iv is None:
            return degraded(currency, "no_iv")

        slope_1 = mid_iv - near_iv
        slope_2 = far_iv - mid_iv
        iv_slope = slope_1
        curvature = slope_2 - slope_1

        skew = np["mark_iv"] / nc["mark_iv"] if nc["mark_iv"] else None

        logger.info(
            f"{currency}: near_iv={near_iv:.4f} "
            f"mid_iv={mid_iv:.4f} "
            f"far_iv={far_iv:.4f} "
            f"iv_slope={iv_slope:.4f} "
            f"skew={skew}"
        )
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
        vbi_state = "COLD" if score < 30 else "WARM" if score <= 60 else "HOT"

        logger.info(f"{currency}: compute OK state={vbi_state} score={score}")

        return {
            "ts_unix_ms": now_ts_ms(),
            "ts_iso_utc": now_iso_utc(),
            "symbol": currency,
            "status": "ok",
            "vbi_state": vbi_state,
            "vbi_score": score,
            "near_iv": round(near_iv, 2),
            "mid_iv": round(mid_iv, 2),
            "far_iv": round(far_iv, 2),
            "iv_slope": round(iv_slope, 2),
            "curvature": round(curvature, 2),
            "skew": round(skew, 3) if skew else None
        }

    except Exception:
        logger.exception(f"{currency}: compute failed")
        return degraded(currency, "exception")


def send_telegram_alert(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram ENV not set, alert skipped")
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text
            },
            timeout=5
        )
        logger.warning("Telegram alert sent")
    except Exception:
        logger.exception("Telegram alert failed")

# ===================== MAIN =====================

def main():
    threading.Thread(target=run_http_server, daemon=True).start()
    logger.info("Starting Deribit VBI service")

    while True:
        logger.info("=== VBI cycle start ===")

        for c in CURRENCIES:
            out = compute_vbi(c)
            if out:
                send_to_db(EVENT_NAME, out)
            
                if out.get("status") == "degraded":
                    degraded_cycles[c] += 1
                    logger.warning(f"{c}: degraded cycle {degraded_cycles[c]}")
            
                    if degraded_cycles[c] >= 3 and not degraded_alert_sent[c]:
                        send_telegram_alert(
                            f"⚠️ {c} DEGRADED\n"
                            f"cycles: {degraded_cycles[c]}\n"
                            f"reason: {out.get('reason')}"
                        )
                        degraded_alert_sent[c] = True
                else:
                    if degraded_cycles[c] > 0:
                        logger.info(f"{c}: recovered after {degraded_cycles[c]} degraded cycles")
            
                    degraded_cycles[c] = 0
                    degraded_alert_sent[c] = False

        send_to_db(
            "deribit_vbi_heartbeat",
            {
                "ts_unix_ms": now_ts_ms(),
                "symbol": "SYSTEM",
                "status": "alive"
            }
        )
        logger.info("Heartbeat sent")

        logger.info(f"Sleeping {CHECK_INTERVAL}s")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
