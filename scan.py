"""
Run-once 3-candle breakout scanner for GitHub Actions (5-minute timeframe, 28 pairs).
Data: FOREXCOM feed via TradingView only (no fallback).

Pattern (all three candles consecutive):
  Candle 1: any candle (the "mother" candle)
  Candle 2: inside candle of candle 1 (lower high AND higher low)
  Candle 3: CLOSES above the highest wick of candles 1 and 2 (bullish breakout),
            or CLOSES below the lowest wick of candles 1 and 2 (bearish breakout)
An alert is sent only after candle 3 has fully closed.

Each run checks the last few closed candles, so a delayed run still catches signals.
Alerted candles are remembered in state.json (the workflow commits this file).
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TV_USER = os.environ.get("TV_USERNAME", "").strip()
TV_PASS = os.environ.get("TV_PASSWORD", "").strip()

EXCHANGE = "FOREXCOM"
TF = 300                 # 5-minute candles, in seconds
LOOKBACK_CANDLES = 3     # how many recent closed candles a run may alert on (as candle 3)
MAX_RETRIES = 3          # re-fetch if the newest candle hasn't appeared yet
RETRY_WAIT = 12          # seconds between retries
RETRY_WINDOW = 100       # only retry within this many seconds of the candle close
STATE_FILE = Path("state.json")

PAIRS = [
    # Majors
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    # Minors (crosses)
    "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD", "GBPNZD",
    "AUDJPY", "AUDCHF", "AUDCAD", "AUDNZD",
    "CADJPY", "CADCHF", "CHFJPY", "NZDJPY", "NZDCHF", "NZDCAD",
]


def log(msg):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S} UTC] {msg}", flush=True)


# ---------- State ----------
def load_state():
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True))


# ---------- Data (FOREXCOM via TradingView) ----------
# Rows are (candle_open_epoch, open, high, low, close), oldest first,
# and may include the still-forming candle.
_tv = None


def connect_tv():
    global _tv
    try:
        from tvDatafeed import TvDatafeed
        _tv = TvDatafeed(TV_USER, TV_PASS) if (TV_USER and TV_PASS) else TvDatafeed()
        log("TradingView connected" + (" (logged in)" if TV_USER and TV_PASS else " (no login)"))
    except Exception as e:
        _tv = None
        log(f"TradingView connection failed: {e}")


def fetch_tv(pair):
    if _tv is None:
        return None
    try:
        from tvDatafeed import Interval
        df = _tv.get_hist(symbol=pair, exchange=EXCHANGE, interval=Interval.in_5_minute, n_bars=10)
        if df is None or len(df) < 4:
            return None
        return [
            (int(idx.to_pydatetime().timestamp()),
             float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]))
            for idx, r in df.iterrows()
        ]
    except Exception as e:
        log(f"{pair}: TradingView error: {e}")
        return None


# ---------- Pattern logic ----------
def closed_rows(rows, boundary):
    return sorted(r for r in rows if r[0] < boundary)  # drop the still-forming candle


def newest_closed_ts(rows, boundary):
    c = closed_rows(rows, boundary) if rows else []
    return c[-1][0] if c else 0


def find_signals(rows, boundary, last_alerted_ts):
    """Return a list of (candle3_open_ts, direction, close, c1_high, c1_low)."""
    closed = closed_rows(rows, boundary)
    out = []
    for i in range(2, len(closed)):
        c1, c2, c3 = closed[i - 2], closed[i - 1], closed[i]
        ts3 = c3[0]
        if ts3 < boundary - LOOKBACK_CANDLES * TF:      # too old to alert on
            continue
        if ts3 <= last_alerted_ts:                       # already alerted
            continue
        if c2[0] != c1[0] + TF or c3[0] != c2[0] + TF:   # must be three consecutive candles
            continue
        _, _, h1, l1, _ = c1
        _, _, h2, l2, _ = c2
        close3 = c3[4]
        if not (h2 < h1 and l2 > l1):                    # candle 2 must be inside candle 1
            continue
        top, bottom = max(h1, h2), min(l1, l2)           # highest wick / lowest wick of both candles
        if close3 > top:                                 # closes above both candles (wicks included)
            out.append((ts3, "Bullish", close3, h1, l1))
        elif close3 < bottom:                            # closes below both candles (wicks included)
            out.append((ts3, "Bearish", close3, h1, l1))
    return out


# ---------- Telegram ----------
def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        log("Telegram not configured; alert printed only:\n" + text)
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT, "text": text},
            timeout=20,
        )
        if not r.ok:
            log(f"Telegram error {r.status_code}: {r.text}")
        return r.ok
    except Exception as e:
        log(f"Telegram error: {e}")
        return False


# ---------- Main ----------
def main():
    boundary = int(time.time()) // TF * TF
    log(f"Scan started; latest closed candle ends {datetime.fromtimestamp(boundary, timezone.utc):%H:%M} UTC")

    state = load_state()
    connect_tv()
    if _tv is None:
        log("No data source available; exiting")
        return

    data = {}
    pending = list(PAIRS)
    for attempt in range(MAX_RETRIES + 1):
        with ThreadPoolExecutor(max_workers=6) as pool:
            for pair, rows in zip(pending, pool.map(fetch_tv, pending)):
                if rows:
                    data[pair] = rows
        pending = [p for p in PAIRS if newest_closed_ts(data.get(p), boundary) < boundary - TF]
        if not pending:
            break
        if attempt < MAX_RETRIES and time.time() - boundary < RETRY_WINDOW:
            log(f"{len(pending)} pair(s) still waiting for the newest candle; retrying")
            time.sleep(RETRY_WAIT)
        else:
            break

    log(f"Data from FOREXCOM/TradingView for {len(data)} of {len(PAIRS)} pairs")

    new_state, lines = dict(state), []
    for pair in PAIRS:
        rows = data.get(pair)
        if not rows:
            continue
        signals = find_signals(rows, boundary, state.get(pair, 0))
        if signals:
            ts3, direction, close3, h1, l1 = signals[-1]
            d = 3 if "JPY" in pair else 5
            closed_at = datetime.fromtimestamp(ts3 + TF, timezone.utc).strftime("%H:%M")
            lines.append(
                f"{pair}: {direction} breakout, closed {close3:.{d}f} "
                f"(range {l1:.{d}f}-{h1:.{d}f}) at {closed_at} UTC"
            )
            new_state[pair] = ts3

    if lines:
        text = "5m inside-candle breakout:\n" + "\n".join(lines)
        log(text.replace("\n", " | "))
        if send_telegram(text):
            save_state(new_state)
        else:
            log("Alert not delivered; state not updated so the next run retries")
    else:
        log("No new breakout signals")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"Run failed: {e}")
        sys.exit(1)
