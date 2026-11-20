"""
Run-once inside-candle scanner for GitHub Actions (5-minute timeframe, 28 pairs).

Each run:
  1. Fetches recent 5m candles (TradingView FOREXCOM feed first, Yahoo Finance as fallback).
  2. Looks at the last few CLOSED candles, so a delayed run still catches signals.
  3. Sends one Telegram message for any inside candle not alerted before.
  4. Remembers what it alerted in state.json (the workflow commits this file).
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

TF = 300                 # 5-minute candles, in seconds
LOOKBACK_CANDLES = 3     # how many recent closed candles a run may alert on
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


# ---------- Data sources ----------
# Every source returns rows of (candle_open_epoch, high, low), oldest first,
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
        log(f"TradingView unavailable, will use Yahoo: {e}")


def fetch_tv(pair):
    if _tv is None:
        return None
    try:
        from tvDatafeed import Interval
        df = _tv.get_hist(symbol=pair, exchange="FOREXCOM", interval=Interval.in_5_minute, n_bars=8)
        if df is None or len(df) < 3:
            return None
        return [
            (int(idx.to_pydatetime().timestamp()), float(r["high"]), float(r["low"]))
            for idx, r in df.iterrows()
        ]
    except Exception as e:
        log(f"{pair}: TradingView error: {e}")
        return None


def fetch_yahoo(pairs):
    out = {}
    if not pairs:
        return out
    try:
        import yfinance as yf
        tickers = {p: f"{p}=X" for p in pairs}
        df = yf.download(
            tickers=list(tickers.values()), interval="5m", period="2d",
            group_by="ticker", progress=False, threads=True, auto_adjust=False,
        )
        for pair, t in tickers.items():
            try:
                bars = df[t].dropna(subset=["High", "Low"])
                rows = [(int(idx.value // 10**9), float(r["High"]), float(r["Low"])) for idx, r in bars.iterrows()]
                if len(rows) >= 3:
                    out[pair] = rows
            except KeyError:
                continue
    except Exception as e:
        log(f"Yahoo error: {e}")
    return out


# ---------- Pattern logic ----------
def closed_rows(rows, boundary):
    return sorted(r for r in rows if r[0] < boundary)  # drop the still-forming candle


def newest_closed_ts(rows, boundary):
    c = closed_rows(rows, boundary) if rows else []
    return c[-1][0] if c else 0


def find_inside(rows, boundary, last_alerted_ts):
    """Return open-timestamps of recent closed candles that are inside candles."""
    closed = closed_rows(rows, boundary)
    hits = []
    for i in range(1, len(closed)):
        ts, high, low = closed[i]
        _, prev_high, prev_low = closed[i - 1]
        if ts < boundary - LOOKBACK_CANDLES * TF:   # too old to alert on
            continue
        if ts <= last_alerted_ts:                    # already alerted
            continue
        if high < prev_high and low > prev_low:
            hits.append(ts)
    return hits


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
    now = time.time()
    boundary = int(now) // TF * TF
    log(f"Scan started; latest closed candle ends {datetime.fromtimestamp(boundary, timezone.utc):%H:%M} UTC")

    state = load_state()
    connect_tv()

    data, source = {}, {}
    pending = list(PAIRS)

    for attempt in range(MAX_RETRIES + 1):
        # TradingView first
        if _tv is not None and pending:
            with ThreadPoolExecutor(max_workers=6) as pool:
                for pair, rows in zip(pending, pool.map(fetch_tv, pending)):
                    if rows:
                        data[pair], source[pair] = rows, "FOREXCOM"
        # Yahoo fallback for anything TradingView didn't give us
        missing = [p for p in pending if p not in data or not data[p]]
        if missing:
            for pair, rows in fetch_yahoo(missing).items():
                data[pair], source[pair] = rows, "Yahoo"

        pending = [p for p in PAIRS if newest_closed_ts(data.get(p), boundary) < boundary - TF]
        if not pending:
            break
        if attempt < MAX_RETRIES and time.time() - boundary < RETRY_WINDOW:
            log(f"{len(pending)} pair(s) still waiting for the newest candle; retrying")
            time.sleep(RETRY_WAIT)
        else:
            break

    n_tv = sum(1 for s in source.values() if s == "FOREXCOM")
    n_yh = sum(1 for s in source.values() if s == "Yahoo")
    log(f"Data: {n_tv} pairs from FOREXCOM/TradingView, {n_yh} from Yahoo, "
        f"{len(PAIRS) - len(data)} with no data")

    new_state, lines, used_yahoo = dict(state), [], False
    for pair in PAIRS:
        rows = data.get(pair)
        if not rows:
            continue
        hits = find_inside(rows, boundary, state.get(pair, 0))
        if hits:
            latest = max(hits)
            closed_at = datetime.fromtimestamp(latest + TF, timezone.utc).strftime("%H:%M")
            tag = "" if source[pair] == "FOREXCOM" else " *"
            used_yahoo = used_yahoo or bool(tag)
            lines.append(f"{pair} (closed {closed_at} UTC){tag}")
            new_state[pair] = latest

    if lines:
        text = "Inside candle (5m):\n" + "\n".join(lines)
        if used_yahoo:
            text += "\n* Yahoo data (FOREXCOM feed unavailable for this pair)"
        log(text.replace("\n", " | "))
        if send_telegram(text):
            save_state(new_state)
        else:
            log("Alert not delivered; state not updated so the next run retries")
    else:
        log("No new inside candles")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"Run failed: {e}")
        sys.exit(1)
