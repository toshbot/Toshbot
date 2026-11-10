"""Tosh Bot forex add-on: watches a few forex pairs and tells you when a trade setup appears.

HOW IT WORKS (plain words)
  * Every 15 minutes (when each 15-minute candle closes) it downloads fresh candles for each pair.
  * It looks for ONE simple setup: the market is trending, price pulled back, then the trend resumed.
  * When the setup appears the page shows ENTER BUY or ENTER SELL with the price to enter, where to put the stop loss,
    where to take profit, and a position size that risks only a small % of your account.
  * Every signal is also tracked on paper (did it hit the target or the stop?) so you can see if it really works
    BEFORE you put real money on it. Results are kept in memory, so they restart from zero if the server restarts.
  * It never places trades. You enter manually in your own broker app.

SETUP: one free key. Get it at twelvedata.com, then in Render > Environment add FOREX_API_KEY = your key.
OPTIONAL env: FOREX_PAIRS (default EUR/USD,GBP/USD,USD/JPY,XAU/USD), FOREX_ACCOUNT_USD (default 100),
              FOREX_RISK_PCT (default 1), FOREX_ENABLED (set false to switch the add-on off).

Self-test (no network):  python forex.py test
Nothing here is financial advice. Most retail forex traders lose money. Signals are not guarantees.
"""
from __future__ import annotations

import asyncio
import html
import json
import math
import os
import re
import sys
import time
import unittest
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

INTERVAL_MIN = 15
CANDLES_WANTED = 500
MIN_CANDLES = 230
MAX_SIGNAL_AGE = 2          # a signal is only "enter now" for 2 candles (30 min) after it forms
STOP_ATR = 1.5              # stop loss distance = 1.5 x ATR
TARGET_R = 2.0              # take profit = 2 x the stop distance
CHASE_ATR = 0.5             # if price already ran 0.5 x ATR past the entry, do not chase it
SESSION_HOURS = (7, 20)     # only signal during London + New York hours (UTC)
EXPIRE_HOURS = 24           # a paper trade that hits neither level in 24h is closed at market
DEFAULT_PAIRS = "EUR/USD,GBP/USD,USD/JPY,XAU/USD"

UTC = timezone.utc


# ============================================================================
# INDICATORS (plain lists, None where there is not enough history yet)
# ============================================================================
def ema(vals: list[float], n: int) -> list[float | None]:
    out: list[float | None] = [None] * len(vals)
    if len(vals) < n:
        return out
    k = 2 / (n + 1)
    cur = sum(vals[:n]) / n
    out[n - 1] = cur
    for i in range(n, len(vals)):
        cur = vals[i] * k + cur * (1 - k)
        out[i] = cur
    return out


def rsi(closes: list[float], n: int = 14) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= n:
        return out
    gains = [max(closes[i] - closes[i - 1], 0.0) for i in range(1, len(closes))]
    losses = [max(closes[i - 1] - closes[i], 0.0) for i in range(1, len(closes))]
    ag, al = sum(gains[:n]) / n, sum(losses[:n]) / n
    for i in range(n, len(closes)):
        if i > n:
            ag = (ag * (n - 1) + gains[i - 1]) / n
            al = (al * (n - 1) + losses[i - 1]) / n
        out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return out


def atr(highs: list[float], lows: list[float], closes: list[float], n: int = 14) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= n:
        return out
    tr = [highs[0] - lows[0]] + [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])) for i in range(1, len(closes))]
    cur = sum(tr[1:n + 1]) / n
    out[n] = cur
    for i in range(n + 1, len(closes)):
        cur = (cur * (n - 1) + tr[i]) / n
        out[i] = cur
    return out


@dataclass
class Candle:
    t: datetime   # OPEN time, UTC
    o: float
    h: float
    l: float
    c: float


@dataclass
class Indicators:
    e20: list
    e50: list
    e200: list
    rsi: list
    atr: list


def indicators(cs: list[Candle]) -> Indicators:
    closes = [c.c for c in cs]
    return Indicators(ema(closes, 20), ema(closes, 50), ema(closes, 200), rsi(closes), atr([c.h for c in cs], [c.l for c in cs], closes))


# ============================================================================
# THE SETUP: trend + pullback + resume
# ============================================================================
def trigger(cs: list[Candle], ind: Indicators, i: int) -> str | None:
    """'BUY' / 'SELL' if candle i completed the setup, else None. Only looks at candle i and earlier."""
    if i < 210:
        return None
    need = (ind.e20[i], ind.e50[i], ind.e200[i], ind.e200[i - 10], ind.rsi[i], ind.atr[i])
    if any(x is None for x in need) or any(ind.rsi[j] is None for j in range(i - 6, i)):
        return None
    c = cs[i]
    recent = [ind.rsi[j] for j in range(i - 6, i)]
    up = ind.e50[i] > ind.e200[i] and ind.e200[i] > ind.e200[i - 10]
    down = ind.e50[i] < ind.e200[i] and ind.e200[i] < ind.e200[i - 10]
    if up and min(recent) <= 40 and c.c > c.o and c.c > ind.e20[i] and ind.rsi[i] > 45:
        return "BUY"
    if down and max(recent) >= 60 and c.c < c.o and c.c < ind.e20[i] and ind.rsi[i] < 55:
        return "SELL"
    return None


# ============================================================================
# SIZING
# ============================================================================
def contract_size(pair: str) -> float:
    return 100.0 if pair.startswith("XAU/") else 5000.0 if pair.startswith("XAG/") else 100000.0


def lots_for_risk(pair: str, price: float, stop_distance: float, risk_usd: float) -> float | None:
    """Lots so that hitting the stop loses about risk_usd. None for pairs where the USD conversion needs another rate (cross pairs)."""
    if stop_distance <= 0 or price <= 0:
        return None
    if pair.endswith("/USD"):
        conv = 1.0
    elif pair.startswith("USD/"):
        conv = 1.0 / price
    else:
        return None
    return risk_usd / (stop_distance * contract_size(pair) * conv)


def decimals(pair: str) -> int:
    return 2 if pair.startswith("XAU/") else 3 if pair.startswith("XAG/") or pair.endswith("/JPY") else 5


# ============================================================================
# SIGNAL + EVALUATION
# ============================================================================
@dataclass
class Signal:
    pair: str
    state: str                 # "ENTER BUY" | "ENTER SELL" | "WAIT" | "CLOSED"
    reason: str
    price: float | None = None
    side: str | None = None
    entry: float | None = None
    stop: float | None = None
    target: float | None = None
    atr: float | None = None
    formed: datetime | None = None    # when the setup candle CLOSED
    lots: float | None = None
    risk_usd: float | None = None
    account_usd: float | None = None
    valid_until: datetime | None = None   # ENTER: last moment the entry is still good. GET READY: when the idea expires
    chase_limit: float | None = None      # do not enter beyond this price
    breakeven: float | None = None        # when price gets here, move the stop to the entry
    trigger: float | None = None          # GET READY: price must close beyond this level to fire the entry
    next_check: datetime | None = None    # GET READY: when the next candle closes
    rsi: float | None = None


def pip_size(pair: str) -> float:
    return 0.01 if pair.endswith("/JPY") else 0.0001


def dist_text(pair: str, d: float) -> str:
    """A price distance in words a trader reads: pips for currency pairs, dollars for gold and silver."""
    d = abs(d)
    if pair.startswith("XAU/"):
        return f"${d:.2f}"
    if pair.startswith("XAG/"):
        return f"${d:.3f}"
    return f"{d / pip_size(pair):.1f} pips"


def min_lot_risk(pair: str, price: float, stop_distance: float, lot: float = 0.01) -> float | None:
    """Dollars lost at the stop if you trade the smallest lot most brokers allow."""
    one = lots_for_risk(pair, price, stop_distance, 1.0)
    return None if not one else lot / one


def market_closed(now: datetime) -> bool:
    """Forex is shut from Friday 22:00 UTC to Sunday 22:00 UTC."""
    wd, h = now.weekday(), now.hour
    return (wd == 4 and h >= 22) or wd == 5 or (wd == 6 and h < 22)


def in_session(t: datetime) -> bool:
    return t.weekday() < 5 and SESSION_HOURS[0] <= t.hour < SESSION_HOURS[1]


def closed_only(cs: list[Candle], now: datetime) -> list[Candle]:
    """Drop the candle that is still forming."""
    if cs and cs[-1].t + timedelta(minutes=INTERVAL_MIN) > now:
        return cs[:-1]
    return cs


def watch_reason(ind: Indicators, i: int) -> str:
    """Plain-words status for a pair with no entry: what the bot is waiting for."""
    e50, e200, e200p, r = ind.e50[i], ind.e200[i], ind.e200[i - 10] if i >= 10 else None, ind.rsi[i]
    if None in (e50, e200, e200p, r):
        return "no setup right now: still collecting enough candles."
    recent = [x for x in ind.rsi[max(0, i - 6):i] if x is not None] + [r]
    if e50 > e200 and e200 > e200p:
        if min(recent) <= 40:
            return f"Uptrend. Price pulled back (RSI {min(recent):.0f}). Waiting for a green candle back above the 20 average, then it says ENTER BUY."
        return f"Uptrend. Waiting for a pullback (RSI {r:.0f}; it needs to dip to 40 or lower first)."
    if e50 < e200 and e200 < e200p:
        if max(recent) >= 60:
            return f"Downtrend. Price bounced up (RSI {max(recent):.0f}). Waiting for a red candle back below the 20 average, then it says ENTER SELL."
        return f"Downtrend. Waiting for a bounce up (RSI {r:.0f}; it needs to rise to 60 or more first)."
    return "No clear trend right now, so nothing to trade."


def evaluate(pair: str, cs: list[Candle], now: datetime, account_usd: float = 100.0, risk_pct: float = 1.0) -> tuple[Signal, list[tuple]]:
    """Returns (current Signal, list of setups that formed in the last few candles as (side, formed_time, entry, stop, target)).
    The second list feeds the paper record so a setup is recorded even if you never open the page."""
    cs = closed_only(cs, now)
    if len(cs) < MIN_CANDLES:
        return Signal(pair, "WAIT", f"only {len(cs)} candles so far (needs {MIN_CANDLES})."), []
    ind = indicators(cs)
    n = len(cs)
    last = cs[-1]
    price = last.c
    a_now = ind.atr[n - 1]
    found = []
    for i in range(n - 1, max(n - 1 - 8, 209), -1):     # look back 8 candles for the paper record
        side = trigger(cs, ind, i)
        if side:
            a = ind.atr[i]
            d = STOP_ATR * a
            e = cs[i].c
            stop, tgt = (e - d, e + TARGET_R * d) if side == "BUY" else (e + d, e - TARGET_R * d)
            found.append((side, cs[i].t + timedelta(minutes=INTERVAL_MIN), e, stop, tgt, i, a))
    base = Signal(pair, "WAIT", watch_reason(ind, n - 1), price=price, atr=a_now)
    live = [f for f in found if f[5] >= n - MAX_SIGNAL_AGE]
    records = [(f[0], f[1], f[2], f[3], f[4]) for f in found]
    if not live:
        ready = ready_signal(pair, cs, ind, n, found, account_usd, risk_pct)
        return (ready or base), records
    side, formed, e, stop, tgt, i, a = live[0]
    why = None
    if not in_session(formed - timedelta(minutes=INTERVAL_MIN)):
        why = "a setup formed but outside London/New York hours, so it is skipped (thin, spiky market)."
    elif (side == "BUY" and price <= stop) or (side == "SELL" and price >= stop):
        why = "the setup failed: price already hit the stop level."
    elif (side == "BUY" and price - e > CHASE_ATR * a) or (side == "SELL" and e - price > CHASE_ATR * a):
        why = "the setup formed but price already ran away from the entry. Do not chase it."
    if why:
        base.reason = why
        return base, records
    risk_usd = account_usd * risk_pct / 100.0
    lots = lots_for_risk(pair, e, abs(e - stop), risk_usd)
    risk_dist = abs(e - stop)
    word = "Uptrend" if side == "BUY" else "Downtrend"
    cand = "green" if side == "BUY" else "red"
    rel = "above" if side == "BUY" else "below"
    why_text = (f"{word}: price pulled back, then a {cand} candle closed {rel} the 20 average with RSI at {ind.rsi[i]:.0f}, so the trend is resuming. "
                f"Enter now at market.")
    return Signal(pair, f"ENTER {side}", why_text, price=price, side=side, entry=e, stop=stop, target=tgt, atr=a, formed=formed, lots=lots, risk_usd=risk_usd,
                  account_usd=account_usd, valid_until=formed + timedelta(minutes=INTERVAL_MIN * MAX_SIGNAL_AGE),
                  chase_limit=e + CHASE_ATR * a if side == "BUY" else e - CHASE_ATR * a,
                  breakeven=e + risk_dist if side == "BUY" else e - risk_dist, rsi=ind.rsi[i]), records


def ready_signal(pair: str, cs: list[Candle], ind: Indicators, n: int, found: list, account_usd: float, risk_pct: float) -> Signal | None:
    """Advance warning: the trend is there and the pullback has happened, so the entry may fire on one of the next few candle closes."""
    i = n - 1
    vals = (ind.e20[i], ind.e50[i], ind.e200[i], ind.e200[i - 10], ind.rsi[i], ind.atr[i])
    if any(v is None for v in vals):
        return None
    e20, e50, e200, e200p, r, a = vals
    win = [(j, ind.rsi[j]) for j in range(i - 5, i + 1) if ind.rsi[j] is not None]
    if e50 > e200 and e200 > e200p:
        side, hits = "BUY", [j for j, v in win if v <= 40]
    elif e50 < e200 and e200 < e200p:
        side, hits = "SELL", [j for j, v in win if v >= 60]
    else:
        return None
    if not hits or any(f[0] == side for f in found):      # no pullback yet, or the entry already fired for this pullback
        return None
    left = max(hits) + 6 - i                              # how many more candle closes can still trigger it
    if left <= 0:
        return None
    last_close = cs[i].t + timedelta(minutes=INTERVAL_MIN)
    if not in_session(last_close):
        return None
    entry = max(cs[i].c, e20) if side == "BUY" else min(cs[i].c, e20)
    d = STOP_ATR * a
    stop, tgt = (entry - d, entry + TARGET_R * d) if side == "BUY" else (entry + d, entry - TARGET_R * d)
    risk_usd = account_usd * risk_pct / 100.0
    extreme = min(v for _, v in win) if side == "BUY" else max(v for _, v in win)
    word = "uptrend" if side == "BUY" else "downtrend"
    mv = "pulled back" if side == "BUY" else "bounced up"
    why_text = (f"{word.capitalize()} and price {mv} (RSI reached {extreme:.0f}). The entry fires when a candle closes "
                f"{'green above' if side == 'BUY' else 'red below'} the 20 average and RSI turns {'above 45' if side == 'BUY' else 'below 55'}.")
    return Signal(pair, f"GET READY {side}", why_text, price=cs[i].c, side=side, entry=entry, stop=stop, target=tgt, atr=a,
                  lots=lots_for_risk(pair, entry, d, risk_usd), risk_usd=risk_usd, account_usd=account_usd,
                  valid_until=last_close + timedelta(minutes=INTERVAL_MIN * left), trigger=e20,
                  next_check=last_close + timedelta(minutes=INTERVAL_MIN), rsi=r)


# ============================================================================
# PAPER RECORD (did past signals hit the target or the stop?)
# ============================================================================
@dataclass
class PaperTrade:
    pair: str
    side: str
    opened: datetime     # time the setup candle closed
    entry: float
    stop: float
    target: float
    status: str = "OPEN"      # OPEN | WIN | LOSS | EXPIRED
    r: float = 0.0
    closed: datetime | None = None


class PaperBook:
    def __init__(self) -> None:
        self.trades: list[PaperTrade] = []
        self._keys: set = set()

    def add(self, pair: str, rec: tuple) -> bool:
        side, formed, e, stop, tgt = rec
        key = (pair, side, formed)
        if key in self._keys or not in_session(formed - timedelta(minutes=INTERVAL_MIN)):
            return False
        self._keys.add(key)
        self.trades.append(PaperTrade(pair, side, formed, e, stop, tgt))
        return True

    def update(self, pair: str, cs: list[Candle], now: datetime) -> None:
        cs = closed_only(cs, now)
        for tr in self.trades:
            if tr.pair != pair or tr.status != "OPEN":
                continue
            risk = abs(tr.entry - tr.stop)
            for c in cs:
                if c.t < tr.opened:
                    continue
                hit_stop = c.l <= tr.stop if tr.side == "BUY" else c.h >= tr.stop
                hit_tgt = c.h >= tr.target if tr.side == "BUY" else c.l <= tr.target
                if hit_stop:   # if both happen in one candle we assume the stop came first (pessimistic)
                    tr.status, tr.r, tr.closed = "LOSS", -1.0, c.t
                    break
                if hit_tgt:
                    tr.status, tr.r, tr.closed = "WIN", TARGET_R, c.t
                    break
            if tr.status == "OPEN" and cs and now - tr.opened > timedelta(hours=EXPIRE_HOURS):
                move = (cs[-1].c - tr.entry) if tr.side == "BUY" else (tr.entry - cs[-1].c)
                tr.status, tr.r, tr.closed = "EXPIRED", round(move / risk, 2) if risk else 0.0, cs[-1].t
        del self.trades[:-200]

    def stats(self) -> dict:
        done = [t for t in self.trades if t.status in ("WIN", "LOSS", "EXPIRED")]
        wins = sum(1 for t in done if t.r > 0)
        return {"total": len(self.trades), "open": sum(1 for t in self.trades if t.status == "OPEN"), "done": len(done), "wins": wins,
                "win_rate": (100.0 * wins / len(done)) if done else None, "total_r": round(sum(t.r for t in done), 2)}


# ============================================================================
# DATA + LIVE TRACKER
# ============================================================================
def parse_twelvedata(payload: dict) -> list[Candle]:
    if payload.get("status") == "error":
        raise RuntimeError(str(payload.get("message", "data provider error"))[:160])
    out = []
    for v in payload.get("values") or []:
        try:
            t = datetime.strptime(v["datetime"][:19], "%Y-%m-%d %H:%M:%S" if " " in v["datetime"] else "%Y-%m-%d").replace(tzinfo=UTC)
            out.append(Candle(t, float(v["open"]), float(v["high"]), float(v["low"]), float(v["close"])))
        except (KeyError, ValueError, TypeError):
            continue
    out.sort(key=lambda c: c.t)     # provider sends newest first
    return out


PAIR_RE = re.compile(r"^[A-Z]{3}/[A-Z]{3}$")
STREAM_FRESH = 45.0        # seconds: no price for this long and the stream counts as down
CREDIT_SOFT_CAP = 700      # the free plan allows about 800 requests a day; stop the optional price polling before the candles run out
MAX_PAIRS_DEFAULT = 6


def clean_pairs(items, limit: int) -> list[str]:
    out: list[str] = []
    for x in items or []:
        p = str(x).strip().upper()
        if PAIR_RE.match(p) and p not in out:
            out.append(p)
    return out[:limit]


def floor_candle(now: datetime) -> datetime:
    return now.replace(minute=now.minute - now.minute % INTERVAL_MIN, second=0, microsecond=0)


def is_stale(cs: list[Candle], now: datetime) -> bool:
    """True when the newest CLOSED candle is the one before the one we expect, i.e. the provider has not published the just-closed candle yet."""
    closed = closed_only(cs, now)
    if not closed:
        return False
    expected = floor_candle(now) - timedelta(minutes=INTERVAL_MIN)
    return expected - timedelta(minutes=2 * INTERVAL_MIN) <= closed[-1].t < expected     # older than that = market break, not a late candle


class ForexTracker:
    def __init__(self, pairs: list[str], api_key: str, account_usd: float = 100.0, risk_pct: float = 1.0,
                 max_pairs: int = MAX_PAIRS_DEFAULT, poll_seconds: float = 60.0) -> None:
        self.api_key = api_key
        self.max_pairs = max(1, max_pairs)
        self.pairs = clean_pairs(pairs, self.max_pairs)
        self.account_usd, self.risk_pct = account_usd, risk_pct
        self.poll_seconds = max(15.0, poll_seconds)
        self.signals: dict[str, Signal] = {}
        self.book = PaperBook()
        self.candles: dict[str, list[Candle]] = {}
        self.errors: dict[str, str] = {}
        self.last_cycle: float | None = None
        self.started = time.time()
        self.client = None
        self.rev = 0                                   # goes up whenever anything the page shows changes
        self._keys: dict[str, tuple] = {}
        self.wake = asyncio.Event()                    # set when new pairs need their first download
        self.ticks: dict[str, tuple[float, float]] = {}   # pair -> (price, time received)
        self.forming: dict[str, list] = {}             # pair -> [candle open epoch, o, h, l, c] built from live prices
        self.tick_src: dict[str, str] = {}             # pair -> 'stream' or 'poll' (where its latest price came from)
        self.credits_day = datetime.now(UTC).date()
        self.credits_used = 0                          # data requests made today (UTC)
        self.last_stream_tick = 0.0
        self.last_poll_tick = 0.0
        self.stream_error = ""
        self.outbox: list[dict] = []                   # messages to send on the live connection (subscribe / unsubscribe)
        self.viewer_ts = 0.0                           # last time the page asked for live data
        self.viewer_pair = ""

    # ---- candles + signals
    def process(self, pair: str, cs: list[Candle], now: datetime) -> Signal:
        sig, records = evaluate(pair, cs, now, self.account_usd, self.risk_pct)
        self.candles[pair] = closed_only(cs, now)
        for rec in records:
            self.book.add(pair, rec)
        self.book.update(pair, cs, now)
        self.signals[pair] = sig
        c = self.candles[pair]
        key = (sig.state, sig.formed, len(c), c[-1].t if c else None)
        if self._keys.get(pair) != key:
            self._keys[pair] = key
            self.rev += 1
        return sig

    def spend(self, n: int = 1) -> int:
        today = datetime.now(UTC).date()
        if today != self.credits_day:
            self.credits_day, self.credits_used = today, 0
        self.credits_used += n
        return self.credits_used

    async def fetch(self, pair: str) -> list[Candle]:
        self.spend()
        r = await self.client.get("https://api.twelvedata.com/time_series", params={
            "symbol": pair, "interval": f"{INTERVAL_MIN}min", "outputsize": CANDLES_WANTED, "timezone": "UTC", "apikey": self.api_key})
        r.raise_for_status()
        return parse_twelvedata(r.json())

    async def cycle(self, targets: list[str] | None = None) -> list[str]:
        """Download candles for the pairs and re-check them. Returns the pairs whose newest candle was not published yet (retry soon)."""
        now = datetime.now(UTC)
        if market_closed(now):
            changed = False
            for p in self.pairs:
                if not self.signals.get(p) or self.signals[p].state != "CLOSED":
                    changed = True
                self.signals[p] = Signal(p, "CLOSED", "the forex market is closed for the weekend. It reopens Sunday 22:00 UTC.")
            if changed:
                self.rev += 1
            self.last_cycle = time.time()
            return []
        stale: list[str] = []
        for p in (self.pairs if targets is None else [t for t in targets if t in self.pairs]):
            try:
                cs = await self.fetch(p)
                self.process(p, cs, datetime.now(UTC))
                self.errors.pop(p, None)
                if is_stale(cs, datetime.now(UTC)):
                    stale.append(p)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                self.errors[p] = (f"{type(exc).__name__}" + (f" (HTTP {status})" if status else "") + f": {exc}")[:200].replace(self.api_key, "<key>")
            await asyncio.sleep(1.5)   # stay under the provider's per-minute limit
        self.last_cycle = time.time()
        return stale

    @staticmethod
    def seconds_to_next_candle(now: datetime | None = None) -> float:
        now = now or datetime.now(UTC)
        mins = INTERVAL_MIN - (now.minute % INTERVAL_MIN)
        nxt = (now.replace(second=0, microsecond=0) + timedelta(minutes=mins)) + timedelta(seconds=6)
        return max(5.0, (nxt - now).total_seconds())

    # ---- the watch list (changed from the drawer on the page)
    def set_pairs(self, items) -> list[str]:
        new = clean_pairs(items, self.max_pairs)
        if not new:
            return list(self.pairs)
        added = [p for p in new if p not in self.pairs]
        removed = [p for p in self.pairs if p not in new]
        self.pairs = new
        for p in removed:
            for d in (self.signals, self.candles, self.errors, self.ticks, self.forming, self._keys, self.tick_src):
                d.pop(p, None)
        if removed:
            self.outbox.append({"action": "unsubscribe", "params": {"symbols": ",".join(removed)}})
        if added:
            self.outbox.append({"action": "subscribe", "params": {"symbols": ",".join(added)}})
            self.wake.set()
        if added or removed:
            self.rev += 1
        return list(self.pairs)

    # ---- live prices
    def on_tick(self, pair: str, price: float, ts: float | None = None, source: str = "stream") -> None:
        now = time.time()
        ts = ts if ts and abs(ts - now) < 3600 else now
        if pair not in self.pairs or not price or price <= 0:
            return
        self.ticks[pair] = (price, now)
        self.tick_src[pair] = source
        if source == "stream":
            self.last_stream_tick = now
        else:
            self.last_poll_tick = now
        bucket = int(ts // (INTERVAL_MIN * 60) * (INTERVAL_MIN * 60))
        fc = self.forming.get(pair)
        if fc is None or fc[0] != bucket:
            prev = fc[4] if fc else None
            if prev is None:
                last = self.candles.get(pair)
                prev = last[-1].c if last and int(last[-1].t.timestamp()) == bucket - INTERVAL_MIN * 60 else price
            self.forming[pair] = [bucket, prev, max(prev, price), min(prev, price), price]
        else:
            fc[2] = max(fc[2], price)
            fc[3] = min(fc[3], price)
            fc[4] = price

    def on_message(self, raw) -> None:
        try:
            m = json.loads(raw)
        except (ValueError, TypeError):
            return
        if not isinstance(m, dict):
            return
        ev = m.get("event")
        if ev == "price":
            try:
                self.on_tick(str(m.get("symbol", "")).upper(), float(m.get("price")), float(m.get("timestamp") or 0) or None)
            except (TypeError, ValueError):
                pass
        elif ev == "subscribe-status":
            fails = m.get("fails") or []
            if fails:
                names = ", ".join(str(f.get("symbol", f) if isinstance(f, dict) else f) for f in fails)[:120]
                self.stream_error = f"live prices not available for: {names}"
            elif m.get("success"):
                self.stream_error = ""
        elif ev == "error" or m.get("status") == "error":
            self.stream_error = str(m.get("message") or "live stream error")[:150].replace(self.api_key, "<key>")

    def stream_alive(self) -> bool:
        return time.time() - self.last_stream_tick < STREAM_FRESH

    def pair_fresh(self, pair: str, within: float) -> bool:
        t = self.ticks.get(pair)
        return bool(t and time.time() - t[1] < within)

    def live_mode(self, pair: str | None = None) -> str:
        """How live the price for this pair is. On the free plan only a few symbols can stream, so this is decided per pair."""
        if market_closed(datetime.now(UTC)):
            return "closed"
        if pair:
            if self.pair_fresh(pair, STREAM_FRESH if self.tick_src.get(pair) == "stream" else self.poll_seconds * 3):
                return "stream" if self.tick_src.get(pair) == "stream" else "poll"
            return "delayed"
        if self.stream_alive():
            return "stream"
        if time.time() - self.last_poll_tick < self.poll_seconds * 3:
            return "poll"
        return "delayed"

    async def run_stream(self) -> None:
        """Live prices over a websocket. Reconnects by itself. If streaming is not allowed on your plan, run_poll covers the page you are looking at."""
        streak = 0
        while True:
            try:
                if market_closed(datetime.now(UTC)):
                    await asyncio.sleep(300)
                    continue
                import websockets
                url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={self.api_key}"
                async with websockets.connect(url, open_timeout=15, max_size=2 ** 20) as ws:
                    await ws.send(json.dumps({"action": "subscribe", "params": {"symbols": ",".join(self.pairs)}}))
                    self.outbox.clear()

                    async def pump() -> None:
                        n = 0
                        while True:
                            await asyncio.sleep(1)
                            n += 1
                            while self.outbox:
                                await ws.send(json.dumps(self.outbox.pop(0)))
                            if n % 10 == 0:
                                await ws.send('{"action":"heartbeat"}')
                    task = asyncio.create_task(pump())
                    try:
                        async for raw in ws:
                            self.on_message(raw)
                            streak = 0
                    finally:
                        task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.stream_error = (f"{type(exc).__name__}: {exc}")[:150].replace(self.api_key, "<key>")
            streak += 1
            await asyncio.sleep(min(60.0, 4.0 * streak))

    async def poll_once(self) -> bool:
        """Ask for the latest price of the pair being viewed, if the stream is not already giving it and the daily allowance has room."""
        if (self.client is None or market_closed(datetime.now(UTC)) or time.time() - self.viewer_ts > 90 or self.viewer_pair not in self.pairs
                or self.pair_fresh(self.viewer_pair, STREAM_FRESH) and self.tick_src.get(self.viewer_pair) == "stream"):
            return False
        if self.spend(0) >= CREDIT_SOFT_CAP:      # protect the candle downloads that the signals depend on
            return False
        self.spend()
        r = await self.client.get("https://api.twelvedata.com/price", params={"symbol": self.viewer_pair, "apikey": self.api_key})
        data = r.json()
        if isinstance(data, dict) and "price" in data:
            self.on_tick(self.viewer_pair, float(data["price"]), source="poll")
            return True
        return False

    async def run_poll(self) -> None:
        """Fallback for symbols that cannot stream on your plan: about once a minute, only for the pair you are looking at."""
        while True:
            await asyncio.sleep(self.poll_seconds)
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    async def run(self) -> None:
        import httpx
        async with httpx.AsyncClient(timeout=25) as c:
            self.client = c
            side = [asyncio.create_task(self.run_stream()), asyncio.create_task(self.run_poll())]
            try:
                targets: list[str] | None = None
                retries = 0
                while True:
                    try:
                        stale = await self.cycle(targets)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        stale = []
                    if stale and retries < 6:      # the just-closed candle is not published yet: ask again in 10 seconds
                        retries += 1
                        targets, wait = stale, 10.0
                    else:
                        retries, targets, wait = 0, None, self.seconds_to_next_candle()
                    try:
                        await asyncio.wait_for(self.wake.wait(), timeout=wait)
                        self.wake.clear()
                        targets = [p for p in self.pairs if p not in self.candles]    # only the newly added pairs
                    except asyncio.TimeoutError:
                        pass
            finally:
                for t in side:
                    t.cancel()
                await asyncio.gather(*side, return_exceptions=True)

    def live_payload(self, pair: str | None = None) -> dict:
        now = time.time()
        if pair and pair in self.pairs:
            self.viewer_ts, self.viewer_pair = now, pair
        quotes = {}
        for p in self.pairs:
            cs = self.candles.get(p) or []
            t = self.ticks.get(p)
            price = t[0] if t and now - t[1] < 300 else (cs[-1].c if cs else None)
            ref = cs[-97].c if len(cs) >= 97 else (cs[0].c if cs else None)      # about 24 hours ago
            sig = self.signals.get(p)
            quotes[p] = {"p": price, "chg": ((price / ref - 1) * 100) if price and ref else None,
                         "state": sig.state if sig else "starting", "live": bool(t and now - t[1] < 60), "err": self.errors.get(p)}
        candle = None
        fc = self.forming.get(pair) if pair else None
        if fc:
            cs = self.candles.get(pair) or []
            last_open = int(cs[-1].t.timestamp()) if cs else 0
            if fc[0] > last_open and fc[0] >= int(now // (INTERVAL_MIN * 60) * (INTERVAL_MIN * 60)) - INTERVAL_MIN * 60:
                candle = list(fc)
        return {"now": int(now), "rev": self.rev, "mode": self.live_mode(pair if pair in self.pairs else None), "quotes": quotes, "candle": candle, "stream_error": self.stream_error}

    def chart_payload(self, pair: str) -> dict:
        """Everything the chart needs for one pair, as plain JSON (times are epoch seconds, UTC)."""
        cs = self.candles.get(pair, [])
        sig = self.signals.get(pair)
        live = None
        if sig and sig.state.startswith("ENTER") and sig.formed:
            live = {"side": sig.side, "entry": sig.entry, "stop": sig.stop, "target": sig.target, "formed": int(sig.formed.timestamp())}
        ready = None
        if sig and sig.state.startswith("GET READY") and sig.trigger:
            ready = {"side": sig.side, "trigger": sig.trigger}
        trades = [{"side": t.side, "t": int(t.opened.timestamp()), "entry": t.entry, "stop": t.stop, "target": t.target, "status": t.status}
                  for t in self.book.trades if t.pair == pair][-40:]
        return {"pair": pair, "dp": decimals(pair), "state": sig.state if sig else "starting", "version": self.rev,
                "candles": [[int(c.t.timestamp()), c.o, c.h, c.l, c.c] for c in cs], "signal": live, "ready": ready, "trades": trades}


    def health(self) -> dict:
        """Status for the dashboard's System health screen."""
        iso = lambda ts: datetime.fromtimestamp(ts, UTC).isoformat() if ts else None
        return {"mode": self.live_mode(), "pairs": list(self.pairs), "last_check_at": iso(self.last_cycle), "started_at": iso(self.started),
                "last_price_at": iso(max(self.last_stream_tick, self.last_poll_tick)), "stream_error": self.stream_error,
                "errors": dict(self.errors), "credits_today": self.credits_used, "ready_now": [p for p in self.pairs if self.signals.get(p) and self.signals[p].state.startswith("GET READY")], "entries_now": [p for p in self.pairs if self.signals.get(p) and self.signals[p].state.startswith("ENTER")],
                "paper": self.book.stats(), "candles": {p: len(self.candles.get(p, [])) for p in self.pairs}}

    def snapshot(self) -> dict:
        return {"pairs": [{"pair": p, "signal": self.signals.get(p), "error": self.errors.get(p)} for p in self.pairs],
                "stats": self.book.stats(), "trades": list(reversed(self.book.trades[-15:])), "last_cycle": self.last_cycle, "rev": self.rev,
                "started": self.started, "account": self.account_usd, "risk_pct": self.risk_pct, "max": self.max_pairs}


class ForexHealth:
    """Small object kept on app.state.forex so the System health screen can report on the forex add-on."""

    def __init__(self, tracker: "ForexTracker | None", key_set: bool) -> None:
        self.tracker, self.key_set = tracker, key_set

    def health(self) -> dict:
        out = {"enabled": True, "key_set": self.key_set}
        if self.tracker:
            out.update(self.tracker.health())
        return out


# ============================================================================
# CHART (plain JavaScript drawn on a canvas; no libraries, nothing to install)
# ============================================================================
CHART_JS = r'''(function () {
"use strict";

function ema(vals, n) {
  var out = new Array(vals.length).fill(null), i, k = 2 / (n + 1), cur = 0;
  if (vals.length < n) return out;
  for (i = 0; i < n; i++) cur += vals[i];
  cur /= n;
  out[n - 1] = cur;
  for (i = n; i < vals.length; i++) { cur = vals[i] * k + cur * (1 - k); out[i] = cur; }
  return out;
}

function aggregate(cs, tfMin) {
  if (tfMin === 15) return cs.slice();
  var sec = tfMin * 60, out = [], cur = null, i, c, b;
  for (i = 0; i < cs.length; i++) {
    c = cs[i];
    b = Math.floor(c[0] / sec) * sec;
    if (!cur || cur[0] !== b) { cur = [b, c[1], c[2], c[3], c[4]]; out.push(cur); }
    else { cur[2] = Math.max(cur[2], c[2]); cur[3] = Math.min(cur[3], c[3]); cur[4] = c[4]; }
  }
  return out;
}

function niceStep(range, ticks) {
  var raw = range / ticks;
  if (!(raw > 0)) return 1;
  var p = Math.pow(10, Math.floor(Math.log10(raw))), f = raw / p;
  return (f < 1.5 ? 1 : f < 3.5 ? 2 : f < 7.5 ? 5 : 10) * p;
}

var MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
function p2(n) { return (n < 10 ? "0" : "") + n; }
function hhmm(t) { var d = new Date(t * 1000); return p2(d.getUTCHours()) + ":" + p2(d.getUTCMinutes()); }
function dayLabel(t) { var d = new Date(t * 1000); return d.getUTCDate() + " " + MON[d.getUTCMonth()]; }
var TFNAME = {15: "M15", 60: "H1", 240: "H4"};

function createChart(canvas, theme) {
  var ctx = canvas.getContext("2d");
  var S = {data: null, live: null, pair: "", tf: 15, n: 80, off: -6, ema: true, cross: null, W: 320, H: 420, dpr: 1};
  var cs = [], e20 = [], e50 = [], e200 = [], idx = {};
  var PAD = {l: 6, r: 66, t: 46, b: 24}, DEFOFF = -6;   // a few empty candles on the right, like MT5

  function baseCandles() {
    var base = S.data ? S.data.candles : [], lv = S.live;
    if (lv && base.length && lv[0] > base[base.length - 1][0]) return base.concat([lv]);   // the still-forming candle, built from live prices
    return base;
  }
  function rebuild() {
    var before = cs.length;
    cs = S.data ? aggregate(baseCandles(), S.tf) : [];
    var cl = cs.map(function (c) { return c[4]; });
    e20 = ema(cl, 20); e50 = ema(cl, 50); e200 = ema(cl, 200);
    idx = {};
    cs.forEach(function (c, i) { idx[c[0]] = i; });
    if (before > 0 && cs.length > before && S.off > 0) S.off += cs.length - before;   // scrolled back in time: do not let new candles push the view
    clamp();
  }
  function clamp() {
    var len = cs.length;
    if (!len) return;   // nothing to fit yet: keep the requested zoom
    S.n = Math.max(15, Math.min(S.n, Math.max(15, len)));
    S.off = Math.max(-S.n * 0.25, Math.min(S.off, Math.max(0, len - 15)));
  }
  function geo() {
    var pw = S.W - PAD.l - PAD.r, ph = S.H - PAD.t - PAD.b;
    return {len: cs.length, pw: pw, ph: ph, first: cs.length - S.n - S.off, step: pw / S.n, x0: PAD.l, x1: PAD.l + pw, y0: PAD.t, y1: PAD.t + ph};
  }

  function draw() {
    var T = theme(), dp = (S.data && S.data.dp) || 5, i, k;
    ctx.setTransform(S.dpr, 0, 0, S.dpr, 0, 0);
    ctx.setLineDash([]);
    ctx.globalAlpha = 1;
    ctx.fillStyle = T.bg;
    ctx.fillRect(0, 0, S.W, S.H);
    if (!cs.length) {
      ctx.fillStyle = T.mute; ctx.font = "14px sans-serif"; ctx.textAlign = "center";
      ctx.fillText("Waiting for the first candles...", S.W / 2, S.H / 2);
      return;
    }
    var g = geo();
    var i0 = Math.max(0, Math.floor(g.first)), i1 = Math.min(g.len - 1, Math.ceil(g.first + S.n));
    var lo = Infinity, hi = -Infinity;
    for (i = i0; i <= i1; i++) { if (cs[i][3] < lo) lo = cs[i][3]; if (cs[i][2] > hi) hi = cs[i][2]; }
    var sg = S.data.signal;
    if (sg) [sg.entry, sg.stop, sg.target].forEach(function (p) { if (p < lo) lo = p; if (p > hi) hi = p; });
    var rd = S.data.ready;
    if (rd) { if (rd.trigger < lo) lo = rd.trigger; if (rd.trigger > hi) hi = rd.trigger; }
    if (!(hi > lo)) hi = lo + 1e-6;
    var pv = (hi - lo) * 0.08; lo -= pv; hi += pv;
    function Y(p) { return g.y1 - (p - lo) / (hi - lo) * g.ph; }
    function X(j) { return g.x0 + (j - g.first + 0.5) * g.step; }
    function fp(p) { return p.toFixed(dp); }

    // grid
    var st = niceStep(hi - lo, 6), ticks = [];
    ctx.lineWidth = 1; ctx.strokeStyle = T.line; ctx.beginPath();
    for (k = Math.ceil(lo / st); k * st <= hi; k++) {
      var yy = Math.round(Y(k * st)) + 0.5;
      ctx.moveTo(g.x0, yy); ctx.lineTo(g.x1, yy);
      ticks.push({y: yy, p: k * st});
    }
    var every = Math.max(1, Math.ceil(84 / g.step)), tl = [];
    for (i = Math.max(0, Math.ceil(g.first)); i <= i1; i++) {
      if (i % every === 0) {
        var vx = Math.round(X(i)) + 0.5;
        ctx.moveTo(vx, g.y0); ctx.lineTo(vx, g.y1);
        var prev = i > 0 ? cs[i - 1][0] : cs[i][0];
        tl.push({x: X(i), text: (new Date(prev * 1000).getUTCDate() !== new Date(cs[i][0] * 1000).getUTCDate() || S.tf >= 240) ? dayLabel(cs[i][0]) : hhmm(cs[i][0])});
      }
    }
    ctx.stroke();

    // trade zone for the live signal (faint green/red boxes), then candles
    var tags = [];
    if (sg) {
      var fsec = S.tf * 60, fi = idx[Math.floor((sg.formed - 900) / fsec) * fsec];
      var zx = fi === undefined ? g.x0 : Math.max(g.x0, X(fi));
      ctx.globalAlpha = 0.10;
      ctx.fillStyle = T.up;
      ctx.fillRect(zx, Math.min(Y(sg.entry), Y(sg.target)), g.x1 - zx, Math.abs(Y(sg.target) - Y(sg.entry)));
      ctx.fillStyle = T.down;
      ctx.fillRect(zx, Math.min(Y(sg.entry), Y(sg.stop)), g.x1 - zx, Math.abs(Y(sg.stop) - Y(sg.entry)));
      ctx.globalAlpha = 1;
    }
    var bw = Math.max(1, Math.floor(g.step * 0.7));
    for (i = i0; i <= i1; i++) {
      var c = cs[i], x = X(i), col = c[4] >= c[1] ? T.up : T.down;
      ctx.strokeStyle = col; ctx.fillStyle = col; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(Math.round(x) + 0.5, Y(c[2])); ctx.lineTo(Math.round(x) + 0.5, Y(c[3])); ctx.stroke();
      var yo = Y(c[1]), yc = Y(c[4]);
      ctx.fillRect(Math.round(x - bw / 2), Math.min(yo, yc), bw, Math.max(1, Math.abs(yo - yc)));
    }

    // moving averages
    if (S.ema) {
      [[e20, "#f5c542"], [e50, "#4da3ff"], [e200, "#c77dff"]].forEach(function (pair) {
        var arr = pair[0], started = false;
        ctx.strokeStyle = pair[1]; ctx.lineWidth = 1.3; ctx.beginPath();
        for (var j = Math.max(0, i0 - 1); j <= i1; j++) {
          if (arr[j] === null) continue;
          if (!started) { ctx.moveTo(X(j), Y(arr[j])); started = true; } else ctx.lineTo(X(j), Y(arr[j]));
        }
        ctx.stroke();
      });
    }

    // paper-trade markers
    (S.data.trades || []).forEach(function (t) {
      var sec = S.tf * 60, b = Math.floor((t.t - 900) / sec) * sec, j = idx[b];
      if (j === undefined || j < i0 || j > i1) return;
      var x = X(j), yb;
      ctx.globalAlpha = t.status === "OPEN" ? 1 : 0.55;
      ctx.fillStyle = t.side === "BUY" ? T.up : T.down;
      ctx.beginPath();
      if (t.side === "BUY") { yb = Y(cs[j][3]) + 6; ctx.moveTo(x, yb); ctx.lineTo(x - 5, yb + 9); ctx.lineTo(x + 5, yb + 9); }
      else { yb = Y(cs[j][2]) - 6; ctx.moveTo(x, yb); ctx.lineTo(x - 5, yb - 9); ctx.lineTo(x + 5, yb - 9); }
      ctx.closePath(); ctx.fill(); ctx.globalAlpha = 1;
    });

    // live signal levels
    function level(p, color, label, dashed) {
      var y = Math.round(Y(p)) + 0.5;
      ctx.strokeStyle = color; ctx.lineWidth = 1.3; ctx.setLineDash(dashed);
      ctx.beginPath(); ctx.moveTo(g.x0, y); ctx.lineTo(g.x1, y); ctx.stroke(); ctx.setLineDash([]);
      ctx.fillStyle = color; ctx.font = "bold 11px -apple-system, sans-serif"; ctx.textAlign = "left";
      ctx.fillText(label, g.x0 + 6, y - 4);
      tags.push({y: y, text: fp(p), bg: color});
    }
    if (sg) {
      level(sg.target, T.up, "TAKE PROFIT", [6, 4]);
      level(sg.stop, T.down, "STOP LOSS", [6, 4]);
      level(sg.entry, T.blue, "ENTER " + sg.side, [2, 3]);
    }
    if (rd) level(rd.trigger, T.amber, "TRIGGER: a candle must close " + (rd.side === "BUY" ? "above" : "below") + " here", [3, 3]);

    // last price
    var last = cs[g.len - 1], lc = last[4] >= last[1] ? T.up : T.down, ly = Math.round(Y(last[4])) + 0.5;
    ctx.strokeStyle = lc; ctx.lineWidth = 1; ctx.setLineDash([2, 3]);
    ctx.beginPath(); ctx.moveTo(g.x0, ly); ctx.lineTo(g.x1, ly); ctx.stroke(); ctx.setLineDash([]);
    tags.push({y: ly, text: fp(last[4]), bg: lc});

    // cover everything outside the plot, then draw the axes on top
    ctx.fillStyle = T.bg;
    ctx.fillRect(g.x1, 0, S.W - g.x1, S.H);
    ctx.fillRect(0, g.y1, S.W, S.H - g.y1);
    ctx.fillRect(0, 0, S.W, g.y0);
    ctx.font = "11px -apple-system, sans-serif"; ctx.textAlign = "left"; ctx.fillStyle = T.mute;
    ticks.forEach(function (t) { ctx.fillText(fp(t.p), g.x1 + 6, t.y + 4); });
    ctx.textAlign = "center";
    tl.forEach(function (t) { if (t.x > g.x0 + 18 && t.x < g.x1 - 18) ctx.fillText(t.text, t.x, g.y1 + 16); });
    tags.sort(function (a, b) { return a.y - b.y; });
    for (k = 1; k < tags.length; k++) if (tags[k].y - tags[k - 1].y < 17) tags[k].y = tags[k - 1].y + 17;   // keep neighbouring tags readable
    tags.forEach(function (t) {
      ctx.fillStyle = t.bg; ctx.fillRect(g.x1 + 1, t.y - 8, S.W - g.x1 - 2, 16);
      ctx.fillStyle = "#fff"; ctx.textAlign = "left"; ctx.font = "bold 11px -apple-system, sans-serif";
      ctx.fillText(t.text, g.x1 + 5, t.y + 4);
    });

    // crosshair
    var shown = last;
    if (S.cross) {
      var cx = Math.max(g.x0, Math.min(S.cross.x, g.x1)), cy = Math.max(g.y0, Math.min(S.cross.y, g.y1));
      var ci = Math.max(0, Math.min(g.len - 1, Math.round(g.first + (cx - g.x0) / g.step - 0.5)));
      var sx = Math.round(X(ci)) + 0.5, price = lo + (g.y1 - cy) / g.ph * (hi - lo);
      shown = cs[ci];
      ctx.strokeStyle = T.mute; ctx.lineWidth = 1; ctx.setLineDash([4, 3]);
      ctx.beginPath(); ctx.moveTo(sx, g.y0); ctx.lineTo(sx, g.y1); ctx.moveTo(g.x0, cy); ctx.lineTo(g.x1, cy); ctx.stroke(); ctx.setLineDash([]);
      ctx.fillStyle = T.mute; ctx.fillRect(g.x1 + 1, cy - 8, S.W - g.x1 - 2, 16);
      ctx.fillStyle = T.bg; ctx.textAlign = "left"; ctx.font = "bold 11px -apple-system, sans-serif"; ctx.fillText(fp(price), g.x1 + 5, cy + 4);
      var tt = dayLabel(shown[0]) + " " + hhmm(shown[0]);
      var tw = 92;
      ctx.fillStyle = T.mute; ctx.fillRect(Math.max(0, Math.min(sx - tw / 2, S.W - tw)), g.y1 + 3, tw, 17);
      ctx.fillStyle = T.bg; ctx.textAlign = "center"; ctx.fillText(tt, Math.max(tw / 2, Math.min(sx, S.W - tw / 2)), g.y1 + 16);
    }

    // header
    ctx.textAlign = "left"; ctx.fillStyle = T.fg; ctx.font = "bold 14px -apple-system, sans-serif";
    ctx.fillText(S.data.pair + "  " + (TFNAME[S.tf] || S.tf + "m"), 8, 18);
    var stx = S.data.state || "";
    ctx.textAlign = "right"; ctx.font = "bold 12px -apple-system, sans-serif";
    ctx.fillStyle = stx === "ENTER BUY" ? T.up : stx === "ENTER SELL" ? T.down : stx.indexOf("GET READY") === 0 ? T.amber : T.mute;
    ctx.fillText(stx, S.W - 8, 18);
    ctx.textAlign = "left"; ctx.font = "11px -apple-system, sans-serif"; ctx.fillStyle = T.mute;
    ctx.fillText("O " + fp(shown[1]) + "  H " + fp(shown[2]) + "  L " + fp(shown[3]) + "  C " + fp(shown[4]), 8, 36);
  }

  function resize() {
    var r = canvas.getBoundingClientRect();
    S.dpr = (typeof window !== "undefined" && window.devicePixelRatio) || 1;
    S.W = r.width; S.H = r.height;
    canvas.width = Math.round(r.width * S.dpr); canvas.height = Math.round(r.height * S.dpr);
    draw();
  }

  // touch + mouse: drag to pan, pinch / wheel to zoom, tap or hover for crosshair
  var ptrs = {}, count = 0, pinch = 0, moved = 0;
  function pos(e) { var r = canvas.getBoundingClientRect(); return {x: e.clientX - r.left, y: e.clientY - r.top}; }
  function pdist() { var a = Object.keys(ptrs).map(function (k) { return ptrs[k]; }); return Math.hypot(a[0].x - a[1].x, a[0].y - a[1].y) || 1; }
  canvas.addEventListener("pointerdown", function (e) {
    if (canvas.setPointerCapture) { try { canvas.setPointerCapture(e.pointerId); } catch (x) { /* ignore */ } }
    if (!ptrs[e.pointerId]) count++;
    ptrs[e.pointerId] = pos(e); moved = 0;
    if (count === 2) pinch = pdist();
  });
  canvas.addEventListener("pointermove", function (e) {
    var p = pos(e), old = ptrs[e.pointerId];
    if (!old) { if (e.pointerType === "mouse") { S.cross = p; draw(); } return; }
    ptrs[e.pointerId] = p;
    if (count >= 2) {
      var d = pdist(); S.n *= pinch / d; pinch = d; S.cross = null; clamp(); draw(); return;
    }
    moved += Math.abs(p.x - old.x) + Math.abs(p.y - old.y);
    if (moved > 5) { S.off += (p.x - old.x) / geo().step; S.cross = null; clamp(); draw(); }
  });
  function up(e) {
    var had = ptrs[e.pointerId];
    if (had) { delete ptrs[e.pointerId]; count = Math.max(0, count - 1); }
    if (had && moved <= 5 && count === 0 && e.pointerType !== "mouse") { S.cross = pos(e); draw(); }
  }
  canvas.addEventListener("pointerup", up);
  canvas.addEventListener("pointercancel", up);
  canvas.addEventListener("pointerleave", function (e) { if (e.pointerType === "mouse") { S.cross = null; draw(); } });
  canvas.addEventListener("wheel", function (e) {
    e.preventDefault(); S.n *= e.deltaY > 0 ? 1.12 : 0.89; clamp(); draw();
  }, {passive: false});
  canvas.addEventListener("dblclick", function () { S.n = S.tf === 15 ? 80 : S.tf === 60 ? 70 : 40; S.off = DEFOFF; S.cross = null; clamp(); draw(); });
  if (typeof ResizeObserver !== "undefined") new ResizeObserver(resize).observe(canvas);
  else if (typeof window !== "undefined") window.addEventListener("resize", resize);

  return {
    setData: function (d) {
      if (d.pair !== S.pair) { S.pair = d.pair; S.live = null; S.off = DEFOFF; cs = []; }
      S.data = d; rebuild(); draw();
    },
    setLive: function (c) { S.live = c; rebuild(); draw(); },
    setTf: function (m) { S.tf = m; S.off = DEFOFF; S.n = m === 15 ? 80 : m === 60 ? 70 : 40; S.cross = null; rebuild(); draw(); },
    setEma: function (on) { S.ema = on; draw(); },
    reset: function () { S.n = S.tf === 15 ? 80 : S.tf === 60 ? 70 : 40; S.off = DEFOFF; S.cross = null; clamp(); draw(); },
    setView: function (n, off) { S.n = n; S.off = off; clamp(); },
    resize: resize, draw: draw
  };
}

var api = {createChart: createChart, aggregate: aggregate, ema: ema, niceStep: niceStep};
if (typeof window !== "undefined") window.FxChart = api;
if (typeof module !== "undefined") module.exports = api;

// ---- page glue (only runs on the real page)
var CAT = [["EUR/USD", "Euro vs US Dollar"], ["GBP/USD", "British Pound vs US Dollar"], ["USD/JPY", "US Dollar vs Japanese Yen"], ["USD/CHF", "US Dollar vs Swiss Franc"],
  ["AUD/USD", "Australian Dollar vs US Dollar"], ["USD/CAD", "US Dollar vs Canadian Dollar"], ["NZD/USD", "New Zealand Dollar vs US Dollar"], ["EUR/GBP", "Euro vs British Pound"],
  ["EUR/JPY", "Euro vs Japanese Yen"], ["GBP/JPY", "British Pound vs Japanese Yen"], ["AUD/JPY", "Australian Dollar vs Japanese Yen"], ["EUR/AUD", "Euro vs Australian Dollar"],
  ["EUR/CHF", "Euro vs Swiss Franc"], ["GBP/CHF", "British Pound vs Swiss Franc"], ["CAD/JPY", "Canadian Dollar vs Japanese Yen"], ["XAU/USD", "Gold vs US Dollar"], ["XAG/USD", "Silver vs US Dollar"]];
function descOf(p) { for (var i = 0; i < CAT.length; i++) if (CAT[i][0] === p) return CAT[i][1]; return "Custom symbol"; }
function dpOf(p) { return p.indexOf("XAU/") === 0 ? 2 : p.indexOf("XAG/") === 0 ? 3 : /\/JPY$/.test(p) ? 3 : 5; }

function boot() {
  var FX = window.FX_BOOT, doc = document, el = function (id) { return doc.getElementById(id); };
  var canvas = el("fxc");
  if (!FX || !canvas) return;
  function theme() {
    var s = getComputedStyle(doc.documentElement);
    function v(n, d) { return (s.getPropertyValue(n) || "").trim() || d; }
    return {bg: v("--chartbg", "#0b0e13"), fg: v("--text", "#f2f2f2"), mute: v("--mute", "#9a9aa0"), line: v("--line", "#2a2c30"), up: v("--green", "#3ecf8e"), down: v("--red", "#ff6b6b"), amber: v("--amber", "#ffb340"), blue: "#4da3ff"};
  }
  function mkStore(kind) {
    return {get: function (k) { try { return window[kind].getItem(k); } catch (e) { return null; } }, set: function (k, v) { try { window[kind].setItem(k, v); } catch (e) { /* ignore */ } }, del: function (k) { try { window[kind].removeItem(k); } catch (e) { /* ignore */ } }};
  }
  var ss = mkStore("sessionStorage"), ls = mkStore("localStorage");
  var chart = createChart(canvas, theme);
  var wl = FX.pairs.slice(), quotes = {}, mode = "starting", prevStates = null, dirty = false, busy = false, actx = null;
  var pair = ss.get("fxpair"), tf = parseInt(ss.get("fxtf") || "15", 10), emaOn = ss.get("fxema") !== "0";
  if (wl.indexOf(pair) < 0) pair = FX.first;
  if ([15, 60, 240].indexOf(tf) < 0) tf = 15;
  chart.setTf(tf); chart.setEma(emaOn);

  function mk(tag, cls, text) { var n = doc.createElement(tag); if (cls) n.className = cls; if (text !== undefined) n.textContent = text; return n; }

  // ---- alerts: banner, beep, vibration
  function unlock() { try { var A = window.AudioContext || window.webkitAudioContext; if (A && !actx) actx = new A(); if (actx && actx.state === "suspended") actx.resume(); } catch (e) { /* ignore */ } }
  doc.addEventListener("pointerdown", unlock, {passive: true});
  function beep() {
    if (!actx) return;
    try { [0, 0.22].forEach(function (d) { var o = actx.createOscillator(), g = actx.createGain(); o.frequency.value = 880; g.gain.value = 0.15; o.connect(g); g.connect(actx.destination); o.start(actx.currentTime + d); o.stop(actx.currentTime + d + 0.16); }); } catch (e) { /* ignore */ }
  }
  function showToast(text, p, st) {
    var t = el("fxtoast");
    t.textContent = text + "  (tap to view)"; t.className = "toast show " + (st.indexOf("GET READY") === 0 ? "ready" : st.indexOf("SELL") >= 0 ? "sell" : "buy");
    t.onclick = function () { t.className = "toast"; pick(p, true); };
    setTimeout(function () { t.className = "toast"; }, 90000);
  }
  function newSignal(p, st) {
    var soon = st.indexOf("GET READY") === 0, text = soon ? st + " " + p + " - heads-up, do not enter yet" : st + " " + p + " - enter now";
    showToast(text, p, st); beep();
    if (navigator.vibrate) { try { navigator.vibrate(soon ? [120] : [200, 100, 200]); } catch (e) { /* ignore */ } }
    doc.title = "(1) " + text;
    ss.set("fxtoast", JSON.stringify({text: text, p: p, st: st, t: Date.now()}));   // survives the page reload that follows a new check
  }
  try { var st0 = JSON.parse(ss.get("fxtoast") || "null"); if (st0 && Date.now() - st0.t < 600000) { showToast(st0.text, st0.p, st0.st); doc.title = "(1) " + st0.text; } else ss.del("fxtoast"); } catch (e) { /* ignore */ }

  // ---- live badge
  function badge() {
    var b = el("fxlive"), txt = "Starting", cls = "live";
    if (mode === "stream") { txt = "LIVE"; cls = "live on"; }
    else if (mode === "poll") { txt = "Live (every minute)"; cls = "live mid"; }
    else if (mode === "closed") { txt = "Market closed"; }
    else if (mode === "delayed") { txt = "Delayed (15 min)"; }
    b.className = cls; b.textContent = "\u25CF " + txt;
    var pd = el("fxpdot"), pt = el("fxptxt");   // the pill in the top bar
    if (pd && pt) {
      pd.className = "dot " + (mode === "stream" ? "ok" : mode === "poll" || mode === "delayed" ? "warn" : "");
      pt.textContent = mode === "stream" ? "Live" : mode === "poll" ? "Live 1m" : mode === "delayed" ? "Delayed" : mode === "closed" ? "Closed" : "Forex";
    }
  }

  // ---- toolbar
  function paint() {
    var bar = el("fxbar"); bar.innerHTML = "";
    var s = mk("button", "chip big", pair + " \u25BE"); s.type = "button"; s.addEventListener("click", openDrawer); bar.appendChild(s);
    var lb = mk("span", "live", ""); lb.id = "fxlive"; bar.appendChild(lb);
    var tfs = el("fxtfs"); tfs.innerHTML = "";
    [[15, "M15"], [60, "H1"], [240, "H4"]].forEach(function (it) {
      var b = mk("button", "chip" + (it[0] === tf ? " on" : ""), it[1]); b.type = "button";
      b.addEventListener("click", function () { tf = it[0]; ss.set("fxtf", String(tf)); chart.setTf(tf); paint(); });
      tfs.appendChild(b);
    });
    var e = mk("button", "chip" + (emaOn ? " on" : ""), "EMA"); e.type = "button";
    e.addEventListener("click", function () { emaOn = !emaOn; ss.set("fxema", emaOn ? "1" : "0"); chart.setEma(emaOn); paint(); });
    tfs.appendChild(e);
    var r = mk("button", "chip", "Reset view"); r.type = "button"; r.addEventListener("click", function () { chart.reset(); });
    tfs.appendChild(r);
    badge();
  }

  // ---- symbols drawer (like MT5 Quotes)
  var drawer = mk("div", "drawer"), dim = mk("div", "dim");
  drawer.innerHTML = '<div class="grab"></div><div class="dh"><b>Symbols</b><button type="button" class="chip on" id="fxdone">Done</button></div>' +
    '<input id="fxq" type="text" autocomplete="off" autocapitalize="characters" placeholder="Search, or type a symbol like GBP/JPY"><div id="fxlist"></div>';
  doc.body.appendChild(dim); doc.body.appendChild(drawer);
  function openDrawer() { unlock(); drawer.className = "drawer open"; dim.className = "dim open"; renderDrawer(); }
  function closeDrawer() { drawer.className = "drawer"; dim.className = "dim"; if (dirty) location.reload(); }
  dim.addEventListener("click", closeDrawer);
  drawer.querySelector("#fxdone").addEventListener("click", closeDrawer);
  drawer.querySelector("#fxq").addEventListener("input", renderDrawer);

  function saveList(list) {
    wl = list; ls.set("fxwl", JSON.stringify(list)); dirty = true;
    postWatch(list).then(function (res) { if (res) { wl = res; ls.set("fxwl", JSON.stringify(res)); } renderDrawer(); });
  }
  function postWatch(list) {
    return fetch("/forex/watch", {method: "POST", credentials: "same-origin", headers: {"Content-Type": "application/json", "X-Fx": "1"}, body: JSON.stringify({pairs: list})})
      .then(function (r) { return r.ok ? r.json() : null; }).then(function (j) { return j ? j.pairs : null; }).catch(function () { return null; });
  }
  function fmt(p, x) { return x === null || x === undefined ? "-" : x.toFixed(dpOf(p)); }
  function renderDrawer() {
    var box = drawer.querySelector("#fxlist"), q = (drawer.querySelector("#fxq").value || "").trim().toUpperCase(), top = box.scrollTop;
    box.innerHTML = "";
    box.appendChild(mk("div", "sec", "Watching " + wl.length + " of " + FX.max));
    wl.forEach(function (p) {
      var qd = quotes[p] || {}, row = mk("div", "qrow"), left = mk("div", "ql"), right = mk("div", "qr");
      left.appendChild(mk("b", "", p)); left.appendChild(mk("div", "sub", descOf(p)));
      right.appendChild(mk("b", "", fmt(p, qd.p)));
      right.appendChild(mk("div", "chg " + (qd.chg > 0 ? "buy" : qd.chg < 0 ? "sell" : ""), qd.chg === null || qd.chg === undefined ? "" : (qd.chg > 0 ? "+" : "") + qd.chg.toFixed(2) + "% 24h"));
      var isE = qd.state === "ENTER BUY" || qd.state === "ENTER SELL", isR = (qd.state || "").indexOf("GET READY") === 0;
      var pill = mk("div", "pill" + (qd.state === "ENTER BUY" ? " buy" : qd.state === "ENTER SELL" ? " sell" : isR ? " ready" : ""), isE || isR ? qd.state : "");
      row.appendChild(left); row.appendChild(right); if (pill.textContent) row.appendChild(pill);
      row.addEventListener("click", function () { closeDrawerKeep(); pick(p, false); });
      if (wl.length > 1) {
        var x = mk("button", "x", "\u00D7"); x.type = "button"; x.setAttribute("aria-label", "Remove " + p);
        x.addEventListener("click", function (ev) { ev.stopPropagation(); if (pair === p) { /* keep chart on a pair that stays */ } saveList(wl.filter(function (w) { return w !== p; })); if (pair === p) { pair = wl[0]; ss.set("fxpair", pair); } });
        row.appendChild(x);
      }
      box.appendChild(row);
    });
    box.appendChild(mk("div", "sec", "Add a symbol"));
    var shown = 0;
    CAT.forEach(function (c) {
      if (wl.indexOf(c[0]) >= 0) return;
      if (q && c[0].indexOf(q) < 0 && c[1].toUpperCase().indexOf(q) < 0) return;
      shown++; box.appendChild(addRow(c[0], c[1]));
    });
    if (/^[A-Z]{3}\/[A-Z]{3}$/.test(q) && wl.indexOf(q) < 0 && !CAT.some(function (c) { return c[0] === q; })) box.appendChild(addRow(q, "Custom symbol (the data provider must support it)"));
    else if (!shown) box.appendChild(mk("div", "sub", "Nothing matches. Type a symbol like GBP/JPY."));
    box.appendChild(mk("div", "sub", wl.length + " symbols use about " + wl.length * 96 + " data requests a day. The free plan allows roughly 800, so keep it to " + FX.max + " or fewer."));
    box.scrollTop = top;
  }
  function addRow(p, d) {
    var row = mk("div", "qrow"), left = mk("div", "ql");
    left.appendChild(mk("b", "", p)); left.appendChild(mk("div", "sub", d));
    var b = mk("button", "chip", wl.length >= FX.max ? "Full" : "+ Add"); b.type = "button"; b.disabled = wl.length >= FX.max;
    b.addEventListener("click", function () { saveList(wl.concat([p])); });
    row.appendChild(left); row.appendChild(b); return row;
  }
  function closeDrawerKeep() { drawer.className = "drawer"; dim.className = "dim"; if (dirty) setTimeout(function () { location.reload(); }, 50); }

  // ---- chart data (candles) + live loop
  function load() {
    fetch("/forex/data?pair=" + encodeURIComponent(pair), {cache: "no-store", credentials: "same-origin"}).then(function (r) {
      if (r.status === 401 || r.status === 303) { location.reload(); return null; }
      return r.ok ? r.json() : null;
    }).then(function (d) {
      if (!d) return;
      if (d.version !== FX.version) { location.reload(); return; }
      el("fxmsg").textContent = "Drag to move, pinch or scroll to zoom, tap for a crosshair, double-tap to reset. Times are UTC. The last candle moves live.";
      chart.setData(d);
      tick();
    }).catch(function () { el("fxmsg").textContent = "Chart data is not available right now. It will retry."; });
  }
  function pick(p, scroll) {
    pair = p; ss.set("fxpair", p); paint(); load();
    if (scroll) el("fxchart").scrollIntoView({behavior: "smooth", block: "start"});
  }
  function onLive(d) {
    quotes = d.quotes || {}; mode = d.mode; badge();
    chart.setLive(d.candle || null);
    var states = {}, p;
    for (p in quotes) states[p] = quotes[p].state;
    if (prevStates) for (p in states) { if (/^(ENTER|GET READY)/.test(states[p]) && prevStates[p] !== states[p]) newSignal(p, states[p]); }
    Array.prototype.forEach.call(doc.querySelectorAll(".fxready"), function (n) {      // live price against the trigger on the GET READY cards
      var q = quotes[n.getAttribute("data-pair")], lp = n.querySelector(".fxlp");
      if (!q || q.p === null || q.p === undefined || !lp) return;
      var trig = parseFloat(n.getAttribute("data-trigger")), buy = n.getAttribute("data-side") === "BUY", ok = buy ? q.p > trig : q.p < trig;
      lp.textContent = q.p.toFixed(dpOf(n.getAttribute("data-pair"))) + (ok ? (buy ? " (above" : " (below") + " the trigger now; if a candle closes like this, the entry fires)" : " (not past the trigger yet)");
    });
    prevStates = states;
    if (drawer.className.indexOf("open") >= 0) renderDrawer();
    if (d.rev !== FX.version) location.reload();   // a new check finished: refresh the cards and candles
  }
  function tick() {
    if (doc.hidden || busy) return;
    busy = true;
    fetch("/forex/live?pair=" + encodeURIComponent(pair), {cache: "no-store", credentials: "same-origin"}).then(function (r) {
      if (r.status === 401 || r.status === 303) { location.reload(); return null; }
      return r.ok ? r.json() : null;
    }).then(function (d) { busy = false; if (d) onLive(d); }).catch(function () { busy = false; mode = "delayed"; badge(); });
  }

  Array.prototype.forEach.call(doc.querySelectorAll("[data-pair]"), function (n) {
    n.addEventListener("click", function () { pick(n.getAttribute("data-pair"), true); });
  });

  // the browser remembers your list and tells the server (the server forgets it when it restarts)
  var saved = null; try { saved = JSON.parse(ls.get("fxwl") || "null"); } catch (e) { saved = null; }
  if (saved && saved.length && saved.join() !== wl.join()) {
    postWatch(saved).then(function (res) { if (res && res.join() !== wl.join()) location.reload(); else if (!res) ls.del("fxwl"); });
  }
  paint(); chart.resize(); load();
  setInterval(tick, 1500);
  doc.addEventListener("visibilitychange", function () { if (!doc.hidden) tick(); });
  window.addEventListener("fxtheme", function () { chart.draw(); });
}
if (typeof document !== "undefined") {
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else boot();
}
})();
'''



CHROME_JS = r'''(function () {
  var SUN = '<svg viewBox="0 0 24 24" width="19" height="19" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>';
  var MOON = '<svg viewBox="0 0 24 24" width="19" height="19" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M20 14.5A8 8 0 0 1 9.5 4 8 8 0 1 0 20 14.5z"/></svg>';
  var b = document.getElementById("themebtn");
  if (b) {
    var paint = function () { b.innerHTML = document.documentElement.getAttribute("data-theme") === "light" ? MOON : SUN; };
    b.addEventListener("click", function () {
      var next = document.documentElement.getAttribute("data-theme") === "light" ? "dark" : "light";
      document.documentElement.setAttribute("data-theme", next);
      try { localStorage.setItem("tosh-theme", next); } catch (e) { /* ignore */ }
      paint();
      window.dispatchEvent(new Event("fxtheme"));
    });
    paint();
  }
  // times on signal cards: show them in the phone's own time zone, and count down to the deadline
  function clock() {
    var i, n, t, s, list = document.querySelectorAll ? document.querySelectorAll(".lt, .cd") : [], now = Date.now() / 1000;
    for (i = 0; i < list.length; i++) {
      n = list[i];
      if (n.className.indexOf("lt") >= 0) {
        if (n.getAttribute("data-done")) continue;
        t = new Date(parseFloat(n.getAttribute("data-t")) * 1000);
        n.textContent = t.toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"}) + " your time (" + n.getAttribute("data-utc") + " UTC)";
        n.setAttribute("data-done", "1");
      } else {
        s = parseFloat(n.getAttribute("data-until")) - now;
        n.textContent = s <= 0 ? "expired" : s < 90 ? "in " + Math.round(s) + " sec" : "in " + Math.ceil(s / 60) + " min";
      }
    }
  }
  clock();
  setInterval(clock, 1000);
})();'''

THEME_HEAD_JS = '(function(){var t=null;try{t=localStorage.getItem("tosh-theme");}catch(e){}if(!t)t=(window.matchMedia&&matchMedia("(prefers-color-scheme: light)").matches)?"light":"dark";document.documentElement.setAttribute("data-theme",t);})();'

_ICONS = {
    "pulse": '<path d="M3 12h4l3-8 4 16 3-8h4"/>',
    "logout": '<path d="M9 4H5a1 1 0 0 0-1 1v14a1 1 0 0 0 1 1h4"/><path d="m16 8 4 4-4 4M20 12H9"/>',
}


def _svg(name: str, size: int = 19) -> str:
    return (f'<svg viewBox="0 0 24 24" width="{size}" height="{size}" fill="none" stroke="currentColor" stroke-width="1.9" '
            f'stroke-linecap="round" stroke-linejoin="round">{_ICONS[name]}</svg>')


# ============================================================================
# PAGE
# ============================================================================
def _ago(ts) -> str:
    if not ts:
        return "never"
    sec = max(0, int(time.time() - ts))
    return f"{sec}s ago" if sec < 90 else f"{sec // 60}m ago" if sec < 5400 else f"{sec // 3600}h ago"


def _fmt(pair: str, x) -> str:
    return "-" if x is None else f"{x:.{decimals(pair)}f}"


def mode_switch(active: str) -> str:
    cls = lambda n: ' class="on"' if n == active else ""
    return f'<div class="modesw"><a{cls("meme")} href="/dashboard">Memecoin</a><a{cls("forex")} href="/forex">Forex</a></div>'



def _t(dt: datetime) -> str:
    return f'<span class="lt" data-t="{int(dt.timestamp())}" data-utc="{dt:%H:%M}">{dt:%H:%M} UTC</span>'


def _cd(dt: datetime) -> str:
    sec = (dt - datetime.now(UTC)).total_seconds()      # the page script keeps this counting down; this is the text before it starts
    txt = "expired" if sec <= 0 else f"in {round(sec)} sec" if sec < 90 else f"in {math.ceil(sec / 60)} min"
    return f'<span class="cd" data-until="{int(dt.timestamp())}">{txt}</span>'


def _min_lot_note(s: Signal) -> str:
    if not s.lots or s.lots >= 0.0095 or not s.account_usd or s.entry is None or s.stop is None:
        return ""
    mr = min_lot_risk(s.pair, s.entry, abs(s.entry - s.stop))
    if mr is None:
        return ""
    return (f'<div class="sub">That size is below the usual 0.01 lot minimum. At 0.01 lot a stop-out would lose about ${mr:.2f} '
            f'({mr / s.account_usd * 100:.0f}% of your ${s.account_usd:.0f}). Skip it, or accept the bigger risk on purpose.</div>')


def enter_card(s: Signal) -> str:
    e, p = html.escape, s.pair
    buy = s.side == "BUY"
    f = lambda x: _fmt(p, x)
    risk_d, reward_d = abs(s.entry - s.stop), abs(s.target - s.entry)
    sym = e(p.replace("/", ""))
    rows = [("Order", f"<b>Market {s.side}</b> (instant execution, not a pending order)"),
            ("Enter at", f"<b>{f(s.entry)}</b><div class=\"sub\">Do not enter {'above' if buy else 'below'} {f(s.chase_limit)}</div>"),
            ("Stop loss", f"<b>{f(s.stop)}</b> ({dist_text(p, risk_d)} away)"),
            ("Take profit", f"<b>{f(s.target)}</b> ({dist_text(p, reward_d)} away)"),
            ("Move stop to entry", f"once price reaches <b>{f(s.breakeven)}</b> (then you cannot lose on it)")]
    if s.lots:
        rows += [("Size", f"<b>{s.lots:.3f} lots</b>"),
                 ("If it works / fails", f'<span class="buy">+${s.risk_usd * TARGET_R:.2f}</span> / <span class="sell">-${s.risk_usd:.2f}</span> (risking {s.risk_usd / s.account_usd * 100:.0f}% of ${s.account_usd:.0f})')]
    else:
        rows.append(("Size", "not calculated for this pair"))
    rows += [("Valid until", f"{_t(s.valid_until)} &middot; {_cd(s.valid_until)}"), ("Signal formed", _t(s.formed))]
    table = "".join(f"<tr><td class=\"k\">{k}</td><td>{v}</td></tr>" for k, v in rows)
    lots_txt = "your size" if not s.lots else f"{s.lots:.2f}" if s.lots >= 0.0095 else "0.01 (the smallest size, see the risk note above)"
    steps = (f"<ol><li>Open MT5, go to <b>Trade</b>, tap <b>+</b> (new order).</li><li>Pick <b>{sym}</b> (your broker may add a letter, like {sym}m).</li>"
             f"<li>Leave it on <b>Market Execution</b>.</li><li>Volume: <b>{lots_txt}</b> (round to your broker's step).</li>"
             f"<li>Set <b>Stop Loss {f(s.stop)}</b> and <b>Take Profit {f(s.target)}</b>.</li><li>Tap <b>{s.side}</b>.</li>"
             f"<li>When price reaches {f(s.breakeven)}, edit the trade and move the stop to {f(s.entry)}.</li></ol>")
    return (f'<div class="card sig {"buy" if buy else "sell"}"><div class="kick">ENTER NOW</div><div class="big">{s.side} {e(p)}</div>'
            f'<table>{table}</table>{_min_lot_note(s)}<div class="sub"><b>Why:</b> {e(s.reason)}</div>'
            f'<div class="sub"><b>Skip it if:</b> the time above has passed, or price is already {"above" if buy else "below"} {f(s.chase_limit)}, or the stop price was touched before you got in. '
            f'Your broker adds a spread, so your fill will be a little different.</div>'
            f'<details><summary>How to place it in MT5</summary>{steps}</details>'
            f'<div style="margin-top:10px"><button type="button" class="chip on" data-pair="{e(p)}">Show on chart</button></div></div>')


def ready_card(s: Signal) -> str:
    e, p = html.escape, s.pair
    buy = s.side == "BUY"
    f = lambda x: _fmt(p, x)
    lots = f"about {s.lots:.3f} lots" if s.lots else "size not calculated for this pair"
    rows = [("Fires when", f"a 15-minute candle closes <b>{'green above' if buy else 'red below'} {f(s.trigger)}</b> and RSI turns {'above 45' if buy else 'below 55'}"),
            ("Next check", f"{_t(s.next_check)} &middot; {_cd(s.next_check)}"),
            ("Idea expires", f"{_t(s.valid_until)} (if it has not fired by then, it is dropped)"),
            ("Live price", f'<span class="fxlp">{f(s.price)} (last candle close)</span>'),
            ("If it fires (estimate)", f"{s.side} near <b>{f(s.entry)}</b>, stop <b>{f(s.stop)}</b>, target <b>{f(s.target)}</b>, {lots}")]
    table = "".join(f"<tr><td class=\"k\">{k}</td><td>{v}</td></tr>" for k, v in rows)
    return (f'<div class="card ready fxready" data-pair="{e(p)}" data-side="{s.side}" data-trigger="{s.trigger}"><div class="kick">GET READY &middot; POSSIBLE {s.side}</div>'
            f'<div class="big">{e(p)}</div><table>{table}</table><div class="sub"><b>What is happening:</b> {e(s.reason)}</div>'
            f'<div class="sub"><b>This is only a heads-up. Do not enter yet.</b> Wait for the green or red <b>ENTER</b> card; it will beep and show the exact levels.</div>'
            f'<div style="margin-top:10px"><button type="button" class="chip on" data-pair="{e(p)}">Show on chart</button></div></div>')


def render_page(snap: dict | None, has_key: bool, logo: str = "") -> str:
    e = html.escape
    body = []
    boot = None
    if not has_key:
        body.append('<div class="card warn"><b>One step left: add your free data key.</b><div class="sub">1) Open twelvedata.com and make a free account.<br>'
                    '2) Copy your API key.<br>3) In Render open your service, go to Environment, add FOREX_API_KEY with that key, save. It redeploys by itself.</div></div>')
    elif snap:
        live = [p for p in snap["pairs"] if p["signal"] and p["signal"].state.startswith("ENTER")]
        soon = [p for p in snap["pairs"] if p["signal"] and p["signal"].state.startswith("GET READY")]
        boot = {"pairs": [p["pair"] for p in snap["pairs"]], "first": (live or soon or snap["pairs"])[0]["pair"], "version": snap["rev"], "max": snap["max"]}
        for p in live:
            body.append(enter_card(p["signal"]))
        for p in soon:
            body.append(ready_card(p["signal"]))
        if not live and not soon:
            body.append('<div class="card"><b>No entry right now.</b><div class="sub">Nothing to do. You get a heads-up (GET READY) before an entry, and an ENTER card with every detail when it fires. This page checks again by itself when the next 15-minute candle closes.</div></div>')
        body.append('<div class="card" id="fxchart"><div class="chips bar" id="fxbar"></div><div class="chips" id="fxtfs"></div><canvas id="fxc"></canvas>'
                    '<div class="sub" id="fxmsg">Loading chart...</div>'
                    '<div class="sub"><span style="color:#f5c542">EMA 20</span> &middot; <span style="color:#4da3ff">EMA 50</span> &middot; <span style="color:#c77dff">EMA 200</span> &middot; triangles = past signals</div></div>')
        rows = []
        for p in snap["pairs"]:
            s = p["signal"]
            state = s.state if s else "starting"
            cls = "buy" if state == "ENTER BUY" else "sell" if state == "ENTER SELL" else "ready" if state.startswith("GET READY") else ""
            note = e(p["error"]) if p["error"] else e(s.reason) if s else "first reading in a few seconds."
            rows.append(f'<div class="row" data-pair="{e(p["pair"])}" style="cursor:pointer"><div><b>{e(p["pair"])}</b><div class="sub">{note}</div></div><div class="pill {cls}">{e(state)}</div></div>')
        body.append('<div class="card"><b>Pairs</b> <span class="sub">(tap one to open its chart)</span>' + "".join(rows) + f'<div class="sub">Last full check {e(_ago(snap["last_cycle"]))}. Candles: 15 minutes.</div></div>')
        st = snap["stats"]
        wr = "n/a yet" if st["win_rate"] is None else f'{st["win_rate"]:.0f}%'
        body.append(f'<div class="card"><b>Paper record (since the server last started)</b><div class="sub">{st["total"]} signals &middot; {st["open"]} open &middot; {st["done"]} finished &middot; win rate {wr} &middot; total {st["total_r"]:+.1f} R</div>')
        if snap["trades"]:
            rr = "".join(f'<tr><td>{e(t.pair)}</td><td class="{"buy" if t.side == "BUY" else "sell"}">{t.side}</td><td>{e(t.status)}</td><td>{t.r:+.1f}R</td></tr>' for t in snap["trades"])
            body.append(f'<table><tr><th>Pair</th><th>Side</th><th>Result</th><th>R</th></tr>{rr}</table>')
        body.append('<div class="sub">R = how many times the amount you risked. +2R = hit target, -1R = hit stop. Spread and slippage are not included, so real results will be a bit worse. Do not use real money until this record has at least 30 finished signals and is positive.</div></div>')
    else:
        body.append('<div class="card warn"><b>Forex tracker is not running.</b></div>')
    script = ""
    if boot:
        script = "<script>window.FX_BOOT=" + json.dumps(boot).replace("</", "<\\/") + ";</script><script>" + CHART_JS + "</script>"
    brand_img = f'<img src="{html.escape(logo, quote=True)}" alt="" width="30" height="30" style="display:block;object-fit:cover;border-radius:9px">' if logo else ""
    icon_link = f'<link rel="icon" href="{html.escape(logo, quote=True)}"><link rel="apple-touch-icon" href="{html.escape(logo, quote=True)}">' if logo else ""
    pill = "Forex" if has_key else "Setup needed"
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta http-equiv="refresh" content="900">
{icon_link}<title>Tosh Bot - Forex</title><script>{THEME_HEAD_JS}</script><style>
:root{{color-scheme:dark;--bg:#000;--text:#f5f7fb;--mute:rgba(235,240,250,.68);--dim:rgba(235,240,250,.45);--green:#34d399;--amber:#ffb340;--red:#ff5d73;--gray:#8b97aa;--g-bg:rgba(255,255,255,.075);--g-line:rgba(255,255,255,.16);--line:rgba(255,255,255,.10);--chartbg:#0b0e13;--font:-apple-system,BlinkMacSystemFont,"SF Pro Display","SF Pro Text","Segoe UI",Roboto,Inter,Helvetica,Arial,sans-serif}}
[data-theme="light"]{{color-scheme:light;--bg:#eaf0fb;--text:#0a0f1a;--mute:rgba(10,15,26,.66);--dim:rgba(10,15,26,.42);--green:#12a979;--amber:#f08c00;--red:#ec3b55;--g-bg:rgba(255,255,255,.72);--g-line:rgba(255,255,255,.9);--line:rgba(10,15,26,.08);--chartbg:#fff}}
:root{{--fg:var(--text);--card:var(--g-bg);--ok:var(--green);--bad:var(--red);--warn:var(--amber)}}
*{{box-sizing:border-box;-webkit-tap-highlight-color:transparent}}html{{background:var(--bg);-webkit-text-size-adjust:100%}}
body{{margin:0;padding:0 16px calc(40px + env(safe-area-inset-bottom));font:16px var(--font);background:var(--bg);color:var(--text);max-width:1080px;margin:0 auto}}
.topbar{{position:sticky;top:0;z-index:20;padding-top:env(safe-area-inset-top);margin:0 -16px;padding-left:16px;padding-right:16px;background:linear-gradient(var(--bg) 72%,transparent)}}
.topin{{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:10px 0 12px}}
.brand{{display:flex;align-items:center;gap:10px;font-weight:800;font-size:18px;letter-spacing:-.025em;color:var(--text);text-decoration:none;min-width:0}}
.tools{{display:flex;align-items:center;gap:6px}}
.ibtn{{display:grid;place-items:center;width:40px;height:40px;border-radius:50%;color:var(--text);background:var(--g-bg);border:1px solid var(--g-line);padding:0;cursor:pointer;text-decoration:none;font:inherit}}.ibtn:active{{transform:scale(.92)}}
.feed{{display:inline-flex;align-items:center;gap:7px;height:40px;padding:0 13px;border-radius:999px;font-size:13px;font-weight:600;background:var(--g-bg);border:1px solid var(--g-line);white-space:nowrap}}
.dot{{width:8px;height:8px;border-radius:50%;background:var(--gray);display:inline-block;flex:none}}.dot.ok{{background:var(--green)}}.dot.warn{{background:var(--amber)}}.dot.bad{{background:var(--red)}}
@media(max-width:430px){{#fxptxt{{display:none}}.feed{{padding:0;width:40px;justify-content:center}}}}@media(max-width:340px){{.bt{{display:none}}}}
.modesw{{display:flex;gap:4px;background:var(--g-bg);border:1px solid var(--g-line);border-radius:999px;padding:4px;margin:0 0 14px}}.modesw a{{flex:1;text-align:center;padding:10px;border-radius:999px;color:var(--mute);text-decoration:none;font-weight:700}}.modesw a.on{{background:var(--text);color:var(--bg)}}
.card{{background:var(--card);border:1px solid var(--g-line);border-radius:24px;padding:16px;margin:12px 0;-webkit-backdrop-filter:blur(20px);backdrop-filter:blur(20px)}}#fxchart.card{{background:var(--chartbg)}}.warn{{border-color:var(--warn)}}
.sig.buy{{border:2px solid var(--ok)}}.sig.sell{{border:2px solid var(--bad)}}.big{{font-size:24px;font-weight:800;margin-bottom:6px}}
.sub{{color:var(--mute);font-size:13px;margin-top:6px;line-height:1.45}}table{{width:100%;border-collapse:collapse;margin-top:6px}}td,th{{text-align:left;padding:7px 2px;font-size:15px}}th{{color:var(--mute);font-size:12px}}
.buy{{color:var(--ok)}}.sell{{color:var(--bad)}}.row{{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;padding:10px 0;border-top:1px solid var(--line)}}
.pill{{font-size:12px;font-weight:700;color:var(--mute);background:rgba(128,128,128,.16);border-radius:999px;padding:4px 10px;white-space:nowrap}}.pill.buy,.pill.sell{{color:#fff;background:var(--ok)}}.pill.sell{{background:var(--bad)}}.pill.ready{{color:#000;background:var(--amber)}}
.card.ready{{border:2px solid var(--amber)}}.kick{{font-size:12px;font-weight:800;letter-spacing:.07em;color:var(--mute)}}.sig.buy .kick{{color:var(--ok)}}.sig.sell .kick{{color:var(--bad)}}.ready .kick{{color:var(--amber)}}
td.k{{color:var(--mute);font-size:13px;width:36%;vertical-align:top}}td{{vertical-align:top}}details{{margin-top:10px}}summary{{font-weight:700;cursor:pointer}}ol{{margin:8px 0 0;padding-left:20px;line-height:1.5;font-size:14px}}
.chips{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px}}.chip{{font:inherit;font-size:13px;font-weight:700;color:var(--mute);background:rgba(128,128,128,.16);border:0;border-radius:999px;padding:8px 13px}}.chip.on{{background:var(--text);color:var(--bg)}}
#fxc{{width:100%;height:440px;display:block;touch-action:none;border-radius:12px;margin-top:4px}}
.chip.big{{font-size:16px;color:var(--text)}}.live{{font-size:12px;font-weight:700;color:var(--mute);margin-left:4px;align-self:center}}.live.on{{color:var(--ok)}}.live.mid{{color:var(--warn)}}.bar{{align-items:center}}
.dim{{position:fixed;inset:0;background:rgba(0,0,0,.5);opacity:0;pointer-events:none;transition:opacity .2s;z-index:40}}.dim.open{{opacity:1;pointer-events:auto}}
.drawer{{position:fixed;left:0;right:0;bottom:0;max-height:82vh;background:var(--chartbg);border:1px solid var(--g-line);border-radius:22px 22px 0 0;padding:10px 16px calc(16px + env(safe-area-inset-bottom));transform:translateY(105%);transition:transform .25s;z-index:41;display:flex;flex-direction:column;box-shadow:0 -10px 40px rgba(0,0,0,.4)}}.drawer.open{{transform:none}}
.grab{{width:44px;height:5px;border-radius:3px;background:rgba(128,128,128,.4);margin:0 auto 10px}}.dh{{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}}
.drawer input{{width:100%;box-sizing:border-box;font:inherit;font-size:16px;color:var(--text);background:rgba(128,128,128,.16);border:0;border-radius:12px;padding:11px 14px;margin-bottom:8px}}#fxlist{{overflow:auto;flex:1}}
.sec{{color:var(--mute);font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.04em;margin:12px 0 4px}}.qrow{{display:flex;align-items:center;gap:10px;padding:11px 0;border-top:1px solid var(--line);cursor:pointer}}.ql{{flex:1;min-width:0}}.qr{{text-align:right}}.chg{{font-size:12px}}
.x{{font:inherit;font-size:22px;line-height:1;color:var(--mute);background:none;border:0;padding:4px 8px}}.chip:disabled{{opacity:.5}}
.toast{{position:fixed;left:12px;right:12px;top:calc(10px + env(safe-area-inset-top));padding:14px 16px;border-radius:16px;font-weight:800;color:#fff;z-index:60;transform:translateY(-140%);transition:transform .3s;box-shadow:0 8px 30px rgba(0,0,0,.4)}}.toast.show{{transform:none}}.toast.buy{{background:var(--ok)}}.toast.sell{{background:var(--bad)}}.toast.ready{{background:var(--amber);color:#000}}
h2{{margin:4px 0 12px}}
</style></head><body><div id="fxtoast" class="toast"></div>
<header class="topbar"><div class="topin">
<a href="/dashboard" class="brand">{brand_img}<span class="bt">Tosh Bot</span></a>
<div class="tools"><span class="feed"><span id="fxpdot" class="dot{'' if has_key else ' bad'}"></span><span id="fxptxt">{pill}</span></span>
<button class="ibtn" id="themebtn" type="button" aria-label="Switch theme"></button>
<a class="ibtn" href="/dashboard#/system" aria-label="System health">{_svg("pulse")}</a>
<a class="ibtn" href="/logout" aria-label="Sign out">{_svg("logout")}</a></div></div></header>
{mode_switch("forex")}<h2>Forex signals</h2>{"".join(body)}
<div class="sub">Signals only. You place the trade yourself. Nothing here is financial advice; most retail forex traders lose money.</div><script>{CHROME_JS}</script>{script}</body></html>"""


# ============================================================================
# WIRING
# ============================================================================
def register_forex_routes(app, auth, logo_uri: str = "") -> None:
    from contextlib import asynccontextmanager
    from fastapi import Depends, Query, Request
    from fastapi.responses import HTMLResponse, JSONResponse

    if os.getenv("FOREX_ENABLED", "true").lower() in ("0", "false", "no", "off"):
        return
    key = os.getenv("FOREX_API_KEY", "").strip()
    max_pairs = int(os.getenv("FOREX_MAX_PAIRS", str(MAX_PAIRS_DEFAULT)))
    pairs = clean_pairs(os.getenv("FOREX_PAIRS", DEFAULT_PAIRS).split(","), max_pairs) or clean_pairs(DEFAULT_PAIRS.split(","), max_pairs)
    tracker = ForexTracker(pairs, key, float(os.getenv("FOREX_ACCOUNT_USD", "100")), float(os.getenv("FOREX_RISK_PCT", "1")),
                           max_pairs, float(os.getenv("FOREX_POLL_SECONDS", "60"))) if key else None
    app.state.forex = ForexHealth(tracker, bool(key))      # lets the System health screen report on forex
    if tracker:
        inner = app.router.lifespan_context

        @asynccontextmanager
        async def with_forex(a):
            async with inner(a) as state:
                task = asyncio.create_task(tracker.run(), name="forex-tracker")
                try:
                    yield state
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        app.router.lifespan_context = with_forex

    nocache = {"Cache-Control": "no-store"}

    @app.get("/forex", include_in_schema=False, dependencies=[Depends(auth)])
    async def forex_page():
        return HTMLResponse(render_page(tracker.snapshot() if tracker else None, bool(key), logo_uri), headers=nocache)

    @app.get("/forex/data", include_in_schema=False, dependencies=[Depends(auth)])
    async def forex_data(pair: str = Query("")):
        if not tracker or pair not in tracker.pairs:
            return JSONResponse({"error": "unknown pair"}, status_code=404, headers=nocache)
        return JSONResponse(tracker.chart_payload(pair), headers=nocache)

    @app.get("/forex/live", include_in_schema=False, dependencies=[Depends(auth)])
    async def forex_live(pair: str = Query("")):
        if not tracker:
            return JSONResponse({"error": "not running"}, status_code=404, headers=nocache)
        return JSONResponse(tracker.live_payload(pair), headers=nocache)

    @app.post("/forex/watch", include_in_schema=False, dependencies=[Depends(auth)])
    async def forex_watch(request: Request):
        if not tracker or request.headers.get("x-fx") != "1":      # the custom header keeps other websites from calling this
            return JSONResponse({"error": "not allowed"}, status_code=400, headers=nocache)
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "bad request"}, status_code=400, headers=nocache)
        items = body.get("pairs") if isinstance(body, dict) else None
        if not isinstance(items, list):
            return JSONResponse({"error": "bad request"}, status_code=400, headers=nocache)
        return JSONResponse({"pairs": tracker.set_pairs([str(x) for x in items[:20]])}, headers=nocache)


# ============================================================================
# SELF-TEST (python forex.py test)
# ============================================================================
def _series(n: int, fn, start: datetime | None = None) -> list[Candle]:
    t0 = start or datetime(2026, 11, 2, 0, 0, tzinfo=UTC)
    out, prev = [], fn(0)
    for i in range(n):
        c = fn(i)
        out.append(Candle(t0 + timedelta(minutes=INTERVAL_MIN * i), prev, max(prev, c) + 0.0002, min(prev, c) - 0.0002, c))
        prev = c
    return out


def _fresh(n: int, price=lambda i: 1.1) -> tuple[list[Candle], datetime]:
    """n closed candles ending at the latest candle that has just closed by the real clock, and a 'now' 10 seconds after that close."""
    now = datetime.now(UTC)
    end_open = floor_candle(now) - timedelta(minutes=INTERVAL_MIN)
    start = end_open - timedelta(minutes=INTERVAL_MIN * (n - 1))
    return _series(n, price, start), floor_candle(now) + timedelta(seconds=1)


class ForexTests(unittest.TestCase):
    def test_indicators_basic(self):
        self.assertAlmostEqual(ema([1.0] * 30, 10)[-1], 1.0)
        self.assertEqual(rsi([float(i) for i in range(40)])[-1], 100.0)
        self.assertLess(rsi([float(-i) for i in range(40)])[-1], 1)
        c = _series(40, lambda i: 1.1 + 0.0001 * i)
        self.assertGreater(atr([x.h for x in c], [x.l for x in c], [x.c for x in c])[-1], 0)

    def test_buy_trigger_hand_built(self):
        n = 260
        cs = [Candle(datetime(2026, 11, 2, tzinfo=UTC) + timedelta(minutes=15 * i), 1.0, 1.001, 0.999, 1.0) for i in range(n)]
        ind = Indicators([1.0] * n, [1.01] * n, [1.0] * n, [50.0] * n, [0.001] * n)
        i = 250
        ind.e200 = [0.9 + 0.0001 * k for k in range(n)]       # rising 200 EMA, below the 50 EMA
        ind.e50 = [v + 0.01 for v in ind.e200]
        ind.rsi[i - 3] = 35.0                                  # dipped
        ind.rsi[i] = 50.0                                      # resumed
        cs[i] = Candle(cs[i].t, 1.0, 1.003, 0.999, 1.002)      # green candle
        ind.e20[i] = 1.0                                       # closed above EMA20
        self.assertEqual(trigger(cs, ind, i), "BUY")
        ind.rsi[i - 3] = 55.0                                  # never dipped: no setup
        self.assertIsNone(trigger(cs, ind, i))

    def test_sell_trigger_mirror(self):
        n = 260
        cs = [Candle(datetime(2026, 11, 2, tzinfo=UTC) + timedelta(minutes=15 * i), 1.0, 1.001, 0.999, 1.0) for i in range(n)]
        ind = Indicators([1.0] * n, [0.99] * n, [1.0] * n, [50.0] * n, [0.001] * n)
        i = 250
        ind.e200 = [1.1 - 0.0001 * k for k in range(n)]
        ind.e50 = [v - 0.01 for v in ind.e200]
        ind.rsi[i - 2] = 65.0
        ind.rsi[i] = 50.0
        cs[i] = Candle(cs[i].t, 1.0, 1.001, 0.997, 0.998)
        ind.e20[i] = 1.0
        self.assertEqual(trigger(cs, ind, i), "SELL")

    def test_no_future_peeking(self):
        cs = _series(300, lambda i: 1.1 + 0.00005 * i + (0.0004 if i % 17 == 0 else 0))
        ind = indicators(cs)
        a = [trigger(cs, ind, i) for i in range(210, 250)]
        ind2 = indicators(cs[:250])
        b = [trigger(cs[:250], ind2, i) for i in range(210, 250)]
        self.assertEqual(a, b)   # results for old candles do not change when newer candles exist

    def test_evaluate_geometry_and_never_crashes(self):
        import random
        rnd = random.Random(7)
        px, seen = 1.1, 0
        walk = []
        for i in range(1200):
            px += rnd.gauss(0.00003, 0.0004)
            walk.append(px)
        cs = _series(1200, lambda i: walk[i], datetime(2026, 11, 2, 0, 0, tzinfo=UTC))
        for end in range(240, 1200):
            now = cs[end - 1].t + timedelta(minutes=INTERVAL_MIN, seconds=30)
            sig, recs = evaluate("EUR/USD", cs[:end], now)
            self.assertTrue(sig.state in ("WAIT", "ENTER BUY", "ENTER SELL") or sig.state.startswith("GET READY"))
            if sig.state.startswith("ENTER"):
                seen += 1
                risk = abs(sig.entry - sig.stop)
                self.assertAlmostEqual(abs(sig.target - sig.entry), TARGET_R * risk, places=9)
                self.assertTrue(sig.stop < sig.entry < sig.target if sig.side == "BUY" else sig.target < sig.entry < sig.stop)
                self.assertTrue(sig.lots is None or sig.lots > 0)
        self.assertGreater(seen, 0)   # the setup really does fire on this data, so the geometry checks above ran

    def test_forming_candle_is_ignored(self):
        cs = _series(250, lambda i: 1.1)
        now = cs[-1].t + timedelta(minutes=5)       # last candle is still forming
        self.assertEqual(len(closed_only(cs, now)), 249)
        self.assertEqual(len(closed_only(cs, cs[-1].t + timedelta(minutes=15, seconds=5))), 250)

    def test_sizing(self):
        self.assertAlmostEqual(lots_for_risk("EUR/USD", 1.1, 0.0015, 10), 10 / (0.0015 * 100000))
        self.assertAlmostEqual(lots_for_risk("USD/JPY", 150.0, 0.30, 10), 10 / (0.30 * 100000 / 150.0))
        self.assertAlmostEqual(lots_for_risk("XAU/USD", 2000.0, 5.0, 10), 10 / (5.0 * 100))
        self.assertIsNone(lots_for_risk("EUR/GBP", 0.86, 0.001, 10))

    def test_market_hours(self):
        self.assertTrue(market_closed(datetime(2026, 11, 7, 12, tzinfo=UTC)))      # Saturday
        self.assertTrue(market_closed(datetime(2026, 11, 6, 23, tzinfo=UTC)))      # Friday night
        self.assertFalse(market_closed(datetime(2026, 11, 9, 10, tzinfo=UTC)))     # Monday
        self.assertTrue(in_session(datetime(2026, 11, 9, 10, tzinfo=UTC)))
        self.assertFalse(in_session(datetime(2026, 11, 9, 3, tzinfo=UTC)))

    def test_paper_book_win_loss_and_both_in_one_candle(self):
        book = PaperBook()
        t0 = datetime(2026, 11, 9, 9, 0, tzinfo=UTC)
        self.assertTrue(book.add("EUR/USD", ("BUY", t0, 1.1000, 1.0985, 1.1030)))
        self.assertFalse(book.add("EUR/USD", ("BUY", t0, 1.1000, 1.0985, 1.1030)))   # same setup is not added twice
        cs = [Candle(t0, 1.1, 1.1010, 1.0995, 1.1005), Candle(t0 + timedelta(minutes=15), 1.1005, 1.1035, 1.1000, 1.1030)]
        book.update("EUR/USD", cs, t0 + timedelta(hours=1))
        self.assertEqual(book.trades[0].status, "WIN"); self.assertEqual(book.trades[0].r, 2.0)
        b2 = PaperBook(); b2.add("EUR/USD", ("BUY", t0, 1.1000, 1.0985, 1.1030))
        b2.update("EUR/USD", [Candle(t0, 1.1, 1.1040, 1.0980, 1.1)], t0 + timedelta(hours=1))
        self.assertEqual(b2.trades[0].status, "LOSS")                                # stop assumed first
        self.assertEqual(b2.stats()["total_r"], -1.0)

    def test_paper_book_skips_off_session_setups(self):
        self.assertFalse(PaperBook().add("EUR/USD", ("BUY", datetime(2026, 11, 9, 2, 0, tzinfo=UTC), 1.1, 1.09, 1.12)))

    def test_parse_twelvedata(self):
        p = {"values": [{"datetime": "2026-11-09 10:15:00", "open": "1.1", "high": "1.2", "low": "1.0", "close": "1.15"},
                        {"datetime": "2026-11-09 10:00:00", "open": "1.0", "high": "1.1", "low": "0.9", "close": "1.1"}, {"bad": 1}]}
        cs = parse_twelvedata(p)
        self.assertEqual([c.c for c in cs], [1.1, 1.15])                             # oldest first, bad row skipped
        with self.assertRaises(RuntimeError):
            parse_twelvedata({"status": "error", "message": "limit reached"})

    def test_tracker_process_and_page(self):
        tr = ForexTracker(["EUR/USD"], "SECRETKEY")
        cs = _series(260, lambda i: 1.1 + 0.00002 * i)
        sig = tr.process("EUR/USD", cs, cs[-1].t + timedelta(minutes=15, seconds=10))
        page = render_page(tr.snapshot(), True)
        self.assertIn("EUR/USD", page); self.assertIn("Paper record", page); self.assertNotIn("SECRETKEY", page)
        self.assertIn("One step left", render_page(None, False))
        self.assertIn('href="/dashboard"', page)
        self.assertTrue(sig.state in ("WAIT", "ENTER BUY", "ENTER SELL") or sig.state.startswith("GET READY"))

    def test_chart_payload_and_page(self):
        tr = ForexTracker(["EUR/USD", "XAU/USD"], "SECRETKEY")
        cs = _series(520, lambda i: 1.1 + 0.00002 * i)
        tr.process("EUR/USD", cs, cs[-1].t + timedelta(minutes=15, seconds=10))
        d = tr.chart_payload("EUR/USD")
        self.assertEqual(d["pair"], "EUR/USD"); self.assertEqual(len(d["candles"]), 520); self.assertEqual(d["dp"], 5)
        self.assertEqual(d["candles"][0][0] + 900, d["candles"][1][0]); json.dumps(d)          # plain JSON, oldest first
        self.assertEqual(tr.chart_payload("XAU/USD")["candles"], [])                            # no data yet is fine
        tr.last_cycle = time.time()
        page = render_page(tr.snapshot(), True)
        self.assertIn('id="fxc"', page); self.assertIn("window.FX_BOOT=", page); self.assertIn('data-pair="XAU/USD"', page)
        self.assertNotIn("SECRETKEY", page); self.assertNotIn('id="fxc"', render_page(None, False))

    def test_forming_candle_not_in_chart(self):
        tr = ForexTracker(["EUR/USD"], "K")
        cs = _series(250, lambda i: 1.1)
        tr.process("EUR/USD", cs, cs[-1].t + timedelta(minutes=5))      # last candle still forming
        self.assertEqual(len(tr.chart_payload("EUR/USD")["candles"]), 249)

    def test_chart_javascript_runs(self):
        import shutil, subprocess, tempfile
        node = shutil.which("node")
        if not node:
            self.skipTest("node is not installed here")
        with tempfile.TemporaryDirectory() as d:
            with open(d + "/chart.js", "w") as fh:
                fh.write(CHART_JS)
            with open(d + "/t.js", "w") as fh:
                fh.write("""
const api = require('./chart.js'); const calls = []; const st = {};
const ctx = new Proxy({}, {get(t,k){ if (k in st) return st[k]; return (...a)=>{calls.push(k);}; }, set(t,k,v){ st[k]=v; return true; }});
const L = {}; const canvas = {getContext:()=>ctx, getBoundingClientRect:()=>({width:360,height:440}), addEventListener:(n,f)=>{L[n]=f;}};
global.window = {devicePixelRatio:2, addEventListener(){}};
const cs = []; let p = 1.1; for (let i=0;i<500;i++){ const o=p; p+=Math.sin(i/9)*0.0006; cs.push([1760000000+i*900,o,Math.max(o,p)+0.0002,Math.min(o,p)-0.0002,p]); }
const ch = api.createChart(canvas, ()=>({bg:'#111',fg:'#fff',mute:'#999',line:'#333',up:'#0f0',down:'#f00',blue:'#08f'}));
ch.setTf(60); ch.resize();
ch.setData({pair:'EUR/USD',dp:5,state:'ENTER BUY',candles:cs,signal:{side:'BUY',entry:1.1,stop:1.098,target:1.104,formed:cs[499][0]+900},trades:[{side:'BUY',t:cs[490][0]+900,status:'OPEN'},{side:'SELL',t:cs[300][0]+900,status:'WIN'}]});
const ev = (x,y,id,type)=>({clientX:x,clientY:y,pointerId:id||1,pointerType:type||'touch',preventDefault(){},});
canvas.getBoundingClientRect = ()=>({left:0,top:0,width:360,height:440});
L.pointerdown(ev(100,200)); L.pointermove(ev(160,200)); L.pointerup(ev(160,200));            // drag
L.pointerdown(ev(100,200,1)); L.pointerdown(ev(200,200,2)); L.pointermove(ev(60,200,1)); L.pointerup(ev(60,200,1)); L.pointerup(ev(200,200,2));   // pinch
L.pointerdown(ev(120,220)); L.pointerup(ev(120,220));                                         // tap -> crosshair
L.wheel(ev(0,0,1,'mouse')); L.dblclick();
ch.setData({pair:'EUR/USD',dp:5,state:'GET READY BUY',candles:cs,signal:null,ready:{side:'BUY',trigger:1.1},trades:[]}); ch.setLive([cs[499][0]+900, 1.1, 1.101, 1.099, 1.1005]); ch.setLive(null); ch.setTf(240); ch.setEma(false); ch.reset(); ch.setData({pair:'EUR/USD',dp:5,state:'WAIT',candles:cs.slice(0,40),signal:null,trades:[]});   // short history must not crash
ch.setData({pair:'EUR/USD',dp:5,state:'WAIT',candles:[],signal:null,trades:[]});
const agg = api.aggregate(cs, 60); if (agg.length < 100 || agg.length > 140) throw new Error('aggregate '+agg.length);
const h = cs.filter(c=>Math.floor(c[0]/3600)===Math.floor(cs[10][0]/3600)); const a1 = agg.find(c=>c[0]===Math.floor(cs[10][0]/3600)*3600);
if (a1[2] !== Math.max(...h.map(c=>c[2])) || a1[3] !== Math.min(...h.map(c=>c[3])) || a1[4] !== h[h.length-1][4] || a1[1] !== h[0][1]) throw new Error('bad OHLC aggregation');
if (calls.length < 500) throw new Error('chart drew too little: '+calls.length);
console.log('ok');
""")
            r = subprocess.run([node, d + "/t.js"], capture_output=True, text=True, timeout=30)
            self.assertEqual(r.stdout.strip(), "ok", r.stderr[-800:])

    def test_clean_pairs_and_watchlist(self):
        self.assertEqual(clean_pairs([" eur/usd", "EUR/USD", "bad", "GBP/JPY", "TOOLONG/USD", "XAU/USD"], 3), ["EUR/USD", "GBP/JPY", "XAU/USD"])
        tr = ForexTracker(["EUR/USD", "GBP/USD"], "K", max_pairs=3)
        r0 = tr.rev
        self.assertEqual(tr.set_pairs(["EUR/USD", "AUD/USD", "NZD/USD", "USD/CAD"]), ["EUR/USD", "AUD/USD", "NZD/USD"])   # limited to max
        self.assertGreater(tr.rev, r0); self.assertTrue(tr.wake.is_set())
        self.assertTrue(any(m["action"] == "subscribe" and "AUD/USD" in m["params"]["symbols"] for m in tr.outbox))
        self.assertTrue(any(m["action"] == "unsubscribe" and "GBP/USD" in m["params"]["symbols"] for m in tr.outbox))
        self.assertEqual(tr.set_pairs(["junk"]), ["EUR/USD", "AUD/USD", "NZD/USD"])                                          # nothing valid: unchanged

    def test_live_ticks_build_the_forming_candle(self):
        tr = ForexTracker(["EUR/USD"], "K")
        cs, now = _fresh(260, lambda i: 1.1 + 0.00002 * i)
        tr.process("EUR/USD", cs, now)
        for px in (1.1060, 1.1066, 1.1055, 1.1062):
            tr.on_tick("EUR/USD", px)
        fc = tr.forming["EUR/USD"]
        self.assertEqual(fc[1], cs[-1].c)                                        # opens at the previous close (no gap)
        self.assertEqual((fc[2], fc[3], fc[4]), (max(cs[-1].c, 1.1066), min(cs[-1].c, 1.1055), 1.1062))
        p = tr.live_payload("EUR/USD")
        self.assertEqual(p["candle"][0], fc[0]); self.assertEqual(p["quotes"]["EUR/USD"]["p"], 1.1062)
        self.assertIn(tr.live_mode(), ("stream", "closed"))
        tr.on_tick("EUR/USD", 1.2, time.time() + 905)                            # a tick stamped in the next candle: starts it from the last price
        self.assertEqual(tr.forming["EUR/USD"][1], 1.1062)
        tr.on_tick("GBP/USD", 1.3); self.assertNotIn("GBP/USD", tr.ticks)       # not on the watch list: ignored
        tr.on_tick("EUR/USD", 0); tr.on_tick("EUR/USD", -1.0); self.assertEqual(tr.ticks["EUR/USD"][0], 1.2)

    def test_forming_candle_hidden_once_the_real_one_arrives(self):
        tr = ForexTracker(["EUR/USD"], "K")
        cs, now = _fresh(260)
        tr.process("EUR/USD", cs, now)
        tr.on_tick("EUR/USD", 1.1)
        self.assertIsNotNone(tr.live_payload("EUR/USD")["candle"])
        nxt = cs + [Candle(floor_candle(now), 1.1, 1.1, 1.1, 1.1)]               # the provider publishes the candle we were building
        tr.process("EUR/USD", nxt, floor_candle(now) + timedelta(minutes=INTERVAL_MIN, seconds=1))
        self.assertIsNone(tr.live_payload("EUR/USD")["candle"])

    def test_stream_messages(self):
        tr = ForexTracker(["EUR/USD"], "SECRET")
        tr.on_message('{"event":"price","symbol":"EUR/USD","price":1.1234,"timestamp":%d}' % int(time.time()))
        self.assertEqual(tr.ticks["EUR/USD"][0], 1.1234); self.assertTrue(tr.stream_alive())
        tr.on_message('{"event":"subscribe-status","success":[],"fails":[{"symbol":"EUR/USD"}]}')
        self.assertIn("EUR/USD", tr.stream_error)
        tr.on_message('{"event":"error","message":"bad key SECRET"}'); self.assertNotIn("SECRET", tr.stream_error)
        tr.on_message("not json"); tr.on_message("[1,2]"); tr.on_message('{"event":"price","symbol":"EUR/USD","price":"x"}')   # junk is ignored

    def test_stale_candle_detection_and_retry(self):
        cs = _series(260, lambda i: 1.1)
        last_open = cs[-1].t
        now = last_open + timedelta(minutes=30, seconds=5)               # candle after `last` has closed but is missing: stale
        self.assertTrue(is_stale(cs, now))
        self.assertFalse(is_stale(cs, last_open + timedelta(minutes=15, seconds=5)))    # up to date
        self.assertFalse(is_stale(cs, last_open + timedelta(hours=5)))   # old data = market break, not a late candle
        tr = ForexTracker(["EUR/USD"], "K")
        calls = []

        async def fake(pair):
            calls.append(1)
            return cs
        tr.fetch = fake
        import unittest.mock as um
        real_sleep = asyncio.sleep
        with um.patch(__name__ + ".market_closed", return_value=False), um.patch(__name__ + ".is_stale", return_value=True), um.patch(__name__ + ".asyncio.sleep", new=lambda s: real_sleep(0)):
            self.assertEqual(asyncio.run(tr.cycle(["EUR/USD"])), ["EUR/USD"])
            self.assertEqual(asyncio.run(tr.cycle([])), [])
        self.assertEqual(len(calls), 1)

    def test_weekend_cycle_marks_closed_once(self):
        import unittest.mock as um
        tr = ForexTracker(["EUR/USD"], "K")
        with um.patch(__name__ + ".market_closed", return_value=True):
            asyncio.run(tr.cycle()); r = tr.rev
            asyncio.run(tr.cycle())
        self.assertEqual(tr.signals["EUR/USD"].state, "CLOSED"); self.assertEqual(tr.rev, r)      # no page reload every 15 minutes

    def test_watch_reason_texts_and_silver_sizing(self):
        n = 260
        ind = Indicators([1.0] * n, [1.01] * n, [1.0] * n, [50.0] * n, [0.001] * n)
        ind.e200 = [0.9 + 0.0001 * k for k in range(n)]
        ind.e50 = [v + 0.01 for v in ind.e200]
        self.assertIn("Waiting for a pullback", watch_reason(ind, 250))
        ind.rsi[248] = 33.0
        self.assertIn("pulled back", watch_reason(ind, 250))
        ind.e50 = [v - 0.02 for v in ind.e200]; ind.e200 = [1.1 - 0.0001 * k for k in range(n)]; ind.e50 = [v - 0.01 for v in ind.e200]
        self.assertIn("Downtrend", watch_reason(ind, 250))
        ind.e50 = [1.0] * n; ind.e200 = [1.0] * n
        self.assertIn("No clear trend", watch_reason(ind, 250))
        self.assertAlmostEqual(lots_for_risk("XAG/USD", 30.0, 0.5, 10), 10 / (0.5 * 5000)); self.assertEqual(decimals("XAG/USD"), 3)

    def test_page_has_dashboard_top_bar_and_theme(self):
        tr = ForexTracker(["EUR/USD"], "K")
        cs, now = _fresh(260)
        tr.process("EUR/USD", cs, now)
        page = render_page(tr.snapshot(), True, "data:image/jpeg;base64,AAAA")
        for needle in ('class="topbar"', 'href="/dashboard#/system"', 'href="/logout"', 'id="themebtn"', 'tosh-theme', 'data:image/jpeg;base64,AAAA', 'data-theme="light"', 'id="fxptxt"'):
            self.assertIn(needle, page)
        nokey = render_page(None, False)
        self.assertIn("Setup needed", nokey); self.assertIn('class="topbar"', nokey); self.assertIn("One step left", nokey)
        self.assertNotIn("<script>window.FX_BOOT", nokey)

    def test_health_for_system_screen(self):
        tr = ForexTracker(["EUR/USD", "GBP/USD"], "K")
        cs, now = _fresh(260)
        tr.process("EUR/USD", cs, now)
        tr.errors["GBP/USD"] = "HTTPStatusError (HTTP 429): limit"
        h = ForexHealth(tr, True).health()
        self.assertTrue(h["enabled"] and h["key_set"]); self.assertEqual(h["pairs"], ["EUR/USD", "GBP/USD"])
        self.assertIn("GBP/USD", h["errors"]); self.assertEqual(h["candles"]["EUR/USD"], 260); self.assertIn(h["mode"], ("delayed", "closed", "stream", "poll"))
        json.dumps(h)
        self.assertEqual(ForexHealth(None, False).health(), {"enabled": True, "key_set": False})

    def test_free_plan_only_some_symbols_stream_so_the_viewed_pair_is_polled(self):
        import unittest.mock as um
        tr = ForexTracker(["EUR/USD", "XAU/USD"], "K", poll_seconds=15)
        cs, now = _fresh(260)
        tr.process("XAU/USD", cs, now)
        tr.on_tick("EUR/USD", 1.1, source="stream")                   # EUR/USD streams, gold does not (not in the free trial list)
        with um.patch(__name__ + ".market_closed", return_value=False):
            self.assertEqual(tr.live_mode("EUR/USD"), "stream"); self.assertEqual(tr.live_mode("XAU/USD"), "delayed")
            tr.live_payload("XAU/USD")                                 # you open the gold chart

            class R:
                def json(self): return {"price": "2650.50"}

            class C:
                calls = 0
                async def get(self, url, params=None):
                    C.calls += 1; return R()
            tr.client = C()
            self.assertTrue(asyncio.run(tr.poll_once()))
            self.assertEqual(tr.ticks["XAU/USD"][0], 2650.5); self.assertEqual(tr.live_mode("XAU/USD"), "poll")
            tr.live_payload("EUR/USD")                                 # a streaming pair is never polled
            self.assertFalse(asyncio.run(tr.poll_once())); self.assertEqual(C.calls, 1)

    def test_polling_stops_before_the_daily_allowance_runs_out(self):
        import unittest.mock as um
        tr = ForexTracker(["EUR/USD"], "K")
        tr.live_payload("EUR/USD")

        class C:
            async def get(self, url, params=None):
                raise AssertionError("must not call the provider")
        tr.client = C()
        tr.credits_used = CREDIT_SOFT_CAP
        with um.patch(__name__ + ".market_closed", return_value=False):
            self.assertFalse(asyncio.run(tr.poll_once()))
        tr.credits_day = datetime.now(UTC).date() - timedelta(days=1)    # a new UTC day resets the count
        self.assertEqual(tr.spend(0), 0)

    def _cards_tracker(self):
        tr = ForexTracker(["EUR/USD", "XAU/USD", "GBP/USD"], "K")
        now = datetime.now(UTC)
        formed = floor_candle(now)
        tr.signals["EUR/USD"] = Signal("EUR/USD", "ENTER BUY", "Uptrend: test reason.", price=1.1, side="BUY", entry=1.1000, stop=1.0980, target=1.1040, atr=0.0013, formed=formed,
                                       lots=0.05, risk_usd=1.0, account_usd=100.0, valid_until=formed + timedelta(minutes=30), chase_limit=1.1007, breakeven=1.1020, rsi=55.0)
        tr.signals["XAU/USD"] = Signal("XAU/USD", "ENTER SELL", "Downtrend: test.", price=2650.0, side="SELL", entry=2650.0, stop=2654.0, target=2642.0, atr=2.6, formed=formed,
                                       lots=0.0025, risk_usd=1.0, account_usd=100.0, valid_until=formed + timedelta(minutes=30), chase_limit=2648.7, breakeven=2646.0)
        tr.signals["GBP/USD"] = Signal("GBP/USD", "GET READY SELL", "Downtrend and bounce.", price=1.3, side="SELL", entry=1.2990, stop=1.3010, target=1.2950, atr=0.0013,
                                       lots=0.05, risk_usd=1.0, account_usd=100.0, valid_until=formed + timedelta(minutes=45), trigger=1.2995, next_check=formed + timedelta(minutes=15), rsi=50.0)
        tr.last_cycle = time.time()
        return tr

    def test_enter_card_has_every_detail(self):
        page = render_page(self._cards_tracker().snapshot(), True)
        for needle in ("ENTER NOW", "Market BUY", "Do not enter above 1.10070", "Stop loss", "20.0 pips away", "40.0 pips away", "Move stop to entry", "1.10200",
                       "0.050 lots", "+$2.00", "-$1.00", "Valid until", "Signal formed", "Skip it if", "How to place it in MT5", "Stop Loss 1.09800", "EURUSDm",
                       "Do not enter below 2648.70", "$4.00 away", "$8.00 away"):
            self.assertIn(needle, page, needle)
        self.assertIn("below the usual 0.01 lot minimum", page)               # gold at $100: the smallest lot risks more than 1%
        self.assertIn("0.01 (the smallest size", page)
        self.assertIn('class="lt"', page); self.assertIn('class="cd"', page); self.assertIn(" min", page.split('class="cd"')[1][:80])

    def test_get_ready_card_and_pills(self):
        page = render_page(self._cards_tracker().snapshot(), True)
        for needle in ("GET READY", "POSSIBLE SELL", "Fires when", "red below 1.29950", "Next check", "Idea expires", "If it fires (estimate)", "Do not enter yet",
                       'data-trigger="1.2995"', 'class="pill ready"', 'data-side="SELL"'):
            self.assertIn(needle, page, needle)
        nothing = ForexTracker(["EUR/USD"], "K"); nothing.last_cycle = time.time()
        self.assertIn("heads-up (GET READY)", render_page(nothing.snapshot(), True))

    def test_distance_and_min_lot_helpers(self):
        self.assertEqual(dist_text("EUR/USD", 0.0020), "20.0 pips"); self.assertEqual(dist_text("USD/JPY", 0.30), "30.0 pips")
        self.assertEqual(dist_text("XAU/USD", -4.0), "$4.00"); self.assertEqual(dist_text("XAG/USD", 0.5), "$0.500")
        self.assertAlmostEqual(min_lot_risk("XAU/USD", 2650.0, 4.0), 0.01 * 4.0 * 100); self.assertIsNone(min_lot_risk("EUR/GBP", 0.86, 0.001))

    def test_enter_signal_fields_and_get_ready_come_first(self):
        import random
        rnd = random.Random(7); px = 1.1; walk = []
        for _ in range(2500):
            px += rnd.gauss(0.00003, 0.0004); walk.append(px)
        cs = _series(2500, lambda i: walk[i], datetime(2026, 11, 2, 0, 0, tzinfo=UTC))
        seen_ready = seen_enter = 0
        ready_before_enter = False; prev = None
        for end in range(260, 2500):
            sig, _ = evaluate("EUR/USD", cs[:end], cs[end - 1].t + timedelta(minutes=15, seconds=10), 100.0, 1.0)
            if sig.state.startswith("GET READY"):
                seen_ready += 1
                self.assertIsNotNone(sig.trigger); self.assertGreater(sig.valid_until, sig.next_check - timedelta(minutes=15)); self.assertGreaterEqual(sig.valid_until, sig.next_check)
                self.assertTrue(sig.stop < sig.entry < sig.target if sig.side == "BUY" else sig.target < sig.entry < sig.stop)
            if sig.state.startswith("ENTER"):
                seen_enter += 1
                self.assertEqual(sig.valid_until, sig.formed + timedelta(minutes=30))
                risk = abs(sig.entry - sig.stop)
                if sig.side == "BUY":
                    self.assertTrue(sig.entry < sig.chase_limit and abs(sig.breakeven - (sig.entry + risk)) < 1e-12)
                else:
                    self.assertTrue(sig.entry > sig.chase_limit and abs(sig.breakeven - (sig.entry - risk)) < 1e-12)
                if prev and prev.startswith("GET READY"):
                    ready_before_enter = True
            prev = sig.state
        self.assertGreater(seen_ready, 0); self.assertGreater(seen_enter, 0); self.assertTrue(ready_before_enter)    # the heads-up really arrives before an entry

    def test_health_lists_pairs_getting_ready(self):
        h = ForexHealth(self._cards_tracker(), True).health()
        self.assertEqual(h["ready_now"], ["GBP/USD"]); self.assertEqual(h["entries_now"], ["EUR/USD", "XAU/USD"])

    def test_next_candle_timer(self):
        s = ForexTracker.seconds_to_next_candle(datetime(2026, 11, 9, 10, 14, 50, tzinfo=UTC))
        self.assertAlmostEqual(s, 16.0, places=0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        unittest.main(argv=["forex"], verbosity=1)
    else:
        print(__doc__)
