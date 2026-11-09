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
import os
import sys
import time
import unittest
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

INTERVAL_MIN = 15
CANDLES_WANTED = 300
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
    return 100.0 if pair.startswith("XAU/") else 100000.0


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
    return 2 if pair.startswith("XAU/") else 3 if pair.endswith("/JPY") else 5


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
    base = Signal(pair, "WAIT", "no setup right now: waiting for a trend, a pullback, then the trend to resume.", price=price, atr=a_now)
    live = [f for f in found if f[5] >= n - MAX_SIGNAL_AGE]
    records = [(f[0], f[1], f[2], f[3], f[4]) for f in found]
    if not live:
        return base, records
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
    word = "uptrend" if side == "BUY" else "downtrend"
    return Signal(pair, f"ENTER {side}", f"{word}, pulled back, then resumed. Enter near {e:.{decimals(pair)}f} now; the setup is only valid for about 30 minutes.",
                  price=price, side=side, entry=e, stop=stop, target=tgt, atr=a, formed=formed, lots=lots, risk_usd=risk_usd), records


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


class ForexTracker:
    def __init__(self, pairs: list[str], api_key: str, account_usd: float = 100.0, risk_pct: float = 1.0) -> None:
        self.pairs, self.api_key = pairs, api_key
        self.account_usd, self.risk_pct = account_usd, risk_pct
        self.signals: dict[str, Signal] = {}
        self.book = PaperBook()
        self.errors: dict[str, str] = {}
        self.last_cycle: float | None = None
        self.started = time.time()
        self.client = None

    def process(self, pair: str, cs: list[Candle], now: datetime) -> Signal:
        sig, records = evaluate(pair, cs, now, self.account_usd, self.risk_pct)
        for rec in records:
            self.book.add(pair, rec)
        self.book.update(pair, cs, now)
        self.signals[pair] = sig
        return sig

    async def fetch(self, pair: str) -> list[Candle]:
        r = await self.client.get("https://api.twelvedata.com/time_series", params={
            "symbol": pair, "interval": f"{INTERVAL_MIN}min", "outputsize": CANDLES_WANTED, "timezone": "UTC", "apikey": self.api_key})
        r.raise_for_status()
        return parse_twelvedata(r.json())

    async def cycle(self) -> None:
        now = datetime.now(UTC)
        if market_closed(now):
            for p in self.pairs:
                self.signals[p] = Signal(p, "CLOSED", "the forex market is closed for the weekend. It reopens Sunday 22:00 UTC.")
            self.last_cycle = time.time()
            return
        for p in self.pairs:
            try:
                self.process(p, await self.fetch(p), datetime.now(UTC))
                self.errors.pop(p, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                self.errors[p] = (f"{type(exc).__name__}" + (f" (HTTP {status})" if status else "") + f": {exc}")[:200].replace(self.api_key, "<key>")
            await asyncio.sleep(1.5)   # stay under the provider's per-minute limit
        self.last_cycle = time.time()

    @staticmethod
    def seconds_to_next_candle(now: datetime | None = None) -> float:
        now = now or datetime.now(UTC)
        mins = INTERVAL_MIN - (now.minute % INTERVAL_MIN)
        nxt = (now.replace(second=0, microsecond=0) + timedelta(minutes=mins)) + timedelta(seconds=20)
        return max(5.0, (nxt - now).total_seconds())

    async def run(self) -> None:
        import httpx
        async with httpx.AsyncClient(timeout=25) as c:
            self.client = c
            while True:
                try:
                    await self.cycle()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                await asyncio.sleep(self.seconds_to_next_candle())

    def snapshot(self) -> dict:
        return {"pairs": [{"pair": p, "signal": self.signals.get(p), "error": self.errors.get(p)} for p in self.pairs],
                "stats": self.book.stats(), "trades": list(reversed(self.book.trades[-15:])), "last_cycle": self.last_cycle,
                "started": self.started, "account": self.account_usd, "risk_pct": self.risk_pct}


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


def render_page(snap: dict | None, has_key: bool) -> str:
    e = html.escape
    body = []
    if not has_key:
        body.append('<div class="card warn"><b>One step left: add your free data key.</b><div class="sub">1) Open twelvedata.com and make a free account.<br>'
                    '2) Copy your API key.<br>3) In Render open your service, go to Environment, add FOREX_API_KEY with that key, save. It redeploys by itself.</div></div>')
    elif snap:
        live = [p for p in snap["pairs"] if p["signal"] and p["signal"].state.startswith("ENTER")]
        for p in live:
            s = p["signal"]
            cls = "buy" if s.side == "BUY" else "sell"
            lots = f"{s.lots:.3f} lots" if s.lots else "size: n/a for this pair"
            body.append(f'<div class="card sig {cls}"><div class="big">{e(s.state)} {e(s.pair)}</div>'
                        f'<table><tr><td>Enter near</td><td><b>{_fmt(s.pair, s.entry)}</b></td></tr><tr><td>Stop loss</td><td><b>{_fmt(s.pair, s.stop)}</b></td></tr>'
                        f'<tr><td>Take profit</td><td><b>{_fmt(s.pair, s.target)}</b></td></tr><tr><td>Size</td><td><b>{e(lots)}</b> (risks about ${s.risk_usd:.2f})</td></tr></table>'
                        f'<div class="sub">{e(s.reason)} Set the stop and target in your broker when you enter.</div></div>')
        if not live:
            body.append('<div class="card"><b>No entry right now.</b><div class="sub">Nothing to do. This page checks again by itself when the next 15-minute candle closes.</div></div>')
        rows = []
        for p in snap["pairs"]:
            s = p["signal"]
            state = s.state if s else "starting"
            cls = "buy" if state == "ENTER BUY" else "sell" if state == "ENTER SELL" else ""
            note = e(p["error"]) if p["error"] else e(s.reason) if s else "first reading in a few seconds."
            rows.append(f'<div class="row"><div><b>{e(p["pair"])}</b><div class="sub">{note}</div></div><div class="pill {cls}">{e(state)}</div></div>')
        body.append('<div class="card"><b>Pairs</b>' + "".join(rows) + f'<div class="sub">Last full check {e(_ago(snap["last_cycle"]))}. Candles: 15 minutes.</div></div>')
        st = snap["stats"]
        wr = "n/a yet" if st["win_rate"] is None else f'{st["win_rate"]:.0f}%'
        body.append(f'<div class="card"><b>Paper record (since the server last started)</b><div class="sub">{st["total"]} signals &middot; {st["open"]} open &middot; {st["done"]} finished &middot; win rate {wr} &middot; total {st["total_r"]:+.1f} R</div>')
        if snap["trades"]:
            rr = "".join(f'<tr><td>{e(t.pair)}</td><td class="{"buy" if t.side == "BUY" else "sell"}">{t.side}</td><td>{e(t.status)}</td><td>{t.r:+.1f}R</td></tr>' for t in snap["trades"])
            body.append(f'<table><tr><th>Pair</th><th>Side</th><th>Result</th><th>R</th></tr>{rr}</table>')
        body.append('<div class="sub">R = how many times the amount you risked. +2R = hit target, -1R = hit stop. Spread and slippage are not included, so real results will be a bit worse. Do not use real money until this record has at least 30 finished signals and is positive.</div></div>')
    else:
        body.append('<div class="card warn"><b>Forex tracker is not running.</b></div>')
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta http-equiv="refresh" content="30">
<title>Tosh Bot - Forex</title><style>
:root{{--bg:#0b0b0c;--card:#17181a;--fg:#f2f2f2;--mute:#9a9aa0;--ok:#3ecf8e;--bad:#ff6b6b;--warn:#e9b44c;--line:#2a2c30}}
@media(prefers-color-scheme:light){{:root{{--bg:#f4f4f6;--card:#fff;--fg:#111;--mute:#666;--line:#e3e3e7}}}}
body{{margin:0;padding:16px 16px calc(40px + env(safe-area-inset-bottom));padding-top:calc(16px + env(safe-area-inset-top));font:16px -apple-system,system-ui,sans-serif;background:var(--bg);color:var(--fg)}}
.modesw{{display:flex;gap:4px;background:var(--card);border-radius:999px;padding:4px;margin:0 0 14px}}.modesw a{{flex:1;text-align:center;padding:10px;border-radius:999px;color:var(--mute);text-decoration:none;font-weight:700}}.modesw a.on{{background:var(--fg);color:var(--bg)}}
.card{{background:var(--card);border-radius:18px;padding:16px;margin:12px 0}}.warn{{border:1px solid var(--warn)}}
.sig.buy{{border:2px solid var(--ok)}}.sig.sell{{border:2px solid var(--bad)}}.big{{font-size:24px;font-weight:800;margin-bottom:6px}}
.sub{{color:var(--mute);font-size:13px;margin-top:6px;line-height:1.45}}table{{width:100%;border-collapse:collapse;margin-top:6px}}td,th{{text-align:left;padding:7px 2px;font-size:15px}}th{{color:var(--mute);font-size:12px}}
.buy{{color:var(--ok)}}.sell{{color:var(--bad)}}.row{{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;padding:10px 0;border-top:1px solid var(--line)}}
.pill{{font-size:12px;font-weight:700;color:var(--mute);background:rgba(128,128,128,.16);border-radius:999px;padding:4px 10px;white-space:nowrap}}.pill.buy,.pill.sell{{color:#fff;background:var(--ok)}}.pill.sell{{background:var(--bad)}}
h2{{margin:4px 0 12px}}
</style></head><body>{mode_switch("forex")}<h2>Forex signals</h2>{"".join(body)}
<div class="sub">Signals only. You place the trade yourself. Nothing here is financial advice; most retail forex traders lose money.</div></body></html>"""


# ============================================================================
# WIRING
# ============================================================================
def register_forex_routes(app, auth) -> None:
    from contextlib import asynccontextmanager
    from fastapi import Depends
    from fastapi.responses import HTMLResponse

    if os.getenv("FOREX_ENABLED", "true").lower() in ("0", "false", "no", "off"):
        return
    key = os.getenv("FOREX_API_KEY", "").strip()
    pairs = [p.strip().upper() for p in os.getenv("FOREX_PAIRS", DEFAULT_PAIRS).split(",") if p.strip()]
    tracker = ForexTracker(pairs, key, float(os.getenv("FOREX_ACCOUNT_USD", "100")), float(os.getenv("FOREX_RISK_PCT", "1"))) if key else None
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

    @app.get("/forex", include_in_schema=False, dependencies=[Depends(auth)])
    async def forex_page():
        return HTMLResponse(render_page(tracker.snapshot() if tracker else None, bool(key)), headers={"Cache-Control": "no-store"})


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
            self.assertIn(sig.state, ("WAIT", "ENTER BUY", "ENTER SELL"))
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
        self.assertIn(sig.state, ("WAIT", "ENTER BUY", "ENTER SELL"))

    def test_next_candle_timer(self):
        s = ForexTracker.seconds_to_next_candle(datetime(2026, 11, 9, 10, 14, 50, tzinfo=UTC))
        self.assertAlmostEqual(s, 30.0, places=0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        unittest.main(argv=["forex"], verbosity=1)
    else:
        print(__doc__)
