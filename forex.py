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
import os
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
        self.candles: dict[str, list[Candle]] = {}
        self.errors: dict[str, str] = {}
        self.last_cycle: float | None = None
        self.started = time.time()
        self.client = None

    def process(self, pair: str, cs: list[Candle], now: datetime) -> Signal:
        sig, records = evaluate(pair, cs, now, self.account_usd, self.risk_pct)
        self.candles[pair] = closed_only(cs, now)
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


    def chart_payload(self, pair: str) -> dict:
        """Everything the chart needs for one pair, as plain JSON (times are epoch seconds, UTC)."""
        cs = self.candles.get(pair, [])
        sig = self.signals.get(pair)
        live = None
        if sig and sig.state.startswith("ENTER") and sig.formed:
            live = {"side": sig.side, "entry": sig.entry, "stop": sig.stop, "target": sig.target, "formed": int(sig.formed.timestamp())}
        trades = [{"side": t.side, "t": int(t.opened.timestamp()), "entry": t.entry, "stop": t.stop, "target": t.target, "status": t.status}
                  for t in self.book.trades if t.pair == pair][-40:]
        return {"pair": pair, "dp": decimals(pair), "state": sig.state if sig else "starting", "version": int(self.last_cycle or 0),
                "candles": [[int(c.t.timestamp()), c.o, c.h, c.l, c.c] for c in cs], "signal": live, "trades": trades}

    def snapshot(self) -> dict:
        return {"pairs": [{"pair": p, "signal": self.signals.get(p), "error": self.errors.get(p)} for p in self.pairs],
                "stats": self.book.stats(), "trades": list(reversed(self.book.trades[-15:])), "last_cycle": self.last_cycle,
                "started": self.started, "account": self.account_usd, "risk_pct": self.risk_pct}


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
  var S = {data: null, tf: 15, n: 80, off: -6, ema: true, cross: null, W: 320, H: 420, dpr: 1};
  var cs = [], e20 = [], e50 = [], e200 = [], idx = {};
  var PAD = {l: 6, r: 66, t: 46, b: 24}, DEFOFF = -6;   // a few empty candles on the right, like MT5

  function rebuild() {
    cs = S.data ? aggregate(S.data.candles, S.tf) : [];
    var cl = cs.map(function (c) { return c[4]; });
    e20 = ema(cl, 20); e50 = ema(cl, 50); e200 = ema(cl, 200);
    idx = {};
    cs.forEach(function (c, i) { idx[c[0]] = i; });
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
    ctx.fillStyle = stx === "ENTER BUY" ? T.up : stx === "ENTER SELL" ? T.down : T.mute;
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
    setData: function (d) { S.data = d; rebuild(); draw(); },
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
function boot() {
  var FX = window.FX_BOOT, el = function (id) { return document.getElementById(id); };
  var canvas = el("fxc");
  if (!FX || !canvas) return;
  function theme() {
    var s = getComputedStyle(document.documentElement);
    function v(n, d) { return (s.getPropertyValue(n) || "").trim() || d; }
    return {bg: v("--card", "#17181a"), fg: v("--fg", "#f2f2f2"), mute: v("--mute", "#9a9aa0"), line: v("--line", "#2a2c30"), up: v("--ok", "#3ecf8e"), down: v("--bad", "#ff6b6b"), blue: "#4da3ff"};
  }
  var store = { get: function (k) { try { return sessionStorage.getItem(k); } catch (e) { return null; } }, set: function (k, v) { try { sessionStorage.setItem(k, v); } catch (e) { /* ignore */ } } };
  var chart = createChart(canvas, theme);
  var pair = store.get("fxpair"), tf = parseInt(store.get("fxtf") || "15", 10), emaOn = store.get("fxema") !== "0";
  if (FX.pairs.indexOf(pair) < 0) pair = FX.first;
  if ([15, 60, 240].indexOf(tf) < 0) tf = 15;
  chart.setTf(tf); chart.setEma(emaOn);

  function chips(box, items, isOn, onPick) {
    box.innerHTML = "";
    items.forEach(function (it) {
      var b = document.createElement("button");
      b.type = "button"; b.className = "chip" + (isOn(it[0]) ? " on" : ""); b.textContent = it[1];
      b.addEventListener("click", function () { onPick(it[0]); });
      box.appendChild(b);
    });
  }
  function paint() {
    chips(el("fxpairs"), FX.pairs.map(function (p) { return [p, p]; }), function (p) { return p === pair; }, function (p) { pick(p); });
    chips(el("fxtfs"), [[15, "M15"], [60, "H1"], [240, "H4"]], function (m) { return m === tf; }, function (m) { tf = m; store.set("fxtf", String(m)); chart.setTf(m); paint(); });
    var e = document.createElement("button");
    e.type = "button"; e.className = "chip" + (emaOn ? " on" : ""); e.textContent = "EMA";
    e.addEventListener("click", function () { emaOn = !emaOn; store.set("fxema", emaOn ? "1" : "0"); chart.setEma(emaOn); paint(); });
    el("fxtfs").appendChild(e);
    var r = document.createElement("button");
    r.type = "button"; r.className = "chip"; r.textContent = "Reset view";
    r.addEventListener("click", function () { chart.reset(); });
    el("fxtfs").appendChild(r);
  }
  function load() {
    fetch("/forex/data?pair=" + encodeURIComponent(pair), {cache: "no-store", credentials: "same-origin"}).then(function (r) {
      if (r.status === 401 || r.status === 303) { location.reload(); return null; }
      return r.json();
    }).then(function (d) {
      if (!d) return;
      if (d.version !== FX.version) { location.reload(); return; }   // a new 15-minute check finished: reload the signal cards too
      el("fxmsg").textContent = "Drag to move, pinch or scroll to zoom, tap for a crosshair, double-tap to reset. Times are UTC. Updates when each 15-minute candle closes.";
      chart.setData(d);
    }).catch(function () { el("fxmsg").textContent = "Chart data is not available right now. It will retry."; });
  }
  function pick(p, scroll) {
    pair = p; store.set("fxpair", p); paint(); load();
    if (scroll) el("fxchart").scrollIntoView({behavior: "smooth", block: "start"});
  }
  Array.prototype.forEach.call(document.querySelectorAll("[data-pair]"), function (n) {
    n.addEventListener("click", function () { pick(n.getAttribute("data-pair"), true); });
  });
  paint(); chart.resize(); load();
  setInterval(load, 30000);
}
if (typeof document !== "undefined") {
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else boot();
}
})();
'''


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
    boot = None
    if not has_key:
        body.append('<div class="card warn"><b>One step left: add your free data key.</b><div class="sub">1) Open twelvedata.com and make a free account.<br>'
                    '2) Copy your API key.<br>3) In Render open your service, go to Environment, add FOREX_API_KEY with that key, save. It redeploys by itself.</div></div>')
    elif snap:
        live = [p for p in snap["pairs"] if p["signal"] and p["signal"].state.startswith("ENTER")]
        boot = {"pairs": [p["pair"] for p in snap["pairs"]], "first": (live[0]["pair"] if live else snap["pairs"][0]["pair"]), "version": int(snap["last_cycle"] or 0)}
        for p in live:
            s = p["signal"]
            cls = "buy" if s.side == "BUY" else "sell"
            lots = f"{s.lots:.3f} lots" if s.lots else "size: n/a for this pair"
            body.append(f'<div class="card sig {cls}"><div class="big">{e(s.state)} {e(s.pair)}</div>'
                        f'<table><tr><td>Enter near</td><td><b>{_fmt(s.pair, s.entry)}</b></td></tr><tr><td>Stop loss</td><td><b>{_fmt(s.pair, s.stop)}</b></td></tr>'
                        f'<tr><td>Take profit</td><td><b>{_fmt(s.pair, s.target)}</b></td></tr><tr><td>Size</td><td><b>{e(lots)}</b> (risks about ${s.risk_usd:.2f})</td></tr></table>'
                        f'<div class="sub">{e(s.reason)} Set the stop and target in your broker when you enter.</div>'
                        f'<div style="margin-top:10px"><button type="button" class="chip on" data-pair="{e(s.pair)}">Show on chart</button></div></div>')
        if not live:
            body.append('<div class="card"><b>No entry right now.</b><div class="sub">Nothing to do. This page checks again by itself when the next 15-minute candle closes.</div></div>')
        body.append('<div class="card" id="fxchart"><div class="chips" id="fxpairs"></div><div class="chips" id="fxtfs"></div><canvas id="fxc"></canvas>'
                    '<div class="sub" id="fxmsg">Loading chart...</div>'
                    '<div class="sub"><span style="color:#f5c542">EMA 20</span> &middot; <span style="color:#4da3ff">EMA 50</span> &middot; <span style="color:#c77dff">EMA 200</span> &middot; triangles = past signals</div></div>')
        rows = []
        for p in snap["pairs"]:
            s = p["signal"]
            state = s.state if s else "starting"
            cls = "buy" if state == "ENTER BUY" else "sell" if state == "ENTER SELL" else ""
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
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta http-equiv="refresh" content="900">
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
.chips{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px}}.chip{{font:inherit;font-size:13px;font-weight:700;color:var(--mute);background:rgba(128,128,128,.16);border:0;border-radius:999px;padding:8px 13px}}.chip.on{{background:var(--fg);color:var(--bg)}}
#fxc{{width:100%;height:440px;display:block;touch-action:none;border-radius:12px;margin-top:4px}}
h2{{margin:4px 0 12px}}
</style></head><body>{mode_switch("forex")}<h2>Forex signals</h2>{"".join(body)}
<div class="sub">Signals only. You place the trade yourself. Nothing here is financial advice; most retail forex traders lose money.</div>{script}</body></html>"""


# ============================================================================
# WIRING
# ============================================================================
def register_forex_routes(app, auth) -> None:
    from contextlib import asynccontextmanager
    from fastapi import Depends, Query
    from fastapi.responses import HTMLResponse, JSONResponse

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

    @app.get("/forex/data", include_in_schema=False, dependencies=[Depends(auth)])
    async def forex_data(pair: str = Query("")):
        if not tracker or pair not in tracker.pairs:
            return JSONResponse({"error": "unknown pair"}, status_code=404, headers={"Cache-Control": "no-store"})
        return JSONResponse(tracker.chart_payload(pair), headers={"Cache-Control": "no-store"})


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
ch.setTf(240); ch.setEma(false); ch.reset(); ch.setData({pair:'EUR/USD',dp:5,state:'WAIT',candles:cs.slice(0,40),signal:null,trades:[]});   // short history must not crash
ch.setData({pair:'EUR/USD',dp:5,state:'WAIT',candles:[],signal:null,trades:[]});
const agg = api.aggregate(cs, 60); if (agg.length < 100 || agg.length > 140) throw new Error('aggregate '+agg.length);
const h = cs.filter(c=>Math.floor(c[0]/3600)===Math.floor(cs[10][0]/3600)); const a1 = agg.find(c=>c[0]===Math.floor(cs[10][0]/3600)*3600);
if (a1[2] !== Math.max(...h.map(c=>c[2])) || a1[3] !== Math.min(...h.map(c=>c[3])) || a1[4] !== h[h.length-1][4] || a1[1] !== h[0][1]) throw new Error('bad OHLC aggregation');
if (calls.length < 500) throw new Error('chart drew too little: '+calls.length);
console.log('ok');
""")
            r = subprocess.run([node, d + "/t.js"], capture_output=True, text=True, timeout=30)
            self.assertEqual(r.stdout.strip(), "ok", r.stderr[-800:])

    def test_next_candle_timer(self):
        s = ForexTracker.seconds_to_next_candle(datetime(2026, 11, 9, 10, 14, 50, tzinfo=UTC))
        self.assertAlmostEqual(s, 30.0, places=0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        unittest.main(argv=["forex"], verbosity=1)
    else:
        print(__doc__)
