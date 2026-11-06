"""cupsey.py: the whole Cupsey monitoring core in ONE file (put it next to main.py).

Sections: wallet registry, transaction decoder, position ledger, significance filter, tests, on-chain verifier.
Pure Python (stdlib only; httpx is imported only by the verify command). No signing, no trading, no secrets.

Use:
  python cupsey.py test                      run the 20 unit tests (synthetic fixtures, no network)
  python cupsey.py verify [address] [--limit 40]   writes <address>_evidence.json (reads SOLANA_RPC_URL, never printed)
  In the running app (after login): /cupsey/verify?download=1   and   /cupsey/wallets
  Evidence only; this never promotes a wallet's status.
In code:  from cupsey import seed_registry, decode_transaction, Ledger, Thresholds, is_significant
SOL values are ESTIMATES (may include ATA rent, close refunds, tips). Alert thresholds are unapproved placeholders.
"""
import argparse
import asyncio
import html
import json
import os
import re
import sys
import time
import unittest
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


# ============================================================================
# WALLET REGISTRY (explicit verification status + evidence; nothing is auto-promoted)
# ============================================================================
# Ordered ranks. RETIRED is never monitored for live alerts.
STATUS_RANK = {"RETIRED": -1, "UNVERIFIED": 0, "POSSIBLY_ASSOCIATED": 1, "STRONGLY_SUPPORTED": 2, "CONFIRMED": 3}
_B58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
CONFIRM_KINDS = {"self_confirmation", "on_chain_correlation"}


@dataclass
class Evidence:
    kind: str           # self_confirmation | on_chain_correlation | third_party_label | aggregator | social_claim
    source: str
    summary: str
    url: str = ""
    observed: str = ""  # date the evidence was seen
    independent: bool = False  # True only if not merely copied from another source


@dataclass
class WalletRecord:
    address: str
    label: str
    status: str = "UNVERIFIED"
    evidence: list[Evidence] = field(default_factory=list)
    notes: str = ""


class WalletRegistry:
    def __init__(self) -> None:
        self._w: dict[str, WalletRecord] = {}

    def add(self, rec: WalletRecord) -> None:
        if not _B58.match(rec.address):
            raise ValueError(f"not a valid base58 address: {rec.address!r}")
        if rec.status not in STATUS_RANK:
            raise ValueError(f"unknown status {rec.status}")
        if rec.status == "CONFIRMED" and not any(e.kind in CONFIRM_KINDS for e in rec.evidence):
            raise ValueError("CONFIRMED needs self_confirmation or on_chain_correlation evidence")
        self._w[rec.address] = rec

    def get(self, address: str) -> WalletRecord | None:
        return self._w.get(address)

    def add_evidence(self, address: str, ev: Evidence) -> None:
        self._w[address].evidence.append(ev)

    def promote(self, address: str, status: str) -> None:
        rec = self._w[address]
        if status == "CONFIRMED" and not any(e.kind in CONFIRM_KINDS for e in rec.evidence):
            raise ValueError("cannot confirm without self_confirmation or on_chain_correlation evidence")
        if status not in STATUS_RANK:
            raise ValueError(f"unknown status {status}")
        rec.status = status

    def alert_eligible(self, address: str, min_status: str | None = None) -> bool:
        """True only if the wallet meets the approved standard. Wallets are never merged."""
        min_status = min_status or os.getenv("CUPSEY_MIN_ALERT_STATUS", "STRONGLY_SUPPORTED")
        rec = self._w.get(address)
        return bool(rec) and rec.status != "RETIRED" and STATUS_RANK[rec.status] >= STATUS_RANK[min_status]

    def all(self) -> list:
        return list(self._w.values())

    def monitored(self) -> list[str]:
        """Wallets worth watching and logging (POSSIBLY_ASSOCIATED and above, not retired)."""
        return [a for a, r in self._w.items() if STATUS_RANK[r.status] >= STATUS_RANK["POSSIBLY_ASSOCIATED"]]


def seed_registry() -> WalletRegistry:
    """Seeded from public research on 2026-11-06. Third-party attribution only; NOT yet checked on-chain."""
    reg = WalletRegistry()
    reg.add(WalletRecord(
        address="2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f", label="Cupsey (current main wallet, attributed)",
        status="STRONGLY_SUPPORTED",
        evidence=[
            Evidence("social_claim", "X @dippy_eth", "Pointed to this address as Cupsey's new wallet the day after his drain (Sep 17 2025)",
                     "https://x.com/dippy_eth/status/1968435704096010348", "2026-11-06"),
            Evidence("aggregator", "Seer KOL Tracker", "Posts Cupsey-labelled swaps from this wallet, Sep-Dec 2025",
                     "https://x.com/Seer_Tracker/status/1996090573480988952", "2026-11-06"),
            Evidence("aggregator", "KOL Explorer", "Lists as Cupsey's wallet; ranked #3 by 90d realized profit",
                     "https://kolexplorer.com/kol/cupsey", "2026-11-06"),
            Evidence("third_party_label", "Solscan label", "Labelled 'cupseyyyyy' on Pump.fun; undated balance ~1,105 SOL",
                     "https://solscan.io/account/2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f", "2026-11-06"),
        ],
        notes="No self-confirmation found. Sources may copy each other. Needs on-chain verification (verify_wallet.py).",
    ))
    reg.add(WalletRecord(
        address="DiLBRo3fAvzWcd7sp3ZHHX1sWMnmG7nBi6gAfZrgs7Qg", label="Cupsey (old wallet, drained Sep 2025)",
        status="RETIRED",
        evidence=[Evidence("self_confirmation", "X @Cupseyy", "Cupsey posted this as the wallet that was drained (Sep 16 2025)",
                           "https://x.com/Cupseyy/status/1967955345999016361", "2026-11-06", True)],
        notes="Compromised. Historical context only. Never alert from it.",
    ))
    return reg


# ============================================================================
# DECODER (classify one transaction for one wallet from balance changes)
# ============================================================================
LAMPORTS = 1_000_000_000
WSOL = "So11111111111111111111111111111111111111112"
MIN_SWAP_SOL = 0.003   # below this (but above dust) we cannot tell a tiny swap from ATA rent -> UNKNOWN
DUST_SOL = 0.0001      # below this there is no payment, so a token move is a transfer


@dataclass(frozen=True)
class DecodedTx:
    signature: str
    wallet: str
    slot: int | None
    block_time: int | None
    kind: str                      # BUY | SELL | TRANSFER | OTHER | UNKNOWN
    mint: str | None = None
    token_delta: float = 0.0       # signed change in the wallet's balance of `mint`
    pre_token_amount: float | None = None
    post_token_amount: float | None = None
    quote_sol: float = 0.0         # signed SOL+wSOL change excluding the network fee (buy < 0, sell > 0)
    fee_sol: float = 0.0
    legs: tuple = field(default_factory=tuple)   # all token deltas, for OTHER (token-to-token) records
    note: str = ""

    @property
    def price_sol(self) -> float | None:
        if self.kind in ("BUY", "SELL") and self.token_delta:
            return abs(self.quote_sol) / abs(self.token_delta)
        return None


def _keys(tx: dict) -> list[str]:
    return [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"].get("accountKeys", [])]


def _token_totals(meta: dict, wallet: str) -> dict[str, list]:
    out: dict[str, list] = {}
    for side, idx in (("preTokenBalances", 0), ("postTokenBalances", 1)):
        for b in meta.get(side) or []:
            if b.get("owner") != wallet:
                continue
            ui = b["uiTokenAmount"]
            rec = out.setdefault(b["mint"], [0, 0, int(ui["decimals"])])
            rec[idx] += int(ui["amount"])
    return out


def decode_transaction(tx: dict, wallet: str) -> DecodedTx:
    msg = tx.get("transaction", {}).get("message") if tx else None
    sigs = (tx or {}).get("transaction", {}).get("signatures") or ["?"]
    base = dict(signature=sigs[0], wallet=wallet, slot=(tx or {}).get("slot"), block_time=(tx or {}).get("blockTime"))
    meta = (tx or {}).get("meta")
    if not msg or meta is None:
        return DecodedTx(kind="UNKNOWN", note="missing transaction or meta", **base)
    if meta.get("err") is not None:
        return DecodedTx(kind="OTHER", note="failed transaction (no balance change counted)", **base)

    keys = _keys(tx)
    fee = meta.get("fee", 0) / LAMPORTS if keys and keys[0] == wallet else 0.0
    native = 0.0
    if wallet in keys:
        i = keys.index(wallet)
        native = (meta["postBalances"][i] - meta["preBalances"][i]) / LAMPORTS + fee

    totals = _token_totals(meta, wallet)
    wsol = totals.get(WSOL)
    wsol_delta = (wsol[1] - wsol[0]) / 10 ** wsol[2] if wsol else 0.0
    quote = native + wsol_delta

    deltas = {m: (post - pre) / 10 ** dec for m, (pre, post, dec) in totals.items() if m != WSOL and post != pre}
    common = dict(quote_sol=quote, fee_sol=fee, **base)
    if not deltas:
        return DecodedTx(kind="OTHER", note="no token balance change (SOL-only or unrelated)", **common)

    if len(deltas) > 1:
        legs = tuple(sorted(deltas.items()))
        signs = {d > 0 for d in deltas.values()}
        note = "token-to-token swap or multi-token transaction; both legs recorded" if len(signs) == 2 else "several tokens moved the same direction"
        return DecodedTx(kind="OTHER", legs=legs, note=note, **common)

    (mint, delta), = deltas.items()
    pre, post, dec = totals[mint]
    t = dict(mint=mint, token_delta=delta, pre_token_amount=pre / 10 ** dec, post_token_amount=post / 10 ** dec, **common)
    if delta > 0:
        if quote <= -MIN_SWAP_SOL:
            return DecodedTx(kind="BUY", **t)
        if abs(quote) < DUST_SOL:
            return DecodedTx(kind="TRANSFER", note="tokens received with no payment", **t)
    else:
        if quote >= MIN_SWAP_SOL:
            return DecodedTx(kind="SELL", **t)
        if abs(quote) < DUST_SOL:
            return DecodedTx(kind="TRANSFER", note="tokens sent with no proceeds", **t)
    return DecodedTx(kind="UNKNOWN", note="ambiguous SOL movement; logged, never alerted", **t)


# ============================================================================
# LEDGER (positions, average-cost PnL, dedupe by signature; feed in chronological order)
# ============================================================================
EPS = 1e-6


@dataclass
class Position:
    wallet: str
    mint: str
    qty: float = 0.0
    cost_sol: float = 0.0          # cost basis of the remaining qty
    bought_sol: float = 0.0
    sold_sol: float = 0.0
    realized_pnl_sol: float = 0.0
    fees_sol: float = 0.0
    buys: int = 0
    sells: int = 0
    opened_at: int | None = None
    closed_at: int | None = None
    status: str = "OPEN"
    complete: bool = True          # False = history gap; PnL is an estimate
    notes: list = field(default_factory=list)


@dataclass
class TradeEvent:
    signature: str
    wallet: str
    mint: str | None
    classification: str            # BUY | ADDITIONAL BUY | PARTIAL SELL | FULL SELL | TRANSFER | OTHER | UNKNOWN
    block_time: int | None
    sol_value: float = 0.0         # estimated
    price_sol: float | None = None
    token_amount: float = 0.0
    pct_of_position: float | None = None   # sells: % of position sold; adds: % of resulting position
    realized_pnl_sol: float | None = None
    position: dict | None = None
    note: str = ""


class Ledger:
    def __init__(self) -> None:
        self.open: dict[tuple, Position] = {}
        self.closed: list[Position] = []
        self.seen: set[str] = set()

    def apply(self, d: DecodedTx) -> TradeEvent | None:
        """Returns None for an already-processed signature (deduplication)."""
        if d.signature in self.seen:
            return None
        self.seen.add(d.signature)
        ev = TradeEvent(d.signature, d.wallet, d.mint, d.kind, d.block_time, note=d.note)
        if d.kind in ("OTHER", "UNKNOWN") or d.mint is None:
            return ev
        key = (d.wallet, d.mint)
        pos = self.open.get(key)
        if d.kind == "TRANSFER":
            return self._transfer(d, ev, key, pos)
        if d.kind == "BUY":
            return self._buy(d, ev, key, pos)
        return self._sell(d, ev, key, pos)

    # ---- helpers
    def _ensure(self, d: DecodedTx, key, pos, note: str):
        pos = pos or Position(d.wallet, d.mint, opened_at=d.block_time)
        self.open[key] = pos
        return pos

    def _buy(self, d, ev, key, pos):
        pre = d.pre_token_amount or 0.0
        existing = pos is not None and pos.qty > EPS
        if pos is None and pre > EPS:   # chain says we already held tokens we never saw bought
            pos = self._ensure(d, key, None, "")
            pos.qty, pos.complete = pre, False
            pos.notes.append("held tokens before tracked history; cost basis unknown")
            existing = True
        pos = pos if (pos and pos.qty > EPS) else self._ensure(d, key, None, "")
        amt, spent = abs(d.token_delta), abs(d.quote_sol)
        pos.qty += amt; pos.cost_sol += spent; pos.bought_sol += spent; pos.fees_sol += d.fee_sol; pos.buys += 1
        ev.classification = "ADDITIONAL BUY" if existing else "BUY"
        ev.sol_value, ev.price_sol, ev.token_amount = spent, d.price_sol, amt
        ev.pct_of_position = 100.0 if not existing else 100.0 * amt / pos.qty
        ev.position = asdict(pos)
        return ev

    def _sell(self, d, ev, key, pos):
        sold, proceeds = abs(d.token_delta), d.quote_sol
        if pos is None or pos.qty <= EPS:
            pos = self._ensure(d, key, None, "")
            pos.complete = False
            pos.notes.append("sale without a tracked buy; cost basis unknown, PnL not computed for it")
            pos.qty = d.pre_token_amount if d.pre_token_amount is not None else sold
        qty_before = pos.qty
        known = min(sold, pos.qty)
        if known < sold - EPS:
            pos.complete = False
            pos.notes.append("sold more than tracked holdings; history incomplete")
        basis = pos.cost_sol * (known / pos.qty) if pos.qty > EPS else 0.0
        proceeds_known = proceeds * (known / sold) if sold > EPS else 0.0
        pnl = proceeds_known - basis if pos.cost_sol > 0 or pos.complete else None
        pos.qty -= known; pos.cost_sol -= basis
        pos.sold_sol += proceeds; pos.fees_sol += d.fee_sol; pos.sells += 1
        if pnl is not None:
            pos.realized_pnl_sol += pnl
        full = (d.post_token_amount <= EPS) if d.post_token_amount is not None else pos.qty <= EPS
        ev.sol_value, ev.price_sol, ev.token_amount, ev.realized_pnl_sol = proceeds, d.price_sol, sold, pnl
        ev.pct_of_position = 100.0 * known / qty_before if qty_before > EPS else None
        if full:
            pos.qty, pos.cost_sol, pos.status, pos.closed_at = 0.0, 0.0, "CLOSED", d.block_time
            self.closed.append(pos); self.open.pop(key, None)
            ev.classification = "FULL SELL"
        else:
            ev.classification = "PARTIAL SELL"
        ev.position = asdict(pos)
        return ev

    def _transfer(self, d, ev, key, pos):
        pos = pos or self._ensure(d, key, None, "")
        pos.complete = False
        if d.token_delta > 0:
            pos.qty += d.token_delta
            pos.notes.append("tokens transferred in at unknown cost")
        else:
            moved = min(abs(d.token_delta), pos.qty)
            if pos.qty > EPS:
                pos.cost_sol -= pos.cost_sol * moved / pos.qty
            pos.qty -= moved
            pos.notes.append("tokens transferred out (not treated as a sale)")
        ev.token_amount, ev.position = abs(d.token_delta), asdict(pos)
        return ev


# ============================================================================
# SIGNIFICANCE (configurable alert filter; defaults are UNAPPROVED placeholders)
# ============================================================================
def _f(name: str, default: float) -> float:
    return float(os.getenv(name, default))


@dataclass
class Thresholds:
    new_position_min_sol: float = 1.0
    add_min_sol: float = 1.0
    add_min_pct: float = 25.0       # % of resulting position
    sell_min_sol: float = 1.0
    sell_min_pct: float = 25.0      # % of position sold
    full_exit_min_sol: float = 0.5

    @classmethod
    def from_env(cls) -> "Thresholds":
        return cls(_f("CUPSEY_NEW_MIN_SOL", 1.0), _f("CUPSEY_ADD_MIN_SOL", 1.0), _f("CUPSEY_ADD_MIN_PCT", 25.0),
                   _f("CUPSEY_SELL_MIN_SOL", 1.0), _f("CUPSEY_SELL_MIN_PCT", 25.0), _f("CUPSEY_FULL_EXIT_MIN_SOL", 0.5))


def is_significant(ev: TradeEvent, t: Thresholds) -> tuple[bool, str]:
    c, v, p = ev.classification, ev.sol_value, ev.pct_of_position or 0.0
    if c == "BUY":
        ok = v >= t.new_position_min_sol
        return ok, f"new position {v:.2f} SOL (min {t.new_position_min_sol})"
    if c == "ADDITIONAL BUY":
        ok = v >= t.add_min_sol and p >= t.add_min_pct
        return ok, f"add {v:.2f} SOL = {p:.0f}% of position (min {t.add_min_sol} SOL and {t.add_min_pct}%)"
    if c == "PARTIAL SELL":
        ok = v >= t.sell_min_sol and p >= t.sell_min_pct
        return ok, f"partial sell {v:.2f} SOL = {p:.0f}% of position (min {t.sell_min_sol} SOL and {t.sell_min_pct}%)"
    if c == "FULL SELL":
        ok = v >= t.full_exit_min_sol
        return ok, f"full exit {v:.2f} SOL (min {t.full_exit_min_sol})"
    return False, "not a trade"


# ============================================================================
# TESTS (synthetic getTransaction-shaped fixtures, NOT real chain data)  ->  python cupsey.py test
# ============================================================================

W = "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f"
M1, M2 = "A" * 44, "B" * 44
_n = [0]


def tb(mint, raw, dec=6, owner=W, idx=1):
    return {"accountIndex": idx, "mint": mint, "owner": owner, "uiTokenAmount": {"amount": str(raw), "decimals": dec}}


def tx(pre_sol, post_sol, pre_tb=(), post_tb=(), fee=5000, err=None, slot=1):
    _n[0] += 1
    return {"slot": slot, "blockTime": 1000 + _n[0],
            "transaction": {"signatures": [f"SIG{_n[0]}"], "message": {"accountKeys": [{"pubkey": W}, {"pubkey": "X" * 44}]}},
            "meta": {"err": err, "fee": fee, "preBalances": [int(pre_sol * 1e9), 0], "postBalances": [int(post_sol * 1e9), 0],
                     "preTokenBalances": list(pre_tb), "postTokenBalances": list(post_tb)}}


def buy(sol, tokens, pre=0, mint=M1):
    return tx(10, 10 - sol - 0.000005, [tb(mint, pre * 10**6)] if pre else [], [tb(mint, (pre + tokens) * 10**6)])


def sell(sol, tokens, pre, mint=M1):
    return tx(10, 10 + sol - 0.000005, [tb(mint, pre * 10**6)], [tb(mint, (pre - tokens) * 10**6)])


class DecoderTests(unittest.TestCase):
    def test_buy(self):
        d = decode_transaction(buy(1.0, 1000), W)
        self.assertEqual(d.kind, "BUY"); self.assertAlmostEqual(d.quote_sol, -1.0, 6); self.assertAlmostEqual(d.price_sol, 0.001, 6)

    def test_sell(self):
        d = decode_transaction(sell(2.0, 1000, 1000), W)
        self.assertEqual(d.kind, "SELL"); self.assertEqual(d.post_token_amount, 0)

    def test_transfer_in_is_not_a_buy(self):
        d = decode_transaction(tx(10, 10, [], [tb(M1, 5 * 10**6)], fee=0), W)
        self.assertEqual(d.kind, "TRANSFER")

    def test_transfer_out_is_not_a_sale(self):
        d = decode_transaction(tx(10, 10, [tb(M1, 5 * 10**6)], [], fee=0), W)
        self.assertEqual(d.kind, "TRANSFER")

    def test_failed_tx_never_a_trade(self):
        d = decode_transaction(tx(10, 9, [], [tb(M1, 10**6)], err={"InstructionError": [0, "x"]}), W)
        self.assertEqual(d.kind, "OTHER")

    def test_token_to_token(self):
        d = decode_transaction(tx(10, 10, [tb(M1, 10**6)], [tb(M2, 10**6, idx=2)], fee=0), W)
        self.assertEqual(d.kind, "OTHER"); self.assertEqual(len(d.legs), 2)

    def test_wsol_funded_buy(self):
        t = tx(10, 10, [tb(WSOL, 2 * 10**9, 9, idx=3)], [tb(WSOL, 10**9, 9, idx=3), tb(M1, 500 * 10**6, idx=2)], fee=0)
        d = decode_transaction(t, W)
        self.assertEqual(d.kind, "BUY"); self.assertAlmostEqual(d.quote_sol, -1.0, 6)

    def test_ata_rent_sized_move_is_unknown_not_buy(self):
        d = decode_transaction(tx(10, 10 - 0.00204, [], [tb(M1, 10**6)], fee=0), W)
        self.assertEqual(d.kind, "UNKNOWN")

    def test_missing_meta(self):
        self.assertEqual(decode_transaction({"transaction": {"message": {"accountKeys": []}, "signatures": ["s"]}}, W).kind, "UNKNOWN")

    def test_other_wallet_tokens_ignored(self):
        t = tx(10, 10, [], [tb(M1, 10**6, owner="Z" * 44)], fee=0)
        self.assertEqual(decode_transaction(t, W).kind, "OTHER")


class LedgerTests(unittest.TestCase):
    def run_seq(self):
        L = Ledger()
        evs = [L.apply(decode_transaction(t, W)) for t in (
            buy(1.0, 1000), buy(1.0, 500, pre=1000), sell(1.5, 750, 1500), sell(0.5, 750, 750))]
        return L, evs

    def test_lifecycle_and_pnl(self):
        L, e = self.run_seq()
        self.assertEqual([x.classification for x in e], ["BUY", "ADDITIONAL BUY", "PARTIAL SELL", "FULL SELL"])
        self.assertAlmostEqual(e[2].realized_pnl_sol, 0.5, 5); self.assertAlmostEqual(e[2].pct_of_position, 50.0, 3)
        self.assertAlmostEqual(e[3].realized_pnl_sol, -0.5, 5)
        self.assertEqual(len(L.closed), 1); self.assertTrue(L.closed[0].complete); self.assertAlmostEqual(L.closed[0].realized_pnl_sol, 0.0, 5)

    def test_dedupe(self):
        L = Ledger(); d = decode_transaction(buy(1, 100), W)
        self.assertIsNotNone(L.apply(d)); self.assertIsNone(L.apply(d))

    def test_sell_without_history_is_flagged_incomplete(self):
        L = Ledger(); e = L.apply(decode_transaction(sell(1.0, 100, 100), W))
        self.assertEqual(e.classification, "FULL SELL"); self.assertFalse(L.closed[0].complete)

    def test_buy_into_unseen_holding(self):
        L = Ledger(); e = L.apply(decode_transaction(buy(1.0, 100, pre=400), W))
        self.assertEqual(e.classification, "ADDITIONAL BUY"); self.assertFalse(L.open[(W, M1)].complete)

    def test_transfer_not_counted_as_sale(self):
        L = Ledger(); L.apply(decode_transaction(buy(1.0, 1000), W))
        e = L.apply(decode_transaction(tx(10, 10, [tb(M1, 1000 * 10**6)], [tb(M1, 400 * 10**6)], fee=0), W))
        self.assertEqual(e.classification, "TRANSFER"); self.assertIsNone(e.realized_pnl_sol)
        self.assertFalse(L.open[(W, M1)].complete)

    def test_reentry_after_close_is_new_buy(self):
        L, _ = self.run_seq()
        self.assertEqual(L.apply(decode_transaction(buy(1.0, 10), W)).classification, "BUY")


class SignificanceTests(unittest.TestCase):
    def test_filters(self):
        L = Ledger(); t = Thresholds()
        small = L.apply(decode_transaction(buy(0.1, 100), W))
        self.assertFalse(is_significant(small, t)[0])
        big = L.apply(decode_transaction(buy(5.0, 100, mint=M2), W))
        self.assertTrue(is_significant(big, t)[0])
        tiny_add = L.apply(decode_transaction(buy(0.2, 1, pre=100, mint=M2), W))
        self.assertFalse(is_significant(tiny_add, t)[0])


class RegistryTests(unittest.TestCase):
    def test_seed_standards(self):
        r = seed_registry()
        self.assertTrue(r.alert_eligible("2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f"))
        self.assertFalse(r.alert_eligible("2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f", "CONFIRMED"))
        self.assertFalse(r.alert_eligible("DiLBRo3fAvzWcd7sp3ZHHX1sWMnmG7nBi6gAfZrgs7Qg", "UNVERIFIED"))
        self.assertEqual(r.monitored(), ["2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f"])

    def test_cannot_confirm_without_proof(self):
        r = seed_registry()
        with self.assertRaises(ValueError):
            r.promote("2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f", "CONFIRMED")
        r.add_evidence("2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f", Evidence("on_chain_correlation", "verify_wallet", "x"))
        r.promote("2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f", "CONFIRMED")

    def test_rejects_bad_address(self):
        with self.assertRaises(ValueError):
            WalletRegistry().add(WalletRecord("not-an-address", "x"))




class PageTests(unittest.TestCase):
    def test_page_escapes_and_handles_empty(self):
        rec = seed_registry().get("2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f")
        out = {"kinds": {"TRANSFER": 4}, "recent_activity": [{"kind": "TRANSFER", "mint": "X", "sol": 0, "time": 1, "signature": "s"}], "window": {"returned": 100},
               "source": "<script>alert(1)</script>", "decoded_oldest": "a", "decoded_newest": "b", "checked_at": "c"}
        page = render_page(out, None, rec)
        self.assertIn("No buys or sells found", page); self.assertNotIn("<script>alert", page)
        self.assertIn("Could not read", render_page(None, "boom", rec))

    def test_page_shows_trades(self):
        rec = seed_registry().get("2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f")
        out = {"kinds": {"BUY": 1}, "recent_activity": [{"kind": "BUY", "mint": "A" * 44, "sol": -1.5, "time": 1, "signature": "s"}], "window": {"returned": 5}}
        self.assertIn("1.500 SOL", render_page(out, None, rec))


class TrackerTests(unittest.TestCase):
    def make(self):
        t = Tracker(W, "https://rpc.invalid/?api-key=SECRET", interval=5)
        return t

    def test_ingest_builds_feed_and_dedupes(self):
        t = self.make()
        txs = [buy(2.0, 1000), sell(3.0, 1000, 1000), tx(10, 10, fee=5000)]   # buy, sell, noise
        self.assertEqual(t.ingest(txs), 3)
        self.assertEqual(t.ingest(txs), 0)   # same transactions again are skipped
        s = t.snapshot()
        self.assertEqual(s["kinds"].get("BUY"), 1); self.assertEqual(s["kinds"].get("SELL"), 1)
        self.assertEqual([x["cls"] for x in s["events"]], ["FULL SELL", "BUY"])   # newest first
        self.assertTrue(s["events"][0]["big"]); self.assertEqual(s["open"], []); self.assertEqual(s["closed"], 1)

    def test_out_of_order_input_is_sorted(self):
        t = self.make()
        a, b = buy(1.0, 100), sell(1.5, 100, 100)
        b["slot"], a["slot"] = 5, 4
        t.ingest([b, a])   # sell arrives first in the list
        self.assertEqual(t.ledger.closed[0].complete, True)

    def test_noise_is_counted_as_foreign_when_not_signed_by_wallet(self):
        t = self.make()
        x = tx(10, 10, fee=0); x["transaction"]["message"]["accountKeys"] = [{"pubkey": "S" * 44}, {"pubkey": W}]
        t.ingest([x]); self.assertEqual(t.snapshot()["foreign"], 1)

    def test_malformed_transaction_does_not_stop_tracking(self):
        t = self.make(); t.ingest([{"slot": 1}, buy(1.0, 10)])
        self.assertEqual(t.kinds["BUY"], 1)

    def test_cycle_backfills_then_polls_incrementally_and_hides_rpc_url(self):
        t = self.make(); calls = []

        async def fake(since, cap):
            calls.append(since); return [buy(1.0, 10)] if len(calls) == 1 else [sell(1.2, 10, 10)]
        t._fetch = fake
        asyncio.run(t.cycle()); asyncio.run(t.cycle())
        self.assertEqual(calls[0], None); self.assertIsNotNone(calls[1])   # second call only asks for what is new
        self.assertEqual(t.snapshot()["closed"], 1)

    def test_helius_failure_on_first_call_switches_to_standard(self):
        t = self.make()

        async def boom(method, params):
            if method == "getTransactionsForAddress":
                raise RuntimeError("method not found")
            return []
        t._rpc = boom
        asyncio.run(t._fetch(None, 100)); self.assertEqual(t.mode, "standard")

    def test_live_page_states(self):
        rec = seed_registry().get(DEFAULT_ADDRESS) or seed_registry().get(SEED_ADDRESS)
        self.assertIn("not running", render_live(None, rec))
        t = self.make(); t.ingest([tx(10, 10, fee=0)]); t.last_ok = time.time()
        page = render_live(t.snapshot(), rec)
        self.assertIn("No buys or sells found yet", page); self.assertIn("Why nothing yet", page); self.assertIn('http-equiv="refresh"', page)
        t.ingest([buy(5.0, 10, mint=M2)])
        page = render_live(t.snapshot(), rec)
        self.assertIn("Open positions", page); self.assertIn("5.000", page); self.assertNotIn("SECRET", page)


# ============================================================================
# ON-CHAIN VERIFIER  ->  browser: /cupsey/verify   |  terminal: python cupsey.py verify <address>
# ============================================================================
SEED_ADDRESS = "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f"
DEFAULT_ADDRESS = (os.getenv("CUPSEY_WALLET") or SEED_ADDRESS).strip()   # set CUPSEY_WALLET on Render to watch a different wallet
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"


def _rpc(client, url, method, params):
    for attempt in range(4):
        r = client.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        if r.status_code == 429:
            time.sleep(1 + attempt)
            continue
        r.raise_for_status()
        body = r.json()
        if "error" in body:
            raise RuntimeError("rpc error: " + str(body["error"])[:150])
        return body["result"]
    raise RuntimeError("rate limited by the RPC provider (try again, or use a dedicated RPC URL)")


def run_verification(address: str, rpc_url: str, limit: int = 40, scan: int = 2000) -> dict:
    """Blocking. Decodes the wallet's recent REAL activity and returns evidence. Never changes any status.

    Path 1 (fast): Helius getTransactionsForAddress, filtered to successful transactions that touch the wallet or change
    its token balances (removes failed-transaction spam), full data in ONE call.
    Path 2 (fallback, any RPC): scan signatures, skip failed ones, sample evenly across the window."""
    import httpx  # only needed here
    if not _B58.match(address):
        raise ValueError("not a valid Solana address")
    kinds, mints, inflows, samples, times, errors = Counter(), set(), Counter(), [], [], []
    reasons, other_samples, recent = Counter(), [], []
    info: dict = {}

    def ingest(t) -> None:
        d = decode_transaction(t, address)
        found = []   # parse first: a malformed transaction raises here, before any counter is touched
        for ins in t["transaction"]["message"].get("instructions", []):
            p = ins.get("parsed")
            if ins.get("program") == "system" and isinstance(p, dict) and p.get("type") == "transfer" and p["info"].get("destination") == address:
                found.append((p["info"]["source"], p["info"]["lamports"] / LAMPORTS))
        kinds[d.kind] += 1
        reasons[f"{d.kind}: {d.note or 'ok'}"] += 1
        if d.kind in ("BUY", "SELL", "TRANSFER") and len(recent) < 25:
            recent.append({"kind": d.kind, "mint": d.mint, "sol": round(d.quote_sol, 4), "time": d.block_time, "signature": d.signature})
        if d.kind in ("OTHER", "UNKNOWN") and len(other_samples) < 5:
            other_samples.append({"signature": d.signature, "kind": d.kind, "note": d.note, "quote_sol": round(d.quote_sol, 6)})
        if d.block_time:
            times.append(d.block_time)
        if d.mint:
            mints.add(d.mint)
        if d.kind in ("BUY", "SELL") and len(samples) < 15:
            samples.append({"signature": d.signature, "kind": d.kind, "mint": d.mint, "sol": round(d.quote_sol, 4), "time_utc": datetime.fromtimestamp(d.block_time, timezone.utc).isoformat() if d.block_time else None})
        for src, sol in found:
            inflows[src] += sol

    def safe_ingest(t) -> None:
        try:
            ingest(t)
        except Exception as exc:   # one bad transaction must not abort the whole check
            kinds["ERROR"] += 1
            msg = (f"{type(exc).__name__}: {exc}")[:160].replace(rpc_url, "<rpc>")
            if msg not in errors and len(errors) < 3:
                errors.append(msg)

    with httpx.Client(timeout=25) as c:
        data = None
        # Strictest filter first: successful, wallet really took part in a token transfer (swaps/transfers only).
        # If the provider rejects it, retry with the looser filter, then fall back to the standard path below.
        attempts = [("successful token transfers involving the wallet", {"status": "succeeded", "tokenAccounts": "balanceChanged", "tokenTransfer": {}}),
                    ("successful transactions touching the wallet or changing its token balances", {"status": "succeeded", "tokenAccounts": "balanceChanged"})]
        helius_max = max(100, min(scan, 500))
        for label, flt in attempts:
            try:
                rows, token = [], None
                while len(rows) < helius_max:
                    cfg = {"transactionDetails": "full", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 1,
                           "sortOrder": "desc", "limit": 100, "commitment": "confirmed", "filters": flt}
                    if token:
                        cfg["paginationToken"] = token
                    res = _rpc(c, rpc_url, "getTransactionsForAddress", [address, cfg]) or {}
                    rows += res.get("data") or []
                    token = res.get("paginationToken")
                    if not token or not res.get("data"):
                        break
                data = rows
                info["source"] = "helius_filtered: " + label
                break
            except Exception as exc:   # not Helius, plan without this method, filter unsupported...
                info.setdefault("helius_filter_errors", []).append(f"{label}: {type(exc).__name__}: {exc}"[:150].replace(rpc_url, "<rpc>"))
        if data is not None:
            for t in data:
                safe_ingest(t)
            window = {"returned": len(data)}
        else:
            info["source"] = "standard_signatures (failed transactions skipped, sampled across the window)"
            sigs, before = [], None
            while len(sigs) < scan:
                cfg = {"limit": min(100, scan - len(sigs)), "commitment": "confirmed"}
                if before:
                    cfg["before"] = before
                page = _rpc(c, rpc_url, "getSignaturesForAddress", [address, cfg])
                if not page:
                    break
                sigs += page
                before = page[-1]["signature"]
                if len(page) < cfg["limit"] or sum(1 for x in sigs if not x.get("err")) >= 3 * limit:
                    break
            ok = [x for x in sigs if not x.get("err")]
            fails = [x for x in sigs if x.get("err")]
            failed_samples = []
            for f in fails[::max(1, len(fails) // 5)][:5]:   # who sends the failed transactions, and why do they fail?
                try:
                    t = _rpc(c, rpc_url, "getTransaction", [f["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 255, "commitment": "confirmed"}])
                    msg = t["transaction"]["message"]
                    keys = [k["pubkey"] if isinstance(k, dict) else k for k in msg.get("accountKeys", [])]
                    failed_samples.append({"signature": f["signature"], "fee_payer_is_wallet": bool(keys) and keys[0] == address, "fee_payer": keys[0] if keys else None,
                                           "error": str(f.get("err"))[:80], "programs": sorted({i.get("programId", "?") for i in msg.get("instructions", [])})[:4]})
                except Exception as exc:
                    failed_samples.append({"signature": f["signature"], "error": f"could not read: {type(exc).__name__}"})
            for sg in ok[::max(1, len(ok) // max(1, limit))][:limit]:
                try:
                    t = _rpc(c, rpc_url, "getTransaction", [sg["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 255, "commitment": "confirmed"}])
                except Exception as exc:
                    kinds["ERROR"] += 1
                    errors.append(f"{type(exc).__name__}: {exc}"[:160].replace(rpc_url, "<rpc>")) if len(errors) < 3 else None
                    continue
                if not t:
                    kinds["MISSING"] += 1
                    continue
                safe_ingest(t)
            sig_times = [x["blockTime"] for x in sigs if x.get("blockTime")]
            window = {"scanned": len(sigs), "failed": len(fails), "succeeded": len(ok),
                      "failed_error_types": dict(Counter(str(x.get("err"))[:60] for x in fails).most_common(3)), "failed_samples": failed_samples,
                      "span_seconds": (max(sig_times) - min(sig_times)) if sig_times else None}
    iso = lambda x: datetime.fromtimestamp(x, timezone.utc).isoformat()
    return {"address": address, "checked_at": datetime.now(timezone.utc).isoformat(), **info, "window": window,
            "kinds": dict(kinds), "reasons": dict(reasons), "other_samples": other_samples, "errors_sample": errors,
            "distinct_tokens": len(mints),
            "decoded_oldest": iso(min(times)) if times else None, "decoded_newest": iso(max(times)) if times else None,
            "rpc_is_public_default": rpc_url == PUBLIC_RPC,
            "largest_sol_inflow_sources": [{"source": k, "sol": round(v, 3)} for k, v in inflows.most_common(5)],
            "sample_trades": samples, "recent_activity": recent,
            "next_steps": ["Compare sample_trades times with trades Cupsey posted himself on X",
                           "Check whether any inflow source is a known exchange or his old wallet",
                           "Only then add on_chain_correlation evidence and consider promotion"]}


def verify(argv) -> int:
    ap = argparse.ArgumentParser(prog="cupsey.py verify")
    ap.add_argument("address", nargs="?", default=DEFAULT_ADDRESS)
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--scan", type=int, default=2000)
    a = ap.parse_args(argv)
    url = os.getenv("SOLANA_RPC_URL") or PUBLIC_RPC
    out = run_verification(a.address, url, a.limit, a.scan)
    path = f"{a.address}_evidence.json"
    json.dump(out, open(path, "w"), indent=2)
    print(json.dumps({k: out[k] for k in ("source", "window", "kinds", "decoded_newest")}, indent=2))
    print("wrote", path)
    return 0


# ============================================================================
# LIVE TRACKER (background loop: polls the wallet, decodes new transactions, keeps a trade feed in memory)
# ============================================================================
class Tracker:
    """Runs forever inside the web app. Reads only (no keys, no signing). Memory only: after a restart it re-reads recent history."""

    def __init__(self, address: str, rpc_url: str, interval: float = 20.0, backfill: int = 1000, thresholds: "Thresholds | None" = None) -> None:
        self.address, self.rpc_url = address, rpc_url
        self.interval, self.backfill = max(5.0, interval), max(100, backfill)
        self.thresholds = thresholds or Thresholds.from_env()
        self.ledger = Ledger()
        self.events: list[dict] = []          # newest first, trades + transfers only
        self.kinds: Counter = Counter()
        self.reasons: Counter = Counter()
        self.checked = self.polls = self.foreign = 0   # foreign = transactions NOT signed by the wallet (spam/dust/others)
        self.started = time.time()
        self.last_poll: float | None = None
        self.last_ok: float | None = None
        self.last_error = ""
        self.newest: int | None = None
        self.oldest: int | None = None
        self.last_trade: int | None = None
        self.mode = "helius"
        self.backfilled = False
        self.last_sig: str | None = None
        self.client = None

    # ---- network
    async def _rpc(self, method: str, params: list):
        for attempt in range(4):
            r = await self.client.post(self.rpc_url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            if r.status_code == 429:
                await asyncio.sleep(1 + attempt)
                continue
            r.raise_for_status()
            body = r.json()
            if "error" in body:
                raise RuntimeError("rpc error: " + str(body["error"])[:150])
            return body["result"]
        raise RuntimeError("rate limited by the RPC provider")

    async def _fetch_helius(self, since: int | None, cap: int) -> list:
        rows, token = [], None
        flt: dict = {"status": "succeeded", "tokenAccounts": "balanceChanged"}
        if since:
            flt["blockTime"] = {"gte": since}
        while len(rows) < cap:
            cfg = {"transactionDetails": "full", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 1,
                   "sortOrder": "desc", "limit": 100, "commitment": "confirmed", "filters": flt}
            if token:
                cfg["paginationToken"] = token
            res = await self._rpc("getTransactionsForAddress", [self.address, cfg]) or {}
            rows += res.get("data") or []
            token = res.get("paginationToken")
            if not token or not res.get("data"):
                break
        return rows

    async def _fetch_standard(self, cap: int) -> list:
        cfg: dict = {"limit": 100, "commitment": "confirmed"}
        if self.last_sig:
            cfg["until"] = self.last_sig
        sigs = await self._rpc("getSignaturesForAddress", [self.address, cfg]) or []
        if sigs:
            self.last_sig = sigs[0]["signature"]
        ok = [s["signature"] for s in sigs if not s.get("err")][:min(cap, 60)]
        sem = asyncio.Semaphore(4)

        async def one(sig):
            async with sem:
                try:
                    return await self._rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}])
                except Exception:
                    return None
        return [t for t in await asyncio.gather(*(one(s) for s in ok)) if t]

    async def _fetch(self, since: int | None, cap: int) -> list:
        if self.mode == "helius":
            try:
                return await self._fetch_helius(since, cap)
            except Exception as exc:
                if since and "blockTime" in str(exc):   # provider dislikes the time filter: re-read the latest page, duplicates are skipped
                    return await self._fetch_helius(None, 100)
                if self.polls == 0:   # first ever call failed: this RPC is probably not Helius
                    self.mode = "standard"
                    self.last_error = "Helius method not available on this RPC; using the slower standard method."
                else:
                    raise
        return await self._fetch_standard(cap)

    # ---- processing
    def ingest(self, txs: list) -> int:
        txs = sorted(txs, key=lambda t: (t.get("slot") or 0, t.get("blockTime") or 0))   # oldest first so positions build correctly
        new = 0
        for t in txs:
            try:
                d = decode_transaction(t, self.address)
                keys = _keys(t)
                signed = bool(keys) and keys[0] == self.address
            except Exception:
                self.kinds["ERROR"] += 1
                continue
            ev = self.ledger.apply(d)
            if ev is None:
                continue
            new += 1
            self.checked += 1
            self.foreign += 0 if signed else 1
            self.kinds[d.kind] += 1
            self.reasons[f"{d.kind}: {d.note or 'ok'}"] += 1
            if d.block_time:
                self.newest = max(self.newest or 0, d.block_time)
                self.oldest = min(self.oldest or d.block_time, d.block_time)
            if d.kind in ("BUY", "SELL", "TRANSFER") and d.mint:
                big, why = is_significant(ev, self.thresholds)
                self.events.insert(0, {"cls": ev.classification, "mint": d.mint, "sol": ev.sol_value, "pct": ev.pct_of_position,
                                       "pnl": ev.realized_pnl_sol, "time": d.block_time, "sig": d.signature, "big": big, "why": why})
                if d.kind in ("BUY", "SELL"):
                    self.last_trade = max(self.last_trade or 0, d.block_time or 0)
        del self.events[300:]
        return new

    async def cycle(self) -> None:
        if not self.backfilled:
            self.ingest(await self._fetch(None, self.backfill))
            self.backfilled = True
        else:
            self.ingest(await self._fetch(self.newest, 300))
        self.last_ok = time.time()

    async def run(self) -> None:
        import httpx
        async with httpx.AsyncClient(timeout=25) as c:
            self.client = c
            while True:
                try:
                    await self.cycle()
                    if not self.last_error.startswith("Helius method"):
                        self.last_error = ""
                except asyncio.CancelledError:
                    raise
                except Exception as exc:   # never let one bad poll stop tracking; never echo the RPC URL (holds the API key)
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    self.last_error = (f"{type(exc).__name__}" + (f" (HTTP {status})" if status else "") + f": {exc}")[:200].replace(self.rpc_url, "<rpc>")
                self.polls += 1
                self.last_poll = time.time()
                await asyncio.sleep(self.interval)

    def snapshot(self) -> dict:
        opens = [asdict(p) for p in self.ledger.open.values() if p.qty > EPS]
        opens.sort(key=lambda p: p.get("opened_at") or 0, reverse=True)
        return {"address": self.address, "started": self.started, "last_poll": self.last_poll, "last_ok": self.last_ok, "error": self.last_error,
                "interval": self.interval, "mode": self.mode, "polls": self.polls, "backfilled": self.backfilled, "checked": self.checked,
                "foreign": self.foreign, "kinds": dict(self.kinds), "reasons": self.reasons.most_common(5), "newest": self.newest,
                "oldest": self.oldest, "last_trade": self.last_trade, "events": self.events[:40], "open": opens[:20],
                "closed": len(self.ledger.closed), "closed_pnl": sum(p.realized_pnl_sol for p in self.ledger.closed if p.complete)}


def _ago(ts) -> str:
    if not ts:
        return "never"
    sec = max(0, int(time.time() - ts))
    return f"{sec}s ago" if sec < 90 else f"{sec // 60}m ago" if sec < 5400 else f"{sec // 3600}h ago" if sec < 172800 else f"{sec // 86400}d ago"


def render_live(s: dict | None, rec: "WalletRecord") -> str:
    """HTML for /cupsey from the live tracker state. No network calls here, so refreshing the page costs nothing."""
    e = html.escape
    short = lambda m: e((m or "?")[:5] + "..." + (m or "")[-4:])
    body = []
    if not s:
        body.append('<div class="card warn"><b>Tracker is not running.</b><br>Check that CUPSEY_TRACKING is not set to false, then redeploy.</div>')
    else:
        k = s["kinds"]
        trades = [x for x in s["events"] if x["cls"] != "TRANSFER"]
        up = "Live" if s["last_ok"] and time.time() - s["last_ok"] < s["interval"] * 4 + 30 else "Not reading"
        body.append(f'<div class="card"><b>{up}</b> &middot; reads the wallet every {s["interval"]:.0f}s &middot; last good read {e(_ago(s["last_ok"]))}'
                    f'<div class="sub">Tracking since {e(_ago(s["started"]))} &middot; method: {e(s["mode"])} &middot; this page refreshes itself</div></div>')
        if s["error"]:
            body.append(f'<div class="card warn"><b>Problem reading the blockchain.</b><br>{e(s["error"])}</div>')
        body.append('<div class="card"><div class="big">%d</div>buys/sells seen<div class="sub">%d transactions read &middot; %d transfers &middot; last real trade: %s &middot; newest activity of any kind: %s</div></div>'
                    % (k.get("BUY", 0) + k.get("SELL", 0), s["checked"], k.get("TRANSFER", 0), e(_ago(s["last_trade"])), e(_ago(s["newest"]))))
        if s["open"]:
            rows = "".join('<tr><td><a href="https://solscan.io/token/%s" target="_blank">%s</a></td><td>%.2f SOL</td><td>%.2f SOL</td><td>%s</td></tr>'
                           % (e(p["mint"]), short(p["mint"]), p["cost_sol"], p["bought_sol"], e(_ago(p["opened_at"]))) for p in s["open"])
            body.append(f'<div class="card"><b>Open positions ({len(s["open"])})</b><table><tr><th>Token</th><th>Holding cost</th><th>Bought</th><th>Opened</th></tr>{rows}</table></div>')
        if trades:
            rows = "".join('<tr><td class="%s">%s%s</td><td><a href="https://solscan.io/tx/%s" target="_blank">%s</a></td><td>%.3f</td><td>%s</td></tr>'
                           % ("sell" if "SELL" in x["cls"] else "buy", e(x["cls"].title()), " &#9733;" if x["big"] else "", e(x["sig"]), short(x["mint"]), x["sol"], e(_ago(x["time"])))
                           for x in trades[:25])
            body.append(f'<div class="card"><b>Latest trades</b><table><tr><th>Type</th><th>Token</th><th>SOL</th><th>When</th></tr>{rows}</table>'
                        f'<div class="sub">&#9733; = passes the size filter (placeholder values). SOL sizes are estimates. Tap a token for the transaction.</div></div>')
        else:
            body.append('<div class="card warn"><b>No buys or sells found yet.</b><br>The tracker is reading this wallet continuously and will show a trade here the moment the blockchain shows tokens swapped for SOL. Nothing is guessed.</div>')
        noise = s["checked"] - sum(k.get(x, 0) for x in ("BUY", "SELL", "TRANSFER"))
        if s["checked"] and not trades:
            body.append('<div class="card"><b>Why nothing yet?</b><div class="sub">%d of %d transactions were not signed by this wallet (someone else touching it), and %d had no token trade in them.%s<br>%s</div></div>'
                        % (s["foreign"], s["checked"], noise,
                           " If almost everything is noise, this address is probably NOT his trading wallet: set CUPSEY_WALLET on Render to the right one." if s["foreign"] > 0.8 * s["checked"] else "",
                           e(" | ".join(f"{n} x {r}" for r, n in s["reasons"]))))
    badge = e(rec.status.replace("_", " "))
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="15">
<title>Cupsey monitor</title><style>
:root{{--bg:#0b0b0c;--card:#17181a;--fg:#f2f2f2;--mute:#9a9aa0;--ok:#3ecf8e;--bad:#ff6b6b;--warn:#e9b44c}}
@media(prefers-color-scheme:light){{:root{{--bg:#f4f4f6;--card:#fff;--fg:#111;--mute:#666}}}}
body{{margin:0;padding:16px;font:16px -apple-system,system-ui,sans-serif;background:var(--bg);color:var(--fg)}}
.card{{background:var(--card);border-radius:16px;padding:16px;margin:12px 0}}.warn{{border:1px solid var(--warn)}}
.big{{font-size:44px;font-weight:700}}.sub{{color:var(--mute);font-size:13px;margin-top:6px;word-break:break-all}}
table{{width:100%;border-collapse:collapse;margin-top:8px}}td,th{{text-align:left;padding:8px 4px;font-size:14px}}th{{color:var(--mute)}}
.buy{{color:var(--ok);font-weight:600}}.sell{{color:var(--bad);font-weight:600}}a{{color:#5b9dff}}.badge{{background:#2b2d31;color:var(--warn);border-radius:999px;padding:3px 10px;font-size:12px;display:inline-block}}
</style></head><body><h2>Cupsey monitor</h2>
<div class="card"><div class="badge">{badge}</div><div class="sub">{e(rec.address)}</div>
<div class="sub">Attribution comes from third parties and is not yet confirmed on-chain by us. Phone alerts are not switched on yet; this page is the live view.</div>
<div class="sub"><a href="https://solscan.io/account/{e(rec.address)}" target="_blank">Open on Solscan</a> &middot; <a href="/cupsey">Refresh</a> &middot; <a href="/cupsey/wallets">Wallets</a> &middot; <a href="/dashboard">Old Tosh scanner</a></div></div>
{"".join(body)}</body></html>"""


# ============================================================================
# WEB ROUTES (called once from main.py; protected by the dashboard login)
# ============================================================================
def render_page(out: dict | None, error: str | None, rec: "WalletRecord") -> str:
    """Plain HTML page for /cupsey. Everything dynamic is escaped."""
    e = html.escape
    def ago(ts):
        if not ts:
            return "unknown"
        sec = max(0, int(time.time() - ts))
        return f"{sec // 86400}d ago" if sec >= 86400 else f"{sec // 3600}h ago" if sec >= 3600 else f"{sec // 60}m ago"
    body = []
    if error or not out:
        body.append(f'<div class="card warn"><b>Could not read the blockchain right now.</b><br>{e(error or "unknown error")}</div>')
    else:
        k = out.get("kinds", {})
        trades = [r for r in out.get("recent_activity", []) if r["kind"] in ("BUY", "SELL")]
        newest = out.get("decoded_newest")
        body.append('<div class="card"><div class="big">%d</div>buys/sells found<div class="sub">in the latest %s transactions checked &middot; transfers in/out: %d</div></div>'
                    % (k.get("BUY", 0) + k.get("SELL", 0), e(str((out.get("window") or {}).get("returned", out.get("transactions_decoded", "?")))), k.get("TRANSFER", 0)))
        if not trades:
            body.append('<div class="card warn"><b>No buys or sells found in this window.</b><br>Either he has not traded recently, or this is not his active wallet. We do not guess: nothing is shown as a trade unless the blockchain shows tokens swapped for SOL.</div>')
        else:
            rows = "".join('<tr><td class="%s">%s</td><td>%s</td><td>%s SOL</td><td>%s</td></tr>' % (r["kind"].lower(), r["kind"], e((r["mint"] or "?")[:6] + "..." + (r["mint"] or "")[-4:]),
                           e(f'{abs(r["sol"]):.3f}'), e(ago(r["time"]))) for r in trades[:15])
            body.append(f'<div class="card"><b>Recent trades</b><table><tr><th>Type</th><th>Token</th><th>Size</th><th>When</th></tr>{rows}</table><div class="sub">SOL sizes are estimates.</div></div>')
        body.append(f'<div class="card sub">Window: {e(str(out.get("decoded_oldest")))} to {e(str(out.get("decoded_newest")))} (UTC)<br>Source: {e(str(out.get("source", "")))}<br>Checked: {e(str(out.get("checked_at", ""))[:19])} UTC</div>')
    badge = e(rec.status.replace("_", " "))
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cupsey monitor</title><style>
:root{{--bg:#0b0b0c;--card:#17181a;--fg:#f2f2f2;--mute:#9a9aa0;--ok:#3ecf8e;--bad:#ff6b6b;--warn:#e9b44c}}
@media(prefers-color-scheme:light){{:root{{--bg:#f4f4f6;--card:#fff;--fg:#111;--mute:#666}}}}
body{{margin:0;padding:16px;font:16px -apple-system,system-ui,sans-serif;background:var(--bg);color:var(--fg)}}
.card{{background:var(--card);border-radius:16px;padding:16px;margin:12px 0}}.warn{{border:1px solid var(--warn)}}
.big{{font-size:44px;font-weight:700}}.sub{{color:var(--mute);font-size:13px;margin-top:6px;word-break:break-all}}
table{{width:100%;border-collapse:collapse;margin-top:8px}}td,th{{text-align:left;padding:8px 4px;font-size:14px}}th{{color:var(--mute)}}
.buy{{color:var(--ok);font-weight:600}}.sell{{color:var(--bad);font-weight:600}}a{{color:#5b9dff}}.badge{{background:#2b2d31;color:var(--warn);border-radius:999px;padding:3px 10px;font-size:12px}}
</style></head><body><h2>Cupsey monitor</h2>
<div class="card"><div class="badge">{badge}</div><div class="sub">{e(rec.address)}</div>
<div class="sub">Attribution comes from third parties and is not yet confirmed on-chain by us. Live alerts are not switched on yet.</div>
<div class="sub"><a href="https://solscan.io/account/{e(rec.address)}" target="_blank">Open on Solscan</a> &middot; <a href="/cupsey">Refresh</a> &middot; <a href="/cupsey/wallets">Wallets</a> &middot; <a href="/dashboard">Old Tosh scanner</a></div></div>
{"".join(body)}</body></html>"""


def register_cupsey_routes(app, auth) -> None:
    """Adds /cupsey (live page), /cupsey/wallets and /cupsey/verify, and starts the background tracker with the app."""
    import asyncio
    from contextlib import asynccontextmanager
    from fastapi import Depends, Query, Request
    from fastapi.responses import HTMLResponse, JSONResponse

    registry = seed_registry()
    if not registry.get(DEFAULT_ADDRESS):   # wallet chosen through CUPSEY_WALLET
        registry.add(WalletRecord(DEFAULT_ADDRESS, "Cupsey (set by CUPSEY_WALLET)", "POSSIBLY_ASSOCIATED", notes="Address supplied by the owner; not verified."))
    tracker: Tracker | None = None
    if os.getenv("CUPSEY_TRACKING", "true").lower() not in ("0", "false", "no", "off"):
        tracker = Tracker(DEFAULT_ADDRESS, app.state.settings.solana_rpc_url, float(os.getenv("CUPSEY_POLL_SECONDS", "20")),
                          int(os.getenv("CUPSEY_BACKFILL_TX", "1000")))
        inner = app.router.lifespan_context

        @asynccontextmanager
        async def with_tracker(a):
            async with inner(a) as state:
                task = asyncio.create_task(tracker.run(), name="cupsey-tracker")
                try:
                    yield state
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        app.router.lifespan_context = with_tracker

    @app.get("/cupsey", include_in_schema=False, dependencies=[Depends(auth)])
    async def cupsey_page():
        return HTMLResponse(render_live(tracker.snapshot() if tracker else None, registry.get(DEFAULT_ADDRESS)), headers={"Cache-Control": "no-store"})

    @app.get("/cupsey/wallets", include_in_schema=False, dependencies=[Depends(auth)])
    async def cupsey_wallets():
        return JSONResponse([asdict(r) for r in registry.all()])

    @app.get("/cupsey/verify", include_in_schema=False, dependencies=[Depends(auth)])
    async def cupsey_verify(request: Request, address: str = DEFAULT_ADDRESS, limit: int = Query(40, ge=5, le=100), scan: int = Query(2000, ge=50, le=5000), download: bool = False):
        url = request.app.state.settings.solana_rpc_url
        try:
            out = await asyncio.to_thread(run_verification, address, url, limit, scan)
        except Exception as exc:   # never echo the RPC URL (it may contain your API key)
            status = getattr(getattr(exc, "response", None), "status_code", None)
            msg = f"{type(exc).__name__}" + (f" (HTTP {status})" if status else "") + (f": {exc}" if isinstance(exc, (ValueError, RuntimeError)) else "")
            return JSONResponse({"error": msg.replace(url, "<rpc>")}, status_code=502)
        headers = {"Content-Disposition": f'attachment; filename="{address}_evidence.json"'} if download else {}
        return JSONResponse(out, headers=headers)


def _cli(argv) -> int:
    if argv[:1] == ["test"]:
        suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    if argv[:1] == ["verify"]:
        return verify(argv[1:])
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
