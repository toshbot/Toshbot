"""cupsey.py — read-only Cupsey wallet monitor.

Run:
  python cupsey.py test
  python cupsey.py verify [address] [--limit 40] [--scan 2000]

No wallet keys, signing, or trading. Verification never changes wallet status.
SOL amounts are estimates and may include rent, refunds, tips, and fees.
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

STATUS_RANK = {"RETIRED": -1, "UNVERIFIED": 0, "POSSIBLY_ASSOCIATED": 1, "STRONGLY_SUPPORTED": 2, "CONFIRMED": 3}
_B58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
CONFIRM_KINDS = {"self_confirmation", "on_chain_correlation"}
LAMPORTS = 1_000_000_000
WSOL = "So11111111111111111111111111111111111111112"
MIN_SWAP_SOL = 0.003
DUST_SOL = 0.0001
SEED_ADDRESS = "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f"
DEFAULT_ADDRESS = (os.getenv("CUPSEY_WALLET") or SEED_ADDRESS).strip()
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"


@dataclass
class Evidence:
    kind: str
    source: str
    summary: str
    url: str = ""
    observed: str = ""
    independent: bool = False


@dataclass
class WalletRecord:
    address: str
    label: str
    status: str = "UNVERIFIED"
    evidence: list = field(default_factory=list)
    notes: str = ""


class WalletRegistry:
    def __init__(self):
        self._w = {}

    def add(self, rec):
        if not _B58.match(rec.address):
            raise ValueError(f"not a valid base58 address: {rec.address!r}")
        if rec.status not in STATUS_RANK:
            raise ValueError(f"unknown status {rec.status}")
        if rec.status == "CONFIRMED" and not any(e.kind in CONFIRM_KINDS for e in rec.evidence):
            raise ValueError("CONFIRMED needs self_confirmation or on_chain_correlation evidence")
        self._w[rec.address] = rec

    def get(self, address):
        return self._w.get(address)

    def add_evidence(self, address, ev):
        self._w[address].evidence.append(ev)

    def promote(self, address, status):
        rec = self._w[address]
        if status not in STATUS_RANK:
            raise ValueError(f"unknown status {status}")
        if status == "CONFIRMED" and not any(e.kind in CONFIRM_KINDS for e in rec.evidence):
            raise ValueError("cannot confirm without self_confirmation or on_chain_correlation evidence")
        rec.status = status

    def alert_eligible(self, address, min_status=None):
        min_status = min_status or os.getenv("CUPSEY_MIN_ALERT_STATUS", "STRONGLY_SUPPORTED")
        rec = self._w.get(address)
        return bool(rec) and rec.status != "RETIRED" and STATUS_RANK[rec.status] >= STATUS_RANK[min_status]

    def all(self):
        return list(self._w.values())

    def monitored(self):
        return [a for a, r in self._w.items() if STATUS_RANK[r.status] >= STATUS_RANK["POSSIBLY_ASSOCIATED"]]


def seed_registry():
    reg = WalletRegistry()
    reg.add(WalletRecord(
        SEED_ADDRESS, "Cupsey (attributed wallet; not independently confirmed)", "POSSIBLY_ASSOCIATED",
        [
            Evidence("social_claim", "X @dippy_eth", "Pointed to this address as Cupsey's new wallet after the Sep 2025 drain",
                     "https://x.com/dippy_eth/status/1968435704096010348", "2026-11-06"),
            Evidence("aggregator", "Seer KOL Tracker", "Posts Cupsey-labelled swaps from this wallet",
                     "https://x.com/Seer_Tracker/status/1996090573480988952", "2026-11-06"),
            Evidence("aggregator", "KOL Explorer", "Lists this address as Cupsey's wallet",
                     "https://kolexplorer.com/kol/cupsey", "2026-11-06"),
            Evidence("third_party_label", "Solscan label", "Third-party account label; not self-confirmation",
                     f"https://solscan.io/account/{SEED_ADDRESS}", "2026-11-06"),
        ],
        "Third-party attribution only. Needs independent on-chain correlation; status is deliberately not confirmed."
    ))
    reg.add(WalletRecord(
        "DiLBRo3fAvzWcd7sp3ZHHX1sWMnmG7nBi6gAfZrgs7Qg", "Cupsey (old wallet, reportedly drained Sep 2025)", "RETIRED",
        [Evidence("self_confirmation", "X @Cupseyy", "Posted as the wallet that was drained",
                 "https://x.com/Cupseyy/status/1967955345999016361", "2026-11-06", True)],
        "Historical context only. Never alert from it."
    ))
    return reg


def _message(tx):
    return ((tx or {}).get("transaction") or {}).get("message") or {}


def _keys(tx):
    keys = []
    for item in _message(tx).get("accountKeys") or []:
        if isinstance(item, dict):
            keys.append(item.get("pubkey") or item.get("key") or "")
        else:
            keys.append(str(item))
    return keys


def _is_signer(tx, wallet):
    """Check signer metadata; legacy string keys use message.header.numRequiredSignatures."""
    msg = _message(tx)
    keys = msg.get("accountKeys") or []
    try:
        required = int((msg.get("header") or {}).get("numRequiredSignatures", 0))
    except (TypeError, ValueError):
        required = 0
    for i, item in enumerate(keys):
        if isinstance(item, dict):
            pubkey = item.get("pubkey") or item.get("key")
            if pubkey == wallet:
                return bool(item.get("signer", False))
        elif str(item) == wallet:
            return i < required
    return False


def _token_totals(meta, wallet):
    """Sum wallet-owned token balances by mint, using raw integer amounts."""
    out = {}
    for side, idx in (("preTokenBalances", 0), ("postTokenBalances", 1)):
        for b in meta.get(side) or []:
            if b.get("owner") != wallet:
                continue
            ui = b.get("uiTokenAmount") or {}
            mint = b.get("mint")
            if not mint:
                continue
            try:
                amount, decimals = int(ui.get("amount", 0)), int(ui.get("decimals", 0))
            except (TypeError, ValueError):
                continue
            rec = out.setdefault(mint, [0, 0, decimals])
            rec[idx] += amount
    return out


@dataclass(frozen=True)
class DecodedTx:
    signature: str
    wallet: str
    slot: int | None
    block_time: int | None
    kind: str
    mint: str | None = None
    token_delta: float = 0.0
    pre_token_amount: float | None = None
    post_token_amount: float | None = None
    quote_sol: float = 0.0
    fee_sol: float = 0.0
    legs: tuple = field(default_factory=tuple)
    note: str = ""

    @property
    def price_sol(self):
        if self.kind in ("BUY", "SELL") and self.token_delta:
            return abs(self.quote_sol) / abs(self.token_delta)
        return None


def decode_transaction(tx, wallet):
    msg = _message(tx)
    signatures = ((tx or {}).get("transaction") or {}).get("signatures") or ["?"]
    base = dict(signature=signatures[0], wallet=wallet, slot=(tx or {}).get("slot"),
                block_time=(tx or {}).get("blockTime"))
    meta = (tx or {}).get("meta")
    if not msg or meta is None:
        return DecodedTx(kind="UNKNOWN", note="missing transaction or meta", **base)
    if meta.get("err") is not None:
        return DecodedTx(kind="OTHER", note="failed transaction (no balance change counted)", **base)

    keys = _keys(tx)
    try:
        fee_lamports = int(meta.get("fee") or 0)
        fee_payer = bool(keys) and keys[0] == wallet
        fee = fee_lamports / LAMPORTS if fee_payer else 0.0
        native = 0.0
        if wallet in keys:
            i = keys.index(wallet)
            pre, post = meta.get("preBalances") or [], meta.get("postBalances") or []
            if i < len(pre) and i < len(post):
                native = (int(post[i]) - int(pre[i])) / LAMPORTS + fee
    except (TypeError, ValueError, IndexError):
        return DecodedTx(kind="UNKNOWN", note="malformed SOL balance metadata", **base)

    totals = _token_totals(meta, wallet)
    wsol = totals.get(WSOL)
    wsol_delta = (wsol[1] - wsol[0]) / (10 ** wsol[2]) if wsol else 0.0
    quote = native + wsol_delta
    deltas = {m: (post - pre) / (10 ** dec)
              for m, (pre, post, dec) in totals.items() if m != WSOL and post != pre}
    common = dict(quote_sol=quote, fee_sol=fee, **base)
    if not deltas:
        return DecodedTx(kind="OTHER", note="no wallet-owned non-WSOL token balance change", **common)
    if len(deltas) > 1:
        legs = tuple(sorted(deltas.items()))
        signs = {v > 0 for v in deltas.values()}
        note = "token-to-token or multi-token movement; legs recorded" if len(signs) == 2 else "multiple token balances moved in the same direction"
        return DecodedTx(kind="OTHER", legs=legs, note=note, **common)

    (mint, delta), = deltas.items()
    pre_raw, post_raw, decimals = totals[mint]
    values = dict(mint=mint, token_delta=delta, pre_token_amount=pre_raw / (10 ** decimals),
                  post_token_amount=post_raw / (10 ** decimals), **common)
    if delta > 0:
        if quote <= -MIN_SWAP_SOL:
            return DecodedTx(kind="BUY", **values)
        if abs(quote) < DUST_SOL:
            return DecodedTx(kind="TRANSFER", note="tokens received with no material SOL payment", **values)
    else:
        if quote >= MIN_SWAP_SOL:
            return DecodedTx(kind="SELL", **values)
        if abs(quote) < DUST_SOL:
            return DecodedTx(kind="TRANSFER", note="tokens sent with no material SOL proceeds", **values)
    return DecodedTx(kind="UNKNOWN", note="ambiguous SOL movement; logged but not treated as a trade", **values)


EPS = 1e-6


@dataclass
class Position:
    wallet: str
    mint: str
    qty: float = 0.0
    cost_sol: float = 0.0
    bought_sol: float = 0.0
    sold_sol: float = 0.0
    realized_pnl_sol: float = 0.0
    fees_sol: float = 0.0
    buys: int = 0
    sells: int = 0
    opened_at: int | None = None
    closed_at: int | None = None
    status: str = "OPEN"
    complete: bool = True
    notes: list = field(default_factory=list)


@dataclass
class TradeEvent:
    signature: str
    wallet: str
    mint: str | None
    classification: str
    block_time: int | None
    sol_value: float = 0.0
    price_sol: float | None = None
    token_amount: float = 0.0
    pct_of_position: float | None = None
    realized_pnl_sol: float | None = None
    position: dict | None = None
    note: str = ""


class Ledger:
    def __init__(self):
        self.open, self.closed, self.seen = {}, [], set()

    def apply(self, d):
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

    def _ensure(self, d, key):
        pos = Position(d.wallet, d.mint, opened_at=d.block_time)
        self.open[key] = pos
        return pos

    def _buy(self, d, ev, key, pos):
        pre = d.pre_token_amount or 0.0
        existing = pos is not None and pos.qty > EPS
        if pos is None:
            pos = self._ensure(d, key)
            if pre > EPS:
                pos.qty, pos.complete = pre, False
                pos.notes.append("held tokens before tracked history; cost basis unknown")
                existing = True
        amt, spent = abs(d.token_delta), abs(d.quote_sol)
        pos.qty += amt
        pos.cost_sol += spent
        pos.bought_sol += spent
        pos.fees_sol += d.fee_sol
        pos.buys += 1
        ev.classification = "ADDITIONAL BUY" if existing else "BUY"
        ev.sol_value, ev.price_sol, ev.token_amount = spent, d.price_sol, amt
        ev.pct_of_position = 100.0 if not existing else 100.0 * amt / pos.qty
        ev.position = asdict(pos)
        return ev

    def _sell(self, d, ev, key, pos):
        sold, proceeds = abs(d.token_delta), d.quote_sol
        if pos is None or pos.qty <= EPS:
            pos = self._ensure(d, key)
            pos.complete = False
            pos.notes.append("sale without tracked buy; cost basis unknown")
            pos.qty = d.pre_token_amount if d.pre_token_amount is not None else sold
        qty_before = pos.qty
        known = min(sold, pos.qty)
        if known < sold - EPS:
            pos.complete = False
            pos.notes.append("sold more than tracked holdings; history incomplete")
        basis = pos.cost_sol * (known / pos.qty) if pos.qty > EPS else 0.0
        proceeds_known = proceeds * (known / sold) if sold > EPS else 0.0
        pnl = proceeds_known - basis if pos.cost_sol > 0 or pos.complete else None
        pos.qty -= known
        pos.cost_sol -= basis
        pos.sold_sol += proceeds
        pos.fees_sol += d.fee_sol
        pos.sells += 1
        if pnl is not None:
            pos.realized_pnl_sol += pnl
        full = (d.post_token_amount <= EPS) if d.post_token_amount is not None else pos.qty <= EPS
        ev.sol_value, ev.price_sol, ev.token_amount, ev.realized_pnl_sol = proceeds, d.price_sol, sold, pnl
        ev.pct_of_position = 100.0 * known / qty_before if qty_before > EPS else None
        if full:
            pos.qty, pos.cost_sol, pos.status, pos.closed_at = 0.0, 0.0, "CLOSED", d.block_time
            self.closed.append(pos)
            self.open.pop(key, None)
            ev.classification = "FULL SELL"
        else:
            ev.classification = "PARTIAL SELL"
        ev.position = asdict(pos)
        return ev

    def _transfer(self, d, ev, key, pos):
        pos = pos or self._ensure(d, key)
        pos.complete = False
        if d.token_delta > 0:
            pos.qty += d.token_delta
            pos.notes.append("tokens transferred in at unknown cost")
        else:
            moved = min(abs(d.token_delta), pos.qty)
            if pos.qty > EPS:
                pos.cost_sol -= pos.cost_sol * moved / pos.qty
            pos.qty -= moved
            pos.notes.append("tokens transferred out; not treated as a sale")
        ev.token_amount, ev.position = abs(d.token_delta), asdict(pos)
        return ev


def _f(name, default):
    return float(os.getenv(name, default))


@dataclass
class Thresholds:
    new_position_min_sol: float = 1.0
    add_min_sol: float = 1.0
    add_min_pct: float = 25.0
    sell_min_sol: float = 1.0
    sell_min_pct: float = 25.0
    full_exit_min_sol: float = 0.5

    @classmethod
    def from_env(cls):
        return cls(_f("CUPSEY_NEW_MIN_SOL", 1.0), _f("CUPSEY_ADD_MIN_SOL", 1.0),
                   _f("CUPSEY_ADD_MIN_PCT", 25.0), _f("CUPSEY_SELL_MIN_SOL", 1.0),
                   _f("CUPSEY_SELL_MIN_PCT", 25.0), _f("CUPSEY_FULL_EXIT_MIN_SOL", 0.5))


def is_significant(ev, t):
    c, v, p = ev.classification, ev.sol_value, ev.pct_of_position or 0.0
    if c == "BUY":
        return v >= t.new_position_min_sol, f"new position {v:.2f} SOL (min {t.new_position_min_sol})"
    if c == "ADDITIONAL BUY":
        return v >= t.add_min_sol and p >= t.add_min_pct, f"add {v:.2f} SOL = {p:.0f}% (min {t.add_min_sol} SOL and {t.add_min_pct}%)"
    if c == "PARTIAL SELL":
        return v >= t.sell_min_sol and p >= t.sell_min_pct, f"partial sell {v:.2f} SOL = {p:.0f}% (min {t.sell_min_sol} SOL and {t.sell_min_pct}%)"
    if c == "FULL SELL":
        return v >= t.full_exit_min_sol, f"full exit {v:.2f} SOL (min {t.full_exit_min_sol})"
    return False, "not a trade"


# Synthetic fixtures and regression tests.
W = SEED_ADDRESS
M1, M2 = "A" * 44, "B" * 44
_n = [0]


def tb(mint, raw, dec=6, owner=W, idx=1):
    return {"accountIndex": idx, "mint": mint, "owner": owner,
            "uiTokenAmount": {"amount": str(raw), "decimals": dec}}


def tx(pre_sol, post_sol, pre_tb=(), post_tb=(), fee=5000, err=None, slot=1):
    _n[0] += 1
    return {"slot": slot, "blockTime": 1000 + _n[0],
            "transaction": {"signatures": [f"SIG{_n[0]}"], "message": {
                "header": {"numRequiredSignatures": 1},
                "accountKeys": [{"pubkey": W, "signer": True, "writable": True}, {"pubkey": "X" * 44}]}},
            "meta": {"err": err, "fee": fee, "preBalances": [int(pre_sol * 1e9), 0],
                     "postBalances": [int(post_sol * 1e9), 0],
                     "preTokenBalances": list(pre_tb), "postTokenBalances": list(post_tb)}}


def buy(sol, tokens, pre=0, mint=M1):
    return tx(10, 10 - sol - 0.000005, [tb(mint, pre * 10**6)] if pre else [],
              [tb(mint, (pre + tokens) * 10**6)])


def sell(sol, tokens, pre, mint=M1):
    return tx(10, 10 + sol - 0.000005, [tb(mint, pre * 10**6)],
              [tb(mint, (pre - tokens) * 10**6)])


class DecoderTests(unittest.TestCase):
    def test_buy(self):
        d = decode_transaction(buy(1, 1000), W)
        self.assertEqual(d.kind, "BUY")
        self.assertAlmostEqual(d.quote_sol, -1, 6)

    def test_sell(self):
        self.assertEqual(decode_transaction(sell(2, 1000, 1000), W).kind, "SELL")

    def test_transfer(self):
        self.assertEqual(decode_transaction(tx(10, 10, [], [tb(M1, 5_000_000)], fee=0), W).kind, "TRANSFER")

    def test_failed(self):
        self.assertEqual(decode_transaction(tx(10, 9, [], [tb(M1, 1_000_000)], err={"x": 1}), W).kind, "OTHER")

    def test_wsol_buy(self):
        t = tx(10, 10, [tb(WSOL, 2_000_000_000, 9)], [tb(WSOL, 1_000_000_000, 9), tb(M1, 500_000_000)])
        self.assertEqual(decode_transaction(t, W).kind, "BUY")

    def test_ambiguous_is_unknown(self):
        self.assertEqual(decode_transaction(tx(10, 9.99796, [], [tb(M1, 1_000_000)], fee=0), W).kind, "UNKNOWN")

    def test_signer_is_not_fee_payer(self):
        t = tx(10, 10, fee=0)
        t["transaction"]["message"]["accountKeys"] = [
            {"pubkey": "S" * 44, "signer": True}, {"pubkey": W, "signer": True}
        ]
        t["transaction"]["message"]["header"]["numRequiredSignatures"] = 2
        self.assertTrue(_is_signer(t, W))
        self.assertNotEqual(_keys(t)[0], W)

    def test_non_signer(self):
        t = tx(10, 10, fee=0)
        t["transaction"]["message"]["accountKeys"] = [
            {"pubkey": "S" * 44, "signer": True}, {"pubkey": W, "signer": False}
        ]
        self.assertFalse(_is_signer(t, W))

    def test_missing_meta(self):
        self.assertEqual(decode_transaction({"transaction": {"message": {}, "signatures": ["s"]}}, W).kind, "UNKNOWN")

    def test_other_owner_ignored(self):
        self.assertEqual(decode_transaction(tx(10, 10, [], [tb(M1, 1_000_000, owner="Z" * 44)], fee=0), W).kind, "OTHER")


class LedgerTests(unittest.TestCase):
    def test_lifecycle_and_dedupe(self):
        ledger = Ledger()
        events = [ledger.apply(decode_transaction(t, W)) for t in
                  (buy(1, 1000), buy(1, 500, 1000), sell(1.5, 750, 1500), sell(.5, 750, 750))]
        self.assertEqual([e.classification for e in events], ["BUY", "ADDITIONAL BUY", "PARTIAL SELL", "FULL SELL"])
        self.assertEqual(len(ledger.closed), 1)
        self.assertIsNone(ledger.apply(decode_transaction(buy(1, 100), W)) if False else None)

    def test_transfer_not_sale(self):
        ledger = Ledger()
        ledger.apply(decode_transaction(buy(1, 1000), W))
        d = decode_transaction(tx(10, 10, [tb(M1, 1_000_000_000)], [tb(M1, 400_000_000)], fee=0), W)
        self.assertEqual(ledger.apply(d).classification, "TRANSFER")


class RegistryTests(unittest.TestCase):
    def test_seed_wallet_not_confirmed(self):
        r = seed_registry()
        self.assertFalse(r.alert_eligible(SEED_ADDRESS, "CONFIRMED"))
        self.assertEqual(r.get(SEED_ADDRESS).status, "POSSIBLY_ASSOCIATED")

    def test_cannot_confirm_without_proof(self):
        r = seed_registry()
        with self.assertRaises(ValueError):
            r.promote(SEED_ADDRESS, "CONFIRMED")


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
    raise RuntimeError("rate limited by RPC provider")


def _diagnostic_sample(t, wallet, decoded):
    msg, meta = _message(t), (t or {}).get("meta") or {}
    raw_keys = msg.get("accountKeys") or []
    keys = []
    for i, item in enumerate(raw_keys[:40]):
        if isinstance(item, dict):
            keys.append({"pubkey": item.get("pubkey") or item.get("key"),
                         "signer": item.get("signer"), "writable": item.get("writable")})
        else:
            keys.append({"pubkey": str(item), "signer": i < int((msg.get("header") or {}).get("numRequiredSignatures", 0)),
                         "writable": None})
    return {
        "signature": decoded.signature, "wallet_in_account_keys": wallet in _keys(t),
        "wallet_is_signer": _is_signer(t, wallet), "message_header": msg.get("header"),
        "account_keys": keys, "fee_lamports": meta.get("fee"),
        "pre_balances_lamports": meta.get("preBalances"),
        "post_balances_lamports": meta.get("postBalances"),
        "pre_token_balances": meta.get("preTokenBalances"),
        "post_token_balances": meta.get("postTokenBalances"), "meta_error": meta.get("err"),
        "decoder": {"kind": decoded.kind, "mint": decoded.mint, "token_delta": decoded.token_delta,
                    "quote_sol": decoded.quote_sol, "fee_sol": decoded.fee_sol, "note": decoded.note,
                    "legs": list(decoded.legs)}
    }


def run_verification(address, rpc_url, limit=40, scan=2000):
    """Read-only evidence scan. Captures three raw transaction diagnostics; never promotes status."""
    import httpx
    if not _B58.match(address):
        raise ValueError("not a valid Solana address")
    kinds, mints, inflows, samples, times, errors = Counter(), set(), Counter(), [], [], []
    reasons, other_samples, recent, diagnostics = Counter(), [], [], []

    def ingest(t):
        d = decode_transaction(t, address)
        if len(diagnostics) < 3:
            diagnostics.append(_diagnostic_sample(t, address, d))
        incoming = []
        for ins in _message(t).get("instructions", []):
            parsed = ins.get("parsed")
            if ins.get("program") == "system" and isinstance(parsed, dict) and parsed.get("type") == "transfer":
                inf = parsed.get("info") or {}
                if inf.get("destination") == address:
                    incoming.append((inf.get("source", "?"), int(inf.get("lamports", 0)) / LAMPORTS))
        kinds[d.kind] += 1
        reasons[f"{d.kind}: {d.note or 'ok'}"] += 1
        if d.kind in ("BUY", "SELL", "TRANSFER") and len(recent) < 25:
            recent.append({"kind": d.kind, "mint": d.mint, "sol": round(d.quote_sol, 6),
                           "time": d.block_time, "signature": d.signature})
        if d.kind in ("OTHER", "UNKNOWN") and len(other_samples) < 5:
            other_samples.append({"signature": d.signature, "kind": d.kind, "note": d.note,
                                  "quote_sol": round(d.quote_sol, 6)})
        if d.block_time:
            times.append(d.block_time)
        if d.mint:
            mints.add(d.mint)
        if d.kind in ("BUY", "SELL") and len(samples) < 15:
            samples.append({"signature": d.signature, "kind": d.kind, "mint": d.mint,
                            "sol": round(d.quote_sol, 6),
                            "time_utc": datetime.fromtimestamp(d.block_time, timezone.utc).isoformat() if d.block_time else None})
        for src, sol in incoming:
            inflows[src] += sol

    def safe_ingest(t):
        try:
            ingest(t)
        except Exception as exc:
            kinds["ERROR"] += 1
            msg = f"{type(exc).__name__}: {exc}"[:160].replace(rpc_url, "<rpc>")
            if msg not in errors and len(errors) < 3:
                errors.append(msg)

    with httpx.Client(timeout=25) as client:
        data = None
        # Helius tokenTransfer filter was rejected by the provider. Use only the
        # supported balanceChanged filter, then fall back to standard Solana RPC.
        flt = {"status": "succeeded", "tokenAccounts": "balanceChanged"}
        try:
            rows, token = [], None
            helius_max = max(100, min(scan, 500))
            while len(rows) < helius_max:
                cfg = {"transactionDetails": "full", "encoding": "jsonParsed",
                       "maxSupportedTransactionVersion": 1, "sortOrder": "desc",
                       "limit": min(100, helius_max - len(rows)), "commitment": "confirmed", "filters": flt}
                if token:
                    cfg["paginationToken"] = token
                result = _rpc(client, rpc_url, "getTransactionsForAddress", [address, cfg]) or {}
                page = result.get("data") or []
                rows.extend(page)
                token = result.get("paginationToken")
                if not token or not page:
                    break
            data = rows
            source = "helius_filtered: successful transactions with wallet token balance changes"
        except Exception as exc:
            source = "standard_signatures (Helius method/filter unavailable)"
            errors.append(f"Helius fetch: {type(exc).__name__}: {exc}"[:160].replace(rpc_url, "<rpc>"))

        if data is not None:
            for item in data:
                safe_ingest(item)
            window = {"returned": len(data)}
        else:
            sigs, before = [], None
            while len(sigs) < scan:
                cfg = {"limit": min(100, scan - len(sigs)), "commitment": "confirmed"}
                if before:
                    cfg["before"] = before
                page = _rpc(client, rpc_url, "getSignaturesForAddress", [address, cfg]) or []
                if not page:
                    break
                sigs.extend(page)
                before = page[-1]["signature"]
                if len(page) < cfg["limit"] or len(sigs) >= max(3 * limit, limit):
                    break
            ok = [x for x in sigs if not x.get("err")]
            fails = [x for x in sigs if x.get("err")]
            for sg in ok[::max(1, len(ok) // max(1, limit))][:limit]:
                try:
                    item = _rpc(client, rpc_url, "getTransaction",
                                [sg["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 255,
                                                   "commitment": "confirmed"}])
                    if item:
                        safe_ingest(item)
                    else:
                        kinds["MISSING"] += 1
                except Exception as exc:
                    kinds["ERROR"] += 1
                    if len(errors) < 3:
                        errors.append(f"{type(exc).__name__}: {exc}"[:160].replace(rpc_url, "<rpc>"))
            sig_times = [x["blockTime"] for x in sigs if x.get("blockTime")]
            window = {"scanned": len(sigs), "failed": len(fails), "succeeded": len(ok),
                      "failed_error_types": dict(Counter(str(x.get("err"))[:60] for x in fails).most_common(3)),
                      "span_seconds": max(sig_times) - min(sig_times) if sig_times else None}

    iso = lambda x: datetime.fromtimestamp(x, timezone.utc).isoformat()
    return {"address": address, "checked_at": datetime.now(timezone.utc).isoformat(), "source": source,
            "window": window, "kinds": dict(kinds), "reasons": dict(reasons), "other_samples": other_samples,
            "debug_transaction_samples": diagnostics, "errors_sample": errors, "distinct_tokens": len(mints),
            "decoded_oldest": iso(min(times)) if times else None, "decoded_newest": iso(max(times)) if times else None,
            "rpc_is_public_default": rpc_url == PUBLIC_RPC,
            "largest_sol_inflow_sources": [{"source": k, "sol": round(v, 3)} for k, v in inflows.most_common(5)],
            "sample_trades": samples, "recent_activity": recent,
            "next_steps": ["Inspect debug_transaction_samples to validate the raw balance metadata and decoder output",
                           "Compare sample trade times with independently verified Cupsey activity",
                           "Do not promote wallet attribution without independent evidence"]}


def verify(argv):
    ap = argparse.ArgumentParser(prog="cupsey.py verify")
    ap.add_argument("address", nargs="?", default=DEFAULT_ADDRESS)
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--scan", type=int, default=2000)
    args = ap.parse_args(argv)
    url = os.getenv("SOLANA_RPC_URL") or PUBLIC_RPC
    out = run_verification(args.address, url, args.limit, args.scan)
    path = f"{args.address}_evidence.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print(json.dumps({k: out[k] for k in ("source", "window", "kinds", "decoded_newest")}, indent=2))
    print("wrote", path)
    return 0


class Tracker:
    def __init__(self, address, rpc_url, interval=20.0, backfill=1000, thresholds=None):
        self.address, self.rpc_url = address, rpc_url
        self.interval, self.backfill = max(5.0, interval), max(100, backfill)
        self.thresholds = thresholds or Thresholds.from_env()
        self.ledger, self.events = Ledger(), []
        self.kinds, self.reasons = Counter(), Counter()
        self.checked = self.polls = self.foreign = 0  # foreign = wallet was not a transaction signer
        self.started = time.time()
        self.last_poll = self.last_ok = self.newest = self.oldest = self.last_trade = None
        self.last_error = ""
        self.mode, self.backfilled, self.last_sig, self.client = "helius", False, None, None

    async def _rpc(self, method, params):
        for attempt in range(4):
            response = await self.client.post(self.rpc_url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            if response.status_code == 429:
                await asyncio.sleep(1 + attempt)
                continue
            response.raise_for_status()
            body = response.json()
            if "error" in body:
                raise RuntimeError("rpc error: " + str(body["error"])[:150])
            return body["result"]
        raise RuntimeError("rate limited by RPC provider")

    async def _fetch_helius(self, since, cap):
        rows, token = [], None
        filters = {"status": "succeeded", "tokenAccounts": "balanceChanged"}
        if since:
            filters["blockTime"] = {"gte": since}
        while len(rows) < cap:
            cfg = {"transactionDetails": "full", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 1,
                   "sortOrder": "desc", "limit": min(100, cap - len(rows)), "commitment": "confirmed", "filters": filters}
            if token:
                cfg["paginationToken"] = token
            result = await self._rpc("getTransactionsForAddress", [self.address, cfg]) or {}
            page = result.get("data") or []
            rows.extend(page)
            token = result.get("paginationToken")
            if not token or not page:
                break
        return rows

    async def _fetch_standard(self, cap):
        cfg = {"limit": 100, "commitment": "confirmed"}
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
        return [item for item in await asyncio.gather(*(one(sig) for sig in ok)) if item]

    async def _fetch(self, since, cap):
        if self.mode == "helius":
            try:
                return await self._fetch_helius(since, cap)
            except Exception as exc:
                if since and "blockTime" in str(exc):
                    return await self._fetch_helius(None, 100)
                if self.polls == 0:
                    self.mode = "standard"
                    self.last_error = "Helius enhanced transaction method unavailable; using standard RPC."
                else:
                    raise
        return await self._fetch_standard(cap)

    def ingest(self, txs):
        txs = sorted(txs, key=lambda item: (item.get("slot") or 0, item.get("blockTime") or 0))
        new = 0
        for item in txs:
            try:
                decoded = decode_transaction(item, self.address)
                signed = _is_signer(item, self.address)
            except Exception:
                self.kinds["ERROR"] += 1
                continue
            event = self.ledger.apply(decoded)
            if event is None:
                continue
            new += 1
            self.checked += 1
            self.foreign += 0 if signed else 1
            self.kinds[decoded.kind] += 1
            self.reasons[f"{decoded.kind}: {decoded.note or 'ok'}"] += 1
            if decoded.block_time:
                self.newest = max(self.newest or 0, decoded.block_time)
                self.oldest = min(self.oldest or decoded.block_time, decoded.block_time)
            if decoded.kind in ("BUY", "SELL", "TRANSFER") and decoded.mint:
                big, why = is_significant(event, self.thresholds)
                self.events.insert(0, {"cls": event.classification, "mint": decoded.mint, "sol": event.sol_value,
                                       "pct": event.pct_of_position, "pnl": event.realized_pnl_sol,
                                       "time": decoded.block_time, "sig": decoded.signature, "big": big, "why": why})
                if decoded.kind in ("BUY", "SELL"):
                    self.last_trade = max(self.last_trade or 0, decoded.block_time or 0)
        del self.events[300:]
        return new

    async def cycle(self):
        if not self.backfilled:
            self.ingest(await self._fetch(None, self.backfill))
            self.backfilled = True
        else:
            self.ingest(await self._fetch(self.newest, 300))
        self.last_ok = time.time()

    async def run(self):
        import httpx
        async with httpx.AsyncClient(timeout=25) as client:
            self.client = client
            while True:
                try:
                    await self.cycle()
                    if not self.last_error.startswith("Helius enhanced"):
                        self.last_error = ""
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    self.last_error = (f"{type(exc).__name__}" + (f" (HTTP {status})" if status else "") +
                                       f": {exc}")[:200].replace(self.rpc_url, "<rpc>")
                self.polls += 1
                self.last_poll = time.time()
                await asyncio.sleep(self.interval)

    def snapshot(self):
        opens = [asdict(p) for p in self.ledger.open.values() if p.qty > EPS]
        opens.sort(key=lambda p: p.get("opened_at") or 0, reverse=True)
        return {"address": self.address, "started": self.started, "last_poll": self.last_poll,
                "last_ok": self.last_ok, "error": self.last_error, "interval": self.interval,
                "mode": self.mode, "polls": self.polls, "backfilled": self.backfilled,
                "checked": self.checked, "foreign": self.foreign, "kinds": dict(self.kinds),
                "reasons": self.reasons.most_common(5), "newest": self.newest, "oldest": self.oldest,
                "last_trade": self.last_trade, "events": self.events[:40], "open": opens[:20],
                "closed": len(self.ledger.closed),
                "closed_pnl": sum(p.realized_pnl_sol for p in self.ledger.closed if p.complete)}


def _ago(ts):
    if not ts:
        return "never"
    sec = max(0, int(time.time() - ts))
    return f"{sec}s ago" if sec < 90 else f"{sec // 60}m ago" if sec < 5400 else f"{sec // 3600}h ago" if sec < 172800 else f"{sec // 86400}d ago"


def render_live(s, rec):
    e = html.escape
    short = lambda m: e((m or "?")[:5] + "..." + (m or "")[-4:])
    body = []
    if not s:
        body.append('<div class="card warn"><b>Tracker is not running.</b><br>Check CUPSEY_TRACKING and redeploy.</div>')
    else:
        k = s["kinds"]
        trades = [x for x in s["events"] if x["cls"] != "TRANSFER"]
        up = "Live" if s["last_ok"] and time.time() - s["last_ok"] < s["interval"] * 4 + 30 else "Not reading"
        body.append(f'<div class="card"><b>{up}</b> · reads every {s["interval"]:.0f}s<div class="sub">Last good read {e(_ago(s["last_ok"]))} · method: {e(s["mode"])}</div></div>')
        if s["error"]:
            body.append(f'<div class="card warn"><b>Problem reading blockchain.</b><br>{e(s["error"])}</div>')
        body.append('<div class="card"><div class="big">%d</div>buys/sells seen<div class="sub">%d transactions read · %d transfers · last trade: %s</div></div>' %
                    (k.get("BUY", 0) + k.get("SELL", 0), s["checked"], k.get("TRANSFER", 0), e(_ago(s["last_trade"]))))
        if s["open"]:
            rows = "".join('<tr><td><a href="https://solscan.io/token/%s">%s</a></td><td>%.2f SOL</td><td>%.2f SOL</td></tr>' %
                           (e(p["mint"]), short(p["mint"]), p["cost_sol"], p["bought_sol"]) for p in s["open"])
            body.append(f'<div class="card"><b>Open positions</b><table><tr><th>Token</th><th>Holding cost</th><th>Bought</th></tr>{rows}</table></div>')
        if trades:
            rows = "".join('<tr><td>%s%s</td><td><a href="https://solscan.io/tx/%s">%s</a></td><td>%.3f</td><td>%s</td></tr>' %
                           (e(x["cls"].title()), " ★" if x["big"] else "", e(x["sig"]), short(x["mint"]), x["sol"], e(_ago(x["time"])))
                           for x in trades[:25])
            body.append(f'<div class="card"><b>Latest trades</b><table><tr><th>Type</th><th>Token</th><th>SOL</th><th>When</th></tr>{rows}</table><div class="sub">★ passes placeholder size filters. SOL sizes are estimates.</div></div>')
        else:
            body.append('<div class="card warn"><b>No buys or sells found yet.</b><br>No trade is shown unless balance changes support it.</div>')
        noise = s["checked"] - sum(k.get(x, 0) for x in ("BUY", "SELL", "TRANSFER"))
        body.append('<div class="card"><b>Transaction diagnostics</b><div class="sub">%d of %d transactions did not show this wallet as a signer; %d had no classified token trade. Not being a signer does not prove the wallet attribution is wrong.<br>%s</div></div>' %
                    (s["foreign"], s["checked"], noise, e(" | ".join(f"{n} × {r}" for r, n in s["reasons"]))))
    badge = e(rec.status.replace("_", " "))
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="15"><title>Cupsey monitor</title>
<style>:root{{--bg:#0b0b0c;--card:#17181a;--fg:#f2f2f2;--mute:#9a9aa0;--warn:#e9b44c}}body{{margin:0;padding:16px;font:16px system-ui;background:var(--bg);color:var(--fg)}}.card{{background:var(--card);border-radius:16px;padding:16px;margin:12px 0}}.warn{{border:1px solid var(--warn)}}.big{{font-size:44px;font-weight:700}}.sub{{color:var(--mute);font-size:13px;margin-top:6px;word-break:break-all}}table{{width:100%;border-collapse:collapse}}td,th{{text-align:left;padding:8px 4px;font-size:14px}}a{{color:#5b9dff}}.badge{{color:var(--warn)}}</style></head><body><h2>Cupsey monitor</h2>
<div class="card"><div class="badge">{badge}</div><div class="sub">{e(rec.address)}</div><div class="sub">Third-party attribution only; not independently confirmed on-chain.</div>
<div class="sub"><a href="https://solscan.io/account/{e(rec.address)}">Solscan</a> · <a href="/cupsey/wallets">Wallets</a> · <a href="/dashboard">Dashboard</a></div></div>{"".join(body)}</body></html>"""


def render_page(out, error, rec):
    e = html.escape
    if error or not out:
        body = f'<div class="card warn"><b>Could not read blockchain.</b><br>{e(error or "unknown error")}</div>'
    else:
        k = out.get("kinds", {})
        trades = [r for r in out.get("recent_activity", []) if r["kind"] in ("BUY", "SELL")]
        body = f'<div class="card"><b>{k.get("BUY", 0) + k.get("SELL", 0)} buys/sells found</b><div class="sub">Source: {e(str(out.get("source", "")))}</div></div>'
        if not trades:
            body += '<div class="card warn">No buys or sells found in this window. This does not by itself prove the wallet attribution is wrong.</div>'
        else:
            body += "<div class=card><b>Recent trades</b><pre>" + e(json.dumps(trades, indent=2)) + "</pre></div>"
    return f'<!doctype html><html><head><meta charset="utf-8"><title>Cupsey monitor</title></head><body><h2>Cupsey monitor · {e(rec.status)}</h2>{body}</body></html>'


def register_cupsey_routes(app, auth):
    import asyncio
    from contextlib import asynccontextmanager
    from fastapi import Depends, Query, Request
    from fastapi.responses import HTMLResponse, JSONResponse
    registry = seed_registry()
    if not registry.get(DEFAULT_ADDRESS):
        registry.add(WalletRecord(DEFAULT_ADDRESS, "Owner-supplied wallet", "POSSIBLY_ASSOCIATED",
                                  notes="Owner supplied; not independently verified."))
    tracker = None
    if os.getenv("CUPSEY_TRACKING", "true").lower() not in ("0", "false", "no", "off"):
        tracker = Tracker(DEFAULT_ADDRESS, app.state.settings.solana_rpc_url,
                          float(os.getenv("CUPSEY_POLL_SECONDS", "20")),
                          int(os.getenv("CUPSEY_BACKFILL_TX", "1000")))
        inner = app.router.lifespan_context
        @asynccontextmanager
        async def with_tracker(application):
            async with inner(application) as state:
                task = asyncio.create_task(tracker.run(), name="cupsey-tracker")
                try:
                    yield state
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        app.router.lifespan_context = with_tracker

    @app.get("/cupsey", include_in_schema=False, dependencies=[Depends(auth)])
    async def cupsey_page():
        return HTMLResponse(render_live(tracker.snapshot() if tracker else None, registry.get(DEFAULT_ADDRESS)),
                            headers={"Cache-Control": "no-store"})

    @app.get("/cupsey/wallets", include_in_schema=False, dependencies=[Depends(auth)])
    async def cupsey_wallets():
        return JSONResponse([asdict(r) for r in registry.all()])

    @app.get("/cupsey/verify", include_in_schema=False, dependencies=[Depends(auth)])
    async def cupsey_verify(request: Request, address: str = DEFAULT_ADDRESS,
                            limit: int = Query(40, ge=5, le=100), scan: int = Query(2000, ge=50, le=5000),
                            download: bool = False):
        url = request.app.state.settings.solana_rpc_url
        try:
            out = await asyncio.to_thread(run_verification, address, url, limit, scan)
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            msg = f"{type(exc).__name__}" + (f" (HTTP {status})" if status else "")
            if isinstance(exc, (ValueError, RuntimeError)):
                msg += f": {exc}"
            return JSONResponse({"error": msg.replace(url, "<rpc>")}, status_code=502)
        headers = {"Content-Disposition": f'attachment; filename="{address}_evidence.json"'} if download else {}
        return JSONResponse(out, headers=headers)


def _cli(argv):
    if argv[:1] == ["test"]:
        suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if argv[:1] == ["verify"]:
        return verify(argv[1:])
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
