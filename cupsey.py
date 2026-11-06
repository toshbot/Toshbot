"""cupsey.py: the whole Cupsey monitoring core in ONE file (put it next to main.py).

Sections: wallet registry, transaction decoder, position ledger, significance filter, tests, on-chain verifier + diagnostics, live tracker.
Pure Python (stdlib only; httpx is imported only when talking to an RPC). No signing, no trading, no secrets. Read-only monitoring.

Use:
  python cupsey.py test                      run the unit tests (synthetic fixtures, no network)
  python cupsey.py verify [address] [--limit 40]   writes <address>_evidence.json (reads SOLANA_RPC_URL, never printed)
  In the running app (after login): /cupsey/verify?download=1   and   /cupsey/wallets
  Evidence only; this never promotes a wallet's status, and on-chain activity alone cannot prove who owns a wallet.
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
DECODER_VERSION = 2


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
    fee_sol: float = 0.0           # only non-zero when the wallet is the fee payer (always account key #0)
    legs: tuple = field(default_factory=tuple)   # all token deltas, for OTHER (token-to-token) records
    note: str = ""
    reason: str = ""               # machine-readable reason code: WHY this kind was chosen
    signer: bool | None = None     # True/False = signer data says the wallet did/did not sign; None = no signer data

    @property
    def price_sol(self) -> float | None:
        if self.kind in ("BUY", "SELL") and self.token_delta:
            return abs(self.quote_sol) / abs(self.token_delta)
        return None


def _account_table(tx: dict) -> list[dict]:
    """One row per account key: pubkey, signer, writable, source, signer_basis. Never raises on odd shapes.

    Signer detection, best evidence first:
      1. jsonParsed account-key flags ({"pubkey", "signer", "writable", "source"}),
      2. the message header: the first `numRequiredSignatures` static keys are the signers,
      3. the number of signatures (always equals numRequiredSignatures),
      4. unknown (None). Unknown is never treated as 'not signed'.
    Keys loaded from address lookup tables (v0 transactions) can never be signers."""
    txn = (tx or {}).get("transaction") or {}
    msg = txn.get("message") or {}
    meta = (tx or {}).get("meta") or {}
    raw = msg.get("accountKeys") or []
    hdr = msg.get("header") or {}
    n_sig, basis = hdr.get("numRequiredSignatures"), "message_header"
    if n_sig is None:
        sigs = txn.get("signatures")
        n_sig, basis = (len(sigs), "signature_count") if isinstance(sigs, list) and sigs else (None, "unknown")
    ro_signed, ro_unsigned = hdr.get("numReadonlySignedAccounts"), hdr.get("numReadonlyUnsignedAccounts")
    static_len = sum(1 for k in raw if not (isinstance(k, dict) and k.get("source") == "lookupTable"))
    table: list[dict] = []
    for i, k in enumerate(raw):
        if isinstance(k, dict):
            pk, signer, writable, source = k.get("pubkey"), k.get("signer"), k.get("writable"), k.get("source") or "transaction"
        else:
            pk, signer, writable, source = k, None, None, "transaction"
        from_table = source == "lookupTable"
        sig_basis = "account_key_flag" if signer is not None else "unknown"
        if signer is None:
            if from_table:
                signer, sig_basis = False, "lookup_table"
            elif n_sig is not None and i < static_len:
                signer, sig_basis = i < n_sig, basis
        if writable is None and not from_table and i < static_len and None not in (n_sig, ro_signed, ro_unsigned):
            writable = (i < n_sig - ro_signed) if i < n_sig else (i < static_len - ro_unsigned)
        table.append({"pubkey": pk, "signer": signer, "writable": writable, "source": source, "signer_basis": sig_basis})
    loaded, post = meta.get("loadedAddresses") or {}, meta.get("postBalances")
    if loaded and isinstance(post, list) and len(table) < len(post):   # json/base64 encoding: lookup-table keys live in meta
        for part, w in (("writable", True), ("readonly", False)):
            for pk in loaded.get(part) or []:
                table.append({"pubkey": pk, "signer": False, "writable": w, "source": "lookupTable", "signer_basis": "lookup_table"})
    return table


def _keys(tx: dict) -> list[str]:
    return [a["pubkey"] for a in _account_table(tx)]


def wallet_role(tx: dict, wallet: str) -> dict:
    """How the wallet took part in a transaction. The FEE PAYER is always account key #0 (protocol rule), but a wallet can sign
    without paying the fee (relayer, co-signer), so 'is_signer' and 'is_fee_payer' are tracked separately."""
    table = _account_table(tx)
    keys = [a["pubkey"] for a in table]
    role = {"in_account_keys": wallet in keys, "index": None, "is_signer": None, "is_writable": None,
            "is_fee_payer": bool(keys) and keys[0] == wallet, "fee_payer": keys[0] if keys else None, "signer_basis": "unknown"}
    if wallet in keys:
        i = keys.index(wallet)
        a = table[i]
        role.update(index=i, is_signer=a["signer"], is_writable=a["writable"], signer_basis=a["signer_basis"])
        if i == 0 and a["signer"] is None:   # the fee payer must have signed
            role.update(is_signer=True, signer_basis="fee_payer")
    elif keys:   # a pubkey that is not in the account list cannot have signed
        role.update(is_signer=False, signer_basis="not_in_account_keys")
    return role


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


def _ownerless_balances(meta: dict) -> int:
    """Token balance rows with no `owner` field (very old transactions / odd providers): they cannot be attributed to anyone."""
    return sum(1 for side in ("preTokenBalances", "postTokenBalances") for b in meta.get(side) or [] if b.get("owner") is None)


def _no_token_outcome(role: dict, native: float, wsol, ownerless: int, has_wallet_tokens: bool) -> tuple[str, str, str]:
    """(kind, reason, note) when the wallet's own token balances did not change. SOL-only activity is OTHER, never a trade."""
    if ownerless and not has_wallet_tokens:
        return "UNKNOWN", "token_owner_missing", "token balances carry no owner field, so they cannot be attributed to the wallet"
    if wsol and wsol[0] != wsol[1]:
        return "OTHER", "wsol_wrap_unwrap", "wrapped/unwrapped SOL only; no other token moved (not a trade)"
    if not role["in_account_keys"] and not has_wallet_tokens:
        return "OTHER", "wallet_not_involved", "wallet is in neither the account list nor the token balances (unrelated transaction)"
    if abs(native) < DUST_SOL and not (native > 0 and role["is_signer"] is False):   # tiny incoming SOL from a stranger is dust, not 'nothing'
        return "OTHER", "no_value_movement", "no token balance change (SOL-only or unrelated)"
    if native > 0:
        who = "wallet did not sign: incoming dust/airdrop/third-party transfer" if role["is_signer"] is False else "no token trade"
        return "OTHER", "sol_received_unsigned" if role["is_signer"] is False else "sol_received", f"received {native:.6f} SOL, {who}"
    return "OTHER", "sol_sent", f"sent {abs(native):.6f} SOL with no token trade"


def decode_transaction(tx: dict, wallet: str) -> DecodedTx:
    txn = (tx or {}).get("transaction") or {}
    msg = txn.get("message")
    sigs = txn.get("signatures") or ["?"]
    base = dict(signature=sigs[0], wallet=wallet, slot=(tx or {}).get("slot"), block_time=(tx or {}).get("blockTime"))
    meta = (tx or {}).get("meta")
    if not msg or meta is None:
        return DecodedTx(kind="UNKNOWN", note="missing transaction or meta", reason="missing_data", **base)
    role = wallet_role(tx, wallet)
    signer = role["is_signer"]
    base["signer"] = signer
    if meta.get("err") is not None:
        return DecodedTx(kind="OTHER", note="failed transaction (no balance change counted)", reason="failed_tx", **base)

    idx, pre_b, post_b = role["index"], meta.get("preBalances"), meta.get("postBalances")
    if not isinstance(pre_b, list) or not isinstance(post_b, list) or (idx is not None and (idx >= len(pre_b) or idx >= len(post_b))):
        return DecodedTx(kind="UNKNOWN", note="SOL balance arrays missing or shorter than the account list", reason="balances_unreadable", **base)
    fee = meta.get("fee", 0) / LAMPORTS if role["is_fee_payer"] else 0.0   # only the fee payer's balance includes the fee
    native = ((post_b[idx] - pre_b[idx]) / LAMPORTS + fee) if idx is not None else 0.0
    try:
        totals = _token_totals(meta, wallet)
    except (KeyError, TypeError, ValueError):
        return DecodedTx(kind="UNKNOWN", note="token balance rows are malformed", reason="token_balance_unreadable", **base)
    wsol = totals.get(WSOL)
    wsol_delta = (wsol[1] - wsol[0]) / 10 ** wsol[2] if wsol else 0.0
    quote = native + wsol_delta

    deltas = {m: (post - pre) / 10 ** dec for m, (pre, post, dec) in totals.items() if m != WSOL and post != pre}
    common = dict(quote_sol=quote, fee_sol=fee, **base)
    if not deltas:
        kind, reason, note = _no_token_outcome(role, native, wsol, _ownerless_balances(meta), bool(totals))
        return DecodedTx(kind=kind, note=note, reason=reason, **common)

    if len(deltas) > 1:
        legs = tuple(sorted(deltas.items()))
        both = {d > 0 for d in deltas.values()} == {True, False}
        note = "token-to-token swap or multi-token transaction; both legs recorded" if both else "several tokens moved the same direction"
        return DecodedTx(kind="OTHER", legs=legs, note=note, reason="token_to_token_swap" if both else "multi_token_same_direction", **common)

    (mint, delta), = deltas.items()
    pre, post, dec = totals[mint]
    t = dict(mint=mint, token_delta=delta, pre_token_amount=pre / 10 ** dec, post_token_amount=post / 10 ** dec, **common)
    unsigned = signer is False
    shape_note = "tokens and SOL moved like a swap, but the wallet did not sign (delegate, relayer or program); not counted as the wallet's own trade"
    if delta > 0:
        if quote <= -MIN_SWAP_SOL:
            if unsigned:
                return DecodedTx(kind="UNKNOWN", note=shape_note, reason="unsigned_swap_shape", **t)
            return DecodedTx(kind="BUY", reason="swap_sol_to_token", **t)
        if abs(quote) < DUST_SOL:
            return DecodedTx(kind="TRANSFER", note="tokens received with no payment", reason="token_in_unsigned" if unsigned else "token_in", **t)
    else:
        if quote >= MIN_SWAP_SOL:
            if unsigned:
                return DecodedTx(kind="UNKNOWN", note=shape_note, reason="unsigned_swap_shape", **t)
            return DecodedTx(kind="SELL", reason="swap_token_to_sol", **t)
        if abs(quote) < DUST_SOL:
            return DecodedTx(kind="TRANSFER", note="tokens sent with no proceeds", reason="token_out_unsigned" if unsigned else "token_out", **t)
    return DecodedTx(kind="UNKNOWN", note="ambiguous SOL movement; logged, never alerted", reason="ambiguous_sol_movement", **t)


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


# ---- richer fixtures: header / signer flags / lookup tables / arbitrary key order -----------------
def rtx(keys, n_signers=1, pre=None, post=None, pre_tb=(), post_tb=(), fee=5000, err=None, header=True, flags=False, n_sigs=None, loaded=None, ro_unsigned=1):
    """getTransaction-shaped fixture with full control over account keys, signers and balances (lamports)."""
    _n[0] += 1
    n = len(keys)
    msg = {"accountKeys": [{"pubkey": k, "signer": i < n_signers, "writable": True, "source": "transaction"} for i, k in enumerate(keys)] if flags else list(keys)}
    if header:
        msg["header"] = {"numRequiredSignatures": n_signers, "numReadonlySignedAccounts": 0, "numReadonlyUnsignedAccounts": ro_unsigned}
    sigs = [f"RSIG{_n[0]}"] * (n_signers if n_sigs is None else n_sigs)
    meta = {"err": err, "fee": fee, "preBalances": list(pre if pre is not None else [0] * n), "postBalances": list(post if post is not None else [0] * n),
            "preTokenBalances": list(pre_tb), "postTokenBalances": list(post_tb)}
    if loaded:
        meta["loadedAddresses"] = loaded
    t = {"slot": 1, "blockTime": 1000 + _n[0], "transaction": {"signatures": sigs, "message": msg}, "meta": meta}
    if not sigs:
        t["transaction"].pop("signatures")
    return t


R, S, X = "R" * 44, "S" * 44, "X" * 44
SOL10 = 10 * 10**9


def dust_in(n=1):
    """SOL dust sent to W by a stranger (W is not a signer)."""
    return [rtx([S, W], 1, pre=[SOL10, 10**9], post=[SOL10 - 1000, 10**9 + 1000], fee=0) for _ in range(n)]


class SignerTests(unittest.TestCase):
    def test_signer_from_account_key_flags_cosigner_is_not_fee_payer(self):
        r = wallet_role(rtx([R, W], 2, flags=True), W)
        self.assertTrue(r["is_signer"]); self.assertFalse(r["is_fee_payer"]); self.assertEqual(r["signer_basis"], "account_key_flag"); self.assertEqual(r["fee_payer"], R)

    def test_signer_from_message_header_and_writable_derivation(self):
        t = rtx([R, S, W, X], 2, ro_unsigned=1)
        r = wallet_role(t, W)
        self.assertIs(r["is_signer"], False); self.assertEqual(r["signer_basis"], "message_header"); self.assertTrue(r["is_writable"])
        self.assertTrue(wallet_role(rtx([R, W, X], 2), W)["is_signer"])
        self.assertFalse(wallet_role(t, X)["is_writable"])   # last key is readonly-unsigned

    def test_signer_falls_back_to_signature_count(self):
        r = wallet_role(rtx([R, W], 2, header=False), W)
        self.assertIs(r["is_signer"], True); self.assertEqual(r["signer_basis"], "signature_count")

    def test_signer_unknown_without_any_metadata(self):
        r = wallet_role(rtx([R, W], 1, header=False, n_sigs=0), W)
        self.assertIsNone(r["is_signer"]); self.assertEqual(r["signer_basis"], "unknown")

    def test_wallet_missing_from_keys_cannot_have_signed(self):
        r = wallet_role(rtx([R, X], 1), W)
        self.assertFalse(r["in_account_keys"]); self.assertIs(r["is_signer"], False)

    def test_lookup_table_keys_are_appended_and_never_signers(self):
        t = rtx([W, X], 1, pre=[0] * 4, post=[0] * 4, loaded={"writable": ["L" * 44], "readonly": ["M" * 44]})
        tbl = _account_table(t)
        self.assertEqual(len(tbl), 4); self.assertEqual(tbl[2]["source"], "lookupTable"); self.assertFalse(tbl[3]["signer"])

    def test_fee_only_taken_from_fee_payer(self):
        # relayer pays the fee, wallet co-signs and spends exactly 1 SOL: no fee must be added back to the wallet's balance change
        t = rtx([R, W, M1[:44]], 2, pre=[SOL10, SOL10, 0], post=[SOL10 - 5000, SOL10 - 10**9, 0], post_tb=[tb(M1, 500 * 10**6, idx=2)])
        d = decode_transaction(t, W)
        self.assertEqual(d.kind, "BUY"); self.assertAlmostEqual(d.quote_sol, -1.0, 9); self.assertEqual(d.fee_sol, 0.0); self.assertTrue(d.signer)
        self.assertEqual(d.reason, "swap_sol_to_token")


class DecoderReasonTests(unittest.TestCase):
    def test_incoming_dust_is_other_with_reason_not_a_trade(self):
        d = decode_transaction(dust_in()[0], W)
        self.assertEqual(d.kind, "OTHER"); self.assertEqual(d.reason, "sol_received_unsigned"); self.assertIs(d.signer, False)

    def test_wsol_wrap_is_not_a_trade(self):
        t = rtx([W, X], 1, pre=[SOL10, 0], post=[SOL10 - 10**9, 0], fee=0, post_tb=[tb(WSOL, 10**9, 9, idx=1)])
        d = decode_transaction(t, W)
        self.assertEqual((d.kind, d.reason), ("OTHER", "wsol_wrap_unwrap"))

    def test_swap_shape_without_wallet_signature_is_unknown_not_buy(self):
        t = rtx([R, W, X], 1, pre=[SOL10, SOL10, 0], post=[SOL10, SOL10 - 10**9, 0], fee=0, post_tb=[tb(M1, 10**6, idx=2)])
        d = decode_transaction(t, W)
        self.assertEqual((d.kind, d.reason), ("UNKNOWN", "unsigned_swap_shape"))

    def test_unsigned_token_arrival_is_transfer_airdrop(self):
        t = rtx([S, W], 1, pre=[SOL10, 10**9], post=[SOL10, 10**9], fee=0, post_tb=[tb(M1, 10**6, idx=1)])
        d = decode_transaction(t, W)
        self.assertEqual((d.kind, d.reason), ("TRANSFER", "token_in_unsigned"))

    def test_token_balances_without_owner_are_unknown(self):
        t = rtx([W, X], 1, pre=[SOL10, 0], post=[SOL10, 0], fee=0, post_tb=[tb(M1, 10**6, owner=None)])
        d = decode_transaction(t, W)
        self.assertEqual((d.kind, d.reason), ("UNKNOWN", "token_owner_missing"))

    def test_short_balance_arrays_are_unknown_not_an_exception(self):
        t = rtx([R, W], 1, pre=[1], post=[1])
        self.assertEqual(decode_transaction(t, W).reason, "balances_unreadable")

    def test_malformed_token_row_is_unknown(self):
        bad = {"accountIndex": 1, "mint": M1, "owner": W, "uiTokenAmount": {"decimals": 6}}
        t = rtx([W, X], 1, pre=[SOL10, 0], post=[SOL10, 0], post_tb=[bad])
        self.assertEqual(decode_transaction(t, W).reason, "token_balance_unreadable")

    def test_token_to_token_reason_and_failed_reason(self):
        d = decode_transaction(tx(10, 10, [tb(M1, 10**6)], [tb(M2, 10**6, idx=2)], fee=0), W)
        self.assertEqual(d.reason, "token_to_token_swap")
        self.assertEqual(decode_transaction(tx(10, 9, err={"x": 1}), W).reason, "failed_tx")

    def test_sol_only_send_and_no_movement(self):
        sent = decode_transaction(rtx([W, X], 1, pre=[SOL10, 0], post=[SOL10 - 10**9, 10**9], fee=0), W)
        self.assertEqual((sent.kind, sent.reason), ("OTHER", "sol_sent"))
        idle = decode_transaction(rtx([W, X], 1, pre=[SOL10, 0], post=[SOL10 - 5000, 0], fee=5000), W)
        self.assertEqual((idle.kind, idle.reason), ("OTHER", "no_value_movement"))

    def test_wallet_not_involved(self):
        self.assertEqual(decode_transaction(rtx([R, X], 1), W).reason, "wallet_not_involved")


class TrackerSignerTests(unittest.TestCase):
    def make(self):
        return Tracker(W, "https://rpc.invalid/?api-key=SECRET", interval=5)

    def test_cosigned_transaction_is_not_foreign(self):
        t = self.make(); t.ingest([rtx([R, W], 2, flags=True, pre=[SOL10, SOL10], post=[SOL10, SOL10])])
        s = t.snapshot(); self.assertEqual((s["signed"], s["foreign"], s["signer_unknown"]), (1, 0, 0))

    def test_unknown_signer_data_is_not_foreign(self):
        t = self.make(); t.ingest([rtx([R, W], 1, header=False, n_sigs=0, pre=[SOL10, SOL10], post=[SOL10, SOL10])])
        s = t.snapshot(); self.assertEqual((s["signed"], s["foreign"], s["signer_unknown"]), (0, 0, 1))

    def test_fee_payer_wallet_counts_as_signed(self):
        t = self.make(); t.ingest([buy(1.0, 10)]); self.assertEqual(t.snapshot()["signed"], 1)

    def test_dashboard_warning_is_careful_and_does_not_condemn_the_wallet(self):
        t = self.make(); t.ingest(dust_in(6)); t.last_ok = time.time()
        page = render_live(t.snapshot(), seed_registry().get(SEED_ADDRESS))
        self.assertIn("Why nothing yet", page); self.assertIn("0 were signed by this wallet", page); self.assertIn("6 were started by someone else", page)
        self.assertIn("does not show who owns it", page); self.assertNotIn("probably NOT", page); self.assertNotIn("SECRET", page)

    def test_transient_error_does_not_switch_mode_but_missing_method_does(self):
        t = self.make()

        async def slow(method, params):
            raise RpcError("timeout", "timed out")
        t._rpc = slow
        with self.assertRaises(RpcError):
            asyncio.run(t._fetch(None, 100))
        self.assertEqual(t.mode, "helius")

        async def missing(method, params):
            if method == "getTransactionsForAddress":
                raise RpcError("method_unavailable", "rpc error: Method not found")
            return []
        t._rpc = missing
        asyncio.run(t._fetch(None, 100)); self.assertEqual(t.mode, "standard"); self.assertIn("method_unavailable", t.last_error)

    def test_tracker_helius_request_uses_only_the_valid_filter(self):
        t = self.make(); seen = []

        async def cap(method, params):
            seen.append(params[1]["filters"]); return {"data": []}
        t._rpc = cap
        asyncio.run(t._fetch_helius(None, 100)); asyncio.run(t._fetch_helius(1234, 100))
        self.assertEqual(seen[0], {"status": "succeeded", "tokenAccounts": "balanceChanged"})
        self.assertEqual(seen[1], {"status": "succeeded", "tokenAccounts": "balanceChanged", "blockTime": {"gte": 1234}})


# ---- fake RPC client for the verifier (no network, no httpx) --------------------------------------
class _Resp:
    def __init__(self, body=None, status=200, bad_json=False):
        self.status_code, self._b, self._bad = status, body, bad_json

    def json(self):
        if self._bad:
            raise ValueError("not json")
        return self._b


class FakeRpc:
    def __init__(self, handlers):
        self.handlers, self.calls = handlers, []

    def post(self, url, json=None):
        self.calls.append(json)
        h = self.handlers.get(json["method"])
        out = h(json["params"]) if callable(h) else h
        if isinstance(out, Exception):
            raise out
        return out if isinstance(out, _Resp) else _Resp({"jsonrpc": "2.0", "id": 1, "result": out})


def _err(code, msg):
    return _Resp({"jsonrpc": "2.0", "id": 1, "error": {"code": code, "message": msg}})


def _buy_tx():
    return rtx([W, X], 1, pre=[SOL10, 0], post=[SOL10 - 10**9 - 5000, 0], post_tb=[tb(M1, 500 * 10**6, idx=1)])


class _NoSleep:
    def __enter__(self):
        global _SLEEP
        self.old, _SLEEP = _SLEEP, (lambda s: None)

    def __exit__(self, *a):
        global _SLEEP
        _SLEEP = self.old


URL = "https://mainnet.helius-rpc.com/?api-key=SECRET-KEY-12345"


class VerifierTests(unittest.TestCase):
    def test_helius_request_has_no_token_transfer_filter(self):
        fake = FakeRpc({"getTransactionsForAddress": {"data": [_buy_tx()], "paginationToken": None}})
        out = run_verification(W, URL, client=fake)
        cfg = fake.calls[0]["params"][1]
        self.assertEqual(cfg["filters"], {"status": "succeeded", "tokenAccounts": "balanceChanged"})
        self.assertNotIn("tokenTransfer", json.dumps(fake.calls))
        self.assertTrue(out["source"].startswith("helius_filtered")); self.assertEqual(out["kinds"], {"BUY": 1})

    def test_falls_back_to_standard_rpc_when_helius_method_is_missing(self):
        t = _buy_tx()
        fake = FakeRpc({"getTransactionsForAddress": _err(-32601, "Method not found"),
                        "getSignaturesForAddress": [{"signature": "S1", "err": None, "blockTime": 5}], "getTransaction": t})
        out = run_verification(W, URL, client=fake)
        self.assertTrue(out["source"].startswith("standard_signatures")); self.assertEqual(out["helius_error_kind"], "method_unavailable")
        self.assertEqual(out["kinds"], {"BUY": 1})

    def test_invalid_params_error_falls_back_too(self):
        fake = FakeRpc({"getTransactionsForAddress": _err(-32602, "Invalid params: unknown field"), "getSignaturesForAddress": []})
        out = run_verification(W, URL, client=fake)
        self.assertEqual(out["helius_error_kind"], "invalid_params"); self.assertTrue(out["source"].startswith("standard"))

    def test_api_errors_are_classified_and_never_leak_secrets(self):
        class TimeoutException(Exception):   # same class name httpx uses
            pass
        cases = {"forbidden": _Resp({}, status=403), "rate_limited": _Resp({}, status=429), "server": _Resp({}, status=502),
                 "bad_response": _Resp(None, bad_json=True), "timeout": TimeoutException(f"timed out talking to {URL}"), "network": ConnectionError(f"cannot reach {URL}")}
        with _NoSleep():
            for kind, h in cases.items():
                fake = FakeRpc({"getTransactionsForAddress": h, "getSignaturesForAddress": h})
                with self.assertRaises(RpcError) as cm:
                    run_verification(W, URL, client=fake)
                self.assertEqual(cm.exception.kind, kind)
                self.assertNotIn("SECRET-KEY-12345", str(cm.exception)); self.assertNotIn("helius-rpc.com/?api", str(cm.exception))

    def test_helius_failure_details_are_in_evidence_without_secrets(self):
        fake = FakeRpc({"getTransactionsForAddress": ConnectionError(f"cannot reach {URL}"), "getSignaturesForAddress": []})
        blob = json.dumps(run_verification(W, URL, client=fake))
        self.assertIn("network", blob)
        for secret in ("SECRET-KEY-12345", "api-key=SECRET"):
            self.assertNotIn(secret, blob)

    def test_path_embedded_keys_and_query_secrets_are_redacted(self):
        url = "https://x.quiknode.pro/abcdef1234567890abcdef12/?token=TOPSECRET99"
        text = redact_text(f"boom {url} and {url.split('?')[0]} and token=TOPSECRET99", url)
        for secret in ("abcdef1234567890abcdef12", "TOPSECRET99"):
            self.assertNotIn(secret, text)
        self.assertEqual(redact({"a": [f"x {url}"]}, url), {"a": ["x <rpc>"]})

    def test_diagnostic_samples_capped_at_three_with_required_fields(self):
        rows = dust_in(5) + [_buy_tx(), rtx([S, W], 1, pre=[SOL10, 10**9], post=[SOL10, 10**9], fee=0, post_tb=[tb(M1, 10**6, idx=1)]),
                             rtx([R, W, X], 1, pre=[SOL10, SOL10, 0], post=[SOL10, SOL10 - 10**9, 0], fee=0, post_tb=[tb(M1, 10**6, idx=2)])]
        out = run_verification(W, URL, client=FakeRpc({"getTransactionsForAddress": {"data": rows}}))
        ds = out["diagnostic_samples"]
        self.assertEqual(len(ds), 3); self.assertEqual(ds[0]["outcome"], "OTHER/sol_received_unsigned"); self.assertEqual(ds[0]["occurrences"], 5)
        self.assertIn("UNKNOWN/unsigned_swap_shape", [x["outcome"] for x in ds])
        for smp in ds:
            for key in ("signature", "account_keys", "message_header", "fee_lamports", "sol_balances", "token_balances", "error", "decoded", "wallet_role", "selected_because"):
                self.assertIn(key, smp)
            for key in ("kind", "reason", "token_mint", "token_delta", "sol_amount", "explanation"):
                self.assertIn(key, smp["decoded"])
            self.assertEqual({"signer", "writable", "is_wallet"} <= set(smp["account_keys"][0]), True)
            self.assertIn("pre", smp["token_balances"]); self.assertIn("post", smp["token_balances"])
        self.assertEqual(out["signer_summary"]["signed"] + out["signer_summary"]["not_signed"] + out["signer_summary"].get("unknown", 0), 8)
        json.dumps(out)

    def test_diagnostic_sample_shows_error_and_fee_details(self):
        t = rtx([R, W], 1, pre=[SOL10, SOL10], post=[SOL10 - 5000, SOL10], err={"InstructionError": [0, "Custom"]}, fee=5000)
        out = run_verification(W, URL, client=FakeRpc({"getTransactionsForAddress": {"data": [t]}}))
        smp = out["diagnostic_samples"][0]
        self.assertIn("InstructionError", smp["error"]); self.assertEqual(smp["fee_lamports"], 5000); self.assertEqual(smp["fee_payer"], R)
        self.assertEqual(smp["message_header"]["numRequiredSignatures"], 1)
        self.assertEqual([r["delta_lamports"] for r in smp["sol_balances"]], [-5000, 0])   # fee payer changed, wallet row always present

    def test_attribution_is_reported_unverified(self):
        out = run_verification(W, URL, client=FakeRpc({"getTransactionsForAddress": {"data": []}}))
        a = out["attribution"]
        self.assertEqual(a["on_chain_attribution"], "UNVERIFIED"); self.assertEqual(a["independent_confirming_evidence"], 0); self.assertTrue(a["registry_evidence_is_third_party_only"])
        self.assertEqual(run_verification("Q" * 44, URL, client=FakeRpc({"getTransactionsForAddress": {"data": []}}))["attribution"]["registry_status"], "NOT_IN_REGISTRY")

    def test_one_malformed_row_does_not_abort_verification(self):
        out = run_verification(W, URL, client=FakeRpc({"getTransactionsForAddress": {"data": [{"slot": 1}, "junk", _buy_tx()]}}))
        self.assertEqual(out["kinds"].get("BUY"), 1)


# ============================================================================
# ON-CHAIN VERIFIER  ->  browser: /cupsey/verify   |  terminal: python cupsey.py verify <address>
# ============================================================================
SEED_ADDRESS = "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f"
DEFAULT_ADDRESS = (os.getenv("CUPSEY_WALLET") or SEED_ADDRESS).strip()   # set CUPSEY_WALLET on Render to watch a different wallet
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"
# The ONLY filter sent to Helius getTransactionsForAddress. (A 'tokenTransfer' filter is not valid and made the provider reject the call.)
HELIUS_FILTER = {"status": "succeeded", "tokenAccounts": "balanceChanged"}
_SLEEP = time.sleep   # indirection so tests do not really wait on rate-limit back-off


# ---- RPC errors (classified, never carrying the URL or key) ----------------
class RpcError(RuntimeError):
    """kind: forbidden | not_found | rate_limited | server | timeout | network | bad_response | method_unavailable | invalid_params | http_error | rpc_error"""

    def __init__(self, kind: str, message: str, code=None, status=None) -> None:
        super().__init__(message)
        self.kind, self.code, self.status = kind, code, status


def _kind_for_status(status: int) -> str:
    return "forbidden" if status in (401, 403) else "not_found" if status == 404 else "timeout" if status == 408 else "server" if status >= 500 else "http_error"


def _kind_for_rpc_error(code, message: str) -> str:
    m = str(message).lower()
    if code == -32601 or any(s in m for s in ("method not found", "unknown method", "not supported", "not available", "unsupported method")):
        return "method_unavailable"
    if code in (-32602, -32600) or any(s in m for s in ("invalid param", "invalid filter", "unknown field", "unknown variant", "unexpected", "invalid request")):
        return "invalid_params"
    if code == -32005 or "rate limit" in m or "too many" in m:
        return "rate_limited"
    if any(s in m for s in ("unauthor", "forbidden", "api key", "api-key", "upgrade", "plan")):
        return "forbidden"
    return "server" if isinstance(code, int) and -32099 <= code <= -32000 else "rpc_error"


def _read_rpc_response(status: int, load_json) -> tuple[bool, object]:
    """(retry, result). 429 asks for a retry; everything else is a result or a classified RpcError."""
    if status == 429:
        return True, None
    if status >= 400:
        raise RpcError(_kind_for_status(status), f"HTTP {status}", status=status)
    try:
        body = load_json()
    except Exception:
        raise RpcError("bad_response", "provider returned something that is not valid JSON", status=status) from None
    if not isinstance(body, dict):
        raise RpcError("bad_response", "provider returned an unexpected response shape", status=status)
    if body.get("error"):
        err = body["error"]
        code, msg = (err.get("code"), err.get("message")) if isinstance(err, dict) else (None, err)
        raise RpcError(_kind_for_rpc_error(code, msg), "rpc error: " + str(msg)[:150], code=code, status=status)
    if "result" not in body:
        raise RpcError("bad_response", "provider response has no result field", status=status)
    return False, body["result"]


def _wrap_exc(exc: Exception, url: str = "") -> RpcError:
    """Turn a transport exception (httpx or otherwise) into an RpcError without importing httpx."""
    if isinstance(exc, RpcError):
        return exc
    names = {c.__name__ for c in type(exc).__mro__}
    kind = "timeout" if ("TimeoutException" in names or isinstance(exc, TimeoutError)) else "network"
    return RpcError(kind, redact_text(f"{type(exc).__name__}: {exc}", url)[:120])


def _rpc(client, url, method, params):
    for attempt in range(4):
        try:
            r = client.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        except Exception as exc:
            raise _wrap_exc(exc, url) from None
        retry, result = _read_rpc_response(r.status_code, r.json)
        if retry:
            _SLEEP(1 + attempt)
            continue
        return result
    raise RpcError("rate_limited", "rate limited by the RPC provider (try again, or use a dedicated RPC URL)", status=429)


def _helius_cfg(token: str | None = None, since: int | None = None) -> dict:
    flt = dict(HELIUS_FILTER)
    if since:
        flt["blockTime"] = {"gte": since}
    cfg = {"transactionDetails": "full", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 1,
           "sortOrder": "desc", "limit": 100, "commitment": "confirmed", "filters": flt}
    if token:
        cfg["paginationToken"] = token
    return cfg


def _helius_page(res) -> tuple[list, str | None]:
    res = {} if res is None else res
    if not isinstance(res, dict) or not isinstance(res.get("data") or [], list):
        raise RpcError("bad_response", "getTransactionsForAddress returned an unexpected shape")
    return res.get("data") or [], res.get("paginationToken")


# ---- secrets never leave this module in diagnostics ------------------------
_SECRET_QS = re.compile(r"(?i)((?:api[-_]?key|apikey|access[-_]?token|token|secret|auth|key)=)[^&\s\"'<>]+")


def _secret_fragments(url: str) -> list[str]:
    from urllib.parse import parse_qsl, urlsplit
    if not url:
        return []
    frags = {url}
    try:
        p = urlsplit(url)
        frags |= {v for _, v in parse_qsl(p.query) if len(v) >= 6}
        frags |= {s for s in p.path.split("/") if len(s) >= 12}   # some providers put the key in the path
        frags |= {x for x in (p.username, p.password) if x and len(x) >= 6}
    except ValueError:
        pass
    return sorted(frags, key=len, reverse=True)


def redact_text(text, rpc_url: str = "") -> str:
    text = str(text)
    for f in _secret_fragments(rpc_url):
        text = text.replace(f, "<rpc>" if f == rpc_url else "<redacted>")
    return _SECRET_QS.sub(r"\1<redacted>", text)


def redact(obj, rpc_url: str = ""):
    """Recursively scrub every string in a JSON-able structure (RPC URL, key fragments, key=value secrets)."""
    if isinstance(obj, str):
        return redact_text(obj, rpc_url)
    if isinstance(obj, dict):
        return {k: redact(v, rpc_url) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [redact(v, rpc_url) for v in obj]
    return obj


# ---- raw transaction diagnostics -------------------------------------------
_PROGRAM_HINTS = {   # hints only, to help a human read the sample; never used for classification
    "11111111111111111111111111111111": "System", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": "SPL Token",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb": "Token-2022", "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL": "Associated Token",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "Pump.fun", "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "PumpSwap",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "Jupiter v6", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM v4"}
_MAX_KEYS, _MAX_ROWS = 40, 20
_KIND_RANK = {"UNKNOWN": 0, "BUY": 1, "SELL": 1, "TRANSFER": 2, "OTHER": 3}


def _explain(d: DecodedTx) -> str:
    if d.note:
        return d.note
    if d.kind == "BUY":
        return f"SOL+wSOL (fee excluded) fell by {abs(d.quote_sol):.6f} while the wallet's balance of the token rose by {d.token_delta:g}"
    if d.kind == "SELL":
        return f"SOL+wSOL (fee excluded) rose by {d.quote_sol:.6f} while the wallet's balance of the token fell by {abs(d.token_delta):g}"
    return d.kind


def build_tx_sample(tx: dict, wallet: str, d: DecodedTx, role: dict | None = None) -> dict:
    """Raw facts + decoder verdict for one transaction. Contains no secrets (transaction data is public chain data)."""
    role = role or wallet_role(tx, wallet)
    txn, meta = tx.get("transaction") or {}, tx.get("meta") or {}
    msg = txn.get("message") or {}
    table = _account_table(tx)
    pre_b, post_b = meta.get("preBalances") or [], meta.get("postBalances") or []
    keys = [{"index": i, "pubkey": a["pubkey"], "signer": a["signer"], "writable": a["writable"], "source": a["source"], "is_wallet": a["pubkey"] == wallet}
            for i, a in enumerate(table) if i < _MAX_KEYS or a["pubkey"] == wallet]
    sol = []
    for i, a in enumerate(table[:min(len(pre_b), len(post_b))]):
        delta = post_b[i] - pre_b[i]
        if delta or a["pubkey"] == wallet:
            sol.append({"index": i, "pubkey": a["pubkey"], "pre_lamports": pre_b[i], "post_lamports": post_b[i], "delta_lamports": delta, "is_wallet": a["pubkey"] == wallet})

    def tbs(side):
        rows = []
        for b in meta.get(side) or []:
            ui, i = b.get("uiTokenAmount") or {}, b.get("accountIndex")
            rows.append({"account_index": i, "token_account": table[i]["pubkey"] if isinstance(i, int) and 0 <= i < len(table) else None, "mint": b.get("mint"),
                         "owner": b.get("owner"), "owned_by_wallet": b.get("owner") == wallet, "amount_raw": ui.get("amount"), "decimals": ui.get("decimals")})
        return rows
    pre_t, post_t = tbs("preTokenBalances"), tbs("postTokenBalances")
    both = pre_t + post_t
    owners = {"wallet": sum(r["owned_by_wallet"] for r in both), "missing": sum(r["owner"] is None for r in both)}
    owners["others"] = len(both) - owners["wallet"] - owners["missing"]
    progs = {i.get("programId") for i in msg.get("instructions") or [] if isinstance(i, dict)}
    for inner in meta.get("innerInstructions") or []:
        progs |= {i.get("programId") for i in inner.get("instructions") or [] if isinstance(i, dict)}
    progs = sorted(p for p in progs if p)
    bt = d.block_time
    err = meta.get("err")
    return {
        "signature": d.signature, "slot": tx.get("slot"), "version": tx.get("version"),
        "block_time_utc": datetime.fromtimestamp(bt, timezone.utc).isoformat() if bt else None,
        "account_keys_total": len(table), "account_keys": keys, "message_header": msg.get("header"), "signature_count": len(txn.get("signatures") or []),
        "fee_lamports": meta.get("fee"), "fee_payer": role["fee_payer"], "wallet_role": role,
        "sol_balances": sol[:30], "sol_accounts_changed": sum(1 for r in sol if r["delta_lamports"]),
        "token_balances": {"pre": pre_t[:_MAX_ROWS], "post": post_t[:_MAX_ROWS], "pre_total": len(pre_t), "post_total": len(post_t), "owner_counts": owners},
        "error": None if err is None else json.dumps(err, default=str)[:300],
        "programs": progs[:12], "program_hints": {p: _PROGRAM_HINTS[p] for p in progs if p in _PROGRAM_HINTS},
        "decoded": {"decoder_version": DECODER_VERSION, "kind": d.kind, "reason": d.reason, "token_mint": d.mint, "token_delta": d.token_delta,
                    "sol_amount": round(d.quote_sol, 9), "fee_sol": d.fee_sol, "pre_token_amount": d.pre_token_amount, "post_token_amount": d.post_token_amount,
                    "legs": [[m, v] for m, v in d.legs], "explanation": _explain(d)}}


class DiagnosticSamples:
    """Keeps the first transaction seen for each distinct (kind, reason) and picks up to three useful ones: the bulk outcome first
    (it explains what most of the window is), then UNKNOWN / trades / transfers before more OTHER."""
    MAX = 3
    MAX_KEYS = 24

    def __init__(self) -> None:
        self.counts: Counter = Counter()
        self.first: dict[tuple, dict] = {}

    def offer(self, tx: dict, d: DecodedTx, role: dict | None = None) -> None:
        key = (d.kind, d.reason or "-")
        self.counts[key] += 1
        if key in self.first or len(self.first) >= self.MAX_KEYS:
            return
        try:
            self.first[key] = build_tx_sample(tx, d.wallet, d, role)
        except Exception as exc:   # a diagnostic must never break the verification
            self.first[key] = {"signature": d.signature, "sample_error": f"could not build sample: {type(exc).__name__}", "decoded": {"kind": d.kind, "reason": d.reason}}

    def pick(self) -> list[dict]:
        if not self.first:
            return []
        bulk = max(self.first, key=lambda k: self.counts[k])
        rest = sorted((k for k in self.first if k != bulk), key=lambda k: (_KIND_RANK.get(k[0], 9), -self.counts[k]))
        out = []
        for n, k in enumerate(([bulk] + rest)[:self.MAX]):
            why = f"most common outcome in this window" if n == 0 else ("decoder could not decide; inspect first" if k[0] == "UNKNOWN" else f"a different outcome ({k[0]})")
            out.append({"selected_because": why, "outcome": f"{k[0]}/{k[1]}", "occurrences": self.counts[k], **self.first[k]})
        return out


def _attribution(address: str) -> dict:
    rec = seed_registry().get(address)
    independent = [e for e in (rec.evidence if rec else []) if e.kind in CONFIRM_KINDS and e.independent]
    return {"registry_status": rec.status if rec else "NOT_IN_REGISTRY", "on_chain_attribution": "UNVERIFIED",
            "registry_evidence_is_third_party_only": not any(e.kind in CONFIRM_KINDS for e in (rec.evidence if rec else [])),
            "independent_confirming_evidence": len(independent),
            "statement": "Nothing in this file proves who controls this wallet. Trading activity alone cannot establish identity; only self-confirmation by the "
                         "claimed owner or independent on-chain correlation can. Treat the wallet as UNVERIFIED until then."}


def _client_ctx(client):
    from contextlib import nullcontext
    if client is not None:
        return nullcontext(client)
    try:
        import httpx  # only needed here
    except ImportError:
        raise RuntimeError("httpx is not installed (pip install httpx)") from None
    return httpx.Client(timeout=25)


def run_verification(address: str, rpc_url: str, limit: int = 40, scan: int = 2000, client=None) -> dict:
    """Blocking. Decodes the wallet's recent REAL activity and returns evidence. Never changes any status.
    `client` is only for tests (anything with .post(url, json=...)); normally an httpx client is created here.

    Path 1 (fast): Helius getTransactionsForAddress, filtered to successful transactions that touch the wallet or change
    its token balances (removes failed-transaction spam), full data in ONE call.
    Path 2 (fallback, any RPC): scan signatures, skip failed ones, sample evenly across the window."""
    if not _B58.match(address):
        raise ValueError("not a valid Solana address")
    kinds, mints, inflows, samples, times, errors = Counter(), set(), Counter(), [], [], []
    reasons, other_samples, recent = Counter(), [], []
    reason_codes, signer_counts, signer_basis = Counter(), Counter(), Counter()
    diag = DiagnosticSamples()
    info: dict = {}

    def ingest(t) -> None:
        d = decode_transaction(t, address)
        role = wallet_role(t, address)
        found = []   # parse first: a malformed transaction raises here, before any counter is touched
        for ins in t["transaction"]["message"].get("instructions", []):
            p = ins.get("parsed")
            if ins.get("program") == "system" and isinstance(p, dict) and p.get("type") == "transfer" and p["info"].get("destination") == address:
                found.append((p["info"]["source"], p["info"]["lamports"] / LAMPORTS))
        kinds[d.kind] += 1
        reasons[f"{d.kind}: {d.note or 'ok'}"] += 1
        reason_codes[f"{d.kind}/{d.reason or '-'}"] += 1
        signer_counts["signed" if role["is_signer"] is True else "not_signed" if role["is_signer"] is False else "unknown"] += 1
        signer_basis[role["signer_basis"]] += 1
        diag.offer(t, d, role)
        if d.kind in ("BUY", "SELL", "TRANSFER") and len(recent) < 25:
            recent.append({"kind": d.kind, "mint": d.mint, "sol": round(d.quote_sol, 4), "time": d.block_time, "signature": d.signature})
        if d.kind in ("OTHER", "UNKNOWN") and len(other_samples) < 5:
            other_samples.append({"signature": d.signature, "kind": d.kind, "reason": d.reason, "note": d.note, "quote_sol": round(d.quote_sol, 6)})
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
            msg = redact_text(f"{type(exc).__name__}: {exc}", rpc_url)[:160]
            if msg not in errors and len(errors) < 3:
                errors.append(msg)

    with _client_ctx(client) as c:
        data = None
        label = "successful transactions touching the wallet or changing its token balances"
        try:
            rows, token, helius_max = [], None, max(100, min(scan, 500))
            while len(rows) < helius_max:
                page, token = _helius_page(_rpc(c, rpc_url, "getTransactionsForAddress", [address, _helius_cfg(token)]))
                rows += page
                if not token or not page:
                    break
            data = rows
            info["source"] = "helius_filtered: " + label
        except Exception as exc:   # not Helius, plan without this method, provider outage... fall back to the standard path
            err = _wrap_exc(exc, rpc_url)
            info["helius_filter_errors"] = [redact_text(f"{label}: {err.kind}: {err}", rpc_url)[:200]]
            info["helius_error_kind"] = err.kind
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
                    role = wallet_role(t, address)
                    msg = t["transaction"]["message"]
                    failed_samples.append({"signature": f["signature"], "fee_payer_is_wallet": role["is_fee_payer"], "wallet_is_signer": role["is_signer"], "fee_payer": role["fee_payer"],
                                           "error": str(f.get("err"))[:80], "programs": sorted({i.get("programId", "?") for i in msg.get("instructions", [])})[:4]})
                except Exception as exc:
                    failed_samples.append({"signature": f["signature"], "error": f"could not read: {type(exc).__name__}"})
            for sg in ok[::max(1, len(ok) // max(1, limit))][:limit]:
                try:
                    t = _rpc(c, rpc_url, "getTransaction", [sg["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 255, "commitment": "confirmed"}])
                except Exception as exc:
                    kinds["ERROR"] += 1
                    if len(errors) < 3:
                        errors.append(redact_text(f"{type(exc).__name__}: {exc}", rpc_url)[:160])
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
    out = {"address": address, "checked_at": datetime.now(timezone.utc).isoformat(), "decoder_version": DECODER_VERSION, **info, "window": window,
           "attribution": _attribution(address),
           "kinds": dict(kinds), "reasons": dict(reasons), "reason_codes": dict(reason_codes.most_common()),
           "signer_summary": {**dict(signer_counts), "basis": dict(signer_basis)},
           "other_samples": other_samples, "errors_sample": errors, "diagnostic_samples": diag.pick(),
           "distinct_tokens": len(mints),
           "decoded_oldest": iso(min(times)) if times else None, "decoded_newest": iso(max(times)) if times else None,
           "rpc_is_public_default": rpc_url == PUBLIC_RPC,
           "largest_sol_inflow_sources": [{"source": k, "sol": round(v, 3)} for k, v in inflows.most_common(5)],
           "sample_trades": samples, "recent_activity": recent,
           "limitations": ["SOL values are estimates (may include ATA rent, close refunds, tips).",
                           "Only SOL/wSOL-quoted swaps are priced; token-to-token and stablecoin swaps are reported as OTHER with both legs.",
                           "A transfer that also pays a new recipient's ATA rent looks like an ambiguous SOL movement and is reported as UNKNOWN."],
           "next_steps": ["Read diagnostic_samples and reason_codes first: they say why transactions were not trades",
                          "Compare sample_trades times with trades the claimed owner posted themselves, if any",
                          "Check whether any inflow source is a known exchange or a previously known wallet of the claimed owner",
                          "Only then add on_chain_correlation evidence and consider promotion"]}
    return redact(out, rpc_url)


def verify(argv) -> int:
    ap = argparse.ArgumentParser(prog="cupsey.py verify")
    ap.add_argument("address", nargs="?", default=DEFAULT_ADDRESS)
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--scan", type=int, default=2000)
    a = ap.parse_args(argv)
    url = os.getenv("SOLANA_RPC_URL") or PUBLIC_RPC
    try:
        out = run_verification(a.address, url, a.limit, a.scan)
    except Exception as exc:   # clean message instead of a traceback; never print the URL or key
        print("verification failed:", redact_text(f"{type(exc).__name__}: {exc}", url)[:300])
        return 1
    path = f"{a.address}_evidence.json"
    with open(path, "w") as fh:
        json.dump(out, fh, indent=2)
    print(json.dumps({k: out[k] for k in ("source", "window", "kinds", "reason_codes", "signer_summary", "decoded_newest")}, indent=2))
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
        self.checked = self.polls = self.foreign = self.signed = self.signer_unknown = 0
        # signed = the wallet signed it; foreign = signer data says the wallet did NOT sign (someone else initiated it); signer_unknown = no signer data
        self.reason_codes: Counter = Counter()
        self.helius_ok = False   # becomes True after the first successful Helius read
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
            try:
                r = await self.client.post(self.rpc_url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            except Exception as exc:
                raise _wrap_exc(exc, self.rpc_url) from None
            retry, result = _read_rpc_response(r.status_code, r.json)
            if retry:
                await asyncio.sleep(1 + attempt)
                continue
            return result
        raise RpcError("rate_limited", "rate limited by the RPC provider", status=429)

    async def _fetch_helius(self, since: int | None, cap: int) -> list:
        rows, token = [], None
        while len(rows) < cap:
            page, token = _helius_page(await self._rpc("getTransactionsForAddress", [self.address, _helius_cfg(token, since)]))
            rows += page
            if not token or not page:
                break
        self.helius_ok = True
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
        got = [t for t in await asyncio.gather(*(one(s) for s in ok)) if t]
        if ok and not got:   # every read failed: say so instead of looking like a quiet wallet
            raise RpcError("server", f"could not read any of {len(ok)} transactions (provider errors); will retry")
        return got

    async def _fetch(self, since: int | None, cap: int) -> list:
        if self.mode == "helius":
            try:
                return await self._fetch_helius(since, cap)
            except Exception as exc:
                kind = getattr(exc, "kind", None)
                if since and "blockTime" in str(exc):   # provider dislikes the time filter: re-read the latest page, duplicates are skipped
                    return await self._fetch_helius(None, 100)
                transient = kind in ("timeout", "network", "rate_limited", "server")
                if not self.helius_ok and not transient:   # never worked and not a hiccup: this RPC is probably not Helius
                    self.mode = "standard"
                    self.last_error = "Helius method not available on this RPC" + (f" ({kind})" if kind else "") + "; using the slower standard method."
                else:
                    raise
        return await self._fetch_standard(cap)

    # ---- processing
    def ingest(self, txs: list) -> int:
        txs = sorted(txs, key=lambda t: (t.get("slot") or 0, t.get("blockTime") or 0))   # oldest first so positions build correctly
        new = 0
        for t in txs:
            try:
                if not isinstance(t, dict) or not (t.get("transaction") or {}).get("message") or t.get("meta") is None:
                    raise ValueError("incomplete transaction")
                d = decode_transaction(t, self.address)
                role = wallet_role(t, self.address)
            except Exception:
                self.kinds["ERROR"] += 1
                continue
            ev = self.ledger.apply(d)
            if ev is None:
                continue
            new += 1
            self.checked += 1
            if role["is_signer"] is True:
                self.signed += 1
            elif role["is_signer"] is False:
                self.foreign += 1
            else:
                self.signer_unknown += 1
            self.reason_codes[f"{d.kind}/{d.reason or '-'}"] += 1
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
                    status = getattr(exc, "status", None) or getattr(getattr(exc, "response", None), "status_code", None)
                    self.last_error = redact_text(f"{type(exc).__name__}" + (f" (HTTP {status})" if status else "") + f": {exc}", self.rpc_url)[:200]
                self.polls += 1
                self.last_poll = time.time()
                await asyncio.sleep(self.interval)

    def snapshot(self) -> dict:
        opens = [asdict(p) for p in self.ledger.open.values() if p.qty > EPS]
        opens.sort(key=lambda p: p.get("opened_at") or 0, reverse=True)
        return {"address": self.address, "started": self.started, "last_poll": self.last_poll, "last_ok": self.last_ok, "error": self.last_error,
                "interval": self.interval, "mode": self.mode, "polls": self.polls, "backfilled": self.backfilled, "checked": self.checked,
                "foreign": self.foreign, "signed": self.signed, "signer_unknown": self.signer_unknown,
                "reason_codes": self.reason_codes.most_common(8), "kinds": dict(self.kinds), "reasons": self.reasons.most_common(5), "newest": self.newest,
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
            signed, foreign, unk = s.get("signed", 0), s["foreign"], s.get("signer_unknown", 0)
            hint = ""
            if signed == 0 and unk == 0 and foreign > 0.8 * s["checked"]:
                hint = (" Nothing in this window was initiated by the wallet itself. That fits a receive-only or inactive address, but it does not show who owns it"
                        " or whether it trades elsewhere: check the address on Solscan before switching CUPSEY_WALLET.")
            codes = " | ".join(f"{n} x {c}" for c, n in s.get("reason_codes", [])[:5])
            body.append('<div class="card"><b>Why nothing yet?</b><div class="sub">Of %d transactions read: %d were signed by this wallet, %d were started by someone else '
                        '(for example incoming dust or airdrops), %d had no signer data. %d had no token trade in them.%s<br>%s<br>%s</div></div>'
                        % (s["checked"], signed, foreign, unk, noise, e(hint), e(codes), e(" | ".join(f"{n} x {r}" for r, n in s["reasons"]))))
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
            body.append('<div class="card warn"><b>No buys or sells found in this window.</b><br>Either the wallet has not traded recently, or it is not an active trading wallet. We do not guess: nothing is shown as a trade unless the blockchain shows tokens swapped for SOL.</div>')
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
            status = getattr(exc, "status", None) or getattr(getattr(exc, "response", None), "status_code", None)
            msg = f"{type(exc).__name__}" + (f" (HTTP {status})" if status else "") + (f": {exc}" if isinstance(exc, (ValueError, RuntimeError)) else "")
            return JSONResponse({"error": redact_text(msg, url)}, status_code=502)
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
