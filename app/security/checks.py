"""Deterministic security checks. Rules:
- A check FAILS only on verified on-chain facts.
- Any provider error, missing account or unexpected data shape is UNAVAILABLE, never a failure and never a pass.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.models import TokenRecord, utcnow
from app.repository import Repository
from app.rpc import SolanaRPC
from app.security.models import CheckResult
from app.security.policy import BURN_ADDRESSES, TOKEN_2022_PROGRAM, TOKEN_PROGRAM, SecurityPolicy

SRC_RPC = "solana_rpc"
BENIGN_EXTENSIONS = {"metadatapointer", "tokenmetadata", "grouppointer", "groupmemberpointer", "tokengroup", "tokengroupmember", "mintcloseauthority"}


@dataclass
class MintData:
    owner: str
    info: dict
    supply_raw: int
    decimals: int


def parse_mint(account: dict) -> MintData:
    parsed = account["data"]["parsed"]
    if parsed.get("type") != "mint":
        raise ValueError("account is not a mint")
    info = parsed["info"]
    return MintData(owner=account["owner"], info=info, supply_raw=int(info["supply"]), decimals=int(info["decimals"]))


def result(name, status, summary, evidence=None, warnings=None, sources=None, mandatory=True) -> CheckResult:
    return CheckResult(name=name, status=status, summary=summary, evidence=evidence or {}, warnings=warnings or [],
                       sources=sources or [SRC_RPC], mandatory=mandatory, retrieved_at=utcnow())


def unavailable(name: str, why: str) -> CheckResult:
    return result(name, "UNAVAILABLE", f"Could not be verified: {why}", warnings=["Missing evidence is never treated as a pass."])


def check_mint_permissions(md: MintData) -> CheckResult:
    mint_auth, freeze_auth = md.info.get("mintAuthority"), md.info.get("freezeAuthority")
    evidence = {"token_program": md.owner, "mint_authority": mint_auth, "freeze_authority": freeze_auth,
                "supply_raw": str(md.supply_raw), "decimals": md.decimals}
    problems = []
    if mint_auth:
        problems.append("Mint authority is active: additional supply can be minted.")
    if freeze_auth:
        problems.append("Freeze authority is active: holder accounts can be frozen.")
    if problems:
        return result("mint_permissions", "FAIL_CRITICAL", " ".join(problems), evidence)
    if md.owner not in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        return result("mint_permissions", "SUSPICIOUS", "Mint is owned by an unrecognised token program.", evidence)
    return result("mint_permissions", "PASS", "Mint and freeze authorities are revoked.", evidence,
                  warnings=["Revoked authorities are a positive indicator, not proof of safety."])


def check_token_extensions(md: MintData, policy: SecurityPolicy) -> CheckResult:
    if md.owner == TOKEN_PROGRAM:
        return result("token_extensions", "PASS", "Classic SPL Token program: no extensions exist.", {"token_program": md.owner})
    if md.owner != TOKEN_2022_PROGRAM:
        return result("token_extensions", "SUSPICIOUS", "Unrecognised token program; extensions cannot be assessed.", {"token_program": md.owner})

    extensions = md.info.get("extensions") or []
    critical, suspicious, names = [], [], []
    for ext in extensions:
        name = str(ext.get("extension", "unknown"))
        state = ext.get("state") or {}
        names.append(name)
        key = name.lower()
        if key == "permanentdelegate":
            if state.get("delegate"):
                critical.append("Permanent delegate is set: it can move or burn any holder's tokens.")
        elif key == "nontransferable":
            critical.append("Token is non-transferable.")
        elif key == "defaultaccountstate":
            if str(state.get("accountState", "")).lower() == "frozen":
                critical.append("New token accounts default to frozen.")
        elif key == "transferfeeconfig":
            fees = [f.get("transferFeeBasisPoints") for f in (state.get("newerTransferFee"), state.get("olderTransferFee")) if isinstance(f, dict)]
            bps = max([int(f) for f in fees if f is not None], default=0)
            if bps > policy.max_transfer_fee_bps:
                critical.append(f"Transfer fee of {bps / 100:.2f}% exceeds the {policy.max_transfer_fee_bps / 100:.2f}% limit.")
            elif bps > 0 or state.get("transferFeeConfigAuthority"):
                suspicious.append(f"Transfer fee configured ({bps / 100:.2f}%) and/or changeable by an authority.")
        elif key == "transferhook":
            if state.get("programId"):
                suspicious.append("Transfer hook program is set: custom code runs on every transfer.")
        elif key not in BENIGN_EXTENSIONS:
            suspicious.append(f"Extension '{name}' needs manual review.")
    evidence = {"token_program": md.owner, "extensions": names, "critical": critical, "suspicious": suspicious}
    if critical:
        return result("token_extensions", "FAIL_CRITICAL", " ".join(critical), evidence)
    if suspicious:
        return result("token_extensions", "SUSPICIOUS", " ".join(suspicious), evidence)
    return result("token_extensions", "PASS", "Token-2022 mint with no risky extensions.", evidence)


METHODOLOGY = ("Percentages are of TOTAL supply. Owners are resolved from the top-20 token accounts (RPC limit). "
               "Accounts owned by the bonding curve or burn addresses are excluded from holder ranking and listed separately. "
               "Balances are aggregated per owner. AMM pool vaults after migration are not yet identified.")


async def check_holders(rpc: SolanaRPC, token: TokenRecord, md: MintData, policy: SecurityPolicy) -> CheckResult:
    if md.supply_raw <= 0:
        return unavailable("holder_concentration", "mint supply is zero or unknown")
    largest = await rpc.get_token_largest_accounts(token.mint)
    if not largest:
        return unavailable("holder_concentration", "no token accounts returned yet")
    accounts = await rpc.get_multiple_accounts([x["address"] for x in largest])
    by_owner: dict[str, int] = {}
    excluded: list[dict] = []
    for item, acct in zip(largest, accounts):
        if not acct:
            return unavailable("holder_concentration", f"token account {item['address']} could not be read")
        owner = acct["data"]["parsed"]["info"]["owner"]
        amount = int(item["amount"])
        if owner in BURN_ADDRESSES:
            excluded.append({"owner": owner, "reason": "burn", "pct_of_supply": round(amount / md.supply_raw * 100, 4)})
        elif token.bonding_curve and owner == token.bonding_curve:
            excluded.append({"owner": owner, "reason": "bonding_curve", "pct_of_supply": round(amount / md.supply_raw * 100, 4)})
        else:
            by_owner[owner] = by_owner.get(owner, 0) + amount
    ranked = sorted(by_owner.items(), key=lambda kv: kv[1], reverse=True)
    holders = [{"owner": o, "pct_of_supply": round(a / md.supply_raw * 100, 4), "is_creator": o == token.creator} for o, a in ranked]
    top1 = holders[0]["pct_of_supply"] if holders else 0.0
    top5 = round(sum(h["pct_of_supply"] for h in holders[:5]), 4)
    top10 = round(sum(h["pct_of_supply"] for h in holders[:10]), 4)
    creator_pct = round(by_owner.get(token.creator, 0) / md.supply_raw * 100, 4) if token.creator else None
    evidence = {"top1_pct": top1, "top5_pct": top5, "top10_pct": top10, "creator_pct": creator_pct,
                "holders": holders[:10], "excluded": excluded, "methodology": METHODOLOGY}
    warnings = [] if token.bonding_curve else ["Bonding curve address unknown: pool/curve accounts may be counted as holders."]
    if top1 > policy.single_holder_reject_pct:
        return result("holder_concentration", "FAIL_CRITICAL",
                      f"One wallet holds {top1:.2f}% of supply (limit {policy.single_holder_reject_pct:.0f}%).", evidence, warnings)
    problems = []
    if top10 > policy.top10_hold_pct:
        problems.append(f"Top 10 wallets hold {top10:.2f}% of supply (limit {policy.top10_hold_pct:.0f}%).")
    if creator_pct is not None and creator_pct > policy.creator_hold_pct:
        problems.append(f"Creator wallet holds {creator_pct:.2f}% of supply (limit {policy.creator_hold_pct:.0f}%).")
    if problems:
        return result("holder_concentration", "SUSPICIOUS", " ".join(problems), evidence, warnings)
    return result("holder_concentration", "PASS", f"Top holder {top1:.2f}%, top 10 {top10:.2f}% of supply.", evidence, warnings)


async def check_creator(rpc: SolanaRPC, repo: Repository, token: TokenRecord, policy: SecurityPolicy, now: datetime | None = None) -> CheckResult:
    if not token.creator:
        return unavailable("creator_wallet", "creator address was not supplied by the launch feed")
    now = now or utcnow()
    sigs = await rpc.get_signatures_for_address(token.creator, limit=100)
    launches_24h = await repo.count_recent_tokens_by_creator(token.creator, now - timedelta(hours=24))
    times = [s["blockTime"] for s in sigs if s.get("blockTime")]
    oldest = datetime.fromtimestamp(min(times), tz=timezone.utc) if times else None
    complete = len(sigs) < 100
    evidence = {
        "creator": token.creator,
        "signatures_sampled": len(sigs),
        "oldest_activity_seen_at": oldest.isoformat() if oldest else None,
        "history_complete": complete,
        "wallet_age_days": round((now - oldest).total_seconds() / 86400, 2) if (oldest and complete) else None,
        "launches_seen_by_tosh_bot_24h": launches_24h,
        "scope_note": "Launch count only covers tokens this system has detected; transfer-level dump analysis is not implemented yet.",
    }
    warnings = []
    if complete and evidence["wallet_age_days"] is not None and evidence["wallet_age_days"] < 0.01:
        warnings.append("Brand-new wallet. This alone is not treated as malicious.")
    if launches_24h >= policy.creator_max_launches_24h:
        return result("creator_wallet", "SUSPICIOUS",
                      f"Creator launched {launches_24h} tokens in 24h (limit {policy.creator_max_launches_24h}).", evidence, warnings)
    return result("creator_wallet", "PASS", f"Creator launched {launches_24h} token(s) in the last 24h.", evidence, warnings)


async def check_liquidity(rpc: SolanaRPC, token: TokenRecord, policy: SecurityPolicy) -> CheckResult:
    if not token.bonding_curve:
        return unavailable("liquidity", "bonding curve address unknown; pool analysis needs Phase 3 market data")
    lamports = await rpc.get_balance(token.bonding_curve)
    sol = lamports / 1e9
    evidence = {"bonding_curve": token.bonding_curve, "curve_lamports": lamports, "curve_sol": round(sol, 6),
                "note": "SOL held by the bonding-curve account (includes a small rent amount). Curve liquidity is protocol-controlled, not creator-lockable."}
    if sol < policy.min_curve_reserve_sol:
        return result("liquidity", "SUSPICIOUS", f"Curve holds only {sol:.4f} SOL (minimum {policy.min_curve_reserve_sol}).", evidence)
    return result("liquidity", "PASS", f"Curve holds {sol:.4f} SOL.", evidence)
