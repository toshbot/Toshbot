from datetime import timedelta

from app.models import utcnow
from app.repository import InMemoryRepository
from app.security.engine import SecurityAnalyzer, decide
from app.security.models import CheckResult
from app.security.policy import REQUIRED_CHECKS, TOKEN_2022_PROGRAM, SecurityPolicy
from tests.helpers import FIXTURE_CREATOR, fixture_mint
from tests.security_helpers import HEALTHY_HOLDERS, OTHER1, FakeRPC, fixture_token, mint_account

POLICY = SecurityPolicy()


async def run(rpc, token=None, repo=None):
    repo = repo or InMemoryRepository()
    token = token or fixture_token()
    await repo.create_token_if_absent(token)
    return await SecurityAnalyzer(rpc, repo, POLICY).analyze(token)


def check(name, status="PASS", age=0):
    return CheckResult(name=name, status=status, summary=f"{name} {status}", retrieved_at=utcnow() - timedelta(seconds=age))


def all_checks(**overrides):
    return [check(n, overrides.get(n, "PASS")) for n in REQUIRED_CHECKS]


def by_name(report, name):
    return next(c for c in report.checks if c.name == name)


# --- end-to-end through the analyzer ---------------------------------------------------------------

async def test_clean_token_is_watch_never_qualified():
    r = await run(FakeRPC())
    assert r.status == "WATCH"
    assert "QUALIFIED" not in r.status
    assert r.not_evaluated  # unbuilt checks are listed, not silently skipped


async def test_active_mint_authority_is_rejected():
    r = await run(FakeRPC(mint=mint_account(mint_auth=OTHER1)))
    assert r.status == "REJECTED" and "Mint authority is active" in r.reason


async def test_active_freeze_authority_is_rejected():
    r = await run(FakeRPC(mint=mint_account(freeze=OTHER1)))
    assert r.status == "REJECTED" and "Freeze authority" in r.reason


async def test_provider_outage_is_insufficient_data_not_rejection():
    r = await run(FakeRPC(fail={"get_account_info", "get_balance", "get_signatures_for_address"}))
    assert r.status == "INSUFFICIENT_DATA"
    assert not any(c.status == "FAIL_CRITICAL" for c in r.checks)
    assert any(c.status == "UNAVAILABLE" for c in r.checks)


async def test_mint_not_visible_yet_is_insufficient_data():
    r = await run(FakeRPC(use_default_mint=False))
    assert r.status == "INSUFFICIENT_DATA"


async def test_malformed_rpc_data_is_unavailable_not_pass_or_fail():
    r = await run(FakeRPC(mint={"owner": "x", "data": "garbage"}))
    assert r.status == "INSUFFICIENT_DATA"


async def test_permanent_delegate_is_rejected():
    ext = [{"extension": "permanentDelegate", "state": {"delegate": OTHER1}}]
    r = await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=ext)))
    assert r.status == "REJECTED" and "Permanent delegate" in r.reason


async def test_excessive_transfer_fee_is_rejected_small_fee_is_hold():
    big = [{"extension": "transferFeeConfig", "state": {"newerTransferFee": {"transferFeeBasisPoints": 2000}, "olderTransferFee": {"transferFeeBasisPoints": 0}}}]
    small = [{"extension": "transferFeeConfig", "state": {"newerTransferFee": {"transferFeeBasisPoints": 100}, "olderTransferFee": {"transferFeeBasisPoints": 100}}}]
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=big)))).status == "REJECTED"
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=small)))).status == "HOLD"


async def test_transfer_hook_and_unknown_extension_are_hold():
    hook = [{"extension": "transferHook", "state": {"programId": OTHER1}}]
    unknown = [{"extension": "somethingNew", "state": {}}]
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=hook)))).status == "HOLD"
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=unknown)))).status == "HOLD"


async def test_benign_token2022_extensions_pass():
    ext = [{"extension": "metadataPointer", "state": {}}, {"extension": "tokenMetadata", "state": {}}]
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=ext)))).status == "WATCH"


async def test_bonding_curve_balance_is_excluded_from_concentration():
    r = await run(FakeRPC())  # curve holds 80% of supply
    h = by_name(r, "holder_concentration")
    assert h.status == "PASS"
    assert any(e["reason"] == "bonding_curve" for e in h.evidence["excluded"])
    assert "methodology" in h.evidence


async def test_whale_wallet_is_rejected():
    rpc = FakeRPC(holders=[("TA1", OTHER1, 45.0), ("TA2", fixture_mint(9), 40.0)])
    r = await run(rpc)
    assert r.status == "REJECTED" and "holds 45.00%" in r.reason


async def test_creator_heavy_holding_is_hold():
    rpc = FakeRPC(holders=[("TA1", fixture_mint(9), 70.0), ("TA2", FIXTURE_CREATOR, 20.0)])
    r = await run(rpc)
    assert r.status == "HOLD" and "Creator wallet holds" in r.reason


async def test_serial_launcher_is_visible_in_report():
    repo = InMemoryRepository()
    for i in range(2, 7):  # 5 more launches by the same creator
        await repo.create_token_if_absent(fixture_token(i))
    r = await run(FakeRPC(), repo=repo)
    creator = by_name(r, "creator_wallet")
    assert r.status == "HOLD" and creator.status == "SUSPICIOUS"
    assert creator.evidence["launches_seen_by_tosh_bot_24h"] >= 5


async def test_unknown_bonding_curve_makes_liquidity_unavailable():
    r = await run(FakeRPC(), token=fixture_token(bonding_curve=None))
    assert by_name(r, "liquidity").status == "UNAVAILABLE" and r.status == "INSUFFICIENT_DATA"


async def test_low_curve_reserve_is_hold():
    r = await run(FakeRPC(curve_lamports=1_000_000))
    assert r.status == "HOLD"


async def test_every_check_has_explanation_evidence_sources_and_timestamp():
    r = await run(FakeRPC())
    assert r.reason and r.analysis_version and r.analyzed_at and r.next_review_at and r.policy
    for c in r.checks:
        assert c.summary and c.evidence and c.sources and c.retrieved_at


# --- decision rules in isolation --------------------------------------------------------------------

def test_missing_mandatory_check_prevents_approval():
    results = [c for c in all_checks() if c.name != "liquidity"]
    status, *_ = decide(results, utcnow(), POLICY)
    assert status == "INSUFFICIENT_DATA"


def test_unavailable_mandatory_check_is_not_a_pass():
    assert decide(all_checks(liquidity="UNAVAILABLE"), utcnow(), POLICY)[0] == "INSUFFICIENT_DATA"


def test_critical_failure_cannot_be_offset_by_any_number_of_passes():
    status, reason, *_ = decide(all_checks(mint_permissions="FAIL_CRITICAL"), utcnow(), POLICY)
    assert status == "REJECTED"


def test_critical_failure_beats_missing_data():
    results = [c for c in all_checks(mint_permissions="FAIL_CRITICAL") if c.name != "liquidity"]
    assert decide(results, utcnow(), POLICY)[0] == "REJECTED"


def test_stale_evidence_cannot_silently_pass():
    stale = [check(n, "PASS", age=POLICY.evidence_max_age_seconds + 60) for n in REQUIRED_CHECKS]
    status, reason, *_ = decide(stale, utcnow(), POLICY)
    assert status == "INSUFFICIENT_DATA" and "stale" in reason


def test_all_pass_never_returns_qualified():
    assert decide(all_checks(), utcnow(), POLICY)[0] == "WATCH"
