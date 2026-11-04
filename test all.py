"""All tests in one file (OPTIONAL: the app runs without it). Uses synthetic FIXTURE data only, never live data.
Run from the repo root next to main.py:   pip install -r requirements.txt && pytest -q test_all.py
"""
import pytest
import json
import pytest
import asyncio
from contextlib import asynccontextmanager
import httpx
from datetime import timedelta
from fastapi.testclient import TestClient
import os
from main import *  # noqa: F401,F403


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZ"  # valid base58 characters
FIXTURE_CREATOR = "1" * 32


def fixture_mint(i: int = 0) -> str:
    return ALPHABET[i % len(ALPHABET)] * 44


def create_message(i: int = 0, **overrides) -> str:
    data = {
        "signature": f"FIXTURE-SIG-{i}",
        "mint": fixture_mint(i),
        "traderPublicKey": FIXTURE_CREATOR,
        "txType": "create",
        "initialBuy": 1000.0,
        "solAmount": 1.5,
        "name": f"Fixture Token {i}",
        "symbol": f"FX{i}",
        "uri": "https://example.invalid/meta.json",
        "pool": "pump",
        "marketCapSol": 30.0,
    }
    data.update(overrides)
    return json.dumps(data)


# ----------------------------------------------------------------------
# security_helpers
# ----------------------------------------------------------------------
SUPPLY = 10**15  # 1B tokens, 6 decimals
CURVE, OTHER1, OTHER2 = fixture_mint(9), fixture_mint(10), fixture_mint(11)


def mint_account(owner=TOKEN_PROGRAM, mint_auth=None, freeze=None, supply=SUPPLY, extensions=None) -> dict:
    info = {"mintAuthority": mint_auth, "freezeAuthority": freeze, "supply": str(supply), "decimals": 6}
    if extensions is not None:
        info["extensions"] = extensions
    return {"owner": owner, "data": {"program": "spl-token", "parsed": {"type": "mint", "info": info}}}


def token_account(owner: str) -> dict:
    return {"data": {"parsed": {"type": "account", "info": {"owner": owner}}}}


def fixture_token(i=1, **kw) -> TokenRecord:
    base = dict(mint=fixture_mint(i), creator=FIXTURE_CREATOR, bonding_curve=CURVE, name="Fixture", symbol="FX")
    base.update(kw)
    return TokenRecord(**base)


# (token account address, owner, percent of supply)
HEALTHY_HOLDERS = [("TA1", CURVE, 80.0), ("TA2", FIXTURE_CREATOR, 5.0), ("TA3", OTHER1, 3.0), ("TA4", OTHER2, 2.0)]


class FakeRPC:
    def __init__(self, mint=None, holders=HEALTHY_HOLDERS, curve_lamports=2_000_000_000, sigs=None, fail=(), use_default_mint=True):
        self.mint = mint_account() if (mint is None and use_default_mint) else mint
        self.holders, self.curve_lamports, self.fail = holders, curve_lamports, set(fail)
        self.sigs = [{"signature": "FIXTURE", "blockTime": 1_700_000_000}] if sigs is None else sigs

    def _maybe_fail(self, name):
        if name in self.fail:
            raise RpcUnavailable(f"fixture outage in {name}")

    async def get_account_info(self, address):
        self._maybe_fail("get_account_info")
        return self.mint

    async def get_token_largest_accounts(self, mint):
        self._maybe_fail("get_token_largest_accounts")
        return [{"address": a, "amount": str(int(SUPPLY * pct / 100))} for a, _, pct in self.holders]

    async def get_multiple_accounts(self, addresses):
        self._maybe_fail("get_multiple_accounts")
        owners = {a: o for a, o, _ in self.holders}
        return [token_account(owners[a]) for a in addresses]

    async def get_balance(self, address):
        self._maybe_fail("get_balance")
        return self.curve_lamports

    async def get_signatures_for_address(self, address, limit=100):
        self._maybe_fail("get_signatures_for_address")
        return self.sigs

    async def aclose(self):
        pass


# ----------------------------------------------------------------------
# test_parser
# ----------------------------------------------------------------------
def test_valid_create_event_is_parsed():
    event = parse_message(create_message(1))
    assert event.mint == fixture_mint(1)
    assert event.creator == FIXTURE_CREATOR
    assert event.symbol == "FX1"
    assert event.received_at is not None  # local receipt time always recorded


def test_missing_optional_metadata_does_not_discard_event():
    msg = json.dumps({"mint": fixture_mint(2), "txType": "create"})
    event = parse_message(msg)
    assert event.mint == fixture_mint(2)
    assert event.name is None and event.creator is None


def test_subscription_ack_is_ignored():
    assert parse_message(json.dumps({"message": "Successfully subscribed to keys."})) is None


def test_trade_event_is_ignored():
    assert parse_message(create_message(3, txType="buy")) is None


def test_invalid_mint_raises():
    with pytest.raises(ParseError):
        parse_message(create_message(4, mint="not-a-real-address"))


def test_malformed_json_raises():
    with pytest.raises(ParseError):
        parse_message("{not json")


def test_provider_error_message_is_not_silently_ignored():
    with pytest.raises(ParseError):
        parse_message(json.dumps({"errors": ["rate limited"]}))


def test_garbage_numbers_become_none_not_crash():
    event = parse_message(create_message(5, solAmount="NaN", initialBuy="abc"))
    assert event.sol_amount is None and event.initial_buy_tokens is None


# ----------------------------------------------------------------------
# test_processor
# ----------------------------------------------------------------------
def make_proc():
    repo, metrics = InMemoryRepository(), Metrics()
    proc = TokenProcessor(repo, asyncio.Queue(), HealthRegistry(), metrics, sleep=lambda s: asyncio.sleep(0))
    return repo, metrics, proc


@pytest.mark.asyncio
async def test_new_token_is_stored_with_fail_closed_status():
    repo, metrics, proc = make_proc()
    assert await proc.handle(parse_message(create_message(1))) is True
    token = next(iter(repo.tokens.values()))
    assert token.status == "INSUFFICIENT_DATA"  # never approved without security checks
    assert len(repo.jobs) == 1 and metrics.stored == 1


@pytest.mark.asyncio
async def test_duplicate_events_do_not_create_duplicate_tokens():
    repo, metrics, proc = make_proc()
    event = parse_message(create_message(1))
    await proc.handle(event)
    await proc.handle(event)
    await proc.handle(parse_message(create_message(1, signature="FIXTURE-OTHER-SIG")))  # same mint, new signature
    assert len(repo.tokens) == 1
    assert len(repo.jobs) == 1
    assert metrics.stored == 1 and metrics.duplicates == 2


@pytest.mark.asyncio
async def test_concurrent_creates_for_same_mint_yield_one_token():
    repo, _, _ = make_proc()


    token = TokenRecord.from_event(parse_message(create_message(2)))
    results = await asyncio.gather(*[repo.create_token_if_absent(token) for _ in range(20)])
    assert results.count(True) == 1


@pytest.mark.asyncio
async def test_storage_failure_retries_then_counts_failed_job():
    repo, metrics, proc = make_proc()

    async def boom(_):
        raise RuntimeError("db down")

    repo.record_launch_event = boom
    await proc._handle_with_retry(parse_message(create_message(3)))
    assert metrics.failed_jobs == 1
    assert proc.health.get("database").status == "error"


# ----------------------------------------------------------------------
# test_listener
# ----------------------------------------------------------------------
class FakeWS:
    def __init__(self, messages, on_done=None):
        self.messages, self.on_done, self.sent = messages, on_done, []

    async def send(self, data):
        self.sent.append(data)

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for m in self.messages:
            yield m
        if self.on_done:
            self.on_done()


def make_connect(script):
    calls = []

    @asynccontextmanager
    async def connect(url):
        step = script[len(calls)]
        calls.append(url)
        if isinstance(step, Exception):
            raise step
        yield step

    return connect, calls


def build_listener(script, **kw):
    queue, health, metrics = asyncio.Queue(), HealthRegistry(), Metrics()
    sleeps = []

    async def fake_sleep(d):
        sleeps.append(d)

    connect, calls = make_connect(script)
    listener = PumpPortalListener("wss://fixture.invalid", queue, health, metrics, connect=connect,
                                  sleep=fake_sleep, base_delay=1.0, max_delay=60.0, rng=lambda: 1.0, **kw)
    return listener, queue, health, metrics, sleeps, calls


@pytest.mark.asyncio
async def test_reconnects_after_failure_and_queues_events():
    stop = asyncio.Event()
    ws = FakeWS([create_message(1), "garbage", json.dumps({"message": "Successfully subscribed"})], on_done=stop.set)
    listener, queue, health, metrics, sleeps, calls = build_listener([OSError("boom"), ws])
    await listener.run(stop)
    assert len(calls) == 2 and sleeps == [1.0]
    assert queue.qsize() == 1
    assert (metrics.parsed, metrics.parse_errors, metrics.ignored) == (1, 1, 1)
    assert json.loads(ws.sent[0]) == {"method": "subscribeNewToken"}
    assert health.get(LISTENER_PROVIDER).reconnect_count == 1


@pytest.mark.asyncio
async def test_exponential_backoff_delays_grow():
    stop = asyncio.Event()
    ws = FakeWS([], on_done=stop.set)
    listener, _, health, _, sleeps, _ = build_listener([OSError("a"), OSError("b"), OSError("c"), ws])
    await listener.run(stop)
    assert sleeps == [1.0, 2.0, 4.0]


@pytest.mark.asyncio
async def test_failure_is_visible_in_health():
    stop = asyncio.Event()
    listener, _, health, _, _, _ = build_listener([OSError("refused"), FakeWS([], on_done=stop.set)])
    seen = {}
    original_sleep = listener._sleep

    async def spy(d):
        seen["status"] = health.get(LISTENER_PROVIDER).status
        seen["error"] = health.get(LISTENER_PROVIDER).last_error
        await original_sleep(d)

    listener._sleep = spy
    await listener.run(stop)
    assert seen["status"] == "error" and "refused" in seen["error"]


@pytest.mark.asyncio
async def test_full_queue_drops_are_counted_not_hidden():
    stop = asyncio.Event()
    queue, health, metrics = asyncio.Queue(maxsize=1), HealthRegistry(), Metrics()
    connect, _ = make_connect([FakeWS([create_message(1), create_message(2)], on_done=stop.set)])

    async def nosleep(d):
        pass

    listener = PumpPortalListener("wss://fixture.invalid", queue, health, metrics, connect=connect, sleep=nosleep)
    await listener.run(stop)
    assert metrics.dropped_queue_full == 1


# ----------------------------------------------------------------------
# test_analysis_worker
# ----------------------------------------------------------------------
def build_worker(rpc, sleeps=None):
    repo, metrics = InMemoryRepository(), Metrics()

    async def fake_sleep(d):
        if sleeps is not None:
            sleeps.append(d)

    worker = AnalysisWorker(repo, SecurityAnalyzer(rpc, repo, SecurityPolicy()), asyncio.Queue(), HealthRegistry(), metrics,
                            retry_delays=[5.0, 20.0, 60.0], sleep=fake_sleep)
    return repo, metrics, worker


@pytest.mark.asyncio
async def test_analysis_updates_token_and_keeps_history():
    repo, _, worker = build_worker(FakeRPC())
    token = fixture_token(1)
    await repo.create_token_if_absent(token)
    await repo.create_analysis_job(token.mint)
    await worker.process(token.mint)
    await worker.analyze_once(token.mint)  # a second analysis must not overwrite the first
    assert len(await repo.list_reports(token.mint, 10)) == 2
    stored = await repo.get_token(token.mint)
    assert stored.status == "WATCH" and stored.latest_report_id
    assert repo.jobs[f"{token.mint}__initial"]["status"] == "WATCH"


@pytest.mark.asyncio
async def test_unavailable_evidence_is_retried_then_left_insufficient():
    sleeps = []
    repo, metrics, worker = build_worker(FakeRPC(fail={"get_account_info"}), sleeps)
    token = fixture_token(2)
    await repo.create_token_if_absent(token)
    await repo.create_analysis_job(token.mint)
    await worker.process(token.mint)
    assert sleeps == [5.0, 20.0, 60.0]
    assert metrics.analyses == 4
    assert (await repo.get_token(token.mint)).status == "INSUFFICIENT_DATA"


@pytest.mark.asyncio
async def test_processor_queues_analysis_only_for_new_tokens():
    repo, metrics = InMemoryRepository(), Metrics()
    analysis_queue: asyncio.Queue = asyncio.Queue()
    proc = TokenProcessor(repo, asyncio.Queue(), HealthRegistry(), metrics, analysis_queue=analysis_queue)
    event = parse_message(create_message(3))
    await proc.handle(event)
    await proc.handle(event)
    assert analysis_queue.qsize() == 1


@pytest.mark.asyncio
async def test_pending_jobs_can_be_resumed_after_restart():
    repo, _, _ = build_worker(FakeRPC())
    await repo.create_token_if_absent(fixture_token(4))
    await repo.create_analysis_job(fixture_token(4).mint)
    assert await repo.list_pending_jobs(10) == [fixture_token(4).mint]


# ----------------------------------------------------------------------
# test_rpc
# ----------------------------------------------------------------------
def make_rpc(handler, sleeps=None, retries=3):
    async def fake_sleep(d):
        if sleeps is not None:
            sleeps.append(d)

    health = HealthRegistry()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return SolanaRPC("https://rpc.fixture.invalid", health, client=client, max_retries=retries, sleep=fake_sleep), health


@pytest.mark.asyncio
async def test_success_returns_result_and_marks_healthy():
    rpc, health = make_rpc(lambda req: httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": 5}}))
    assert await rpc.call("getBalance", ["x"]) == {"value": 5}
    assert health.get(RPC_PROVIDER).status == "connected"


@pytest.mark.asyncio
async def test_rate_limit_is_retried_with_backoff():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(429, headers={"retry-after": "2"})
        return httpx.Response(200, json={"result": 1})

    sleeps = []
    rpc, _ = make_rpc(handler, sleeps)
    assert await rpc.call("m", []) == 1
    assert calls["n"] == 3 and sleeps == [2.0, 2.0]


@pytest.mark.asyncio
async def test_persistent_failure_raises_unavailable_and_is_visible_in_health():
    rpc, health = make_rpc(lambda req: httpx.Response(503), retries=2)
    with pytest.raises(RpcUnavailable):
        await rpc.call("m", [])
    assert health.get(RPC_PROVIDER).status == "error" and "503" in health.get(RPC_PROVIDER).last_error


@pytest.mark.asyncio
async def test_definitive_rpc_error_is_not_retried():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        return httpx.Response(200, json={"error": {"code": -32602, "message": "Invalid param"}})

    rpc, _ = make_rpc(handler)
    with pytest.raises(RpcError) as exc:
        await rpc.call("m", [])
    assert not isinstance(exc.value, RpcUnavailable) and calls["n"] == 1


@pytest.mark.asyncio
async def test_timeout_is_retried_then_unavailable():
    def handler(req):
        raise httpx.ConnectTimeout("slow")

    rpc, _ = make_rpc(handler, retries=1)
    with pytest.raises(RpcUnavailable):
        await rpc.call("m", [])


# ----------------------------------------------------------------------
# test_security_engine
# ----------------------------------------------------------------------
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

@pytest.mark.asyncio
async def test_clean_token_is_watch_never_qualified():
    r = await run(FakeRPC())
    assert r.status == "WATCH"
    assert "QUALIFIED" not in r.status
    assert r.not_evaluated  # unbuilt checks are listed, not silently skipped


@pytest.mark.asyncio
async def test_active_mint_authority_is_rejected():
    r = await run(FakeRPC(mint=mint_account(mint_auth=OTHER1)))
    assert r.status == "REJECTED" and "Mint authority is active" in r.reason


@pytest.mark.asyncio
async def test_active_freeze_authority_is_rejected():
    r = await run(FakeRPC(mint=mint_account(freeze=OTHER1)))
    assert r.status == "REJECTED" and "Freeze authority" in r.reason


@pytest.mark.asyncio
async def test_provider_outage_is_insufficient_data_not_rejection():
    r = await run(FakeRPC(fail={"get_account_info", "get_balance", "get_signatures_for_address"}))
    assert r.status == "INSUFFICIENT_DATA"
    assert not any(c.status == "FAIL_CRITICAL" for c in r.checks)
    assert any(c.status == "UNAVAILABLE" for c in r.checks)


@pytest.mark.asyncio
async def test_mint_not_visible_yet_is_insufficient_data():
    r = await run(FakeRPC(use_default_mint=False))
    assert r.status == "INSUFFICIENT_DATA"


@pytest.mark.asyncio
async def test_malformed_rpc_data_is_unavailable_not_pass_or_fail():
    r = await run(FakeRPC(mint={"owner": "x", "data": "garbage"}))
    assert r.status == "INSUFFICIENT_DATA"


@pytest.mark.asyncio
async def test_permanent_delegate_is_rejected():
    ext = [{"extension": "permanentDelegate", "state": {"delegate": OTHER1}}]
    r = await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=ext)))
    assert r.status == "REJECTED" and "Permanent delegate" in r.reason


@pytest.mark.asyncio
async def test_excessive_transfer_fee_is_rejected_small_fee_is_hold():
    big = [{"extension": "transferFeeConfig", "state": {"newerTransferFee": {"transferFeeBasisPoints": 2000}, "olderTransferFee": {"transferFeeBasisPoints": 0}}}]
    small = [{"extension": "transferFeeConfig", "state": {"newerTransferFee": {"transferFeeBasisPoints": 100}, "olderTransferFee": {"transferFeeBasisPoints": 100}}}]
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=big)))).status == "REJECTED"
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=small)))).status == "HOLD"


@pytest.mark.asyncio
async def test_transfer_hook_and_unknown_extension_are_hold():
    hook = [{"extension": "transferHook", "state": {"programId": OTHER1}}]
    unknown = [{"extension": "somethingNew", "state": {}}]
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=hook)))).status == "HOLD"
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=unknown)))).status == "HOLD"


@pytest.mark.asyncio
async def test_benign_token2022_extensions_pass():
    ext = [{"extension": "metadataPointer", "state": {}}, {"extension": "tokenMetadata", "state": {}}]
    assert (await run(FakeRPC(mint=mint_account(owner=TOKEN_2022_PROGRAM, extensions=ext)))).status == "WATCH"


@pytest.mark.asyncio
async def test_bonding_curve_balance_is_excluded_from_concentration():
    r = await run(FakeRPC())  # curve holds 80% of supply
    h = by_name(r, "holder_concentration")
    assert h.status == "PASS"
    assert any(e["reason"] == "bonding_curve" for e in h.evidence["excluded"])
    assert "methodology" in h.evidence


@pytest.mark.asyncio
async def test_whale_wallet_is_rejected():
    rpc = FakeRPC(holders=[("TA1", OTHER1, 45.0), ("TA2", fixture_mint(9), 40.0)])
    r = await run(rpc)
    assert r.status == "REJECTED" and "holds 45.00%" in r.reason


@pytest.mark.asyncio
async def test_creator_heavy_holding_is_hold():
    rpc = FakeRPC(holders=[("TA1", fixture_mint(9), 70.0), ("TA2", FIXTURE_CREATOR, 20.0)])
    r = await run(rpc)
    assert r.status == "HOLD" and "Creator wallet holds" in r.reason


@pytest.mark.asyncio
async def test_serial_launcher_is_visible_in_report():
    repo = InMemoryRepository()
    for i in range(2, 7):  # 5 more launches by the same creator
        await repo.create_token_if_absent(fixture_token(i))
    r = await run(FakeRPC(), repo=repo)
    creator = by_name(r, "creator_wallet")
    assert r.status == "HOLD" and creator.status == "SUSPICIOUS"
    assert creator.evidence["launches_seen_by_tosh_bot_24h"] >= 5


@pytest.mark.asyncio
async def test_unknown_bonding_curve_makes_liquidity_unavailable():
    r = await run(FakeRPC(), token=fixture_token(bonding_curve=None))
    assert by_name(r, "liquidity").status == "UNAVAILABLE" and r.status == "INSUFFICIENT_DATA"


@pytest.mark.asyncio
async def test_low_curve_reserve_is_hold():
    r = await run(FakeRPC(curve_lamports=1_000_000))
    assert r.status == "HOLD"


@pytest.mark.asyncio
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


# ----------------------------------------------------------------------
# test_cache
# ----------------------------------------------------------------------
class CountingRepo(InMemoryRepository):
    """Counts reads so tests can prove the cache avoids database reads."""

    def __init__(self):
        super().__init__()
        self.reads = 0

    async def list_recent_tokens(self, limit, statuses=None):
        self.reads += 1
        return await super().list_recent_tokens(limit, statuses)

    async def count_tokens_by_status(self):
        self.reads += 1
        return await super().count_tokens_by_status()


@pytest.mark.asyncio
async def test_cache_serves_lists_and_counts_without_database_reads():
    inner = CountingRepo()
    for i in range(1, 4):
        await inner.create_token_if_absent(fixture_token(i))
    repo = CachedRepository(inner, size=10)
    await repo.warm()
    reads_after_warm = inner.reads
    for _ in range(20):  # simulate a dashboard polling
        assert len(await repo.list_recent_tokens(50)) == 3
        assert (await repo.count_tokens_by_status())["INSUFFICIENT_DATA"] == 3
    assert inner.reads == reads_after_warm


@pytest.mark.asyncio
async def test_cache_tracks_new_tokens_and_status_changes():
    repo = CachedRepository(CountingRepo(), size=10)
    await repo.warm()
    t = fixture_token(1)
    assert await repo.create_token_if_absent(t) is True
    assert await repo.create_token_if_absent(t) is False  # duplicate does not double-count
    assert (await repo.count_tokens_by_status())["INSUFFICIENT_DATA"] == 1
    await repo.update_token_decision(t.mint, "REJECTED", "fixture", "r1", utcnow())
    counts = await repo.count_tokens_by_status()
    assert counts["INSUFFICIENT_DATA"] == 0 and counts["REJECTED"] == 1
    assert [x.mint for x in await repo.list_recent_tokens(10, ["REJECTED"])] == [t.mint]


@pytest.mark.asyncio
async def test_cache_evicts_oldest_beyond_size():
    repo = CachedRepository(CountingRepo(), size=2)
    await repo.warm()
    for i in range(1, 4):
        await repo.create_token_if_absent(fixture_token(i))
    assert len(await repo.list_recent_tokens(10)) == 2


@pytest.mark.asyncio
async def test_cache_counts_creator_launches_in_memory():
    repo = CachedRepository(CountingRepo(), size=10)
    await repo.warm()
    for i in range(1, 4):
        await repo.create_token_if_absent(fixture_token(i))
    assert await repo.count_recent_tokens_by_creator(fixture_token(1).creator, utcnow() - timedelta(hours=24)) == 3


@pytest.mark.asyncio
async def test_daily_limit_throttles_new_tokens_and_counts_them():
    repo, metrics = InMemoryRepository(), Metrics()
    proc = TokenProcessor(repo, asyncio.Queue(), HealthRegistry(), metrics, max_new_per_day=2)
    for i in range(1, 5):
        await proc.handle(parse_message(create_message(i)))
    assert len(repo.tokens) == 2 and metrics.throttled == 2


@pytest.mark.asyncio
async def test_lean_mode_skips_event_and_job_writes():
    repo = InMemoryRepository()
    proc = TokenProcessor(repo, asyncio.Queue(), HealthRegistry(), Metrics(), store_events=False, store_jobs=False)
    await proc.handle(parse_message(create_message(1)))
    assert len(repo.tokens) == 1 and not repo.events and not repo.jobs


@pytest.mark.asyncio
async def test_lean_worker_saves_only_the_final_report_when_data_is_unavailable():
    repo, metrics = InMemoryRepository(), Metrics()

    async def nosleep(d):
        pass

    worker = AnalysisWorker(repo, SecurityAnalyzer(FakeRPC(fail={"get_account_info"}), repo, SecurityPolicy()), asyncio.Queue(), HealthRegistry(),
                            metrics, retry_delays=[1.0, 2.0], sleep=nosleep, save_all_attempts=False, store_jobs=False)
    token = fixture_token(1)
    await repo.create_token_if_absent(token)
    await worker.process(token.mint)
    assert metrics.analyses == 3                       # tried three times
    assert len(await repo.list_reports(token.mint, 10)) == 1  # but only one billable save


@pytest.mark.asyncio
async def test_lean_mode_keeps_uninteresting_tokens_in_memory_only():
    inner = InMemoryRepository()
    repo = CachedRepository(inner, size=10, persist_statuses={"WATCH", "HOLD"})
    await repo.warm()
    t = fixture_token(1)
    assert await repo.create_token_if_absent(t) is True
    assert await repo.create_token_if_absent(t) is False          # de-duplicated in memory
    assert not inner.tokens                                        # nothing written yet
    await repo.save_security_report(_fake_report(t.mint, "REJECTED"))
    await repo.update_token_decision(t.mint, "REJECTED", "fixture", "mem", utcnow(), None)
    assert not inner.tokens and not inner.reports                  # rejected: still memory only
    assert (await repo.get_latest_report(t.mint)).status == "REJECTED"   # but the dashboard can still show it
    assert [x.status for x in await repo.list_recent_tokens(10)] == ["REJECTED"]


@pytest.mark.asyncio
async def test_lean_mode_saves_interesting_tokens_once_with_their_report():
    inner = InMemoryRepository()
    repo = CachedRepository(inner, size=10, persist_statuses={"WATCH", "HOLD"})
    await repo.warm()
    t = fixture_token(2)
    await repo.create_token_if_absent(t)
    await repo.save_security_report(_fake_report(t.mint, "WATCH"))
    await repo.update_token_decision(t.mint, "WATCH", "fixture", "mem", utcnow(), 61.5)
    assert inner.tokens[t.mint].status == "WATCH" and inner.tokens[t.mint].opportunity_score == 61.5
    assert len(inner.reports[t.mint]) == 1
    assert inner.tokens[t.mint].latest_report_id != "mem"          # points at the real saved report


def _fake_report(mint, status):
    from datetime import timezone
    return SecurityReport(mint=mint, analysis_version="fixture", analyzed_at=utcnow().astimezone(timezone.utc), status=status, reason="fixture")


@pytest.mark.asyncio
async def test_partial_evidence_score_is_computed_and_marked_incomplete():
    repo = InMemoryRepository()
    token = fixture_token(3)
    await repo.create_token_if_absent(token)
    report = await SecurityAnalyzer(FakeRPC(), repo, SecurityPolicy()).analyze(token)
    opp = report.opportunity
    assert report.status == "WATCH" and 0 <= opp["score"] <= 100
    assert opp["complete"] is False and opp["available_weight"] == 60 and opp["missing"]


@pytest.mark.asyncio
async def test_no_score_when_inputs_are_unavailable():
    repo = InMemoryRepository()
    token = fixture_token(4)
    await repo.create_token_if_absent(token)
    report = await SecurityAnalyzer(FakeRPC(fail={"get_balance"}), repo, SecurityPolicy()).analyze(token)
    assert report.opportunity is None   # never invent a neutral value for missing evidence


# ----------------------------------------------------------------------
# test_api
# ----------------------------------------------------------------------
def client_with_tokens(n=3, api_key="secret"):
    repo = InMemoryRepository()
    for i in range(1, n + 1):  # FIXTURE tokens, not live data
        asyncio.run(repo.create_token_if_absent(TokenRecord.from_event(parse_message(create_message(i)))))
    settings = Settings(_env_file=None, storage_backend="memory", listener_enabled=False, analysis_enabled=False, api_key=api_key)
    return TestClient(create_app(settings, repo))


def test_recent_tokens_returns_stored_tokens():
    with client_with_tokens(3) as c:
        r = c.get("/api/tokens/recent", headers={"X-API-Key": "secret"})
        assert r.status_code == 200
        body = r.json()
        assert len(body) == 3
        assert all(t["status"] == "INSUFFICIENT_DATA" for t in body)


def test_limit_is_respected_and_validated():
    with client_with_tokens(3) as c:
        h = {"X-API-Key": "secret"}
        assert len(c.get("/api/tokens/recent?limit=2", headers=h).json()) == 2
        assert c.get("/api/tokens/recent?limit=0", headers=h).status_code == 422


def test_api_requires_key():
    with client_with_tokens(1) as c:
        assert c.get("/api/tokens/recent").status_code == 401
        assert c.get("/api/tokens/recent", headers={"X-API-Key": "wrong"}).status_code == 401
        assert c.get("/health").status_code == 200


def test_system_health_reports_listener_state():
    with client_with_tokens(1) as c:
        body = c.get("/api/system/health", headers={"X-API-Key": "secret"}).json()
        assert body["database_reachable"] is True
        assert body["listener_enabled"] is False
        assert "queue_depth" in body and "metrics" in body


def test_security_report_endpoints():


    repo = InMemoryRepository()
    token = fixture_token(1)  # FIXTURE data
    asyncio.run(repo.create_token_if_absent(token))
    report = asyncio.run(SecurityAnalyzer(FakeRPC(), repo, SecurityPolicy()).analyze(token))
    asyncio.run(repo.save_security_report(report))
    settings = Settings(_env_file=None, storage_backend="memory", listener_enabled=False, analysis_enabled=False, api_key="secret")
    with TestClient(create_app(settings, repo)) as c:
        h = {"X-API-Key": "secret"}
        body = c.get(f"/api/tokens/{token.mint}/security", headers=h).json()
        assert body["status"] == "WATCH" and body["checks"] and body["reason"]
        assert len(c.get(f"/api/tokens/{token.mint}/security/history", headers=h).json()) == 1
        assert c.get(f"/api/tokens/{'2' * 44}/security", headers=h).status_code == 404
        assert c.get("/api/tokens/not-a-mint/security", headers=h).status_code == 422


def test_stats_status_filter_and_token_detail():
    repo = InMemoryRepository()
    for i in range(1, 4):  # FIXTURE tokens
        asyncio.run(repo.create_token_if_absent(TokenRecord.from_event(parse_message(create_message(i)))))
    mint = next(iter(repo.tokens))
    asyncio.run(repo.update_token_decision(mint, "REJECTED", "fixture reason", "r1", repo.tokens[mint].detected_at))
    settings = Settings(_env_file=None, storage_backend="memory", listener_enabled=False, analysis_enabled=False, api_key="secret")
    with TestClient(create_app(settings, repo)) as c:
        h = {"X-API-Key": "secret"}
        stats = c.get("/api/stats", headers=h).json()
        assert stats["total"] == 3 and stats["counts"]["REJECTED"] == 1 and stats["counts"]["INSUFFICIENT_DATA"] == 2
        rejected = c.get("/api/tokens/recent?status=REJECTED", headers=h).json()
        assert [t["mint"] for t in rejected] == [mint]
        assert len(c.get("/api/tokens/recent?status=REJECTED,INSUFFICIENT_DATA", headers=h).json()) == 3
        assert c.get("/api/tokens/recent?status=BOGUS", headers=h).status_code == 422
        assert c.get(f"/api/tokens/{mint}", headers=h).json()["status"] == "REJECTED"
        assert c.get(f"/api/tokens/{'2' * 44}", headers=h).status_code == 404


def test_builtin_dashboard_requires_basic_auth_and_serves_data():
    repo = InMemoryRepository()
    for i in range(1, 3):  # FIXTURE tokens
        asyncio.run(repo.create_token_if_absent(TokenRecord.from_event(parse_message(create_message(i)))))
    settings = Settings(_env_file=None, storage_backend="memory", listener_enabled=False, analysis_enabled=False,
                        api_key="secret", dashboard_user="boss", dashboard_password="pw")
    with TestClient(create_app(settings, repo)) as c:
        assert c.get("/dashboard").status_code == 401
        assert c.get("/dashboard/api/stats").status_code == 401
        assert c.get("/dashboard", auth=("boss", "wrong")).status_code == 401
        page = c.get("/dashboard", auth=("boss", "pw"))
        assert page.status_code == 200 and "Tosh Bot" in page.text
        assert c.get("/dashboard/api/stats", auth=("boss", "pw")).json()["total"] == 2
        assert len(c.get("/dashboard/api/tokens/recent", auth=("boss", "pw")).json()) == 2
        assert c.get("/api/stats").status_code == 401  # the API-key door is still closed without the key


def test_dashboard_is_disabled_in_production_without_password():
    settings = Settings(_env_file=None, storage_backend="memory", listener_enabled=False, analysis_enabled=False,
                        environment="production", api_key="secret", dashboard_password="")
    with TestClient(create_app(settings, InMemoryRepository())) as c:
        assert c.get("/dashboard").status_code == 503


# ----------------------------------------------------------------------
# test_firestore_integration
# ----------------------------------------------------------------------
"""Runs only against the Firestore emulator (set FIRESTORE_EMULATOR_HOST). Skipped otherwise."""


@pytest.mark.skipif(not os.environ.get("FIRESTORE_EMULATOR_HOST"), reason="Firestore emulator not configured")
@pytest.mark.asyncio
async def test_firestore_dedupes_and_lists():
    repo = build_repository(Settings(_env_file=None, storage_backend="firestore", firestore_collection_prefix="test_"))
    token = TokenRecord.from_event(parse_message(create_message(7)))
    results = await asyncio.gather(*[repo.create_token_if_absent(token) for _ in range(5)])
    assert results.count(True) <= 1  # 0 if a previous run already created it
    assert any(t.mint == token.mint for t in await repo.list_recent_tokens(50))
    assert await repo.ping()
    await repo.aclose()


# ----------------------------------------------------------------------
# test_buy_signal  (live trade feed -> QUALIFIED / BUY)
# ----------------------------------------------------------------------
T0 = 1_000_000.0


def sig_wallet(i: int) -> str:
    return fixture_mint(20 + i)


def feed(tracker, mint, *, buyers=12, sellers=3, mcap0=30.0, mcap1=34.0, start=T0, end=T0 + 140, creator_sells=0.0,
         buy_sol=0.5, sell_sol=0.3, last_gap=0.0):
    """Synthetic, clearly fake trades spread over start..end with a smoothly rising market cap."""
    n = buyers + sellers + (1 if creator_sells else 0)
    ts_end = end - last_gap
    trades = []
    for k in range(n):
        ts = start + 1 + (ts_end - start - 1) * k / max(1, n - 1)
        mcap = mcap0 + (mcap1 - mcap0) * k / max(1, n - 1)
        if k < buyers:
            trades.append(Trade(mint, sig_wallet(k), "buy", buy_sol, 1000.0, mcap, ts))
        elif k < buyers + sellers:
            trades.append(Trade(mint, sig_wallet(k - buyers), "sell", sell_sol, 500.0, mcap, ts))
        else:
            trades.append(Trade(mint, FIXTURE_CREATOR, "sell", creator_sells, 500.0, mcap, ts))
    for t in trades:
        tracker.add(t)


class Clock:
    def __init__(self, t=T0):
        self.t = t

    def __call__(self):
        return self.t


def signal_setup(**feed_kw):
    clock = Clock()
    tracker = TradeTracker(1800, 150, clock=clock)
    token = fixture_token()
    assert tracker.track(token.mint, token.creator)
    feed(tracker, token.mint, **feed_kw)
    clock.t = T0 + 140
    return clock, tracker, token


def analyze_with(tracker, token, curve_sol=20.0):
    repo = InMemoryRepository()
    rpc = FakeRPC(curve_lamports=int(curve_sol * 1e9))
    analyzer = SecurityAnalyzer(rpc, repo, SecurityPolicy(), tracker=tracker)

    async def go():
        await repo.create_token_if_absent(token)
        return await analyzer.analyze(token)

    return asyncio.run(go())


def test_parse_trade_accepts_only_wellformed_buys_and_sells():
    good = json.dumps({"mint": fixture_mint(1), "traderPublicKey": FIXTURE_CREATOR, "txType": "buy", "solAmount": 0.5,
                       "tokenAmount": 100.0, "marketCapSol": 31.2})
    t = parse_trade(good, now=5.0)
    assert (t.side, t.sol, t.mcap_sol, t.ts) == ("buy", 0.5, 31.2, 5.0)
    assert parse_trade(create_message(1)) is None                       # launches are not trades
    assert parse_trade("garbage") is None and parse_trade("{}") is None
    assert parse_trade(json.dumps({"mint": fixture_mint(1), "txType": "buy", "solAmount": 1})) is None  # no wallet
    assert parse_trade(json.dumps({"mint": fixture_mint(1), "traderPublicKey": FIXTURE_CREATOR, "txType": "buy", "solAmount": -1})) is None


def test_buy_signal_fires_when_every_condition_holds():
    _, tracker, token = signal_setup()
    report = analyze_with(tracker, token)
    assert report.status == "QUALIFIED", report.reason
    assert report.opportunity["complete"] and report.opportunity["score"] >= SecurityPolicy().qualify_min_score
    names = {c.name: c.status for c in report.checks}
    assert names["sell_evidence"] == names["trading_manipulation"] == names["momentum"] == "PASS"


def test_no_buy_without_enough_trade_evidence():
    clock, tracker, token = signal_setup(buyers=4, sellers=1)
    report = analyze_with(tracker, token)
    assert report.status == "WATCH" and "No buy signal yet" in report.reason


def test_no_buy_without_sell_evidence():
    _, tracker, token = signal_setup(sellers=0)
    report = analyze_with(tracker, token)
    assert report.status == "WATCH"
    assert {c.name: c.status for c in report.checks}["sell_evidence"] == "UNAVAILABLE"


def test_no_buy_when_price_is_falling_or_flat():
    _, tracker, token = signal_setup(mcap0=34.0, mcap1=30.0)
    assert analyze_with(tracker, token).status == "WATCH"
    _, tracker, token = signal_setup(mcap0=30.0, mcap1=30.0)
    assert analyze_with(tracker, token).status == "WATCH"


def test_no_buy_on_a_vertical_spike():
    _, tracker, token = signal_setup(mcap0=20.0, mcap1=60.0)
    report = analyze_with(tracker, token)
    assert report.status == "WATCH" and "vertical spike" in report.reason


def test_creator_selling_puts_the_token_on_hold():
    _, tracker, token = signal_setup(creator_sells=1.0)
    report = analyze_with(tracker, token)
    assert report.status == "HOLD" and "creator sold" in report.reason


def test_one_wallet_dominating_volume_is_suspicious():
    clock, tracker, token = signal_setup()
    for k in range(3):  # a whale trading back and forth
        tracker.add(Trade(token.mint, sig_wallet(30), "buy", 6.0, 1.0, 34.0, T0 + 130 + k))
    report = analyze_with(tracker, token)
    assert report.status == "HOLD"


def test_stale_trade_feed_cannot_be_a_buy():
    _, tracker, token = signal_setup(last_gap=100.0)  # last trade is 100s old
    report = analyze_with(tracker, token)
    assert report.status == "WATCH" and "no trade in the last" in report.reason


def test_buy_signal_cannot_outlive_its_watch_window():
    _, tracker, token = signal_setup()
    assert analyze_with(tracker, token).status == "QUALIFIED"
    tracker.untrack(token.mint)  # window ended
    assert not tracker.track(token.mint, token.creator)  # and it can never be re-watched
    assert analyze_with(tracker, token).status == "WATCH"


def test_rejected_token_is_never_a_buy_and_stops_being_watched():
    clock, tracker, token = signal_setup()
    clock.t = T0 + 140
    repo = InMemoryRepository()
    bad = mint_account(freeze=OTHER1)  # freeze authority present = critical failure
    analyzer = SecurityAnalyzer(FakeRPC(mint=bad, curve_lamports=20 * 10**9), repo, SecurityPolicy(), tracker=tracker)
    report = asyncio.run(analyzer.analyze(token))
    assert report.status == "REJECTED" and not tracker.is_tracked(token.mint)


def test_without_a_tracker_nothing_is_ever_qualified():
    report = asyncio.run(SecurityAnalyzer(FakeRPC(curve_lamports=20 * 10**9), InMemoryRepository(), SecurityPolicy()).analyze(fixture_token()))
    assert report.status == "WATCH"


def test_tracker_subscriptions_and_capacity():
    tracker = TradeTracker(60, 2, clock=Clock())
    assert tracker.track(fixture_mint(1)) and tracker.track(fixture_mint(2)) and not tracker.track(fixture_mint(3))
    subs, unsubs = tracker.drain_subs()
    assert sorted(subs) == sorted([fixture_mint(1), fixture_mint(2)]) and unsubs == []
    tracker.untrack(fixture_mint(1))
    assert tracker.drain_subs() == ([], [fixture_mint(1)])
    tracker.resubscribe_all()
    assert tracker.drain_subs()[0] == [fixture_mint(2)]


def test_trades_for_unwatched_tokens_are_ignored():
    tracker = TradeTracker(60, 5, clock=Clock())
    assert not tracker.add(Trade(fixture_mint(5), sig_wallet(1), "buy", 1.0, 1.0, 30.0, T0))


def test_listener_routes_trades_and_subscribes_watched_tokens():
    async def go():
        clock = Clock()
        tracker = TradeTracker(1800, 10, clock=clock)
        mint = fixture_mint(4)
        tracker.track(mint, FIXTURE_CREATOR)
        stop = asyncio.Event()
        trade = json.dumps({"mint": mint, "traderPublicKey": sig_wallet(1), "txType": "buy", "solAmount": 0.4, "marketCapSol": 31.0})
        class SlowWS(FakeWS):  # stays connected a moment, like a real feed, so the subscription sender gets to run
            async def _gen(self):
                async for m in super()._gen():
                    yield m
                await asyncio.sleep(0.1)

        ws = SlowWS([trade, create_message(1)], on_done=stop.set)
        listener, queue, *_ = build_listener([ws], tracker=tracker)
        task = asyncio.create_task(listener.run(stop))
        await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert tracker.trades_seen == 1 and queue.qsize() == 1  # the trade fed the tracker; only the launch was queued
        sent = [json.loads(x) for x in ws.sent]
        assert {"method": "subscribeNewToken"} in sent
        assert {"method": "subscribeTokenTrade", "keys": [mint]} in sent
    asyncio.run(go())


def test_review_scheduler_requeues_and_ends_windows():
    clock = Clock()
    tracker = TradeTracker(100, 10, clock=clock)
    tracker.track(fixture_mint(1))
    q: asyncio.Queue = asyncio.Queue()
    sched = ReviewScheduler(tracker, q, interval=30)
    clock.t = T0 + 31
    sched.tick()
    assert q.qsize() == 1 and tracker.is_tracked(fixture_mint(1))
    clock.t = T0 + 101
    sched.tick()
    assert q.qsize() == 2 and not tracker.is_tracked(fixture_mint(1))  # one final analysis, then the watch ends


def test_unchanged_reviews_are_not_saved_every_time():
    async def go():
        repo = InMemoryRepository()
        token = fixture_token()
        await repo.create_token_if_absent(token)
        worker = AnalysisWorker(repo, SecurityAnalyzer(FakeRPC(), repo, SecurityPolicy()), asyncio.Queue(), HealthRegistry(), Metrics(),
                                review_save_seconds=1000)
        for _ in range(3):
            await worker.analyze_once(token.mint)
        assert len(repo.reports[token.mint]) == 1  # saved once (INSUFFICIENT -> WATCH); the two unchanged re-reviews are skipped
    asyncio.run(go())
