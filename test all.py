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
