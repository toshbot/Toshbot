import asyncio

from app.analysis import AnalysisWorker
from app.health import HealthRegistry, Metrics
from app.parser import parse_message
from app.processor import TokenProcessor
from app.repository import InMemoryRepository
from app.security.engine import SecurityAnalyzer
from app.security.policy import SecurityPolicy
from tests.helpers import create_message
from tests.security_helpers import FakeRPC, fixture_token


def build(rpc, sleeps=None):
    repo, metrics = InMemoryRepository(), Metrics()

    async def fake_sleep(d):
        if sleeps is not None:
            sleeps.append(d)

    worker = AnalysisWorker(repo, SecurityAnalyzer(rpc, repo, SecurityPolicy()), asyncio.Queue(), HealthRegistry(), metrics,
                            retry_delays=[5.0, 20.0, 60.0], sleep=fake_sleep)
    return repo, metrics, worker


async def test_analysis_updates_token_and_keeps_history():
    repo, _, worker = build(FakeRPC())
    token = fixture_token(1)
    await repo.create_token_if_absent(token)
    await repo.create_analysis_job(token.mint)
    await worker.process(token.mint)
    await worker.analyze_once(token.mint)  # a second analysis must not overwrite the first
    assert len(await repo.list_reports(token.mint, 10)) == 2
    stored = await repo.get_token(token.mint)
    assert stored.status == "WATCH" and stored.latest_report_id
    assert repo.jobs[f"{token.mint}__initial"]["status"] == "WATCH"


async def test_unavailable_evidence_is_retried_then_left_insufficient():
    sleeps = []
    repo, metrics, worker = build(FakeRPC(fail={"get_account_info"}), sleeps)
    token = fixture_token(2)
    await repo.create_token_if_absent(token)
    await repo.create_analysis_job(token.mint)
    await worker.process(token.mint)
    assert sleeps == [5.0, 20.0, 60.0]
    assert metrics.analyses == 4
    assert (await repo.get_token(token.mint)).status == "INSUFFICIENT_DATA"


async def test_processor_queues_analysis_only_for_new_tokens():
    repo, metrics = InMemoryRepository(), Metrics()
    analysis_queue: asyncio.Queue = asyncio.Queue()
    proc = TokenProcessor(repo, asyncio.Queue(), HealthRegistry(), metrics, analysis_queue=analysis_queue)
    event = parse_message(create_message(3))
    await proc.handle(event)
    await proc.handle(event)
    assert analysis_queue.qsize() == 1


async def test_pending_jobs_can_be_resumed_after_restart():
    repo, _, _ = build(FakeRPC())
    await repo.create_token_if_absent(fixture_token(4))
    await repo.create_analysis_job(fixture_token(4).mint)
    assert await repo.list_pending_jobs(10) == [fixture_token(4).mint]
