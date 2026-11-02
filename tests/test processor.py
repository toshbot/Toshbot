import asyncio

from app.health import HealthRegistry, Metrics
from app.parser import parse_message
from app.processor import TokenProcessor
from app.repository import InMemoryRepository
from tests.helpers import create_message


def make():
    repo, metrics = InMemoryRepository(), Metrics()
    proc = TokenProcessor(repo, asyncio.Queue(), HealthRegistry(), metrics, sleep=lambda s: asyncio.sleep(0))
    return repo, metrics, proc


async def test_new_token_is_stored_with_fail_closed_status():
    repo, metrics, proc = make()
    assert await proc.handle(parse_message(create_message(1))) is True
    token = next(iter(repo.tokens.values()))
    assert token.status == "INSUFFICIENT_DATA"  # never approved without security checks
    assert len(repo.jobs) == 1 and metrics.stored == 1


async def test_duplicate_events_do_not_create_duplicate_tokens():
    repo, metrics, proc = make()
    event = parse_message(create_message(1))
    await proc.handle(event)
    await proc.handle(event)
    await proc.handle(parse_message(create_message(1, signature="FIXTURE-OTHER-SIG")))  # same mint, new signature
    assert len(repo.tokens) == 1
    assert len(repo.jobs) == 1
    assert metrics.stored == 1 and metrics.duplicates == 2


async def test_concurrent_creates_for_same_mint_yield_one_token():
    repo, _, _ = make()
    from app.models import TokenRecord

    token = TokenRecord.from_event(parse_message(create_message(2)))
    results = await asyncio.gather(*[repo.create_token_if_absent(token) for _ in range(20)])
    assert results.count(True) == 1


async def test_storage_failure_retries_then_counts_failed_job():
    repo, metrics, proc = make()

    async def boom(_):
        raise RuntimeError("db down")

    repo.record_launch_event = boom
    await proc._handle_with_retry(parse_message(create_message(3)))
    assert metrics.failed_jobs == 1
    assert proc.health.get("database").status == "error"
