import asyncio
import logging

from app.health import HealthRegistry, Metrics
from app.models import LaunchEvent, TokenRecord
from app.repository import Repository

log = logging.getLogger("tosh.processor")


class TokenProcessor:
    """Consumes launch events from the queue: record event, register token once, create analysis job."""

    def __init__(self, repo: Repository, queue: asyncio.Queue, health: HealthRegistry, metrics: Metrics,
                 *, max_attempts: int = 3, sleep=asyncio.sleep) -> None:
        self.repo, self.queue, self.health, self.metrics = repo, queue, health, metrics
        self.max_attempts, self._sleep = max_attempts, sleep

    async def handle(self, event: LaunchEvent) -> bool:
        """Idempotent: safe to retry and safe for duplicate / delayed / out-of-order events."""
        await self.repo.record_launch_event(event)
        created = await self.repo.create_token_if_absent(TokenRecord.from_event(event))
        await self.repo.create_analysis_job(event.mint)  # always attempted so a retry can finish a half-done earlier attempt
        if created:
            self.metrics.stored += 1
            log.info("token detected", extra={"mint": event.mint, "symbol": event.symbol})
        else:
            self.metrics.duplicates += 1
        return created

    async def _handle_with_retry(self, event: LaunchEvent) -> None:
        for attempt in range(1, self.max_attempts + 1):
            try:
                await self.handle(event)
                self.health.mark_success("database")
                return
            except Exception as exc:
                self.health.mark_failure("database", f"{type(exc).__name__}: {exc}")
                log.warning("storage attempt failed", extra={"mint": event.mint, "attempt": attempt, "error": str(exc)})
                if attempt < self.max_attempts:
                    await self._sleep(min(2 ** (attempt - 1), 10))
        self.metrics.failed_jobs += 1
        log.error("event could not be stored", extra={"mint": event.mint, "event": event.model_dump_json()})

    async def run(self) -> None:
        while True:
            event = await self.queue.get()
            try:
                await self._handle_with_retry(event)
            finally:
                self.queue.task_done()
