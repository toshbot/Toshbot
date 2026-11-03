import asyncio
import logging

from app.health import HealthRegistry, Metrics
from app.repository import Repository
from app.security.engine import SecurityAnalyzer
from app.security.models import SecurityReport

log = logging.getLogger("tosh.analysis")


class AnalysisWorker:
    """Takes mints from the analysis queue, runs the security engine, stores an append-only report,
    and updates the token's current status. Re-tries only while evidence is unavailable."""

    def __init__(self, repo: Repository, analyzer: SecurityAnalyzer, queue: asyncio.Queue, health: HealthRegistry,
                 metrics: Metrics, *, retry_delays: list[float] | None = None, sleep=asyncio.sleep) -> None:
        self.repo, self.analyzer, self.queue, self.health, self.metrics = repo, analyzer, queue, health, metrics
        self.retry_delays = retry_delays if retry_delays is not None else [5.0, 20.0, 60.0]
        self._sleep = sleep

    async def analyze_once(self, mint: str) -> SecurityReport | None:
        token = await self.repo.get_token(mint)
        if token is None:
            log.error("analysis requested for unknown token", extra={"mint": mint})
            return None
        report = await self.analyzer.analyze(token)
        report_id = await self.repo.save_security_report(report)
        await self.repo.update_token_decision(mint, report.status, report.reason, report_id, report.analyzed_at)
        self.metrics.analyses += 1
        log.info("analysis complete", extra={"mint": mint, "status": report.status, "report_id": report_id})
        return report

    async def process(self, mint: str) -> None:
        report = None
        for delay in [0.0, *self.retry_delays]:
            if delay:
                await self._sleep(delay)
            report = await self.analyze_once(mint)
            if report is None or report.status != "INSUFFICIENT_DATA":
                break
        if report is not None:
            await self.repo.complete_job(mint, report.status)

    async def run(self) -> None:
        while True:
            mint = await self.queue.get()
            try:
                await self.process(mint)
                self.health.mark_success("database")
            except Exception as exc:
                self.metrics.failed_analyses += 1
                self.health.mark_failure("database", f"analysis: {type(exc).__name__}: {exc}")
                log.exception("analysis failed", extra={"mint": mint})
            finally:
                self.queue.task_done()
