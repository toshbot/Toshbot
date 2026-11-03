import asyncio
import hmac
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

from app.analysis import AnalysisWorker
from app.config import Settings
from app.health import HealthRegistry, Metrics
from app.listener import PumpPortalListener
from app.logging_config import configure_logging
from app.models import BASE58_ADDRESS, TokenRecord
from app.processor import TokenProcessor
from app.repository import Repository, build_repository
from app.rpc import SolanaRPC
from app.security.engine import SecurityAnalyzer
from app.security.models import SecurityReport
from app.security.policy import SecurityPolicy

log = logging.getLogger("tosh.app")
VALID_STATUSES = ["REJECTED", "INSUFFICIENT_DATA", "HOLD", "WATCH", "QUALIFIED", "REVOKED"]


async def require_api_key(request: Request, x_api_key: str | None = Header(default=None)) -> None:
    expected = request.app.state.settings.api_key
    if not expected:
        return  # only possible in development; production refuses to start without a key
    if not x_api_key or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def policy_from_settings(s: Settings) -> SecurityPolicy:
    return SecurityPolicy(
        evidence_max_age_seconds=s.evidence_max_age_seconds, single_holder_reject_pct=s.single_holder_reject_pct,
        top10_hold_pct=s.top10_hold_pct, creator_hold_pct=s.creator_hold_pct, max_transfer_fee_bps=s.max_transfer_fee_bps,
        creator_max_launches_24h=s.creator_max_launches_24h, min_curve_reserve_sol=s.min_curve_reserve_sol)


def create_app(settings: Settings | None = None, repository: Repository | None = None, rpc: SolanaRPC | None = None) -> FastAPI:
    settings = settings or Settings()
    if settings.environment == "production" and not settings.api_key:
        raise RuntimeError("API_KEY must be set when ENVIRONMENT=production")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging(settings.log_level)
        if not settings.api_key:
            log.warning("API_KEY is empty: API is unauthenticated (development only)")
        repo = repository or build_repository(settings)
        if settings.storage_backend == "memory" and repository is None:
            log.warning("STORAGE_BACKEND=memory: data is NOT persistent")
        health, metrics = HealthRegistry(), Metrics()
        queue: asyncio.Queue = asyncio.Queue(maxsize=settings.queue_maxsize)
        analysis_queue: asyncio.Queue = asyncio.Queue(maxsize=settings.analysis_queue_maxsize)
        stop = asyncio.Event()
        app.state.repo, app.state.health, app.state.metrics = repo, health, metrics
        app.state.queue, app.state.analysis_queue = queue, analysis_queue

        solana = rpc or SolanaRPC(settings.solana_rpc_url, health, timeout=settings.rpc_timeout_seconds,
                                  max_concurrency=settings.rpc_max_concurrency)
        processor = TokenProcessor(repo, queue, health, metrics, max_attempts=settings.processor_max_attempts,
                                   analysis_queue=analysis_queue if settings.analysis_enabled else None)
        tasks = [asyncio.create_task(processor.run(), name="processor")]
        analysis_tasks: list[asyncio.Task] = []
        if settings.analysis_enabled:
            analyzer = SecurityAnalyzer(solana, repo, policy_from_settings(settings))
            for i in range(settings.analysis_workers):
                worker = AnalysisWorker(repo, analyzer, analysis_queue, health, metrics, retry_delays=settings.retry_delay_list)
                analysis_tasks.append(asyncio.create_task(worker.run(), name=f"analysis-{i}"))
            try:  # resume jobs left pending by a restart
                for mint in await repo.list_pending_jobs(500):
                    analysis_queue.put_nowait(mint)
            except Exception as exc:
                log.warning("could not re-queue pending jobs", extra={"error": str(exc)})
        else:
            log.warning("analysis disabled by configuration")
        if settings.listener_enabled:
            listener = PumpPortalListener(
                settings.pumpportal_ws_url, queue, health, metrics,
                base_delay=settings.reconnect_base_seconds, max_delay=settings.reconnect_max_seconds,
                stable_after=settings.stable_connection_seconds)
            tasks.append(asyncio.create_task(listener.run(stop), name="listener"))
        else:
            log.warning("listener disabled by configuration")
        try:
            yield
        finally:
            stop.set()
            for t in tasks[1:]:  # stop the feed first
                t.cancel()
            try:
                await asyncio.wait_for(queue.join(), timeout=10)
            except asyncio.TimeoutError:
                log.warning("shutdown: queue not drained", extra={"remaining": queue.qsize()})
            for t in [tasks[0], *analysis_tasks]:
                t.cancel()
            await asyncio.gather(*tasks, *analysis_tasks, return_exceptions=True)
            if rpc is None:
                await solana.aclose()
            await repo.aclose()

    app = FastAPI(title="Tosh Bot API", version="0.2.0", lifespan=lifespan)
    app.state.settings = settings
    app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origin_list, allow_methods=["GET"], allow_headers=["X-API-Key"])

    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root() -> dict:
        return {"service": "toshbot", "status": "ok", "health": "/health"}

    @app.api_route("/health", methods=["GET", "HEAD"])
    async def liveness() -> dict:
        return {"status": "ok"}

    @app.get("/api/tokens/recent", response_model=list[TokenRecord], dependencies=[Depends(require_api_key)])
    async def recent_tokens(request: Request, limit: int = Query(50, ge=1, le=200),
                            status: str | None = Query(None, description="Comma-separated statuses")):
        statuses = [x.strip().upper() for x in status.split(",") if x.strip()] if status else None
        if statuses and any(x not in VALID_STATUSES for x in statuses):
            raise HTTPException(status_code=422, detail="Unknown status")
        return await request.app.state.repo.list_recent_tokens(limit, statuses)

    @app.get("/api/stats", dependencies=[Depends(require_api_key)])
    async def stats(request: Request) -> dict:
        counts = await request.app.state.repo.count_tokens_by_status()
        full = {s: counts.get(s, 0) for s in VALID_STATUSES}
        return {"total": sum(full.values()), "counts": full}

    def _valid_mint(mint: str) -> str:
        if not BASE58_ADDRESS.match(mint):
            raise HTTPException(status_code=422, detail="Invalid mint address")
        return mint

    @app.get("/api/tokens/{mint}", response_model=TokenRecord, dependencies=[Depends(require_api_key)])
    async def token_detail(request: Request, mint: str):
        token = await request.app.state.repo.get_token(_valid_mint(mint))
        if token is None:
            raise HTTPException(status_code=404, detail="Unknown token")
        return token

    @app.get("/api/tokens/{mint}/security", response_model=SecurityReport, dependencies=[Depends(require_api_key)])
    async def latest_security(request: Request, mint: str):
        report = await request.app.state.repo.get_latest_report(_valid_mint(mint))
        if report is None:
            raise HTTPException(status_code=404, detail="No security report yet for this token")
        return report

    @app.get("/api/tokens/{mint}/security/history", response_model=list[SecurityReport], dependencies=[Depends(require_api_key)])
    async def security_history(request: Request, mint: str, limit: int = Query(20, ge=1, le=100)):
        return await request.app.state.repo.list_reports(_valid_mint(mint), limit)

    @app.get("/api/system/health", dependencies=[Depends(require_api_key)])
    async def system_health(request: Request) -> dict:
        st = request.app.state
        try:
            db_ok = await asyncio.wait_for(st.repo.ping(), timeout=5)
        except Exception:
            db_ok = False
        return {
            "database_reachable": db_ok,
            "providers": st.health.snapshot(settings.feed_stale_after_seconds),
            "queue_depth": st.queue.qsize(),
            "analysis_queue_depth": st.analysis_queue.qsize(),
            "listener_enabled": settings.listener_enabled,
            "analysis_enabled": settings.analysis_enabled,
            "metrics": st.metrics.as_dict(),
        }

    return app


app = create_app()
