import asyncio
import hmac
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

from app.config import Settings
from app.health import HealthRegistry, Metrics
from app.listener import PumpPortalListener
from app.logging_config import configure_logging
from app.models import TokenRecord
from app.processor import TokenProcessor
from app.repository import Repository, build_repository

log = logging.getLogger("tosh.app")


async def require_api_key(request: Request, x_api_key: str | None = Header(default=None)) -> None:
    expected = request.app.state.settings.api_key
    if not expected:
        return  # only possible in development; production refuses to start without a key
    if not x_api_key or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def create_app(settings: Settings | None = None, repository: Repository | None = None) -> FastAPI:
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
        stop = asyncio.Event()
        app.state.repo, app.state.health, app.state.metrics, app.state.queue = repo, health, metrics, queue

        processor = TokenProcessor(repo, queue, health, metrics, max_attempts=settings.processor_max_attempts)
        tasks = [asyncio.create_task(processor.run(), name="processor")]
        if settings.listener_enabled:
            listener = PumpPortalListener(
                settings.pumpportal_ws_url, queue, health, metrics,
                base_delay=settings.reconnect_base_seconds, max_delay=settings.reconnect_max_seconds,
                stable_after=settings.stable_connection_seconds,
            )
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
                await asyncio.wait_for(queue.join(), timeout=10)  # let queued events be stored
            except asyncio.TimeoutError:
                log.warning("shutdown: queue not drained", extra={"remaining": queue.qsize()})
            tasks[0].cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await repo.aclose()

    app = FastAPI(title="Tosh Bot API", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origin_list, allow_methods=["GET"], allow_headers=["X-API-Key"])

    @app.get("/health")
    async def liveness() -> dict:
        return {"status": "ok"}

    @app.get("/api/tokens/recent", response_model=list[TokenRecord], dependencies=[Depends(require_api_key)])
    async def recent_tokens(request: Request, limit: int = Query(50, ge=1, le=200)):
        return await request.app.state.repo.list_recent_tokens(limit)

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
            "listener_enabled": settings.listener_enabled,
            "metrics": st.metrics.as_dict(),
        }

    return app


app = create_app()
