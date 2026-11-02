import asyncio
import json
import logging
import random
import time
from contextlib import asynccontextmanager

from app.health import HealthRegistry, Metrics
from app.parser import ParseError, parse_message

log = logging.getLogger("tosh.listener")
PROVIDER = "pumpportal_ws"


@asynccontextmanager
async def default_connect(url: str):
    import websockets

    async with websockets.connect(url, ping_interval=20, ping_timeout=20, open_timeout=15, max_size=2**20) as ws:
        yield ws


class PumpPortalListener:
    """One shared WebSocket connection. Parsing is cheap; everything else happens off the queue."""

    def __init__(
        self,
        url: str,
        queue: asyncio.Queue,
        health: HealthRegistry,
        metrics: Metrics,
        *,
        connect=default_connect,
        sleep=asyncio.sleep,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        stable_after: float = 30.0,
        clock=time.monotonic,
        rng=random.random,
    ) -> None:
        self.url, self.queue, self.health, self.metrics = url, queue, health, metrics
        self._connect, self._sleep = connect, sleep
        self._base, self._max, self._stable = base_delay, max_delay, stable_after
        self._clock, self._rng = clock, rng

    def _handle(self, raw) -> None:
        self.metrics.received += 1
        self.health.mark_message(PROVIDER)
        try:
            event = parse_message(raw)
        except ParseError as exc:
            self.metrics.parse_errors += 1
            log.warning("unparseable message", extra={"error": str(exc), "sample": str(raw)[:300]})
            return
        if event is None:
            self.metrics.ignored += 1
            return
        self.metrics.parsed += 1
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.metrics.dropped_queue_full += 1
            log.error("queue full, launch event dropped", extra={"mint": event.mint})

    async def run(self, stop: asyncio.Event) -> None:
        attempt = 0
        while not stop.is_set():
            started: float | None = None
            try:
                async with self._connect(self.url) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    started = self._clock()
                    self.health.mark_connected(PROVIDER)
                    log.info("connected and subscribed", extra={"url": self.url})
                    async for raw in ws:
                        self._handle(raw)
                        if stop.is_set():
                            break
                if not stop.is_set():
                    self.health.mark_disconnected(PROVIDER, "connection closed by server")
            except asyncio.CancelledError:
                self.health.mark_disconnected(PROVIDER, "shutdown")
                raise
            except Exception as exc:
                self.health.mark_failure(PROVIDER, f"{type(exc).__name__}: {exc}")
                log.warning("connection failed", extra={"error": f"{type(exc).__name__}: {exc}"})

            if stop.is_set():
                self.health.mark_disconnected(PROVIDER, "shutdown")
                break
            if started is not None and self._clock() - started >= self._stable:
                attempt = 0  # the last connection was healthy, so restart backoff
            delay = min(self._max, self._base * (2**attempt)) * (0.5 + 0.5 * self._rng())
            attempt += 1
            self.health.get(PROVIDER).reconnect_count += 1
            log.info("reconnecting", extra={"delay_seconds": round(delay, 2), "attempt": attempt})
            await self._sleep(delay)
