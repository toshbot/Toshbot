import asyncio
import json
from contextlib import asynccontextmanager

from app.health import HealthRegistry, Metrics
from app.listener import PROVIDER, PumpPortalListener
from tests.helpers import create_message


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


def build(script, **kw):
    queue, health, metrics = asyncio.Queue(), HealthRegistry(), Metrics()
    sleeps = []

    async def fake_sleep(d):
        sleeps.append(d)

    connect, calls = make_connect(script)
    listener = PumpPortalListener("wss://fixture.invalid", queue, health, metrics, connect=connect,
                                  sleep=fake_sleep, base_delay=1.0, max_delay=60.0, rng=lambda: 1.0, **kw)
    return listener, queue, health, metrics, sleeps, calls


async def test_reconnects_after_failure_and_queues_events():
    stop = asyncio.Event()
    ws = FakeWS([create_message(1), "garbage", json.dumps({"message": "Successfully subscribed"})], on_done=stop.set)
    listener, queue, health, metrics, sleeps, calls = build([OSError("boom"), ws])
    await listener.run(stop)
    assert len(calls) == 2 and sleeps == [1.0]
    assert queue.qsize() == 1
    assert (metrics.parsed, metrics.parse_errors, metrics.ignored) == (1, 1, 1)
    assert json.loads(ws.sent[0]) == {"method": "subscribeNewToken"}
    assert health.get(PROVIDER).reconnect_count == 1


async def test_exponential_backoff_delays_grow():
    stop = asyncio.Event()
    ws = FakeWS([], on_done=stop.set)
    listener, _, health, _, sleeps, _ = build([OSError("a"), OSError("b"), OSError("c"), ws])
    await listener.run(stop)
    assert sleeps == [1.0, 2.0, 4.0]


async def test_failure_is_visible_in_health():
    stop = asyncio.Event()
    listener, _, health, _, _, _ = build([OSError("refused"), FakeWS([], on_done=stop.set)])
    seen = {}
    original_sleep = listener._sleep

    async def spy(d):
        seen["status"] = health.get(PROVIDER).status
        seen["error"] = health.get(PROVIDER).last_error
        await original_sleep(d)

    listener._sleep = spy
    await listener.run(stop)
    assert seen["status"] == "error" and "refused" in seen["error"]


async def test_full_queue_drops_are_counted_not_hidden():
    stop = asyncio.Event()
    queue, health, metrics = asyncio.Queue(maxsize=1), HealthRegistry(), Metrics()
    connect, _ = make_connect([FakeWS([create_message(1), create_message(2)], on_done=stop.set)])

    async def nosleep(d):
        pass

    listener = PumpPortalListener("wss://fixture.invalid", queue, health, metrics, connect=connect, sleep=nosleep)
    await listener.run(stop)
    assert metrics.dropped_queue_full == 1
