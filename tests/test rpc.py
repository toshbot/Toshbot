import httpx
import pytest

from app.health import HealthRegistry
from app.rpc import PROVIDER, RpcError, RpcUnavailable, SolanaRPC


def make(handler, sleeps=None, retries=3):
    async def fake_sleep(d):
        if sleeps is not None:
            sleeps.append(d)

    health = HealthRegistry()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return SolanaRPC("https://rpc.fixture.invalid", health, client=client, max_retries=retries, sleep=fake_sleep), health


async def test_success_returns_result_and_marks_healthy():
    rpc, health = make(lambda req: httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": 5}}))
    assert await rpc.call("getBalance", ["x"]) == {"value": 5}
    assert health.get(PROVIDER).status == "connected"


async def test_rate_limit_is_retried_with_backoff():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(429, headers={"retry-after": "2"})
        return httpx.Response(200, json={"result": 1})

    sleeps = []
    rpc, _ = make(handler, sleeps)
    assert await rpc.call("m", []) == 1
    assert calls["n"] == 3 and sleeps == [2.0, 2.0]


async def test_persistent_failure_raises_unavailable_and_is_visible_in_health():
    rpc, health = make(lambda req: httpx.Response(503), retries=2)
    with pytest.raises(RpcUnavailable):
        await rpc.call("m", [])
    assert health.get(PROVIDER).status == "error" and "503" in health.get(PROVIDER).last_error


async def test_definitive_rpc_error_is_not_retried():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        return httpx.Response(200, json={"error": {"code": -32602, "message": "Invalid param"}})

    rpc, _ = make(handler)
    with pytest.raises(RpcError) as exc:
        await rpc.call("m", [])
    assert not isinstance(exc.value, RpcUnavailable) and calls["n"] == 1


async def test_timeout_is_retried_then_unavailable():
    def handler(req):
        raise httpx.ConnectTimeout("slow")

    rpc, _ = make(handler, retries=1)
    with pytest.raises(RpcUnavailable):
        await rpc.call("m", [])
