import asyncio

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models import TokenRecord
from app.parser import parse_message
from app.repository import InMemoryRepository
from tests.helpers import create_message


def client_with_tokens(n=3, api_key="secret"):
    repo = InMemoryRepository()
    for i in range(1, n + 1):  # FIXTURE tokens, not live data
        asyncio.run(repo.create_token_if_absent(TokenRecord.from_event(parse_message(create_message(i)))))
    settings = Settings(_env_file=None, storage_backend="memory", listener_enabled=False, api_key=api_key)
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
