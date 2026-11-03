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
    settings = Settings(_env_file=None, storage_backend="memory", listener_enabled=False, analysis_enabled=False, api_key=api_key)
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


def test_security_report_endpoints():
    from app.security.engine import SecurityAnalyzer
    from app.security.policy import SecurityPolicy
    from tests.security_helpers import FakeRPC, fixture_token

    repo = InMemoryRepository()
    token = fixture_token(1)  # FIXTURE data
    asyncio.run(repo.create_token_if_absent(token))
    report = asyncio.run(SecurityAnalyzer(FakeRPC(), repo, SecurityPolicy()).analyze(token))
    asyncio.run(repo.save_security_report(report))
    settings = Settings(_env_file=None, storage_backend="memory", listener_enabled=False, analysis_enabled=False, api_key="secret")
    with TestClient(create_app(settings, repo)) as c:
        h = {"X-API-Key": "secret"}
        body = c.get(f"/api/tokens/{token.mint}/security", headers=h).json()
        assert body["status"] == "WATCH" and body["checks"] and body["reason"]
        assert len(c.get(f"/api/tokens/{token.mint}/security/history", headers=h).json()) == 1
        assert c.get(f"/api/tokens/{'2' * 44}/security", headers=h).status_code == 404
        assert c.get("/api/tokens/not-a-mint/security", headers=h).status_code == 422


def test_stats_status_filter_and_token_detail():
    repo = InMemoryRepository()
    for i in range(1, 4):  # FIXTURE tokens
        asyncio.run(repo.create_token_if_absent(TokenRecord.from_event(parse_message(create_message(i)))))
    mint = next(iter(repo.tokens))
    asyncio.run(repo.update_token_decision(mint, "REJECTED", "fixture reason", "r1", repo.tokens[mint].detected_at))
    settings = Settings(_env_file=None, storage_backend="memory", listener_enabled=False, analysis_enabled=False, api_key="secret")
    with TestClient(create_app(settings, repo)) as c:
        h = {"X-API-Key": "secret"}
        stats = c.get("/api/stats", headers=h).json()
        assert stats["total"] == 3 and stats["counts"]["REJECTED"] == 1 and stats["counts"]["INSUFFICIENT_DATA"] == 2
        rejected = c.get("/api/tokens/recent?status=REJECTED", headers=h).json()
        assert [t["mint"] for t in rejected] == [mint]
        assert len(c.get("/api/tokens/recent?status=REJECTED,INSUFFICIENT_DATA", headers=h).json()) == 3
        assert c.get("/api/tokens/recent?status=BOGUS", headers=h).status_code == 422
        assert c.get(f"/api/tokens/{mint}", headers=h).json()["status"] == "REJECTED"
        assert c.get(f"/api/tokens/{'2' * 44}", headers=h).status_code == 404
