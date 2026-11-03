"""Tosh Bot: the whole backend in ONE file (detection, security engine, API and dashboard).

Render settings
  Build command : pip install -r requirements.txt
  Start command : uvicorn main:app --host 0.0.0.0 --port $PORT
Environment variables (set them in Render, never in GitHub)
  ENVIRONMENT=production   API_KEY=<long random>   DASHBOARD_USER=<name>   DASHBOARD_PASSWORD=<long random>
  STORAGE_BACKEND=firestore   FIREBASE_PROJECT_ID=<id>   GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/firebase-key.json
  SOLANA_RPC_URL=<dedicated RPC url>   PYTHON_VERSION=3.12.3
Open /health, then /dashboard.
Detection and analysis only: no trading, no guarantees. Missing data is never treated as a pass.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
import json
import logging
import sys
from datetime import datetime, timezone
import re
from typing import Any, Literal
from pydantic import BaseModel, Field
from datetime import datetime
from pydantic import BaseModel
import math
from typing import Any
from dataclasses import asdict, dataclass, field
import asyncio
import httpx
import inspect
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from datetime import datetime, timedelta
import random
import time
from contextlib import asynccontextmanager
import base64
import hmac
from pathlib import Path
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

log = logging.getLogger("tosh")


# ============================================================================
# app/config.py
# ============================================================================
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    environment: str = "development"
    api_key: str = ""
    dashboard_user: str = "admin"
    dashboard_password: str = ""  # protects the built-in /dashboard page; empty = disabled in production
    cors_origins: str = "http://localhost:3000"
    log_level: str = "INFO"

    storage_backend: str = "firestore"  # "firestore" | "memory"
    firebase_project_id: str = ""
    google_application_credentials: str = ""
    firestore_collection_prefix: str = ""

    pumpportal_ws_url: str = "wss://pumpportal.fun/api/data"
    listener_enabled: bool = True
    reconnect_base_seconds: float = 1.0
    reconnect_max_seconds: float = 60.0
    stable_connection_seconds: float = 30.0
    feed_stale_after_seconds: float = 120.0
    queue_maxsize: int = 1000
    processor_max_attempts: int = 3

    # Solana RPC (use a dedicated provider URL, e.g. Helius, for real use; keep the key only in env vars)
    solana_rpc_url: str = "https://api.mainnet-beta.solana.com"
    rpc_timeout_seconds: float = 10.0
    rpc_max_concurrency: int = 4

    # Analysis workers
    analysis_enabled: bool = True
    analysis_workers: int = 2
    analysis_queue_maxsize: int = 2000
    analysis_retry_delays: str = "5,20,60"  # seconds between re-tries while evidence is unavailable

    # Security thresholds. EXPERIMENTAL starting values, not calibrated. Authority/extension rules are NOT configurable.
    evidence_max_age_seconds: int = 900
    single_holder_reject_pct: float = 30.0
    top10_hold_pct: float = 40.0
    creator_hold_pct: float = 15.0
    max_transfer_fee_bps: int = 500
    creator_max_launches_24h: int = 5
    min_curve_reserve_sol: float = 0.1

    @property
    def retry_delay_list(self) -> list[float]:
        return [float(x) for x in self.analysis_retry_delays.split(",") if x.strip()]

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


# ============================================================================
# app/logging_config.py
# ============================================================================
_STANDARD = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())


# ============================================================================
# app/models.py
# ============================================================================
BASE58_ADDRESS = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

TokenStatus = Literal["REJECTED", "INSUFFICIENT_DATA", "HOLD", "WATCH", "QUALIFIED", "REVOKED"]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LaunchEvent(BaseModel):
    """A validated new-token event. `raw` is untrusted external data: store it, never act on it."""

    mint: str
    signature: str | None = None
    creator: str | None = None
    name: str | None = None
    symbol: str | None = None
    uri: str | None = None
    pool: str | None = None
    bonding_curve: str | None = None
    initial_buy_tokens: float | None = None
    sol_amount: float | None = None
    market_cap_sol: float | None = None
    event_timestamp: datetime | None = None  # original launch time, only if the provider supplied it
    received_at: datetime = Field(default_factory=utcnow)  # local receipt time
    raw: dict[str, Any] = Field(default_factory=dict)


class TokenRecord(BaseModel):
    mint: str
    name: str | None = None
    symbol: str | None = None
    uri: str | None = None
    creator: str | None = None
    signature: str | None = None
    pool: str | None = None
    bonding_curve: str | None = None
    source: str = "pumpportal"
    launch_timestamp: datetime | None = None
    detected_at: datetime = Field(default_factory=utcnow)
    # Fail closed: nothing has been verified yet, so the status is never anything but INSUFFICIENT_DATA in Phase 1.
    status: TokenStatus = "INSUFFICIENT_DATA"
    status_reason: str = "Security analysis has not been performed yet (Phase 1 detection only)."
    status_updated_at: datetime = Field(default_factory=utcnow)
    latest_report_id: str | None = None
    schema_version: int = 1

    @classmethod
    def from_event(cls, event: LaunchEvent) -> "TokenRecord":
        return cls(
            mint=event.mint,
            name=event.name,
            symbol=event.symbol,
            uri=event.uri,
            creator=event.creator,
            signature=event.signature,
            pool=event.pool,
            bonding_curve=event.bonding_curve,
            launch_timestamp=event.event_timestamp,
            detected_at=event.received_at,
            status_updated_at=event.received_at,
        )


# ============================================================================
# app/security/models.py
# ============================================================================
CheckStatus = Literal["PASS", "FAIL_CRITICAL", "SUSPICIOUS", "UNAVAILABLE"]


class CheckResult(BaseModel):
    name: str
    status: CheckStatus
    summary: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    sources: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    mandatory: bool = True
    retrieved_at: datetime = Field(default_factory=utcnow)


class SecurityReport(BaseModel):
    """Append-only: a new analysis creates a new report; old reports are never overwritten."""

    mint: str
    analysis_version: str
    analyzed_at: datetime
    status: TokenStatus
    reason: str
    reasons: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    checks: list[CheckResult] = Field(default_factory=list)
    not_evaluated: list[str] = Field(default_factory=list)
    next_review_at: datetime | None = None
    policy: dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# app/security/policy.py
# ============================================================================
ANALYSIS_VERSION = "security-0.1.0"

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
BURN_ADDRESSES = {"1nc1nerator11111111111111111111111111111111", "11111111111111111111111111111111"}

REQUIRED_CHECKS = ["mint_permissions", "token_extensions", "holder_concentration", "creator_wallet", "liquidity"]

# Mandatory per the spec but NOT built yet. Listed in every report so nobody mistakes "not checked" for "passed".
NOT_EVALUATED = [
    "sell_simulation (sellability not verified)",
    "trading_manipulation (wash trading / coordinated buying)",
    "metadata_and_links (website / social review)",
    "creator_dump_history (transfer-level analysis of the creator wallet)",
    "post_migration_pool_liquidity (needs market-data providers, Phase 3)",
    "opportunity_score (Phase 3): QUALIFIED is impossible until it exists",
]


class SecurityPolicy(BaseModel):
    """Thresholds are experimental. Authority and dangerous-extension rules are hard-coded, not settings."""

    evidence_max_age_seconds: int = 900
    single_holder_reject_pct: float = 30.0
    top10_hold_pct: float = 40.0
    creator_hold_pct: float = 15.0
    max_transfer_fee_bps: int = 500
    creator_max_launches_24h: int = 5
    min_curve_reserve_sol: float = 0.1
    review_seconds_insufficient: int = 60
    review_seconds_hold: int = 300
    review_seconds_watch: int = 600


# ============================================================================
# app/parser.py
# ============================================================================
MAX_RAW_JSON_CHARS = 20_000


class ParseError(ValueError):
    """The message was malformed or reported a provider error."""


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _text(value: Any, limit: int = 500) -> str | None:
    if value is None:
        return None
    s = str(value).strip()[:limit]
    return s or None


def _addr(value: Any) -> str | None:
    s = _text(value, 64)
    return s if s and BASE58_ADDRESS.match(s) else None


def _timestamp(value: Any) -> datetime | None:
    n = _num(value)
    if n is None or n <= 0:
        return None
    if n > 1e11:  # milliseconds
        n /= 1000
    try:
        return datetime.fromtimestamp(n, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def parse_message(raw: str | bytes, received_at: datetime | None = None) -> LaunchEvent | None:
    """Parse one WebSocket message.

    Returns a LaunchEvent for a new-token event, None for harmless non-token messages
    (subscription acknowledgements, trades), and raises ParseError for malformed
    messages or provider-reported errors. Missing optional metadata never discards an event.
    """
    if isinstance(raw, (bytes, bytearray)):
        raw = bytes(raw).decode("utf-8", errors="replace")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ParseError(f"invalid JSON: {exc.msg}") from exc
    if not isinstance(data, dict):
        raise ParseError("message is not a JSON object")

    if "mint" not in data:
        if "errors" in data or "error" in data:
            raise ParseError(f"provider reported an error: {str(data)[:300]}")
        if "message" in data:
            return None  # e.g. subscription acknowledgement
        raise ParseError("message has no mint")

    tx_type = data.get("txType")
    if tx_type is not None and tx_type != "create":
        return None  # trade or other event, not a launch

    mint = _addr(data.get("mint"))
    if mint is None:
        raise ParseError("invalid mint address")

    raw_json = json.dumps(data, default=str)
    stored_raw = data if len(raw_json) <= MAX_RAW_JSON_CHARS else {"_truncated": True}

    return LaunchEvent(
        mint=mint,
        signature=_text(data.get("signature"), 128),
        creator=_addr(data.get("traderPublicKey")),
        name=_text(data.get("name")),
        symbol=_text(data.get("symbol"), 50),
        uri=_text(data.get("uri"), 1000),
        pool=_text(data.get("pool"), 50),
        bonding_curve=_addr(data.get("bondingCurveKey")),
        initial_buy_tokens=_num(data.get("initialBuy")),
        sol_amount=_num(data.get("solAmount")),
        market_cap_sol=_num(data.get("marketCapSol")),
        event_timestamp=_timestamp(data.get("timestamp") or data.get("blockTime")),
        received_at=received_at or utcnow(),
        raw=stored_raw,
    )


# ============================================================================
# app/health.py
# ============================================================================
@dataclass
class Metrics:
    received: int = 0
    parsed: int = 0
    ignored: int = 0
    parse_errors: int = 0
    dropped_queue_full: int = 0
    stored: int = 0
    duplicates: int = 0
    failed_jobs: int = 0
    analyses: int = 0
    failed_analyses: int = 0

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class ProviderHealth:
    name: str
    status: str = "unknown"  # unknown | connected | disconnected | error
    connected_at: datetime | None = None
    last_message_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    reconnect_count: int = 0


@dataclass
class HealthRegistry:
    providers: dict[str, ProviderHealth] = field(default_factory=dict)

    def get(self, name: str) -> ProviderHealth:
        return self.providers.setdefault(name, ProviderHealth(name=name))

    def mark_connected(self, name: str) -> None:
        p = self.get(name)
        now = utcnow()
        p.status, p.connected_at, p.last_success_at = "connected", now, now
        p.consecutive_failures = 0

    def mark_message(self, name: str) -> None:
        p = self.get(name)
        now = utcnow()
        p.last_message_at = p.last_success_at = now

    def mark_success(self, name: str) -> None:
        p = self.get(name)
        p.last_success_at = utcnow()
        p.consecutive_failures = 0
        if p.status in ("unknown", "error"):
            p.status = "connected"

    def mark_failure(self, name: str, error: str) -> None:
        p = self.get(name)
        p.status, p.last_error, p.last_failure_at = "error", error[:500], utcnow()
        p.consecutive_failures += 1

    def mark_disconnected(self, name: str, reason: str) -> None:
        p = self.get(name)
        p.status, p.last_error, p.last_failure_at = "disconnected", reason[:500], utcnow()

    def snapshot(self, stale_after_seconds: float) -> dict[str, dict]:
        now = utcnow()
        out: dict[str, dict] = {}
        for name, p in self.providers.items():
            d = asdict(p)
            d["effective_status"] = p.status
            if p.status == "connected":
                last = p.last_message_at or p.connected_at
                if last and (now - last).total_seconds() > stale_after_seconds:
                    d["effective_status"] = "stale"  # connected but no data: do not trust as healthy
            out[name] = d
        return out


# ============================================================================
# app/rpc.py
# ============================================================================
RPC_PROVIDER = "solana_rpc"


class RpcError(Exception):
    """The provider answered with a definitive error (or a non-retryable HTTP status)."""


class RpcUnavailable(RpcError):
    """Timeouts, 429s, 5xx, network or malformed responses after all retries."""


class SolanaRPC:
    """Minimal JSON-RPC client: timeouts, bounded concurrency, safe retries, rate-limit handling, health reporting."""

    def __init__(self, url: str, health: HealthRegistry, *, client: httpx.AsyncClient | None = None,
                 timeout: float = 10.0, max_retries: int = 3, max_concurrency: int = 4,
                 sleep=asyncio.sleep, commitment: str = "confirmed") -> None:
        self.url, self.health, self.max_retries, self.commitment = url, health, max_retries, commitment
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = client is None
        self._sem = asyncio.Semaphore(max_concurrency)
        self._sleep = sleep

    async def call(self, method: str, params: list) -> Any:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        last = "unknown error"
        for attempt in range(self.max_retries + 1):
            delay = min(8.0, 0.5 * 2**attempt)
            try:
                async with self._sem:
                    resp = await self._client.post(self.url, json=payload)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = type(exc).__name__
            else:
                if resp.status_code == 429 or resp.status_code >= 500:
                    last = f"HTTP {resp.status_code}"
                    ra = resp.headers.get("retry-after", "")
                    if ra.isdigit():
                        delay = min(float(ra), 15.0)
                elif resp.status_code >= 400:
                    self.health.mark_failure(RPC_PROVIDER, f"HTTP {resp.status_code}")
                    raise RpcError(f"HTTP {resp.status_code}")
                else:
                    try:
                        body = resp.json()
                    except ValueError:
                        body = None
                    if not isinstance(body, dict):
                        last = "invalid JSON response"
                    elif "error" in body:
                        err = body["error"]
                        code = err.get("code") if isinstance(err, dict) else None
                        if code in (-32005, -32004):  # node unhealthy / behind: transient
                            last = f"rpc error {code}"
                        else:
                            self.health.mark_success(RPC_PROVIDER)  # the provider is up; the request was rejected
                            raise RpcError(str(err)[:300])
                    elif "result" in body:
                        self.health.mark_success(RPC_PROVIDER)
                        return body["result"]
                    else:
                        last = "malformed response"
            if attempt < self.max_retries:
                await self._sleep(delay)
        self.health.mark_failure(RPC_PROVIDER, last)
        log.warning("rpc call failed", extra={"method": method, "error": last})
        raise RpcUnavailable(last)

    def _cfg(self, **extra) -> dict:
        return {"commitment": self.commitment, **extra}

    async def get_account_info(self, address: str) -> dict | None:
        r = await self.call("getAccountInfo", [address, self._cfg(encoding="jsonParsed")])
        return r.get("value") if isinstance(r, dict) else None

    async def get_multiple_accounts(self, addresses: list[str]) -> list[dict | None]:
        r = await self.call("getMultipleAccounts", [addresses, self._cfg(encoding="jsonParsed")])
        return r["value"]

    async def get_token_largest_accounts(self, mint: str) -> list[dict]:
        r = await self.call("getTokenLargestAccounts", [mint, self._cfg()])
        return r["value"]

    async def get_balance(self, address: str) -> int:
        r = await self.call("getBalance", [address, self._cfg()])
        return int(r["value"])

    async def get_signatures_for_address(self, address: str, limit: int = 100) -> list[dict]:
        return await self.call("getSignaturesForAddress", [address, self._cfg(limit=limit)])

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()


# ============================================================================
# app/repository.py
# ============================================================================
JOB_VERSION = "phase1-detection-0"


class ConfigError(RuntimeError):
    pass


class Repository(ABC):
    """Storage interface. Every write is idempotent so retries and duplicate events are safe."""

    @abstractmethod
    async def record_launch_event(self, event: LaunchEvent) -> bool:
        """Store the raw launch event. Returns True if newly stored."""

    @abstractmethod
    async def create_token_if_absent(self, token: TokenRecord) -> bool:
        """Atomically create the token keyed by mint. Returns True only if created."""

    @abstractmethod
    async def create_analysis_job(self, mint: str) -> bool:
        """Create the pending analysis job for a mint once. Returns True if newly created."""

    @abstractmethod
    async def list_recent_tokens(self, limit: int, statuses: list[str] | None = None) -> list[TokenRecord]: ...

    @abstractmethod
    async def count_tokens_by_status(self) -> dict[str, int]: ...

    @abstractmethod
    async def get_token(self, mint: str) -> TokenRecord | None: ...

    @abstractmethod
    async def update_token_decision(self, mint: str, status: str, reason: str, report_id: str, at: datetime) -> None: ...

    @abstractmethod
    async def save_security_report(self, report: SecurityReport) -> str:
        """Append a report (never overwrites). Returns its id."""

    @abstractmethod
    async def get_latest_report(self, mint: str) -> SecurityReport | None: ...

    @abstractmethod
    async def list_reports(self, mint: str, limit: int) -> list[SecurityReport]: ...

    @abstractmethod
    async def list_pending_jobs(self, limit: int) -> list[str]: ...

    @abstractmethod
    async def complete_job(self, mint: str, status: str) -> None: ...

    @abstractmethod
    async def count_recent_tokens_by_creator(self, creator: str, since: datetime) -> int: ...

    @abstractmethod
    async def ping(self) -> bool: ...

    async def aclose(self) -> None:
        return None


def event_doc_id(event: LaunchEvent) -> str:
    return f"{event.mint}_{event.signature}" if event.signature else f"{event.mint}_{int(event.received_at.timestamp() * 1000)}"


class InMemoryRepository(Repository):
    """NOT persistent. For tests and explicit local experiments only."""

    def __init__(self) -> None:
        self.tokens: dict[str, TokenRecord] = {}
        self.events: dict[str, dict] = {}
        self.jobs: dict[str, dict] = {}
        self.reports: dict[str, list[tuple[str, SecurityReport]]] = {}

    async def record_launch_event(self, event: LaunchEvent) -> bool:
        key = event_doc_id(event)
        if key in self.events:
            return False
        self.events[key] = event.model_dump()
        return True

    async def create_token_if_absent(self, token: TokenRecord) -> bool:
        if token.mint in self.tokens:
            return False
        self.tokens[token.mint] = token
        return True

    async def create_analysis_job(self, mint: str) -> bool:
        key = f"{mint}__initial"
        if key in self.jobs:
            return False
        self.jobs[key] = {"mint": mint, "status": "pending", "analysis_version": JOB_VERSION, "created_at": utcnow()}
        return True

    async def list_recent_tokens(self, limit: int, statuses: list[str] | None = None) -> list[TokenRecord]:
        items = [t for t in self.tokens.values() if not statuses or t.status in statuses]
        return sorted(items, key=lambda t: t.detected_at, reverse=True)[:limit]

    async def count_tokens_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for t in self.tokens.values():
            counts[t.status] = counts.get(t.status, 0) + 1
        return counts

    async def get_token(self, mint: str) -> TokenRecord | None:
        return self.tokens.get(mint)

    async def update_token_decision(self, mint, status, reason, report_id, at) -> None:
        self.tokens[mint] = self.tokens[mint].model_copy(
            update={"status": status, "status_reason": reason, "latest_report_id": report_id, "status_updated_at": at})

    async def save_security_report(self, report: SecurityReport) -> str:
        items = self.reports.setdefault(report.mint, [])
        report_id = f"{report.mint[:6]}-{len(items) + 1}"
        items.append((report_id, report.model_copy(deep=True)))
        return report_id

    async def get_latest_report(self, mint: str) -> SecurityReport | None:
        items = self.reports.get(mint)
        return items[-1][1] if items else None

    async def list_reports(self, mint: str, limit: int) -> list[SecurityReport]:
        return [r for _, r in reversed(self.reports.get(mint, []))][:limit]

    async def list_pending_jobs(self, limit: int) -> list[str]:
        return [j["mint"] for j in self.jobs.values() if j["status"] == "pending"][:limit]

    async def complete_job(self, mint: str, status: str) -> None:
        job = self.jobs.get(f"{mint}__initial")
        if job:
            job["status"], job["completed_at"] = status, utcnow()

    async def count_recent_tokens_by_creator(self, creator: str, since: datetime) -> int:
        return sum(1 for t in self.tokens.values() if t.creator == creator and t.detected_at >= since)

    async def ping(self) -> bool:
        return True


class FirestoreRepository(Repository):
    """Firestore (Firebase) storage. Document id = mint, so uniqueness is enforced by the database."""

    def __init__(self, client, prefix: str = "") -> None:
        from google.api_core.exceptions import AlreadyExists

        self._AlreadyExists = AlreadyExists
        self._client = client
        self._tokens = client.collection(f"{prefix}tokens")
        self._events = client.collection(f"{prefix}token_launch_events")
        self._jobs = client.collection(f"{prefix}analysis_runs")

    async def _create(self, ref, data: dict) -> bool:
        try:
            await ref.create(data)  # atomic: fails if the document already exists
            return True
        except self._AlreadyExists:
            return False

    async def record_launch_event(self, event: LaunchEvent) -> bool:
        return await self._create(self._events.document(event_doc_id(event)), event.model_dump())

    async def create_token_if_absent(self, token: TokenRecord) -> bool:
        return await self._create(self._tokens.document(token.mint), token.model_dump())

    async def create_analysis_job(self, mint: str) -> bool:
        data = {"mint": mint, "status": "pending", "analysis_version": JOB_VERSION, "created_at": utcnow()}
        return await self._create(self._jobs.document(f"{mint}__initial"), data)

    async def list_recent_tokens(self, limit: int, statuses: list[str] | None = None) -> list[TokenRecord]:
        from google.cloud import firestore
        from google.cloud.firestore_v1.base_query import FieldFilter

        if not statuses:
            query = self._tokens.order_by("detected_at", direction=firestore.Query.DESCENDING).limit(limit)
            return [TokenRecord.model_validate(doc.to_dict()) async for doc in query.stream()]
        # Equality-only queries need no composite index; sort in Python. Capped for cost.
        found: list[TokenRecord] = []
        for status in statuses:
            query = self._tokens.where(filter=FieldFilter("status", "==", status)).limit(500)
            found += [TokenRecord.model_validate(doc.to_dict()) async for doc in query.stream()]
        return sorted(found, key=lambda t: t.detected_at, reverse=True)[:limit]

    async def count_tokens_by_status(self) -> dict[str, int]:
        from google.cloud.firestore_v1.base_query import FieldFilter

        counts: dict[str, int] = {}
        for status in ("REJECTED", "INSUFFICIENT_DATA", "HOLD", "WATCH", "QUALIFIED", "REVOKED"):
            result = await self._tokens.where(filter=FieldFilter("status", "==", status)).count().get()
            counts[status] = int(result[0][0].value)
        return counts

    async def get_token(self, mint: str) -> TokenRecord | None:
        doc = await self._tokens.document(mint).get()
        return TokenRecord.model_validate(doc.to_dict()) if doc.exists else None

    async def update_token_decision(self, mint, status, reason, report_id, at) -> None:
        await self._tokens.document(mint).update(
            {"status": status, "status_reason": reason, "latest_report_id": report_id, "status_updated_at": at})

    def _reports(self, mint: str):
        return self._tokens.document(mint).collection("security_reports")

    async def save_security_report(self, report: SecurityReport) -> str:
        ref = self._reports(report.mint).document()  # auto id: every analysis is a new document
        await ref.set(report.model_dump(mode="python"))
        return ref.id

    async def list_reports(self, mint: str, limit: int) -> list[SecurityReport]:
        from google.cloud import firestore

        query = self._reports(mint).order_by("analyzed_at", direction=firestore.Query.DESCENDING).limit(limit)
        return [SecurityReport.model_validate(doc.to_dict()) async for doc in query.stream()]

    async def get_latest_report(self, mint: str) -> SecurityReport | None:
        reports = await self.list_reports(mint, 1)
        return reports[0] if reports else None

    async def list_pending_jobs(self, limit: int) -> list[str]:
        from google.cloud.firestore_v1.base_query import FieldFilter

        query = self._jobs.where(filter=FieldFilter("status", "==", "pending")).limit(limit)
        return [doc.to_dict()["mint"] async for doc in query.stream()]

    async def complete_job(self, mint: str, status: str) -> None:
        await self._jobs.document(f"{mint}__initial").update({"status": status, "completed_at": utcnow()})

    async def count_recent_tokens_by_creator(self, creator: str, since: datetime) -> int:
        from google.cloud.firestore_v1.base_query import FieldFilter

        # equality-only query (no composite index needed); the date filter is applied here
        query = self._tokens.where(filter=FieldFilter("creator", "==", creator)).limit(500)
        return sum([1 async for doc in query.stream() if doc.to_dict().get("detected_at") >= since])

    async def ping(self) -> bool:
        try:
            await self._tokens.limit(1).get()
            return True
        except Exception:
            return False

    async def aclose(self) -> None:
        result = self._client.close()
        if inspect.isawaitable(result):
            await result


def build_repository(settings: Settings) -> Repository:
    if settings.storage_backend == "memory":
        return InMemoryRepository()
    if settings.storage_backend != "firestore":
        raise ConfigError(f"Unknown STORAGE_BACKEND '{settings.storage_backend}'")

    from google.cloud import firestore

    if os.environ.get("FIRESTORE_EMULATOR_HOST"):
        from google.auth.credentials import AnonymousCredentials

        client = firestore.AsyncClient(project=settings.firebase_project_id or "tosh-bot-emulator", credentials=AnonymousCredentials())
    else:
        if not settings.google_application_credentials:
            raise ConfigError(
                "Firestore selected but GOOGLE_APPLICATION_CREDENTIALS is not set. "
                "Provide a service-account JSON path, use the emulator, or set STORAGE_BACKEND=memory explicitly."
            )
        client = firestore.AsyncClient.from_service_account_json(
            settings.google_application_credentials, project=settings.firebase_project_id or None
        )
    return FirestoreRepository(client, settings.firestore_collection_prefix)


# ============================================================================
# app/security/checks.py
# ============================================================================
"""Deterministic security checks. Rules:
- A check FAILS only on verified on-chain facts.
- Any provider error, missing account or unexpected data shape is UNAVAILABLE, never a failure and never a pass.
"""


SRC_RPC = "solana_rpc"
BENIGN_EXTENSIONS = {"metadatapointer", "tokenmetadata", "grouppointer", "groupmemberpointer", "tokengroup", "tokengroupmember", "mintcloseauthority"}


@dataclass
class MintData:
    owner: str
    info: dict
    supply_raw: int
    decimals: int


def parse_mint(account: dict) -> MintData:
    parsed = account["data"]["parsed"]
    if parsed.get("type") != "mint":
        raise ValueError("account is not a mint")
    info = parsed["info"]
    return MintData(owner=account["owner"], info=info, supply_raw=int(info["supply"]), decimals=int(info["decimals"]))


def result(name, status, summary, evidence=None, warnings=None, sources=None, mandatory=True) -> CheckResult:
    return CheckResult(name=name, status=status, summary=summary, evidence=evidence or {}, warnings=warnings or [],
                       sources=sources or [SRC_RPC], mandatory=mandatory, retrieved_at=utcnow())


def unavailable(name: str, why: str) -> CheckResult:
    return result(name, "UNAVAILABLE", f"Could not be verified: {why}", warnings=["Missing evidence is never treated as a pass."])


def check_mint_permissions(md: MintData) -> CheckResult:
    mint_auth, freeze_auth = md.info.get("mintAuthority"), md.info.get("freezeAuthority")
    evidence = {"token_program": md.owner, "mint_authority": mint_auth, "freeze_authority": freeze_auth,
                "supply_raw": str(md.supply_raw), "decimals": md.decimals}
    problems = []
    if mint_auth:
        problems.append("Mint authority is active: additional supply can be minted.")
    if freeze_auth:
        problems.append("Freeze authority is active: holder accounts can be frozen.")
    if problems:
        return result("mint_permissions", "FAIL_CRITICAL", " ".join(problems), evidence)
    if md.owner not in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        return result("mint_permissions", "SUSPICIOUS", "Mint is owned by an unrecognised token program.", evidence)
    return result("mint_permissions", "PASS", "Mint and freeze authorities are revoked.", evidence,
                  warnings=["Revoked authorities are a positive indicator, not proof of safety."])


def check_token_extensions(md: MintData, policy: SecurityPolicy) -> CheckResult:
    if md.owner == TOKEN_PROGRAM:
        return result("token_extensions", "PASS", "Classic SPL Token program: no extensions exist.", {"token_program": md.owner})
    if md.owner != TOKEN_2022_PROGRAM:
        return result("token_extensions", "SUSPICIOUS", "Unrecognised token program; extensions cannot be assessed.", {"token_program": md.owner})

    extensions = md.info.get("extensions") or []
    critical, suspicious, names = [], [], []
    for ext in extensions:
        name = str(ext.get("extension", "unknown"))
        state = ext.get("state") or {}
        names.append(name)
        key = name.lower()
        if key == "permanentdelegate":
            if state.get("delegate"):
                critical.append("Permanent delegate is set: it can move or burn any holder's tokens.")
        elif key == "nontransferable":
            critical.append("Token is non-transferable.")
        elif key == "defaultaccountstate":
            if str(state.get("accountState", "")).lower() == "frozen":
                critical.append("New token accounts default to frozen.")
        elif key == "transferfeeconfig":
            fees = [f.get("transferFeeBasisPoints") for f in (state.get("newerTransferFee"), state.get("olderTransferFee")) if isinstance(f, dict)]
            bps = max([int(f) for f in fees if f is not None], default=0)
            if bps > policy.max_transfer_fee_bps:
                critical.append(f"Transfer fee of {bps / 100:.2f}% exceeds the {policy.max_transfer_fee_bps / 100:.2f}% limit.")
            elif bps > 0 or state.get("transferFeeConfigAuthority"):
                suspicious.append(f"Transfer fee configured ({bps / 100:.2f}%) and/or changeable by an authority.")
        elif key == "transferhook":
            if state.get("programId"):
                suspicious.append("Transfer hook program is set: custom code runs on every transfer.")
        elif key not in BENIGN_EXTENSIONS:
            suspicious.append(f"Extension '{name}' needs manual review.")
    evidence = {"token_program": md.owner, "extensions": names, "critical": critical, "suspicious": suspicious}
    if critical:
        return result("token_extensions", "FAIL_CRITICAL", " ".join(critical), evidence)
    if suspicious:
        return result("token_extensions", "SUSPICIOUS", " ".join(suspicious), evidence)
    return result("token_extensions", "PASS", "Token-2022 mint with no risky extensions.", evidence)


METHODOLOGY = ("Percentages are of TOTAL supply. Owners are resolved from the top-20 token accounts (RPC limit). "
               "Accounts owned by the bonding curve or burn addresses are excluded from holder ranking and listed separately. "
               "Balances are aggregated per owner. AMM pool vaults after migration are not yet identified.")


async def check_holders(rpc: SolanaRPC, token: TokenRecord, md: MintData, policy: SecurityPolicy) -> CheckResult:
    if md.supply_raw <= 0:
        return unavailable("holder_concentration", "mint supply is zero or unknown")
    largest = await rpc.get_token_largest_accounts(token.mint)
    if not largest:
        return unavailable("holder_concentration", "no token accounts returned yet")
    accounts = await rpc.get_multiple_accounts([x["address"] for x in largest])
    by_owner: dict[str, int] = {}
    excluded: list[dict] = []
    for item, acct in zip(largest, accounts):
        if not acct:
            return unavailable("holder_concentration", f"token account {item['address']} could not be read")
        owner = acct["data"]["parsed"]["info"]["owner"]
        amount = int(item["amount"])
        if owner in BURN_ADDRESSES:
            excluded.append({"owner": owner, "reason": "burn", "pct_of_supply": round(amount / md.supply_raw * 100, 4)})
        elif token.bonding_curve and owner == token.bonding_curve:
            excluded.append({"owner": owner, "reason": "bonding_curve", "pct_of_supply": round(amount / md.supply_raw * 100, 4)})
        else:
            by_owner[owner] = by_owner.get(owner, 0) + amount
    ranked = sorted(by_owner.items(), key=lambda kv: kv[1], reverse=True)
    holders = [{"owner": o, "pct_of_supply": round(a / md.supply_raw * 100, 4), "is_creator": o == token.creator} for o, a in ranked]
    top1 = holders[0]["pct_of_supply"] if holders else 0.0
    top5 = round(sum(h["pct_of_supply"] for h in holders[:5]), 4)
    top10 = round(sum(h["pct_of_supply"] for h in holders[:10]), 4)
    creator_pct = round(by_owner.get(token.creator, 0) / md.supply_raw * 100, 4) if token.creator else None
    evidence = {"top1_pct": top1, "top5_pct": top5, "top10_pct": top10, "creator_pct": creator_pct,
                "holders": holders[:10], "excluded": excluded, "methodology": METHODOLOGY}
    warnings = [] if token.bonding_curve else ["Bonding curve address unknown: pool/curve accounts may be counted as holders."]
    if top1 > policy.single_holder_reject_pct:
        return result("holder_concentration", "FAIL_CRITICAL",
                      f"One wallet holds {top1:.2f}% of supply (limit {policy.single_holder_reject_pct:.0f}%).", evidence, warnings)
    problems = []
    if top10 > policy.top10_hold_pct:
        problems.append(f"Top 10 wallets hold {top10:.2f}% of supply (limit {policy.top10_hold_pct:.0f}%).")
    if creator_pct is not None and creator_pct > policy.creator_hold_pct:
        problems.append(f"Creator wallet holds {creator_pct:.2f}% of supply (limit {policy.creator_hold_pct:.0f}%).")
    if problems:
        return result("holder_concentration", "SUSPICIOUS", " ".join(problems), evidence, warnings)
    return result("holder_concentration", "PASS", f"Top holder {top1:.2f}%, top 10 {top10:.2f}% of supply.", evidence, warnings)


async def check_creator(rpc: SolanaRPC, repo: Repository, token: TokenRecord, policy: SecurityPolicy, now: datetime | None = None) -> CheckResult:
    if not token.creator:
        return unavailable("creator_wallet", "creator address was not supplied by the launch feed")
    now = now or utcnow()
    sigs = await rpc.get_signatures_for_address(token.creator, limit=100)
    launches_24h = await repo.count_recent_tokens_by_creator(token.creator, now - timedelta(hours=24))
    times = [s["blockTime"] for s in sigs if s.get("blockTime")]
    oldest = datetime.fromtimestamp(min(times), tz=timezone.utc) if times else None
    complete = len(sigs) < 100
    evidence = {
        "creator": token.creator,
        "signatures_sampled": len(sigs),
        "oldest_activity_seen_at": oldest.isoformat() if oldest else None,
        "history_complete": complete,
        "wallet_age_days": round((now - oldest).total_seconds() / 86400, 2) if (oldest and complete) else None,
        "launches_seen_by_tosh_bot_24h": launches_24h,
        "scope_note": "Launch count only covers tokens this system has detected; transfer-level dump analysis is not implemented yet.",
    }
    warnings = []
    if complete and evidence["wallet_age_days"] is not None and evidence["wallet_age_days"] < 0.01:
        warnings.append("Brand-new wallet. This alone is not treated as malicious.")
    if launches_24h >= policy.creator_max_launches_24h:
        return result("creator_wallet", "SUSPICIOUS",
                      f"Creator launched {launches_24h} tokens in 24h (limit {policy.creator_max_launches_24h}).", evidence, warnings)
    return result("creator_wallet", "PASS", f"Creator launched {launches_24h} token(s) in the last 24h.", evidence, warnings)


async def check_liquidity(rpc: SolanaRPC, token: TokenRecord, policy: SecurityPolicy) -> CheckResult:
    if not token.bonding_curve:
        return unavailable("liquidity", "bonding curve address unknown; pool analysis needs Phase 3 market data")
    lamports = await rpc.get_balance(token.bonding_curve)
    sol = lamports / 1e9
    evidence = {"bonding_curve": token.bonding_curve, "curve_lamports": lamports, "curve_sol": round(sol, 6),
                "note": "SOL held by the bonding-curve account (includes a small rent amount). Curve liquidity is protocol-controlled, not creator-lockable."}
    if sol < policy.min_curve_reserve_sol:
        return result("liquidity", "SUSPICIOUS", f"Curve holds only {sol:.4f} SOL (minimum {policy.min_curve_reserve_sol}).", evidence)
    return result("liquidity", "PASS", f"Curve holds {sol:.4f} SOL.", evidence)


# ============================================================================
# app/security/engine.py
# ============================================================================
def decide(results: list[CheckResult], now: datetime, policy: SecurityPolicy) -> tuple[str, str, list[str], datetime | None]:
    """Fail-closed decision. Security has veto power: no score or pass can offset a critical failure.
    Phase 2 can never return QUALIFIED (no opportunity engine yet)."""
    by_name = {r.name: r for r in results}
    critical = [r for r in results if r.status == "FAIL_CRITICAL"]
    if critical:
        reasons = [f"{r.name}: {r.summary}" for r in critical]
        return "REJECTED", "Verified critical security problem. " + " | ".join(reasons), reasons, None

    problems = []
    for name in REQUIRED_CHECKS:
        r = by_name.get(name)
        if r is None:
            problems.append(f"{name}: check did not run")
        elif r.mandatory and r.status == "UNAVAILABLE":
            problems.append(f"{name}: {r.summary}")
        elif r.mandatory and (now - r.retrieved_at).total_seconds() > policy.evidence_max_age_seconds:
            problems.append(f"{name}: evidence is stale")
    if problems:
        return ("INSUFFICIENT_DATA", "Mandatory evidence incomplete, so approval is withheld. " + " | ".join(problems), problems,
                now + timedelta(seconds=policy.review_seconds_insufficient))

    suspicious = [f"{r.name}: {r.summary}" for r in results if r.status == "SUSPICIOUS"]
    if suspicious:
        return "HOLD", "Unresolved suspicious indicators. " + " | ".join(suspicious), suspicious, now + timedelta(seconds=policy.review_seconds_hold)

    return ("WATCH", "Implemented security checks passed. Not QUALIFIED: sell simulation, manipulation checks and the opportunity score do not exist yet.",
            [], now + timedelta(seconds=policy.review_seconds_watch))


class SecurityAnalyzer:
    def __init__(self, rpc: SolanaRPC, repo: Repository, policy: SecurityPolicy) -> None:
        self.rpc, self.repo, self.policy = rpc, repo, policy

    async def _safe(self, name: str, coro) -> CheckResult:
        try:
            return await coro
        except RpcError as exc:
            return unavailable(name, f"data provider problem ({exc})")
        except (KeyError, TypeError, ValueError, IndexError, AttributeError) as exc:
            return unavailable(name, f"unexpected data shape ({type(exc).__name__}: {exc})")
        except Exception as exc:  # never convert an internal error into a pass OR a failure
            log.exception("check crashed", extra={"check": name})
            return unavailable(name, f"internal error ({type(exc).__name__})")

    async def _load_mint(self, mint: str) -> tuple[MintData | None, str | None]:
        try:
            account = await self.rpc.get_account_info(mint)
            if account is None:
                return None, "mint account not visible yet (token may be too new)"
            return parse_mint(account), None
        except RpcError as exc:
            return None, f"data provider problem ({exc})"
        except (KeyError, TypeError, ValueError) as exc:
            return None, f"unexpected mint data ({type(exc).__name__}: {exc})"

    async def analyze(self, token: TokenRecord) -> SecurityReport:
        md, err = await self._load_mint(token.mint)
        if md is None:
            mint_checks = [unavailable(n, err) for n in ("mint_permissions", "token_extensions", "holder_concentration")]
            holders = None
        else:
            mint_checks = [await self._safe("mint_permissions", _wrap(check_mint_permissions, md)),
                           await self._safe("token_extensions", _wrap(check_token_extensions, md, self.policy))]
            holders = self._safe("holder_concentration", check_holders(self.rpc, token, md, self.policy))
        others = [self._safe("creator_wallet", check_creator(self.rpc, self.repo, token, self.policy)),
                  self._safe("liquidity", check_liquidity(self.rpc, token, self.policy))]
        gathered = await asyncio.gather(*(([holders] if holders else []) + others))
        results = mint_checks + (list(gathered[:1]) if holders else []) + list(gathered[1 if holders else 0:])
        now = utcnow()
        status, reason, reasons, next_review = decide(results, now, self.policy)
        warnings = sorted({w for r in results for w in r.warnings})
        return SecurityReport(mint=token.mint, analysis_version=ANALYSIS_VERSION, analyzed_at=now, status=status, reason=reason,
                              reasons=reasons, warnings=warnings, checks=results, not_evaluated=NOT_EVALUATED,
                              next_review_at=next_review, policy=self.policy.model_dump())


async def _wrap(fn, *args):
    return fn(*args)


# ============================================================================
# app/analysis.py
# ============================================================================
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


# ============================================================================
# app/listener.py
# ============================================================================
LISTENER_PROVIDER = "pumpportal_ws"


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
        self.health.mark_message(LISTENER_PROVIDER)
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
                    self.health.mark_connected(LISTENER_PROVIDER)
                    log.info("connected and subscribed", extra={"url": self.url})
                    async for raw in ws:
                        self._handle(raw)
                        if stop.is_set():
                            break
                if not stop.is_set():
                    self.health.mark_disconnected(LISTENER_PROVIDER, "connection closed by server")
            except asyncio.CancelledError:
                self.health.mark_disconnected(LISTENER_PROVIDER, "shutdown")
                raise
            except Exception as exc:
                self.health.mark_failure(LISTENER_PROVIDER, f"{type(exc).__name__}: {exc}")
                log.warning("connection failed", extra={"error": f"{type(exc).__name__}: {exc}"})

            if stop.is_set():
                self.health.mark_disconnected(LISTENER_PROVIDER, "shutdown")
                break
            if started is not None and self._clock() - started >= self._stable:
                attempt = 0  # the last connection was healthy, so restart backoff
            delay = min(self._max, self._base * (2**attempt)) * (0.5 + 0.5 * self._rng())
            attempt += 1
            self.health.get(LISTENER_PROVIDER).reconnect_count += 1
            log.info("reconnecting", extra={"delay_seconds": round(delay, 2), "attempt": attempt})
            await self._sleep(delay)


# ============================================================================
# app/processor.py
# ============================================================================
class TokenProcessor:
    """Consumes launch events from the queue: record event, register token once, create analysis job."""

    def __init__(self, repo: Repository, queue: asyncio.Queue, health: HealthRegistry, metrics: Metrics,
                 *, max_attempts: int = 3, sleep=asyncio.sleep, analysis_queue: asyncio.Queue | None = None) -> None:
        self.repo, self.queue, self.health, self.metrics = repo, queue, health, metrics
        self.max_attempts, self._sleep = max_attempts, sleep
        self.analysis_queue = analysis_queue

    async def handle(self, event: LaunchEvent) -> bool:
        """Idempotent: safe to retry and safe for duplicate / delayed / out-of-order events."""
        await self.repo.record_launch_event(event)
        created = await self.repo.create_token_if_absent(TokenRecord.from_event(event))
        await self.repo.create_analysis_job(event.mint)  # always attempted so a retry can finish a half-done earlier attempt
        if created:
            self.metrics.stored += 1
            if self.analysis_queue is not None:
                try:
                    self.analysis_queue.put_nowait(event.mint)
                except asyncio.QueueFull:  # the job stays 'pending' in storage and is re-queued on restart
                    log.error("analysis queue full", extra={"mint": event.mint})
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


# ============================================================================
# app/main.py
# ============================================================================
VALID_STATUSES = ["REJECTED", "INSUFFICIENT_DATA", "HOLD", "WATCH", "QUALIFIED", "REVOKED"]


async def require_api_key(request: Request, x_api_key: str | None = Header(default=None)) -> None:
    expected = request.app.state.settings.api_key
    if not expected:
        return  # only possible in development; production refuses to start without a key
    if not x_api_key or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


async def require_dashboard_auth(request: Request) -> None:
    """HTTP Basic Auth for the built-in dashboard. Fails closed in production when no password is set."""
    cfg = request.app.state.settings
    if not cfg.dashboard_password:
        if cfg.environment == "production":
            raise HTTPException(status_code=503, detail="Dashboard is disabled: set DASHBOARD_PASSWORD")
        return
    header = request.headers.get("authorization", "")
    if header.lower().startswith("basic "):
        try:
            user, _, pw = base64.b64decode(header[6:]).decode("utf-8").partition(":")
            if hmac.compare_digest(user.encode(), cfg.dashboard_user.encode()) and hmac.compare_digest(pw.encode(), cfg.dashboard_password.encode()):
                return
        except Exception:
            pass
    raise HTTPException(status_code=401, detail="Authentication required", headers={"WWW-Authenticate": 'Basic realm="Tosh Bot"'})


def policy_from_settings(s: Settings) -> SecurityPolicy:
    return SecurityPolicy(
        evidence_max_age_seconds=s.evidence_max_age_seconds, single_holder_reject_pct=s.single_holder_reject_pct,
        top10_hold_pct=s.top10_hold_pct, creator_hold_pct=s.creator_hold_pct, max_transfer_fee_bps=s.max_transfer_fee_bps,
        creator_max_launches_24h=s.creator_max_launches_24h, min_curve_reserve_sol=s.min_curve_reserve_sol)


def _valid_mint(mint: str) -> str:
    if not BASE58_ADDRESS.match(mint):
        raise HTTPException(status_code=422, detail="Invalid mint address")
    return mint


DASHBOARD_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0b0e11" media="(prefers-color-scheme: dark)">
<meta name="theme-color" content="#f4f5f7" media="(prefers-color-scheme: light)">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-title" content="Tosh Bot">
<title>Tosh Bot</title>
<script>
(function () {
  var t = null;
  try { t = localStorage.getItem("tosh-theme"); } catch (e) {}
  if (!t) t = (window.matchMedia && matchMedia("(prefers-color-scheme: light)").matches) ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", t);
})();
</script>
<style>
:root{
  color-scheme:dark;
  --bg:#05070c;--text:#f5f7fb;--mute:rgba(235,240,250,.68);--dim:rgba(235,240,250,.45);
  --blue:#4c9bff;--cyan:#3de0f5;--gold:#f0b90b;--green:#34d399;--amber:#ffb340;--red:#ff5d73;--gray:#8b97aa;
  --g-bg:rgba(255,255,255,.075);--g-bg2:rgba(255,255,255,.12);--g-line:rgba(255,255,255,.16);--g-hi:rgba(255,255,255,.36);--g-lo:rgba(255,255,255,.05);
  --g-shadow:0 14px 44px rgba(0,0,0,.38);--line:rgba(255,255,255,.10);--card2:rgba(255,255,255,.06);--r:28px;
  --o1:rgba(30,100,255,.85);--o2:rgba(150,80,255,.75);--o3:rgba(0,200,235,.55);--o4:rgba(255,160,30,.42);--o5:rgba(255,60,130,.38);
  --font:-apple-system,BlinkMacSystemFont,"SF Pro Display","SF Pro Text","Segoe UI",Roboto,Inter,Helvetica,Arial,sans-serif;
  --mono:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace;
  --spring:cubic-bezier(.34,1.56,.64,1);
}
[data-theme="light"]{
  color-scheme:light;
  --bg:#eaf0fb;--text:#0a0f1a;--mute:rgba(10,15,26,.66);--dim:rgba(10,15,26,.42);
  --blue:#0a74ff;--green:#12a979;--amber:#f08c00;--red:#ec3b55;
  --g-bg:rgba(255,255,255,.52);--g-bg2:rgba(255,255,255,.72);--g-line:rgba(255,255,255,.9);--g-hi:rgba(255,255,255,1);--g-lo:rgba(255,255,255,.4);
  --g-shadow:0 12px 40px rgba(40,60,110,.16);--line:rgba(10,15,26,.08);--card2:rgba(10,15,26,.045);
  --o1:rgba(59,130,246,.45);--o2:rgba(167,139,250,.40);--o3:rgba(34,211,238,.38);--o4:rgba(250,200,60,.38);--o5:rgba(255,120,170,.30);
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{-webkit-text-size-adjust:100%;background:var(--bg)}
body{margin:0;background:transparent;color:var(--text);font:15px/1.45 var(--font);-webkit-font-smoothing:antialiased;font-variant-numeric:tabular-nums}
/* the "wallpaper" behind the glass: slow-drifting colour light */
body::before{content:"";position:fixed;inset:-20%;z-index:-1;pointer-events:none;will-change:transform;
  background:radial-gradient(46% 40% at 16% 12%,var(--o1),transparent 72%),radial-gradient(42% 36% at 88% 20%,var(--o2),transparent 72%),
             radial-gradient(46% 40% at 74% 88%,var(--o3),transparent 72%),radial-gradient(38% 34% at 6% 84%,var(--o4),transparent 72%),
             radial-gradient(34% 30% at 50% 52%,var(--o5),transparent 72%);
  animation:drift 28s ease-in-out infinite alternate}
@keyframes drift{from{transform:translate3d(-2%,-1%,0) scale(1)}to{transform:translate3d(3%,2%,0) scale(1.12)}}
@media(prefers-reduced-motion:reduce){body::before{animation:none}*{transition:none!important}}
a{color:inherit;text-decoration:none}
button{font:inherit;color:inherit;cursor:pointer}
svg{display:block}
.mono{font-family:var(--mono);font-size:12px}
.mute{color:var(--mute)}.dim{color:var(--dim)}

/* ===== Liquid Glass material ===== */
.card,.chip,.tile,.fact,.verdict,.hero,.search,.btn,.ibtn,.feed,#tabbar,#rail,.fchip,.toast{
  background:var(--g-bg);
  -webkit-backdrop-filter:blur(28px) saturate(190%);backdrop-filter:blur(28px) saturate(190%);
  border:1px solid var(--g-line);
  box-shadow:var(--g-shadow),inset 0 1.5px 0 var(--g-hi),inset 0 -1px 0 var(--g-lo),inset 0 0 18px rgba(255,255,255,.04);
}
.card,.chip,.tile,.fact,.verdict,.hero,.search,.btn,.ibtn,.feed,.fchip{position:relative}
/* specular sheen: light catching the curved edge */
.card::before,.chip::before,.tile::before,.fact::before,.verdict::before,.hero::before,.search::before,.btn::before,.ibtn::before,.feed::before,.fchip::before,#tabbar::before,#rail::before{
  content:"";position:absolute;inset:0;border-radius:inherit;pointer-events:none;
  background:linear-gradient(135deg,rgba(255,255,255,.26) 0%,rgba(255,255,255,0) 32%,rgba(255,255,255,0) 68%,rgba(255,255,255,.10) 100%);
  opacity:.9;mix-blend-mode:overlay}
[data-theme="light"] .card::before,[data-theme="light"] .chip::before,[data-theme="light"] .hero::before,[data-theme="light"] #tabbar::before{opacity:.55;mix-blend-mode:normal}
@supports not ((backdrop-filter:blur(1px)) or (-webkit-backdrop-filter:blur(1px))){
  .card,.chip,.tile,.fact,.verdict,.hero,.search,.btn,.ibtn,.feed,#tabbar,#rail,.fchip{background:rgba(30,36,48,.92)}
  [data-theme="light"] .card,[data-theme="light"] .chip,[data-theme="light"] .tile,[data-theme="light"] .fact,[data-theme="light"] .verdict,[data-theme="light"] .hero,[data-theme="light"] .search,[data-theme="light"] .btn,[data-theme="light"] .ibtn,[data-theme="light"] .feed,[data-theme="light"] #tabbar,[data-theme="light"] #rail,[data-theme="light"] .fchip{background:rgba(255,255,255,.94)}
}

/* shell */
#shell{display:flex;min-height:100vh}
#rail{display:none;width:244px;flex:none;margin:14px;padding:18px 12px;border-radius:32px;position:sticky;top:14px;height:calc(100vh - 28px);flex-direction:column;gap:6px}
#main{flex:1;min-width:0}
.topbar{position:sticky;top:0;z-index:20;padding-top:env(safe-area-inset-top)}
.topbar::before{content:"";position:absolute;inset:0;z-index:-1;pointer-events:none;
  -webkit-backdrop-filter:blur(18px);backdrop-filter:blur(18px);
  -webkit-mask-image:linear-gradient(#000 55%,transparent);mask-image:linear-gradient(#000 55%,transparent);
  background:linear-gradient(var(--bg),transparent);opacity:.82}
.topin{max-width:1080px;margin:0 auto;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 16px 14px}
.brand{display:flex;align-items:center;gap:10px;font-weight:800;font-size:18px;letter-spacing:-.025em}
.brand small{display:block;font-size:11px;font-weight:500;color:var(--dim);letter-spacing:0;margin-top:-2px}
.tools{display:flex;align-items:center;gap:8px}
.ibtn{display:grid;place-items:center;width:42px;height:42px;border-radius:50%;color:var(--text);transition:transform .35s var(--spring)}
.ibtn:active{transform:scale(.9)}
.feed{display:inline-flex;align-items:center;gap:8px;height:42px;padding:0 16px;border-radius:999px;font-size:13px;font-weight:600}
.dot{width:8px;height:8px;border-radius:50%;background:var(--gray);display:inline-block;flex:none}
.dot.ok{background:var(--green);animation:pulse 2s infinite}
.dot.warn{background:var(--amber)}.dot.bad{background:var(--red)}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(52,211,153,.6)}70%{box-shadow:0 0 0 8px rgba(52,211,153,0)}100%{box-shadow:0 0 0 0 rgba(52,211,153,0)}}
.page{max-width:1080px;margin:0 auto;padding:6px 16px calc(130px + env(safe-area-inset-bottom))}
#banner{display:none;max-width:1048px;margin:4px 16px 0;padding:11px 16px;border-radius:20px;background:rgba(255,93,115,.16);border:1px solid rgba(255,93,115,.45);color:var(--red);font-size:13px;-webkit-backdrop-filter:blur(20px);backdrop-filter:blur(20px)}
@media(min-width:1112px){#banner{margin-left:auto;margin-right:auto}}

/* nav: floating glass tab bar (phones) and glass sidebar (desktop) */
.nav a{display:flex;align-items:center;gap:12px;padding:12px 14px;border-radius:20px;color:var(--mute);font-weight:600;transition:background .25s,color .25s}
.nav a:hover{background:var(--card2);color:var(--text)}
.nav a.on{background:var(--g-bg2);color:var(--text);box-shadow:inset 0 1px 0 var(--g-hi),inset 0 -1px 0 var(--g-lo)}
.nav a.on svg{color:var(--blue)}
#tabbar{position:fixed;left:14px;right:14px;bottom:calc(10px + env(safe-area-inset-bottom));z-index:30;display:flex;gap:2px;padding:6px;border-radius:34px}
#tabbar a{position:relative;flex:1;display:flex;flex-direction:column;align-items:center;gap:3px;padding:8px 0 7px;font-size:10.5px;font-weight:650;color:var(--mute);border-radius:28px;transition:background .3s var(--spring),color .2s,transform .35s var(--spring)}
#tabbar a:active{transform:scale(.93)}
#tabbar a.on{color:var(--blue);background:var(--g-bg2);box-shadow:inset 0 1.5px 0 var(--g-hi),inset 0 -1px 0 var(--g-lo),0 4px 14px rgba(0,0,0,.14)}
@media(min-width:900px){#rail{display:flex}#tabbar{display:none}.page{padding-bottom:60px}.mob-only{display:none!important}}
@media(max-width:899px){.desk-only{display:none!important}}

/* type + cards */
h1.large{font-size:34px;line-height:1.08;font-weight:800;letter-spacing:-.04em;margin:8px 2px 6px}
.sub{color:var(--mute);margin:0 2px 18px;max-width:640px}
.card{border-radius:var(--r)}
.pad{padding:18px}
.sec{display:flex;align-items:center;justify-content:space-between;margin:28px 6px 10px}
.sec h2{margin:0;font-size:19px;font-weight:750;letter-spacing:-.025em}
.sec a{font-size:14px;color:var(--blue);font-weight:650}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
@media(max-width:899px){.grid2{grid-template-columns:1fr}}

/* hero */
.hero{overflow:hidden;padding:24px;border-radius:36px;
  background:radial-gradient(120% 150% at 0% 0%,rgba(76,155,255,.34),transparent 58%),radial-gradient(90% 120% at 100% 0%,rgba(240,185,11,.16),transparent 52%),var(--g-bg)}
[data-theme="light"] .hero{background:radial-gradient(120% 150% at 0% 0%,rgba(10,116,255,.18),transparent 58%),radial-gradient(90% 120% at 100% 0%,rgba(250,200,60,.22),transparent 52%),var(--g-bg)}
.hero-row{position:relative;display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}
.live{display:inline-flex;align-items:center;gap:8px;font-size:12px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:var(--mute)}
.bignum{font-size:60px;line-height:1;font-weight:800;letter-spacing:-.05em;margin:12px 0 2px}
.donutbox{display:flex;align-items:center;gap:18px}
.donut{width:132px;height:132px;flex:none;filter:drop-shadow(0 6px 16px rgba(0,0,0,.25))}
.legend{display:grid;gap:6px;font-size:13px}
.legend div{display:flex;align-items:center;gap:8px;color:var(--mute)}
.legend i{width:9px;height:9px;border-radius:50%;display:inline-block}
.legend b{color:var(--text);margin-left:auto;padding-left:14px;font-weight:650}
.chip{display:block;padding:14px;border-radius:28px;transition:transform .35s var(--spring)}
.chip:active{transform:scale(.95)}
.chip .ci{position:relative;display:grid;place-items:center;width:36px;height:36px;border-radius:50%;color:var(--c);background:rgba(128,128,128,.16);background:color-mix(in srgb,var(--c) 22%,transparent);box-shadow:inset 0 1px 0 var(--g-hi)}
.chip .cl{position:relative;margin-top:10px;font-size:12px;color:var(--mute);font-weight:600}
.chip .cn{position:relative;font-size:28px;font-weight:800;letter-spacing:-.04em;line-height:1.15}

/* token rows */
.list{overflow:hidden}
.tok{position:relative;display:flex;align-items:center;gap:12px;padding:13px 16px;border-top:1px solid var(--line);transition:background .2s,transform .35s var(--spring)}
.tok:first-child{border-top:0}
.tok:hover{background:var(--card2)}
.tok:active{background:var(--card2);transform:scale(.985)}
.av{display:grid;place-items:center;flex:none;border-radius:50%;color:#fff;font-weight:800;letter-spacing:-.02em;text-shadow:0 1px 2px rgba(0,0,0,.3);box-shadow:inset 0 1.5px 0 rgba(255,255,255,.45),inset 0 -2px 4px rgba(0,0,0,.2),0 4px 12px rgba(0,0,0,.25)}
.tmain{flex:1;min-width:0}
.tt{display:flex;gap:8px;align-items:baseline;min-width:0}
.tt b{font-size:16px;font-weight:750;white-space:nowrap}
.tt span{color:var(--mute);font-size:13.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ts{font-size:12px;color:var(--dim);font-family:var(--mono);margin-top:1px}
.why{font-size:12.5px;color:var(--mute);margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tr{display:flex;flex-direction:column;align-items:flex-end;gap:6px;color:var(--dim);flex:none}
.pill{--c:var(--gray);display:inline-flex;align-items:center;gap:6px;white-space:nowrap;padding:4px 11px 4px 9px;border-radius:999px;font-size:12px;font-weight:700;color:var(--c);
  background:rgba(128,128,128,.18);background:color-mix(in srgb,var(--c) 18%,transparent);box-shadow:inset 0 1px 0 rgba(255,255,255,.22),inset 0 0 0 .5px color-mix(in srgb,var(--c) 40%,transparent)}
.pill i{width:6px;height:6px;border-radius:50%;background:var(--c);box-shadow:0 0 8px var(--c)}
.p-REJECTED,.p-REVOKED,.p-FAIL_CRITICAL{--c:var(--red)}
.p-HOLD,.p-SUSPICIOUS{--c:var(--amber)}
.p-WATCH{--c:var(--blue)}
.p-QUALIFIED,.p-PASS{--c:var(--green)}
.p-INSUFFICIENT_DATA,.p-UNAVAILABLE{--c:var(--gray)}

/* search + filters: glass capsules */
.searchbar{position:sticky;top:calc(64px + env(safe-area-inset-top));z-index:10;padding:6px 0 8px}
.search{display:flex;align-items:center;gap:10px;height:50px;padding:0 18px;border-radius:999px;color:var(--dim)}
.search:focus-within{border-color:var(--blue);box-shadow:var(--g-shadow),inset 0 1.5px 0 var(--g-hi),0 0 0 4px rgba(76,155,255,.22)}
.search input{position:relative;flex:1;min-width:0;background:none;border:0;outline:0;color:var(--text);font:inherit;font-size:16px}
.search input::placeholder{color:var(--dim)}
.filters{display:flex;gap:8px;overflow-x:auto;padding:10px 2px 4px;scrollbar-width:none}
.filters::-webkit-scrollbar{display:none}
.fchip{flex:none;padding:8px 16px;border-radius:999px;color:var(--mute);font-size:13.5px;font-weight:650;transition:transform .35s var(--spring)}
.fchip:active{transform:scale(.94)}
.fchip.on{background:var(--text);color:var(--bg);border-color:transparent}
.fchip em{font-style:normal;opacity:.6;margin-left:6px}

/* states */
.empty{padding:46px 24px;text-align:center;color:var(--mute)}
.empty .eic{position:relative;display:inline-grid;place-items:center;width:60px;height:60px;border-radius:50%;background:var(--card2);color:var(--dim);margin-bottom:12px;box-shadow:inset 0 1px 0 var(--g-hi)}
.empty b{display:block;color:var(--text);font-size:17px;margin-bottom:4px}
.skrow{height:70px;border-top:1px solid var(--line);background:linear-gradient(90deg,transparent,rgba(255,255,255,.10),transparent);background-size:200% 100%;animation:sh 1.4s infinite}
.skrow:first-child{border-top:0}
@keyframes sh{0%{background-position:200% 0}100%{background-position:-200% 0}}
.toast{position:fixed;left:50%;bottom:calc(104px + env(safe-area-inset-bottom));transform:translateX(-50%) translateY(20px) scale(.9);opacity:0;padding:10px 18px;border-radius:999px;font-size:13px;font-weight:650;transition:.35s var(--spring);z-index:60;pointer-events:none}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0) scale(1)}

/* token detail */
.back{display:inline-flex;align-items:center;gap:4px;color:var(--blue);font-weight:650;margin:2px 2px 12px}
.thead{display:flex;align-items:center;gap:14px;margin:0 2px 16px}
.thead h1{margin:0;font-size:28px;font-weight:800;letter-spacing:-.035em;line-height:1.1}
.copy{display:inline-flex;align-items:center;gap:6px;background:none;border:0;padding:0;color:var(--dim);font-family:var(--mono);font-size:12px;word-break:break-all;text-align:left}
.verdict{padding:22px;border-radius:34px;
  background:radial-gradient(130% 120% at 0% 0%,color-mix(in srgb,var(--c) 30%,transparent),transparent 62%),var(--g-bg)}
.verdict .big{position:relative;display:flex;align-items:center;gap:12px}
.verdict h2{position:relative;margin:0;font-size:26px;font-weight:800;letter-spacing:-.035em;color:var(--c)}
.verdict p{position:relative;margin:10px 0 0}
.facts{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}
.fact{padding:13px 16px;border-radius:24px}
.fact .k{position:relative;font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:var(--dim);font-weight:700}
.fact .v{position:relative;margin-top:3px;font-weight:650;font-size:14px;overflow:hidden;text-overflow:ellipsis}
.btns{display:flex;gap:10px;margin-top:14px;flex-wrap:wrap}
.btn{display:inline-flex;align-items:center;gap:8px;padding:11px 18px;border-radius:999px;font-weight:650;font-size:14px;transition:transform .35s var(--spring)}
.btn:active{transform:scale(.94)}
.acc{border-top:1px solid var(--line)}
.acc:first-child{border-top:0}
.acc summary{list-style:none;display:flex;align-items:center;gap:12px;padding:15px 16px;cursor:pointer}
.acc summary::-webkit-details-marker{display:none}
.acc .chev{margin-left:auto;color:var(--dim);transition:transform .3s var(--spring)}
.acc[open] .chev{transform:rotate(90deg)}
.cic{display:grid;place-items:center;width:36px;height:36px;border-radius:50%;flex:none;color:var(--c);background:rgba(128,128,128,.16);background:color-mix(in srgb,var(--c) 20%,transparent);box-shadow:inset 0 1px 0 rgba(255,255,255,.28)}
.acc .ct{font-weight:700;font-size:15px}
.acc .cs{font-size:13px;color:var(--mute);margin-top:1px}
.acc .cb{padding:0 16px 16px 64px;font-size:13.5px;color:var(--mute)}
.acc .cb p{margin:0 0 8px}
.warns{position:relative;margin:6px 0 0;padding-left:18px;color:var(--amber);font-size:12.5px}
.stack{display:flex;height:14px;border-radius:999px;overflow:hidden;background:var(--card2);margin:10px 0 8px;box-shadow:inset 0 1px 3px rgba(0,0,0,.25)}
.stack span{display:block;height:100%;min-width:2px}
.hrow{display:flex;justify-content:space-between;font-size:12.5px;padding:3px 0}
details.raw summary{cursor:pointer;color:var(--dim);font-size:12px;margin-top:8px;list-style:none}
pre{background:rgba(0,0,0,.28);border:1px solid var(--line);border-radius:16px;padding:12px;overflow:auto;max-height:240px;font-size:11.5px;color:var(--mute);font-family:var(--mono)}
[data-theme="light"] pre{background:rgba(255,255,255,.6)}
.kv{position:relative;display:grid;grid-template-columns:1fr auto;gap:8px 14px;font-size:14px}
.kv dt{color:var(--mute)}.kv dd{margin:0;text-align:right;font-weight:650}
.tile{padding:16px;border-radius:28px}
.tile .l{position:relative;font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:var(--dim);font-weight:700}
.tile .v{position:relative;font-size:26px;font-weight:800;letter-spacing:-.035em;margin-top:4px}
.tile .h{position:relative;font-size:12px;color:var(--dim)}
.prow{position:relative;display:flex;align-items:center;gap:10px;padding:12px 0;border-top:1px solid var(--line)}
.prow:first-child{border-top:0}
.card.pad>*,.hero>*{position:relative}
footer{max-width:1080px;margin:0 auto;padding:0 20px 24px;color:var(--dim);font-size:12px;line-height:1.5}
@media(min-width:900px){footer{padding-bottom:30px}}
</style>
</head>
<body>
<div id="shell">
  <aside id="rail">
    <a href="#/" class="brand" style="padding:6px 10px 18px"><span id="logo1"></span><span>Tosh Bot<small>Solana token intelligence</small></span></a>
    <nav class="nav" id="railnav"></nav>
    <div style="flex:1"></div>
    <nav class="nav" id="railsys"></nav>
  </aside>
  <div id="main">
    <header class="topbar"><div class="topin">
      <a href="#/" class="brand mob-only"><span id="logo2"></span><span>Tosh Bot</span></a>
      <span class="desk-only dim" style="font-size:13px">Live security analysis for new Solana launches</span>
      <div class="tools">
        <span class="feed"><span id="pdot" class="dot"></span><span id="ptxt">Connecting…</span></span>
        <button class="ibtn" id="themebtn" aria-label="Switch theme"></button>
        <a class="ibtn mob-only" href="#/system" aria-label="System health" id="sysbtn"></a>
      </div>
    </div></header>
    <div id="banner"></div>
    <main class="page" id="app"></main>
    <footer>Detection and analysis only. Nothing here is financial advice or a guarantee that a token is safe. Missing data is never counted as a pass.</footer>
  </div>
</div>
<nav id="tabbar"></nav>
<div id="toast" class="toast"></div>

<script>
"use strict";
const API = "/dashboard/api";
const NS = "http://www.w3.org/2000/svg";
const LABEL = {REJECTED:"Rejected",INSUFFICIENT_DATA:"Insufficient data",HOLD:"Hold",WATCH:"Watch",QUALIFIED:"Qualified",REVOKED:"Revoked",PASS:"Passed",FAIL_CRITICAL:"Critical",SUSPICIOUS:"Suspicious",UNAVAILABLE:"Unavailable"};
const COLOR = {REJECTED:"#f6465d",INSUFFICIENT_DATA:"#6b7686",HOLD:"#f5a524",WATCH:"#3b82f6",QUALIFIED:"#2ebd85",REVOKED:"#f6465d"};
const ORDER = ["INSUFFICIENT_DATA","HOLD","WATCH","QUALIFIED","REJECTED","REVOKED"];
const GROUPS = {all:null, review:["INSUFFICIENT_DATA","HOLD"], passed:["WATCH","QUALIFIED"], rejected:["REJECTED","REVOKED"]};
const STATUS_HELP = {
  REJECTED:"A verified, serious security problem was found.",
  INSUFFICIENT_DATA:"Some required checks could not be completed yet. Nothing is approved without them.",
  HOLD:"Something looks suspicious but is not proven. It needs a closer look.",
  WATCH:"Every check built so far passed. This is not a buy signal.",
  QUALIFIED:"All required checks and the opportunity bar were met.",
  REVOKED:"This token no longer meets the requirements."
};
const CHECK_TITLE = {mint_permissions:"Mint & freeze permissions",token_extensions:"Token rules & extensions",holder_concentration:"Holder concentration",creator_wallet:"Creator wallet",liquidity:"Liquidity"};
const CHECK_HELP = {
  mint_permissions:"Can anyone create more tokens, or freeze holders' balances?",
  token_extensions:"Does the token have special rules like hidden fees or blocked transfers?",
  holder_concentration:"Do just a few wallets control most of the supply?",
  creator_wallet:"What has the wallet that launched this token been doing?",
  liquidity:"Is there real money in the pool to trade against?"
};
const ICONS = {
  home:'<path d="M3 11.5 12 4l9 7.5"/><path d="M5 10v10h5v-6h4v6h5V10"/>',
  radar:'<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="4.5"/><path d="M12 12 18 6"/>',
  shield:'<path d="M12 3 4 6v6c0 4.5 3.2 8 8 9 4.8-1 8-4.5 8-9V6z"/><path d="m8.5 12 2.5 2.5L15.5 10"/>',
  eye:'<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/>',
  ban:'<circle cx="12" cy="12" r="9"/><path d="m5.6 5.6 12.8 12.8"/>',
  pulse:'<path d="M3 12h4l3-8 4 16 3-8h4"/>',
  sun:'<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>',
  moon:'<path d="M20 14.5A8 8 0 0 1 9.5 4 8 8 0 1 0 20 14.5z"/>',
  copy:'<rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V6a2 2 0 0 1 2-2h8"/>',
  ext:'<path d="M14 4h6v6"/><path d="M20 4 10 14"/><path d="M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5"/>',
  chev:'<path d="m9 6 6 6-6 6"/>',
  back:'<path d="m15 6-6 6 6 6"/>',
  check:'<path d="m5 12.5 4.5 4.5L19 7.5"/>',
  alert:'<path d="M12 6.5v7"/><path d="M12 17.5h.01"/>',
  x:'<path d="m6.5 6.5 11 11M17.5 6.5l-11 11"/>',
  help:'<path d="M9.5 9a2.5 2.5 0 1 1 3.5 2.3c-.7.4-1 1-1 1.7"/><path d="M12 17.5h.01"/>',
  search:'<circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/>'
};

/* ---------- helpers: all text goes through textContent, so token names can never inject HTML ---------- */
function h(tag, attrs, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === false || v == null) continue;
    if (k === "class") el.className = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v);
  }
  for (const kid of kids.flat()) {
    if (kid == null || kid === false) continue;
    el.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
  }
  return el;
}
function sv(tag, attrs, ...kids) {
  const el = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs || {})) if (v != null) el.setAttribute(k, v);
  for (const kid of kids.flat()) if (kid) el.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
  return el;
}
function icon(name, size) {
  const s = sv("svg", {viewBox: "0 0 24 24", width: size || 20, height: size || 20, fill: "none", stroke: "currentColor", "stroke-width": "1.9", "stroke-linecap": "round", "stroke-linejoin": "round"});
  s.innerHTML = ICONS[name] || "";   // static strings defined above, never user data
  return s;
}
function logo(size) {
  const s = sv("svg", {viewBox: "0 0 40 40", width: size, height: size});
  s.innerHTML = '<defs><linearGradient id="lg' + size + '" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#3b82f6"/><stop offset="1" stop-color="#22d3ee"/></linearGradient></defs>' +
    '<rect width="40" height="40" rx="12" fill="url(#lg' + size + ')"/>' +
    '<path d="M20 8 10 12v7c0 6 4.2 10.3 10 12 5.8-1.7 10-6 10-12v-7z" fill="rgba(255,255,255,.18)" stroke="#fff" stroke-width="1.8" stroke-linejoin="round"/>' +
    '<path d="M13.5 20h3l2-4.5 3 9 2-4.5h3" fill="none" stroke="#fff" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"/>';
  return s;
}
async function api(path) {
  const r = await fetch(API + path, {cache: "no-store"});
  if (!r.ok) {
    let d = "HTTP " + r.status;
    try { const j = await r.json(); if (j.detail) d = j.detail; } catch (e) {}
    const err = new Error(d); err.status = r.status; throw err;
  }
  return r.json();
}
const short = (a, n = 4) => !a ? "—" : a.length <= 2 * n + 1 ? a : a.slice(0, n) + "…" + a.slice(-n);
const fmt = (iso) => iso ? new Date(iso).toLocaleString([], {dateStyle: "medium", timeStyle: "short"}) : "—";
function ago(iso) {
  if (!iso) return "—";
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 5) return "just now"; if (s < 60) return s + "s ago"; if (s < 3600) return Math.floor(s / 60) + "m ago";
  if (s < 86400) return Math.floor(s / 3600) + "h ago"; return Math.floor(s / 86400) + "d ago";
}
function setNum(el, n) {
  const from = el._v || 0; el._v = n;
  if (from === n) { el.textContent = Number(n).toLocaleString(); return; }
  const t0 = performance.now();
  (function step(t) {
    const p = Math.min(1, (t - t0) / 550);
    el.textContent = Math.round(from + (n - from) * (1 - Math.pow(1 - p, 3))).toLocaleString();
    if (p < 1) requestAnimationFrame(step);
  })(t0);
}
let toastTimer;
function toast(msg) {
  const t = document.getElementById("toast"); t.textContent = msg; t.classList.add("show");
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.classList.remove("show"), 1600);
}
async function copyText(txt) {
  try { await navigator.clipboard.writeText(txt); toast("Copied"); }
  catch (e) { window.prompt("Copy this:", txt); }
}
const pill = (s) => h("span", {class: "pill p-" + s}, h("i"), LABEL[s] || s);
function avatar(t, size) {
  const sym = ((t.symbol || t.name || "?") + "").replace(/[^A-Za-z0-9]/g, "").slice(0, 2).toUpperCase() || "?";
  let n = 0; for (const ch of t.mint) n = (n * 31 + ch.charCodeAt(0)) >>> 0;
  const hue = n % 360;
  return h("span", {class: "av", style: "width:" + size + "px;height:" + size + "px;font-size:" + Math.round(size * 0.36) + "px;background:linear-gradient(135deg,hsl(" + hue + " 68% 56%),hsl(" + ((hue + 45) % 360) + " 68% 38%))"}, sym);
}
function tokRow(t, withReason) {
  return h("a", {class: "tok", href: "#/token/" + t.mint}, avatar(t, 44),
    h("div", {class: "tmain"},
      h("div", {class: "tt"}, h("b", {}, t.symbol || "—"), h("span", {}, t.name || "Unnamed")),
      h("div", {class: "ts"}, short(t.mint, 5) + " · " + ago(t.detected_at)),
      withReason && t.status_reason ? h("div", {class: "why"}, t.status_reason) : null),
    h("div", {class: "tr"}, pill(t.status), icon("chev", 16)));
}
const skeleton = (n) => h("div", {class: "card list"}, Array.from({length: n || 6}, () => h("div", {class: "skrow"})));
const empty = (iconName, title, text) => h("div", {class: "card empty"}, h("div", {class: "eic"}, icon(iconName, 26)), h("b", {}, title), text || "");
const sec = (title, linkText, href) => h("div", {class: "sec"}, h("h2", {}, title), linkText ? h("a", {href}, linkText) : null);
const dotState = (st) => st === "connected" ? "ok" : (st === "stale" || st === "unknown") ? "warn" : "bad";

/* ---------- charts ---------- */
function donut(counts, total) {
  const r = 52, C = 2 * Math.PI * r, svg = sv("svg", {viewBox: "0 0 140 140", class: "donut"});
  svg.append(sv("circle", {cx: 70, cy: 70, r, fill: "none", stroke: "rgba(128,128,128,.18)", "stroke-width": 14}));
  let off = 0;
  for (const k of ORDER) {
    const v = counts[k]; if (!v || !total) continue;
    const part = v / total * C, len = Math.max(1.5, part - (part > 6 ? 3 : 0));
    svg.append(sv("circle", {cx: 70, cy: 70, r, fill: "none", stroke: COLOR[k], "stroke-width": 14, "stroke-linecap": "round", "stroke-dasharray": len + " " + (C - len), "stroke-dashoffset": -off, transform: "rotate(-90 70 70)"}));
    off += part;
  }
  const a = sv("text", {x: 70, y: 70, "text-anchor": "middle", "font-size": 26, "font-weight": 800, fill: "currentColor"}, Number(total).toLocaleString());
  const b = sv("text", {x: 70, y: 90, "text-anchor": "middle", "font-size": 10.5, fill: "#8793a5"}, "tokens");
  svg.append(a, b);
  return svg;
}
function areaChart(tokens) {
  const minute = 60000, now = Date.now();
  let start = now - 30 * minute;
  if (tokens.length >= 200) start = Math.max(start, Math.min(...tokens.map((t) => +new Date(t.detected_at))));
  const first = Math.floor(start / minute) * minute, n = Math.max(2, Math.floor((now - first) / minute) + 1);
  const counts = new Array(n).fill(0);
  for (const t of tokens) { const i = Math.floor((+new Date(t.detected_at) - first) / minute); if (i >= 0 && i < n) counts[i]++; }
  const max = Math.max(1, ...counts), W = 600, H = 150, pad = 8;
  const px = (i) => i * (W / (n - 1)), py = (c) => H - pad - (c / max) * (H - pad * 2);
  let d = "M" + px(0) + " " + py(counts[0]);
  for (let i = 1; i < n; i++) { const cx = (px(i - 1) + px(i)) / 2; d += " C" + cx + " " + py(counts[i - 1]) + " " + cx + " " + py(counts[i]) + " " + px(i) + " " + py(counts[i]); }
  const svg = sv("svg", {viewBox: "0 0 " + W + " " + (H + 22), style: "width:100%;height:auto"});
  svg.innerHTML = '<defs><linearGradient id="ag" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#3b82f6" stop-opacity=".45"/><stop offset="1" stop-color="#3b82f6" stop-opacity="0"/></linearGradient></defs>';
  for (let g = 1; g <= 3; g++) svg.append(sv("line", {x1: 0, x2: W, y1: H * g / 4, y2: H * g / 4, stroke: "rgba(128,128,128,.14)", "stroke-width": 1}));
  svg.append(sv("path", {d: d + " L" + W + " " + H + " L0 " + H + " Z", fill: "url(#ag)"}), sv("path", {d, fill: "none", stroke: "#3b82f6", "stroke-width": 2.6, "stroke-linecap": "round"}));
  const lab = (x, anchor, txt) => svg.append(sv("text", {x, y: H + 16, fill: "#8793a5", "font-size": 11, "text-anchor": anchor}, txt));
  lab(0, "start", new Date(first).toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"}));
  lab(W, "end", "peak " + max + " / min");
  return svg;
}

/* ---------- views: each returns {root, refresh, every} ---------- */
function homeView() {
  const num = h("div", {class: "bignum"}, "0"), rate = h("div", {class: "mute"}), donutBox = h("div"), legend = h("div", {class: "legend"});
  const mkChip = (label, route, color, ic) => { const n = h("div", {class: "cn"}, "0"); const a = h("a", {class: "chip", href: "#/" + route, style: "--c:" + color}, h("span", {class: "ci"}, icon(ic, 18)), h("div", {class: "cl"}, label), n); a._n = n; return a; };
  const cPassed = mkChip("Passed checks", "passed", "#3b82f6", "shield"), cReview = mkChip("In review", "review", "#f5a524", "eye"), cRej = mkChip("Rejected", "rejected", "#f6465d", "ban");
  const chart = h("div"), latest = h("div"), sys = h("div");
  latest.append(skeleton(5));
  const root = h("div", {},
    h("section", {class: "hero"},
      h("div", {class: "hero-row"},
        h("div", {}, h("span", {class: "live"}, h("span", {class: "dot ok"}), "Live scan · Solana"), num, h("div", {style: "font-weight:600"}, "tokens detected"), rate),
        h("div", {class: "donutbox"}, donutBox, legend))),
    h("div", {class: "grid3", style: "margin-top:14px"}, cPassed, cReview, cRej),
    h("div", {class: "grid2", style: "margin-top:2px"},
      h("div", {}, sec("Launches per minute"), h("div", {class: "card pad"}, chart)),
      h("div", {}, sec("System", "Details", "#/system"), h("div", {class: "card pad"}, sys))),
    sec("Latest launches", "See all", "#/scanner"), latest);
  async function refresh() {
    const [s, recent, hl] = await Promise.all([api("/stats"), api("/tokens/recent?limit=200"), api("/system/health")]);
    const c = s.counts;
    setNum(num, s.total);
    setNum(cPassed._n, c.WATCH + c.QUALIFIED); setNum(cReview._n, c.INSUFFICIENT_DATA + c.HOLD); setNum(cRej._n, c.REJECTED + c.REVOKED);
    rate.textContent = recent.length ? "Last launch " + ago(recent[0].detected_at) : "Waiting for the first launch…";
    donutBox.replaceChildren(donut(c, s.total));
    legend.replaceChildren(...ORDER.filter((k) => c[k] > 0).map((k) => h("div", {}, h("i", {style: "background:" + COLOR[k]}), LABEL[k], h("b", {}, c[k].toLocaleString()))));
    chart.replaceChildren(recent.length ? areaChart(recent) : h("div", {class: "dim", style: "padding:40px 0;text-align:center"}, "No launches yet"));
    latest.replaceChildren(recent.length ? h("div", {class: "card list"}, recent.slice(0, 8).map((t) => tokRow(t, false))) : empty("radar", "Nothing detected yet", "New launches appear here within seconds once the feed is connected."));
    const rows = Object.values(hl.providers).map((p) => h("div", {class: "prow"}, h("span", {class: "dot " + dotState(p.effective_status)}), h("div", {style: "flex:1;font-weight:600"}, p.name), h("span", {class: "dim", style: "font-size:12.5px"}, p.effective_status + " · " + ago(p.last_success_at))));
    rows.push(h("div", {class: "prow"}, h("span", {class: "dot " + (hl.database_reachable ? "ok" : "bad")}), h("div", {style: "flex:1;font-weight:600"}, "database"), h("span", {class: "dim", style: "font-size:12.5px"}, hl.database_reachable ? "reachable" : "unreachable")));
    sys.replaceChildren(...rows);
  }
  return {root, refresh, every: 5000};
}

function listView(o) {
  let all = [], q = "", group = o.group || "all";
  const list = h("div"), chips = h("div", {class: "filters"});
  list.append(skeleton(7));
  const input = h("input", {type: "search", placeholder: "Search name, symbol, mint or creator", autocomplete: "off", autocapitalize: "off", spellcheck: "false", oninput: (e) => { q = e.target.value.trim().toLowerCase(); draw(); }});
  const bar = o.search ? h("div", {class: "searchbar"}, h("label", {class: "search"}, icon("search", 18), input), chips) : null;
  const count = (g) => GROUPS[g] ? all.filter((t) => GROUPS[g].includes(t.status)).length : all.length;
  function drawChips() {
    if (!o.search) return;
    chips.replaceChildren(...[["all", "All"], ["review", "In review"], ["passed", "Passed"], ["rejected", "Rejected"]].map(([g, lbl]) =>
      h("button", {class: "fchip" + (group === g ? " on" : ""), onclick: () => { group = g; draw(); }}, lbl, h("em", {}, count(g)))));
  }
  function draw() {
    drawChips();
    const rows = all.filter((t) => (!GROUPS[group] || GROUPS[group].includes(t.status)) && (!q || [t.name, t.symbol, t.mint, t.creator].some((v) => v && v.toLowerCase().includes(q))));
    list.replaceChildren(rows.length ? h("div", {class: "card list"}, rows.map((t) => tokRow(t, true))) : (all.length ? empty("search", "No matches", "Try a different search or filter.") : empty(o.icon, o.emptyTitle, o.emptyText)));
  }
  const root = h("div", {}, h("h1", {class: "large"}, o.title), h("p", {class: "sub"}, o.subtitle), bar, list);
  async function refresh() { all = await api(o.url); draw(); }
  return {root, refresh, every: 5000};
}

function systemView() {
  const tiles = h("div", {class: "grid3"}), provs = h("div"), counters = h("div");
  const root = h("div", {}, h("h1", {class: "large"}, "System health"), h("p", {class: "sub"}, "Connections, queues and counters since the backend last started."), tiles,
    sec("Providers"), h("div", {class: "card pad"}, provs), sec("Counters"), h("div", {class: "card pad"}, counters));
  const NAMES = {received:"Messages received",parsed:"Launches parsed",ignored:"Ignored (non-launch)",parse_errors:"Parse errors",dropped_queue_full:"Dropped (queue full)",stored:"New tokens stored",duplicates:"Duplicates skipped",failed_jobs:"Failed storage jobs",analyses:"Analyses run",failed_analyses:"Failed analyses"};
  const BAD = new Set(["parse_errors", "dropped_queue_full", "failed_jobs", "failed_analyses"]);
  const tile = (l, v, hint, color) => h("div", {class: "tile"}, h("div", {class: "l"}, l), h("div", {class: "v", style: color ? "color:" + color : null}, v), hint ? h("div", {class: "h"}, hint) : null);
  async function refresh() {
    const d = await api("/system/health");
    tiles.replaceChildren(tile("Database", d.database_reachable ? "Online" : "Down", null, d.database_reachable ? "#2ebd85" : "#f6465d"),
      tile("Event queue", d.queue_depth, "waiting to store"), tile("Analysis queue", d.analysis_queue_depth, d.analysis_enabled ? "waiting for analysis" : "analysis off"));
    const ps = Object.values(d.providers);
    provs.replaceChildren(...(ps.length ? ps.map((p) => h("div", {class: "prow", style: "align-items:flex-start"}, h("span", {class: "dot " + dotState(p.effective_status), style: "margin-top:7px"}),
      h("div", {style: "flex:1;min-width:0"}, h("div", {style: "display:flex;justify-content:space-between;font-weight:650"}, p.name, h("span", {class: "mute", style: "font-weight:500"}, p.effective_status)),
        h("div", {class: "dim", style: "font-size:12.5px"}, "Last success " + ago(p.last_success_at) + " · last message " + ago(p.last_message_at) + " · reconnects " + p.reconnect_count),
        p.last_error ? h("pre", {style: "color:var(--red);max-height:80px;margin:8px 0 0"}, p.last_error) : null))) : [h("span", {class: "dim"}, "No provider has reported yet.")]));
    counters.replaceChildren(h("dl", {class: "kv"}, ...Object.entries(d.metrics).flatMap(([k, v]) => [h("dt", {}, NAMES[k] || k), h("dd", {style: BAD.has(k) && v > 0 ? "color:var(--red)" : null}, Number(v).toLocaleString())])),
      h("p", {class: "dim", style: "font-size:12px;margin:14px 0 0"}, "A connected feed with no messages for a while shows as “stale”. Rate limits and processing latency are not tracked yet."));
  }
  return {root, refresh, every: 5000};
}

function stateInfo(status) { return {icon: status === "PASS" ? "check" : status === "FAIL_CRITICAL" ? "x" : status === "SUSPICIOUS" ? "alert" : "help", cls: "p-" + status}; }

function holdersBlock(c) {
  const hs = Array.isArray(c.evidence.holders) ? c.evidence.holders : [], ex = Array.isArray(c.evidence.excluded) ? c.evidence.excluded : [];
  if (c.name !== "holder_concentration" || !hs.length) return null;
  const palette = ["#3b82f6", "#22d3ee", "#a78bfa", "#f0b90b", "#2ebd85", "#fb7185", "#94a3b8", "#60a5fa", "#34d399", "#fbbf24"];
  const segs = hs.map((x, i) => h("span", {style: "width:" + Math.max(0.4, x.pct_of_supply) + "%;background:" + palette[i % palette.length], title: short(x.owner, 5) + " " + x.pct_of_supply + "%"}));
  ex.forEach((x) => segs.push(h("span", {style: "width:" + Math.max(0.4, x.pct_of_supply) + "%;background:rgba(128,128,128,.35)", title: x.reason})));
  return h("div", {style: "margin:8px 0 4px"}, h("div", {class: "stack"}, segs),
    hs.map((x, i) => h("div", {class: "hrow"}, h("span", {}, h("i", {style: "display:inline-block;width:9px;height:9px;border-radius:3px;margin-right:8px;background:" + palette[i % palette.length]}), h("span", {class: "mono"}, short(x.owner, 6)), x.is_creator ? "  · creator" : ""), h("b", {style: "color:var(--text)"}, Number(x.pct_of_supply).toFixed(2) + "%"))),
    ex.length ? h("div", {class: "hrow dim"}, h("span", {}, "Excluded (" + ex.map((x) => x.reason).join(", ") + ")"), h("span", {}, ex.reduce((a, x) => a + x.pct_of_supply, 0).toFixed(2) + "%")) : null);
}

function checkRow(c) {
  const si = stateInfo(c.status);
  return h("details", {class: "acc"},
    h("summary", {}, h("span", {class: "cic " + si.cls}, icon(si.icon, 17)),
      h("div", {style: "min-width:0"}, h("div", {class: "ct"}, CHECK_TITLE[c.name] || c.name), h("div", {class: "cs"}, c.summary.length > 90 ? c.summary.slice(0, 88) + "…" : c.summary)),
      h("span", {class: "chev"}, icon("chev", 16))),
    h("div", {class: "cb"}, h("p", {class: "dim"}, CHECK_HELP[c.name] || ""), h("p", {style: "color:var(--text)"}, c.summary), holdersBlock(c),
      c.warnings.length ? h("ul", {class: "warns"}, c.warnings.map((w) => h("li", {}, w))) : null,
      h("details", {class: "raw"}, h("summary", {}, "Show raw evidence"), h("pre", {}, JSON.stringify(c.evidence, null, 2))),
      h("div", {class: "dim", style: "font-size:12px;margin-top:8px"}, "Source: " + c.sources.join(", ") + " · retrieved " + fmt(c.retrieved_at))));
}

function tokenView(mint) {
  const box = h("div", {});
  box.append(skeleton(4));
  const root = h("div", {}, h("a", {href: "#/scanner", class: "back"}, icon("back", 18), "Back"), box);
  let lastKey = "";
  async function refresh() {
    const t = await api("/tokens/" + encodeURIComponent(mint));
    let r = null, hist = [];
    try { r = await api("/tokens/" + encodeURIComponent(mint) + "/security"); hist = await api("/tokens/" + encodeURIComponent(mint) + "/security/history?limit=20"); }
    catch (e) { if (e.status !== 404) throw e; }
    const key = t.status + "|" + (r ? r.analyzed_at : "none") + "|" + hist.length;
    if (key === lastKey) return;   // nothing new: leave the page (and any opened sections) untouched
    lastKey = key;
    const fact = (k, v, copy) => h("div", {class: "fact"}, h("div", {class: "k"}, k), h("div", {class: "v" + (copy ? " mono" : "")}, v));
    box.replaceChildren(...[
      h("div", {class: "thead"}, avatar(t, 58), h("div", {style: "min-width:0"}, h("h1", {}, t.symbol || "Token"), h("div", {class: "mute"}, t.name || "Unnamed"),
        h("button", {class: "copy", onclick: () => copyText(t.mint)}, short(t.mint, 8), icon("copy", 13)))),
      h("section", {class: "verdict", style: "--c:" + COLOR[t.status]},
        h("div", {class: "big"}, pill(t.status), h("span", {class: "dim", style: "font-size:12.5px"}, "Security verdict")),
        h("h2", {style: "margin-top:12px"}, LABEL[t.status]), h("p", {class: "mute", style: "margin-top:4px"}, STATUS_HELP[t.status] || ""),
        h("p", {}, r ? r.reason : t.status_reason),
        r && r.warnings.length ? h("ul", {class: "warns"}, r.warnings.map((x) => h("li", {}, x))) : null,
        r ? h("p", {class: "dim", style: "font-size:12px"}, "Analysed " + ago(r.analyzed_at) + " · logic " + r.analysis_version + (r.next_review_at ? " · next review " + fmt(r.next_review_at) : "")) : null),
      h("div", {class: "facts"}, fact("Detected", fmt(t.detected_at)), fact("Launch time", t.launch_timestamp ? fmt(t.launch_timestamp) : "Not supplied"), fact("Creator", short(t.creator, 6), true), fact("Bonding curve", short(t.bonding_curve, 6), true)),
      h("div", {class: "btns"},
        h("a", {class: "btn", href: "https://solscan.io/token/" + t.mint, target: "_blank", rel: "noopener noreferrer"}, "Solscan", icon("ext", 15)),
        h("a", {class: "btn", href: "https://pump.fun/coin/" + t.mint, target: "_blank", rel: "noopener noreferrer"}, "Pump.fun", icon("ext", 15))),
      sec("Security checks"),
      r ? h("div", {class: "card list"}, r.checks.map(checkRow)) : empty("pulse", "Analysis has not completed yet", "The security engine runs shortly after detection. This page refreshes automatically."),
      r && r.not_evaluated.length ? [sec("Not checked yet"), h("div", {class: "card pad"}, h("p", {class: "dim", style: "margin:0 0 8px;font-size:13px"}, "These checks do not exist yet. “Not checked” is never the same as “passed”."), h("ul", {class: "mute", style: "margin:0;padding-left:18px;font-size:13.5px"}, r.not_evaluated.map((x) => h("li", {}, x))))] : null,
      hist.length > 1 ? [sec("Analysis history"), h("div", {class: "card list"}, hist.map((x) => h("div", {class: "tok"}, h("div", {class: "tmain mute"}, fmt(x.analyzed_at)), pill(x.status))))] : null,
    ].flat().filter(Boolean));
  }
  return {root, refresh, every: 8000};
}

/* ---------- router ---------- */
const NAV = [["", "Home", "home"], ["scanner", "Scanner", "radar"], ["passed", "Passed", "shield"], ["review", "Review", "eye"], ["rejected", "Rejected", "ban"]];
function buildNav() {
  const mk = (cls) => (r, l, ic) => h("a", {href: "#/" + r, "data-r": r}, icon(ic, cls === "rail" ? 20 : 22), cls === "rail" ? l : h("span", {}, l));
  document.getElementById("railnav").replaceChildren(...NAV.map(([r, l, ic]) => mk("rail")(r, l, ic)));
  document.getElementById("railsys").replaceChildren(mk("rail")("system", "System health", "pulse"));
  document.getElementById("tabbar").replaceChildren(...NAV.map(([r, l, ic]) => mk("tab")(r, l, ic)));
  document.getElementById("logo1").replaceChildren(logo(36));
  document.getElementById("logo2").replaceChildren(logo(30));
  document.getElementById("sysbtn").replaceChildren(icon("pulse", 19));
}
function themeIcon() {
  const dark = document.documentElement.getAttribute("data-theme") !== "light";
  document.getElementById("themebtn").replaceChildren(icon(dark ? "sun" : "moon", 19));
}
document.getElementById("themebtn").addEventListener("click", () => {
  const next = document.documentElement.getAttribute("data-theme") === "light" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", next);
  try { localStorage.setItem("tosh-theme", next); } catch (e) {}
  themeIcon();
});

const app = document.getElementById("app"), banner = document.getElementById("banner");
let timer = null, gen = 0;
function build(name, arg) {
  switch (name) {
    case "scanner": return listView({title: "Scanner", subtitle: "Every new launch, newest first. Search or filter by security status.", url: "/tokens/recent?limit=200", search: true, icon: "radar", emptyTitle: "No tokens detected yet", emptyText: "If the backend is running, open System health to confirm the feed is connected."});
    case "passed": return listView({title: "Passed checks", subtitle: "Tokens whose implemented security checks passed. Not a recommendation, and not a guarantee of safety.", url: "/tokens/recent?limit=200&status=QUALIFIED,WATCH", icon: "shield", emptyTitle: "No tokens have passed yet", emptyText: "Watch means the checks built so far passed. Qualified needs sell simulation and the opportunity score, which are not built yet."});
    case "review": case "investigating": return listView({title: "In review", subtitle: "Required evidence is missing, or something suspicious is unresolved.", url: "/tokens/recent?limit=200&status=INSUFFICIENT_DATA,HOLD", icon: "eye", emptyTitle: "Nothing in review", emptyText: "Tokens land here while data is being collected, or when something looks suspicious but is not proven."});
    case "rejected": return listView({title: "Rejected", subtitle: "A verified, serious security problem was found. Open a token to see the exact evidence.", url: "/tokens/recent?limit=200&status=REJECTED,REVOKED", icon: "ban", emptyTitle: "No rejected tokens", emptyText: "Rejections appear here with the on-chain evidence behind them."});
    case "system": return systemView();
    case "token": return tokenView(arg || "");
    default: return homeView();
  }
}
function route() {
  clearInterval(timer);
  const parts = location.hash.replace(/^#\/?/, "").split("/");
  let name = parts[0] || "", arg = decodeURIComponent(parts[1] || "");
  if (name === "investigating") name = "review";
  document.querySelectorAll("[data-r]").forEach((a) => a.classList.toggle("on", a.dataset.r === name || (name === "token" && a.dataset.r === "scanner")));
  const view = build(name, arg), my = ++gen;
  app.replaceChildren(view.root); banner.style.display = "none"; window.scrollTo(0, 0);
  const run = async () => {
    try { await view.refresh(); if (my === gen) banner.style.display = "none"; }
    catch (e) { if (my === gen) { banner.textContent = "Can't reach live data (" + e.message + "). Showing the last data received. Retrying automatically."; banner.style.display = "block"; } }
  };
  run(); timer = setInterval(run, view.every);
}
async function pill2() {
  const d = document.getElementById("pdot"), t = document.getElementById("ptxt");
  try {
    const x = await api("/system/health"), f = x.providers.pumpportal_ws && x.providers.pumpportal_ws.effective_status;
    if (!x.listener_enabled) { d.className = "dot warn"; t.textContent = "Listener off"; }
    else if (f === "connected") { d.className = "dot ok"; t.textContent = "Feed live"; }
    else if (f) { d.className = "dot " + (f === "stale" ? "warn" : "bad"); t.textContent = "Feed " + f; }
    else { d.className = "dot"; t.textContent = "Connecting…"; }
  } catch (e) { d.className = "dot bad"; t.textContent = "Offline"; }
}
buildNav(); themeIcon();
window.addEventListener("hashchange", route);
route(); pill2(); setInterval(pill2, 10000);
</script>
</body>
</html>
'''


def build_api_router(auth) -> APIRouter:
    router = APIRouter(dependencies=[Depends(auth)])

    @router.get("/tokens/recent", response_model=list[TokenRecord])
    async def recent_tokens(request: Request, limit: int = Query(50, ge=1, le=200),
                            status: str | None = Query(None, description="Comma-separated statuses")):
        statuses = [x.strip().upper() for x in status.split(",") if x.strip()] if status else None
        if statuses and any(x not in VALID_STATUSES for x in statuses):
            raise HTTPException(status_code=422, detail="Unknown status")
        return await request.app.state.repo.list_recent_tokens(limit, statuses)

    @router.get("/stats")
    async def stats(request: Request) -> dict:
        counts = await request.app.state.repo.count_tokens_by_status()
        full = {s: counts.get(s, 0) for s in VALID_STATUSES}
        return {"total": sum(full.values()), "counts": full}

    @router.get("/tokens/{mint}", response_model=TokenRecord)
    async def token_detail(request: Request, mint: str):
        token = await request.app.state.repo.get_token(_valid_mint(mint))
        if token is None:
            raise HTTPException(status_code=404, detail="Unknown token")
        return token

    @router.get("/tokens/{mint}/security", response_model=SecurityReport)
    async def latest_security(request: Request, mint: str):
        report = await request.app.state.repo.get_latest_report(_valid_mint(mint))
        if report is None:
            raise HTTPException(status_code=404, detail="No security report yet for this token")
        return report

    @router.get("/tokens/{mint}/security/history", response_model=list[SecurityReport])
    async def security_history(request: Request, mint: str, limit: int = Query(20, ge=1, le=100)):
        return await request.app.state.repo.list_reports(_valid_mint(mint), limit)

    @router.get("/system/health")
    async def system_health(request: Request) -> dict:
        st = request.app.state
        try:
            db_ok = await asyncio.wait_for(st.repo.ping(), timeout=5)
        except Exception:
            db_ok = False
        return {
            "database_reachable": db_ok,
            "providers": st.health.snapshot(st.settings.feed_stale_after_seconds),
            "queue_depth": st.queue.qsize(),
            "analysis_queue_depth": st.analysis_queue.qsize(),
            "listener_enabled": st.settings.listener_enabled,
            "analysis_enabled": st.settings.analysis_enabled,
            "metrics": st.metrics.as_dict(),
        }

    return router


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
        return {"service": "toshbot", "status": "ok", "health": "/health", "dashboard": "/dashboard"}

    @app.api_route("/health", methods=["GET", "HEAD"])
    async def liveness() -> dict:
        return {"status": "ok"}

    # Same JSON API, two doors: /api (X-API-Key, for programs) and /dashboard/api (Basic Auth, for the built-in page).
    app.include_router(build_api_router(require_api_key), prefix="/api")
    app.include_router(build_api_router(require_dashboard_auth), prefix="/dashboard/api")

    @app.get("/dashboard", include_in_schema=False, dependencies=[Depends(require_dashboard_auth)])
    async def dashboard_page():
        return HTMLResponse(DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    return app


app = create_app()
