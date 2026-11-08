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
from collections import Counter, OrderedDict, deque
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
import hashlib
import hmac
from html import escape as _html_escape
from urllib.parse import parse_qs
from pathlib import Path
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
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

    # Firestore cost control (the free plan allows 20,000 writes and 50,000 reads per day)
    cache_recent_tokens: int = 1000        # keep the newest N tokens in memory so the dashboard costs zero reads
    store_launch_events: bool = False      # raw launch event documents (1 extra write per token)
    store_jobs: bool = False               # analysis job documents (2 extra writes per token; only needed to resume after a restart)
    save_all_attempts: bool = False        # False = save only the final analysis report instead of every retry
    persist_statuses: str = "WATCH,HOLD,QUALIFIED,REVOKED"  # lean database writes: only these verdicts are saved (others stay in memory). Empty = save everything
    max_new_tokens_per_day: int = 0        # 0 = unlimited. Set e.g. 5000 to stay inside the free write quota

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

    # Live buy signal (trade feed). EXPERIMENTAL starting values, not calibrated on real results.
    signals_enabled: bool = True           # watch trades of WATCH tokens and allow QUALIFIED (= BUY light). False = never QUALIFIED
    signal_window_minutes: float = 30.0    # how long one token is watched for a buy signal
    signal_max_tracked: int = 150          # most tokens watched at once
    signal_review_seconds: int = 30        # how often a watched token is re-evaluated
    security_recheck_seconds: int = 120    # RPC security checks are reused between signal reviews for this long
    review_save_seconds: int = 180         # unchanged re-reviews are saved at most this often (protects database quota)
    signal_min_observe_seconds: int = 90
    signal_min_trades: int = 15
    signal_min_unique_buyers: int = 8
    signal_min_distinct_sellers: int = 2
    signal_max_top_trader_pct: float = 35.0
    signal_max_wash_pct: float = 50.0
    signal_momentum_window_seconds: int = 120
    signal_min_mcap_change_pct: float = 3.0
    signal_max_mcap_change_pct: float = 150.0
    signal_max_drawdown_pct: float = 20.0
    signal_min_buy_pressure_pct: float = 60.0
    signal_min_new_buyers: int = 3
    signal_max_trade_age_seconds: int = 45
    signal_max_mcap_sol: float = 60.0
    signal_min_curve_sol: float = 5.0
    qualify_min_score: float = 65.0

    # Popularity ("buzz"). Only ever RANKS tokens that already passed the security checks; it never relaxes a security rule or a BUY condition.
    buzz_enabled: bool = True
    buzz_interval_seconds: float = 45.0     # how often the popularity of the top candidates is refreshed
    buzz_max_tokens: int = 40               # how many candidates are scored per round (best evidence score first)
    buzz_rank_weight: float = 0.25          # share of the Top picks ranking that comes from popularity (0 = ignore it)
    buzz_dexscreener: bool = True           # free DEX Screener market data (trades, boosts, token profile)
    buzz_telegram_members: bool = True      # Telegram group size via your bot token (public groups and channels only)
    buzz_pumpfun_comments: bool = False     # UNOFFICIAL pump.fun comments endpoint. Off by default: not verified, may break or be blocked
    buzz_comments_url: str = "https://frontend-api-v3.pump.fun/replies/{mint}?limit=50&offset=0"

    @property
    def persist_status_set(self) -> set[str] | None:
        items = {x.strip().upper() for x in self.persist_statuses.split(",") if x.strip()}
        return items or None

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
    opportunity_score: float | None = None  # PARTIAL evidence score 0-100 (see security report); None = not computed
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


class HotToken(TokenRecord):
    """A token as returned by the API, with its popularity attached. Built per request, never stored."""

    buzz: dict[str, Any] | None = None
    rank: float | None = None


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
    opportunity: dict[str, Any] | None = None


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


# Used when the live trade feed is on: the items below are still NOT built.
NOT_EVALUATED_LIVE = [
    "sell_simulation (no transaction is simulated; replaced by sell_evidence: real sells by other wallets seen on the feed)",
    "metadata_and_links (website / social review)",
    "creator_dump_history (transfer-level analysis of the creator wallet)",
    "post_migration_pool_liquidity (after the token leaves the bonding curve the signal ends)",
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
    # live buy-signal thresholds (experimental)
    signal_review_seconds: int = 30
    signal_min_observe_seconds: int = 90
    signal_min_trades: int = 15
    signal_min_unique_buyers: int = 8
    signal_min_distinct_sellers: int = 2
    signal_max_top_trader_pct: float = 35.0
    signal_max_wash_pct: float = 50.0
    signal_momentum_window_seconds: int = 120
    signal_min_mcap_change_pct: float = 3.0
    signal_max_mcap_change_pct: float = 150.0
    signal_max_drawdown_pct: float = 20.0
    signal_min_buy_pressure_pct: float = 60.0
    signal_min_new_buyers: int = 3
    signal_max_trade_age_seconds: int = 45
    signal_max_mcap_sol: float = 60.0
    signal_min_curve_sol: float = 5.0
    qualify_min_score: float = 65.0


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


@dataclass(frozen=True)
class Trade:
    mint: str
    wallet: str
    side: str  # "buy" | "sell"
    sol: float
    tokens: float
    mcap_sol: float | None
    ts: float  # local receipt time (epoch seconds)


def parse_trade(raw: str | bytes, now: float | None = None) -> Trade | None:
    """Parse a PumpPortal trade message. Returns None for anything that is not a well-formed buy/sell
    (launches, acknowledgements, garbage). Never raises: bad data is dropped, never guessed."""
    if isinstance(raw, (bytes, bytearray)):
        raw = bytes(raw).decode("utf-8", errors="replace")
    if '"txType"' not in raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict) or data.get("txType") not in ("buy", "sell"):
        return None
    mint, wallet = _addr(data.get("mint")), _addr(data.get("traderPublicKey"))
    sol, tokens = _num(data.get("solAmount")), _num(data.get("tokenAmount"))
    if mint is None or wallet is None or sol is None or sol < 0:
        return None
    mcap = _num(data.get("marketCapSol"))
    return Trade(mint, wallet, data["txType"], sol, tokens or 0.0, mcap if mcap and mcap > 0 else None,
                 time.time() if now is None else now)


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
    throttled: int = 0

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
    async def update_token_decision(self, mint: str, status: str, reason: str, report_id: str, at: datetime, score: float | None = None) -> None: ...

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

    async def update_token_decision(self, mint, status, reason, report_id, at, score=None) -> None:
        self.tokens[mint] = self.tokens[mint].model_copy(
            update={"status": status, "status_reason": reason, "latest_report_id": report_id, "status_updated_at": at, "opportunity_score": score})

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


class CachedRepository(Repository):
    """Wraps a real repository and answers the dashboard's reads from memory.

    Keeps the newest N tokens, status counts and per-creator launch times in RAM, so refreshing the dashboard
    costs zero database reads.

    LEAN MODE (persist_statuses set): new tokens and their reports live in memory only, and are written to the
    database once, only when the final verdict is one of persist_statuses (for example WATCH or HOLD). That cuts
    writes from ~7 per token to ~2 for the interesting few percent, which fits the free database plan.
    Trade-offs: tokens with other verdicts (e.g. rejected) disappear on restart, and only the newest N are listable."""

    def __init__(self, inner: Repository, size: int = 1000, persist_statuses=None) -> None:
        self.inner, self.size = inner, size
        self.persist = frozenset(persist_statuses) if persist_statuses else None
        self.recent: dict[str, TokenRecord] = {}  # oldest first
        self.counts: dict[str, int] = {}
        self.creator_log: dict[str, list[datetime]] = {}
        self.mem_reports: dict[str, list[SecurityReport]] = {}
        self.persisted: set[str] = set()
        self.seen: dict[str, None] = {}
        self.warm_ok = False

    @property
    def lean(self) -> bool:
        return self.persist is not None

    async def warm(self) -> None:
        tokens = await self.inner.list_recent_tokens(self.size)
        counts = await self.inner.count_tokens_by_status()
        self.recent = {t.mint: t for t in reversed(tokens)}
        self.counts = dict(counts)
        self.creator_log, self.persisted = {}, {t.mint for t in tokens}
        for t in tokens:
            if t.creator:
                self.creator_log.setdefault(t.creator, []).append(t.detected_at)
        self.warm_ok = True

    def _bump(self, status: str, delta: int) -> None:
        self.counts[status] = max(0, self.counts.get(status, 0) + delta)

    def _insert(self, token: TokenRecord) -> None:
        self.recent[token.mint] = token
        while len(self.recent) > self.size:
            gone = self.recent.pop(next(iter(self.recent)))
            self.mem_reports.pop(gone.mint, None)
        self._bump(token.status, 1)
        if token.creator:
            log_ = self.creator_log.setdefault(token.creator, [])
            log_.append(token.detected_at)
            if len(log_) > 300:
                del log_[:-300]

    async def record_launch_event(self, event: LaunchEvent) -> bool:
        return await self.inner.record_launch_event(event)

    async def create_token_if_absent(self, token: TokenRecord) -> bool:
        if self.lean and self.warm_ok:  # memory only: no database write until the verdict is interesting
            if token.mint in self.seen or token.mint in self.recent:
                return False
            self.seen[token.mint] = None
            while len(self.seen) > 50000:
                self.seen.pop(next(iter(self.seen)))
            self._insert(token)
            return True
        created = await self.inner.create_token_if_absent(token)
        if created and self.warm_ok:
            self.persisted.add(token.mint)
            self._insert(token)
        return created

    async def create_analysis_job(self, mint: str) -> bool:
        return await self.inner.create_analysis_job(mint)

    async def list_recent_tokens(self, limit: int, statuses: list[str] | None = None) -> list[TokenRecord]:
        if not self.warm_ok:
            return await self.inner.list_recent_tokens(limit, statuses)
        items = [t for t in self.recent.values() if not statuses or t.status in statuses]
        return sorted(items, key=lambda t: t.detected_at, reverse=True)[:limit]

    async def count_tokens_by_status(self) -> dict[str, int]:
        return dict(self.counts) if self.warm_ok else await self.inner.count_tokens_by_status()

    async def get_token(self, mint: str) -> TokenRecord | None:
        return self.recent.get(mint) or await self.inner.get_token(mint)

    async def update_token_decision(self, mint, status, reason, report_id, at, score=None) -> None:
        old = self.recent.get(mint)
        if old is None and self.warm_ok:
            old = await self.inner.get_token(mint)  # rare: token older than the cache window
        fields = {"status": status, "status_reason": reason, "latest_report_id": report_id, "status_updated_at": at, "opportunity_score": score}
        new = old.model_copy(update=fields) if old is not None else None
        if self.lean and self.warm_ok:
            if new is not None and (status in self.persist or mint in self.persisted):
                reports = self.mem_reports.get(mint)
                real_id = await self.inner.save_security_report(reports[-1]) if reports else report_id
                new = new.model_copy(update={"latest_report_id": real_id})
                if not await self.inner.create_token_if_absent(new):
                    await self.inner.update_token_decision(mint, status, reason, real_id, at, score)
                self.persisted.add(mint)
        else:
            await self.inner.update_token_decision(mint, status, reason, report_id, at, score)
        if self.warm_ok and old is not None:
            self._bump(old.status, -1)
            self._bump(status, 1)
            if mint in self.recent:
                self.recent[mint] = new

    async def save_security_report(self, report: SecurityReport) -> str:
        if self.lean and self.warm_ok:
            items = self.mem_reports.setdefault(report.mint, [])
            items.append(report.model_copy(deep=True))
            return f"mem-{report.mint[:6]}-{len(items)}"
        return await self.inner.save_security_report(report)

    async def get_latest_report(self, mint: str) -> SecurityReport | None:
        if self.lean and mint not in self.persisted and self.mem_reports.get(mint):
            return self.mem_reports[mint][-1]
        return await self.inner.get_latest_report(mint)

    async def list_reports(self, mint: str, limit: int) -> list[SecurityReport]:
        if self.lean and mint not in self.persisted and self.mem_reports.get(mint):
            return list(reversed(self.mem_reports[mint]))[:limit]
        return await self.inner.list_reports(mint, limit)

    async def list_pending_jobs(self, limit: int) -> list[str]:
        return await self.inner.list_pending_jobs(limit)

    async def complete_job(self, mint: str, status: str) -> None:
        await self.inner.complete_job(mint, status)

    async def count_recent_tokens_by_creator(self, creator: str, since: datetime) -> int:
        if not self.warm_ok:
            return await self.inner.count_recent_tokens_by_creator(creator, since)
        return sum(1 for ts in self.creator_log.get(creator, []) if ts >= since)

    async def ping(self) -> bool:
        return await self.inner.ping()

    async def aclose(self) -> None:
        await self.inner.aclose()


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

    async def update_token_decision(self, mint, status, reason, report_id, at, score=None) -> None:
        await self._tokens.document(mint).update(
            {"status": status, "status_reason": reason, "latest_report_id": report_id, "status_updated_at": at, "opportunity_score": score})

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


# ============================================================================
# app/signal.py  (live trade tracking and the BUY signal)
# ============================================================================
SRC_TRADES = "pumpportal_trade_feed"
SIGNAL_CHECKS = ("sell_evidence", "trading_manipulation", "momentum")


@dataclass
class TradeStats:
    observed_seconds: float
    n_trades: int
    last_trade_age: float | None
    unique_buyers: int          # independent buyers (the creator is excluded)
    distinct_sellers: int       # independent wallets that sold (the creator is excluded)
    creator_sold_sol: float
    volume_sol: float
    top_trader_pct: float       # biggest single wallet's share of all traded SOL
    wash_pct: float             # share of traded SOL from wallets that both bought and sold
    mcap_now: float | None
    mcap_ref: float | None
    mcap_change_pct: float | None
    drawdown_pct: float | None  # fall from the highest market cap seen while watching
    buy_pressure_pct: float | None  # buy SOL / all SOL traded in the momentum window
    new_buyers: int             # independent buyers first seen inside the momentum window
    window_seconds: int


def compute_stats(trades, creator: str | None, since: float, now: float, window: int) -> TradeStats:
    cutoff = now - window
    first_buy: dict[str, float] = {}
    sellers: set[str] = set()
    vol: dict[str, float] = {}
    sides: dict[str, set[str]] = {}
    buy_sol = sell_sol = wbuy = wsell = creator_sold = 0.0
    prices: list[tuple[float, float]] = []
    for t in trades:
        vol[t.wallet] = vol.get(t.wallet, 0.0) + t.sol
        sides.setdefault(t.wallet, set()).add(t.side)
        is_creator = bool(creator) and t.wallet == creator
        if t.mcap_sol:
            prices.append((t.ts, t.mcap_sol))
        if t.side == "buy":
            buy_sol += t.sol
            if not is_creator:
                first_buy.setdefault(t.wallet, t.ts)
            if t.ts >= cutoff:
                wbuy += t.sol
        else:
            sell_sol += t.sol
            if is_creator:
                creator_sold += t.sol
            else:
                sellers.add(t.wallet)
            if t.ts >= cutoff:
                wsell += t.sol
    total = buy_sol + sell_sol
    top = (max(vol.values()) / total * 100) if total > 0 else 0.0
    wash = (sum(v for w, v in vol.items() if len(sides[w]) == 2) / total * 100) if total > 0 else 0.0
    mcap_now = prices[-1][1] if prices else None
    ref = None
    if prices:
        before = [p for p in prices if p[0] <= cutoff]
        ref = (before[-1] if before else prices[0])[1]
    change = ((mcap_now - ref) / ref * 100) if (mcap_now is not None and ref) else None
    peak = max((p[1] for p in prices), default=None)
    dd = ((peak - mcap_now) / peak * 100) if (peak and mcap_now is not None) else None
    wtotal = wbuy + wsell
    return TradeStats(
        observed_seconds=max(0.0, now - since), n_trades=len(trades),
        last_trade_age=(now - trades[-1].ts) if trades else None,
        unique_buyers=len(first_buy), distinct_sellers=len(sellers), creator_sold_sol=creator_sold, volume_sol=total,
        top_trader_pct=top, wash_pct=wash, mcap_now=mcap_now, mcap_ref=ref, mcap_change_pct=change, drawdown_pct=dd,
        buy_pressure_pct=(wbuy / wtotal * 100) if wtotal > 0 else None,
        new_buyers=sum(1 for ts in first_buy.values() if ts >= cutoff), window_seconds=window)


class TradeTracker:
    """In-memory trade book for the few tokens that passed the security checks (WATCH). It is NOT stored in the
    database, so a restart clears it and every BUY signal is re-earned from fresh trades. Subscriptions are
    requested here and sent by the listener on the single shared WebSocket connection."""

    def __init__(self, window_seconds: float = 1800.0, max_tracked: int = 150, max_trades: int = 3000, clock=time.time,
                 momentum_window: int = 120) -> None:
        self.window, self.max_tracked, self.max_trades, self._clock = window_seconds, max_tracked, max_trades, clock
        self.momentum_window = momentum_window
        self._tok: dict[str, dict] = {}
        self._finished: OrderedDict[str, float] = OrderedDict()
        self.pending_sub: set[str] = set()
        self.pending_unsub: set[str] = set()
        self.trades_seen = 0

    def is_tracked(self, mint: str) -> bool:
        return mint in self._tok

    def active(self) -> int:
        return len(self._tok)

    def track(self, mint: str, creator: str | None = None) -> bool:
        """True if this token is being watched. False if its window already ended or the tracker is full."""
        if mint in self._tok:
            return True
        if mint in self._finished or len(self._tok) >= self.max_tracked:
            return False
        now = self._clock()
        self._tok[mint] = {"since": now, "creator": creator, "trades": deque(maxlen=self.max_trades), "last_review": now}
        self.pending_sub.add(mint)
        self.pending_unsub.discard(mint)
        return True

    def untrack(self, mint: str) -> None:
        if self._tok.pop(mint, None) is None:
            return
        self._finished[mint] = self._clock()
        while len(self._finished) > 5000:
            self._finished.popitem(last=False)
        self.pending_sub.discard(mint)
        self.pending_unsub.add(mint)

    def add(self, trade: Trade) -> bool:
        rec = self._tok.get(trade.mint)
        if rec is None:
            return False
        rec["trades"].append(trade)
        self.trades_seen += 1
        return True

    def resubscribe_all(self) -> None:
        self.pending_sub |= set(self._tok)

    def drain_subs(self) -> tuple[list[str], list[str]]:
        subs, unsubs = sorted(self.pending_sub), sorted(self.pending_unsub)
        self.pending_sub, self.pending_unsub = set(), set()
        return subs, unsubs

    def expired(self, now: float | None = None) -> list[str]:
        now = self._clock() if now is None else now
        return [m for m, r in self._tok.items() if now - r["since"] >= self.window]

    def due(self, interval: float, now: float | None = None) -> list[str]:
        now = self._clock() if now is None else now
        out = []
        for m, r in self._tok.items():
            if now - r["last_review"] >= interval:
                r["last_review"] = now
                out.append(m)
        return out

    def snapshot(self, mint: str, now: float | None = None) -> TradeStats | None:
        rec = self._tok.get(mint)
        if rec is None:
            return None
        now = self._clock() if now is None else now
        return compute_stats(list(rec["trades"]), rec["creator"], rec["since"], now, self.momentum_window)


@dataclass
class SignalResult:
    state: str  # "BUY" | "WAIT" | "HOLD"
    checks: list
    reasons: list


def _sig_check(name, status, summary, evidence=None, warnings=None) -> CheckResult:
    return result(name, status, summary, evidence=evidence, warnings=warnings, sources=[SRC_TRADES], mandatory=False)


def _waiting(name: str, why: str) -> CheckResult:
    return _sig_check(name, "UNAVAILABLE", f"Waiting: {why}")


def evaluate_signal(stats: TradeStats | None, p: SecurityPolicy, curve_sol: float, note: str | None = None) -> SignalResult:
    """BUY only when every live condition holds at this moment. Anything missing or stale is WAIT, never BUY.
    HOLD only for trading behaviour that looks manipulated or like a creator dump."""
    if stats is None:
        why = note or "no live trade data for this token"
        return SignalResult("WAIT", [_waiting(n, why) for n in SIGNAL_CHECKS], [why])
    ev = {k: (round(v, 3) if isinstance(v, float) else v) for k, v in stats.__dict__.items()}
    blockers: list[str] = []
    fresh = stats.last_trade_age is not None and stats.last_trade_age <= p.signal_max_trade_age_seconds
    enough = stats.observed_seconds >= p.signal_min_observe_seconds and stats.n_trades >= p.signal_min_trades

    # 1. sell evidence (observed, not simulated)
    if stats.distinct_sellers >= p.signal_min_distinct_sellers:
        sell = _sig_check("sell_evidence", "PASS", f"{stats.distinct_sellers} different wallets (not the creator) have sold on the curve, so exits work. This is observed evidence, not a simulation.", ev)
    else:
        sell = _waiting("sell_evidence", f"only {stats.distinct_sellers} independent wallet(s) have sold so far (needs {p.signal_min_distinct_sellers}).")
        blockers.append("sell evidence: " + sell.summary)

    # 2. manipulation
    flags = []
    if stats.creator_sold_sol > 0:
        flags.append(f"the creator sold {stats.creator_sold_sol:.2f} SOL worth")
    if enough and stats.top_trader_pct > p.signal_max_top_trader_pct:
        flags.append(f"one wallet made {stats.top_trader_pct:.0f}% of all trading (max {p.signal_max_top_trader_pct:.0f}%)")
    if enough and stats.wash_pct > p.signal_max_wash_pct:
        flags.append(f"{stats.wash_pct:.0f}% of volume came from wallets that both bought and sold (max {p.signal_max_wash_pct:.0f}%)")
    if flags:
        manip = _sig_check("trading_manipulation", "SUSPICIOUS", "Suspicious trading: " + "; ".join(flags) + ".", ev)
    elif not enough:
        manip = _waiting("trading_manipulation", f"{stats.n_trades} trades over {stats.observed_seconds:.0f}s (needs {p.signal_min_trades} trades over {p.signal_min_observe_seconds}s).")
        blockers.append("manipulation check: " + manip.summary)
    elif stats.unique_buyers < p.signal_min_unique_buyers:
        manip = _waiting("trading_manipulation", f"only {stats.unique_buyers} independent buyers (needs {p.signal_min_unique_buyers}).")
        blockers.append("manipulation check: " + manip.summary)
    else:
        manip = _sig_check("trading_manipulation", "PASS", f"{stats.unique_buyers} independent buyers, top wallet {stats.top_trader_pct:.0f}% of volume, round-trip volume {stats.wash_pct:.0f}%, creator has not sold.", ev)

    # 3. momentum
    unmet = []
    if not fresh:
        unmet.append("no trade in the last %ds" % p.signal_max_trade_age_seconds if stats.last_trade_age is None or stats.last_trade_age > p.signal_max_trade_age_seconds else "")
    if stats.mcap_change_pct is None:
        unmet.append("no price data yet")
    else:
        if stats.mcap_change_pct < p.signal_min_mcap_change_pct:
            unmet.append(f"price {stats.mcap_change_pct:+.1f}% in {stats.window_seconds}s (needs at least +{p.signal_min_mcap_change_pct:.0f}%)")
        if stats.mcap_change_pct > p.signal_max_mcap_change_pct:
            unmet.append(f"price {stats.mcap_change_pct:+.0f}% in {stats.window_seconds}s is a vertical spike (max +{p.signal_max_mcap_change_pct:.0f}%), chasing it is risky")
        if stats.drawdown_pct is not None and stats.drawdown_pct > p.signal_max_drawdown_pct:
            unmet.append(f"price is {stats.drawdown_pct:.0f}% below its peak (max {p.signal_max_drawdown_pct:.0f}%)")
    if stats.buy_pressure_pct is None or stats.buy_pressure_pct < p.signal_min_buy_pressure_pct:
        unmet.append("buy pressure %s (needs %.0f%%)" % ("n/a" if stats.buy_pressure_pct is None else f"{stats.buy_pressure_pct:.0f}%", p.signal_min_buy_pressure_pct))
    if stats.new_buyers < p.signal_min_new_buyers:
        unmet.append(f"only {stats.new_buyers} new buyers in {stats.window_seconds}s (needs {p.signal_min_new_buyers})")
    if stats.mcap_now is not None and stats.mcap_now > p.signal_max_mcap_sol:
        unmet.append(f"market cap {stats.mcap_now:.0f} SOL is above the {p.signal_max_mcap_sol:.0f} SOL entry limit")
    if curve_sol < p.signal_min_curve_sol:
        unmet.append(f"curve holds {curve_sol:.1f} SOL (needs {p.signal_min_curve_sol:.0f})")
    unmet = [u for u in unmet if u]
    if unmet:
        mom = _waiting("momentum", "; ".join(unmet) + ".")
        blockers.append("momentum: " + "; ".join(unmet))
    else:
        mom = _sig_check("momentum", "PASS", f"Price {stats.mcap_change_pct:+.1f}% in {stats.window_seconds}s, buy pressure {stats.buy_pressure_pct:.0f}%, {stats.new_buyers} new buyers, {stats.drawdown_pct or 0:.0f}% below peak.", ev)

    checks = [sell, manip, mom]
    if manip.status == "SUSPICIOUS":
        return SignalResult("HOLD", checks, [manip.summary])
    if all(c.status == "PASS" for c in checks):
        return SignalResult("BUY", checks, [])
    return SignalResult("WAIT", checks, blockers)


def signal_components(stats: TradeStats | None, p: SecurityPolicy) -> list[dict] | None:
    """The three scoring inputs the evidence score was missing. None until there is price data and enough trades."""
    if stats is None or stats.mcap_change_pct is None or stats.n_trades < p.signal_min_trades:
        return None
    c = stats.mcap_change_pct
    if c <= 0:
        mom = 0.0
    elif c <= 30:
        mom = c / 30 * 100
    elif c <= p.signal_max_mcap_change_pct:
        mom = 100 - (c - 30) / max(1.0, p.signal_max_mcap_change_pct - 30) * 50
    else:
        mom = 30.0
    mom = _clamp(mom - 2 * (stats.drawdown_pct or 0.0))
    growth = _clamp(50 * min(1.0, stats.unique_buyers / 25) + 50 * min(1.0, stats.new_buyers / 8))
    auth = _clamp(100 - 1.5 * stats.top_trader_pct - stats.wash_pct)
    return [
        {"name": "trading_momentum", "weight": 20, "score": round(mom, 1), "detail": f"price {c:+.1f}% in {stats.window_seconds}s, {stats.drawdown_pct or 0:.0f}% below peak"},
        {"name": "independent_buyer_growth", "weight": 10, "score": round(growth, 1), "detail": f"{stats.unique_buyers} independent buyers, {stats.new_buyers} new in {stats.window_seconds}s"},
        {"name": "volume_authenticity", "weight": 10, "score": round(auth, 1), "detail": f"top wallet {stats.top_trader_pct:.0f}% of volume, round-trip volume {stats.wash_pct:.0f}%"},
    ]


OPP_WEIGHTS = {"liquidity_quality": 20, "holder_distribution": 15, "wallet_intelligence": 15, "developer_history": 10}
OPP_MISSING = ["trading_momentum (weight 20)", "independent_buyer_growth (weight 10)", "volume_authenticity (weight 10)"]


def _clamp(x: float) -> float:
    return max(0.0, min(100.0, x))


def opportunity_score(results: list[CheckResult], extra: list[dict] | None = None) -> dict | None:
    """PARTIAL evidence-quality score from the inputs that exist today (60% of the planned weights).
    It is NOT a profit probability. Returns None when any input is unavailable (never guesses a neutral value)."""
    by = {r.name: r for r in results}
    for need in ("liquidity", "holder_concentration", "creator_wallet"):
        if need not in by or by[need].status == "UNAVAILABLE":
            return None
    liq, hold, cre = by["liquidity"].evidence, by["holder_concentration"].evidence, by["creator_wallet"].evidence
    sol = float(liq["curve_sol"])
    top1, top10 = float(hold["top1_pct"]), float(hold["top10_pct"])
    creator_pct = float(hold.get("creator_pct") or 0.0)
    launches = int(cre["launches_seen_by_tosh_bot_24h"])
    comps = [
        {"name": "liquidity_quality", "weight": OPP_WEIGHTS["liquidity_quality"], "score": round(_clamp((sol - 0.1) / (30 - 0.1) * 100), 1),
         "detail": f"{sol:.2f} SOL in the bonding curve (full marks at 30 SOL)"},
        {"name": "holder_distribution", "weight": OPP_WEIGHTS["holder_distribution"], "score": round(_clamp(100 - 2 * top1 - top10), 1),
         "detail": f"top wallet {top1:.1f}%, top 10 wallets {top10:.1f}% of supply"},
        {"name": "wallet_intelligence", "weight": OPP_WEIGHTS["wallet_intelligence"], "score": round(_clamp(100 - 15 * max(0, launches - 1) - 3 * creator_pct), 1),
         "detail": f"creator holds {creator_pct:.1f}% and launched {launches} token(s) in 24h"},
        {"name": "developer_history", "weight": OPP_WEIGHTS["developer_history"], "score": round(_clamp(100 - 25 * max(0, launches - 1)), 1),
         "detail": f"{launches} launch(es) by this creator seen in 24h"},
    ]
    if extra:
        comps += extra
    total_w = sum(c["weight"] for c in comps)
    score = round(sum(c["weight"] * c["score"] for c in comps) / total_w, 1)
    complete = bool(extra) and total_w >= 100
    return {"score": score, "available_weight": total_w, "complete": complete, "components": comps,
            "missing": [] if complete else OPP_MISSING,
            "note": ("Complete evidence score including live trade data. Not a prediction and not a recommendation." if complete
                     else "Partial evidence score from the inputs available today. Not a prediction and not a recommendation.")}


class SecurityAnalyzer:
    def __init__(self, rpc: SolanaRPC, repo: Repository, policy: SecurityPolicy, tracker: "TradeTracker | None" = None,
                 security_cache_seconds: float = 0.0) -> None:
        self.rpc, self.repo, self.policy = rpc, repo, policy
        self.tracker, self._sec_ttl = tracker, security_cache_seconds
        self._sec_cache: dict[str, tuple[datetime, list[CheckResult]]] = {}

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

    async def _security_results(self, token: TokenRecord) -> list[CheckResult]:
        """The on-chain checks. With the live signal on, a clean result is reused briefly so the 30 second
        signal reviews do not repeat every RPC call."""
        cached = self._sec_cache.get(token.mint)
        if self.tracker is not None and self._sec_ttl and cached and (utcnow() - cached[0]).total_seconds() < self._sec_ttl:
            return list(cached[1])
        results = await self._run_security(token)
        if self.tracker is not None and self._sec_ttl:
            if all(r.status == "PASS" for r in results):
                if len(self._sec_cache) > 2000:
                    self._sec_cache.clear()
                self._sec_cache[token.mint] = (utcnow(), list(results))
            else:
                self._sec_cache.pop(token.mint, None)
        return results

    async def _run_security(self, token: TokenRecord) -> list[CheckResult]:
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
        return mint_checks + (list(gathered[:1]) if holders else []) + list(gathered[1 if holders else 0:])

    def _signal_overlay(self, token: TokenRecord, results: list[CheckResult], status: str, reason: str, reasons: list[str],
                        next_review, now: datetime):
        """Only a token whose security verdict is WATCH can move. BUY (QUALIFIED) needs every live condition AND a
        complete evidence score at or above the bar. Missing or stale trade data always leaves the token at WATCH."""
        tr, p = self.tracker, self.policy
        if status in ("REJECTED", "REVOKED"):
            tr.untrack(token.mint)
        if status != "WATCH":
            return status, reason, reasons, next_review, [], None
        tracked = tr.track(token.mint, token.creator)
        stats = tr.snapshot(token.mint) if tracked else None
        note = None if tracked else "live trade watching is not active for this token (its watch window ended or the tracker is full)."
        liq = next((r for r in results if r.name == "liquidity" and r.status != "UNAVAILABLE"), None)
        curve_sol = float(liq.evidence.get("curve_sol", 0.0)) if liq else 0.0
        sig = evaluate_signal(stats, p, curve_sol, note=note)
        opp = opportunity_score(results, signal_components(stats, p))
        review = now + timedelta(seconds=p.signal_review_seconds) if tracked else next_review
        if sig.state == "HOLD":
            return "HOLD", "Suspicious trading activity. " + " | ".join(sig.reasons), sig.reasons, review, sig.checks, opp
        if sig.state == "BUY":
            if opp and opp["complete"] and opp["score"] >= p.qualify_min_score:
                msg = (f"BUY conditions are met right now. All implemented security checks passed, other wallets have sold successfully, "
                       f"trading looks organic and momentum is positive. Evidence score {opp['score']:.1f}/100. "
                       "This is a signal, not a guarantee, and it is re-checked every "
                       f"{p.signal_review_seconds} seconds.")
                return "QUALIFIED", msg, [], review, sig.checks, opp
            low = [f"evidence score {opp['score']:.1f} is below the {p.qualify_min_score:.0f} needed"] if opp else ["evidence score could not be completed"]
            return status, "Security checks passed and trading conditions look fine, but " + low[0] + ".", low, review, sig.checks, opp
        why = "; ".join(sig.reasons) if sig.reasons else "waiting for trade data"
        return status, "Security checks passed. No buy signal yet: " + why, [], review, sig.checks, opp

    async def analyze(self, token: TokenRecord) -> SecurityReport:
        results = await self._security_results(token)
        now = utcnow()
        status, reason, reasons, next_review = decide(results, now, self.policy)
        warnings = sorted({w for r in results for w in r.warnings})
        if self.tracker is not None:
            status, reason, reasons, next_review, extra, opp = self._signal_overlay(token, results, status, reason, reasons, next_review, now)
            if opp is None and status in ("WATCH", "HOLD", "QUALIFIED"):
                opp = opportunity_score(results)
            results = results + extra
            warnings = sorted({w for r in results for w in r.warnings})
            version, not_eval = ANALYSIS_VERSION + "+signal", NOT_EVALUATED_LIVE
        else:
            opp = opportunity_score(results) if status in ("WATCH", "HOLD") else None
            version, not_eval = ANALYSIS_VERSION, NOT_EVALUATED
        return SecurityReport(mint=token.mint, analysis_version=version, analyzed_at=now, status=status, reason=reason,
                              reasons=reasons, warnings=warnings, checks=results, not_evaluated=not_eval,
                              next_review_at=next_review, policy=self.policy.model_dump(), opportunity=opp)


async def _wrap(fn, *args):
    return fn(*args)


# ============================================================================
# app/analysis.py
# ============================================================================
class AnalysisWorker:
    """Takes mints from the analysis queue, runs the security engine, stores an append-only report,
    and updates the token's current status. Re-tries only while evidence is unavailable."""

    def __init__(self, repo: Repository, analyzer: SecurityAnalyzer, queue: asyncio.Queue, health: HealthRegistry,
                 metrics: Metrics, *, retry_delays: list[float] | None = None, sleep=asyncio.sleep,
                 save_all_attempts: bool = True, store_jobs: bool = True, review_save_seconds: float = 0.0) -> None:
        self.repo, self.analyzer, self.queue, self.health, self.metrics = repo, analyzer, queue, health, metrics
        self.review_save_seconds = review_save_seconds
        self._last_saved: dict[str, float] = {}
        self.retry_delays = retry_delays if retry_delays is not None else [5.0, 20.0, 60.0]
        self._sleep = sleep
        self.save_all_attempts, self.store_jobs = save_all_attempts, store_jobs

    async def analyze_once(self, mint: str, final: bool = True) -> SecurityReport | None:
        token = await self.repo.get_token(mint)
        if token is None:
            log.error("analysis requested for unknown token", extra={"mint": mint})
            return None
        report = await self.analyzer.analyze(token)
        self.metrics.analyses += 1
        # Lean mode: skip saving intermediate "insufficient data" retries (each save is a billable write).
        should_save = self.save_all_attempts or final or report.status != "INSUFFICIENT_DATA"
        if should_save and self.review_save_seconds > 0:
            # Re-reviews every 30s must not burn the database quota: an unchanged verdict is saved at most every
            # review_save_seconds. A status change and every QUALIFIED review (it must stay fresh) are always saved.
            last, mono = self._last_saved.get(mint), time.monotonic()
            if last is not None and report.status == token.status and report.status != "QUALIFIED" and mono - last < self.review_save_seconds:
                should_save = False
            else:
                if len(self._last_saved) > 5000:
                    self._last_saved.clear()
                self._last_saved[mint] = mono
        if should_save:
            report_id = await self.repo.save_security_report(report)
            score = report.opportunity["score"] if report.opportunity else None
            await self.repo.update_token_decision(mint, report.status, report.reason, report_id, report.analyzed_at, score)
            log.info("analysis complete", extra={"mint": mint, "status": report.status, "report_id": report_id})
        return report

    async def process(self, mint: str) -> None:
        report = None
        delays = [0.0, *self.retry_delays]
        for i, delay in enumerate(delays):
            if delay:
                await self._sleep(delay)
            report = await self.analyze_once(mint, final=(i == len(delays) - 1))
            if report is None or report.status != "INSUFFICIENT_DATA":
                break
        if report is not None and self.store_jobs:
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


class ReviewScheduler:
    """Re-queues watched tokens for a fresh signal check, and ends their watch window. When a window ends the token
    gets one last analysis with no live data, so a BUY signal can never outlive its evidence."""

    def __init__(self, tracker: TradeTracker, analysis_queue: asyncio.Queue, *, interval: float = 30.0, sleep=asyncio.sleep) -> None:
        self.tracker, self.queue, self.interval, self._sleep = tracker, analysis_queue, interval, sleep

    def _enqueue(self, mint: str) -> None:
        if self.queue.maxsize and self.queue.qsize() > self.queue.maxsize * 0.8:
            return  # never let reviews crowd out brand-new launches
        try:
            self.queue.put_nowait(mint)
        except asyncio.QueueFull:
            pass

    def tick(self) -> None:
        for mint in self.tracker.expired():
            self.tracker.untrack(mint)
            self._enqueue(mint)
        for mint in self.tracker.due(self.interval):
            self._enqueue(mint)

    async def run(self) -> None:
        while True:
            await self._sleep(max(1.0, self.interval / 3))
            try:
                self.tick()
            except Exception:
                log.exception("review scheduler tick failed")


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
        tracker: TradeTracker | None = None,
    ) -> None:
        self.tracker = tracker
        self.url, self.queue, self.health, self.metrics = url, queue, health, metrics
        self._connect, self._sleep = connect, sleep
        self._base, self._max, self._stable = base_delay, max_delay, stable_after
        self._clock, self._rng = clock, rng

    def _handle(self, raw) -> None:
        self.metrics.received += 1
        self.health.mark_message(LISTENER_PROVIDER)
        if self.tracker is not None:
            trade = parse_trade(raw)
            if trade is not None:
                self.tracker.add(trade)
                return
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

    async def _sub_pump(self, ws) -> None:
        """Sends trade subscribe / unsubscribe requests for watched tokens over the one shared connection."""
        try:
            while True:
                subs, unsubs = self.tracker.drain_subs()
                for method, keys in (("subscribeTokenTrade", subs), ("unsubscribeTokenTrade", unsubs)):
                    for i in range(0, len(keys), 50):
                        await ws.send(json.dumps({"method": method, "keys": keys[i:i + 50]}))
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("trade subscription sender stopped", extra={"error": f"{type(exc).__name__}: {exc}"})

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
                    pump = None
                    if self.tracker is not None:
                        self.tracker.resubscribe_all()  # after a reconnect every watched token must be subscribed again
                        pump = asyncio.create_task(self._sub_pump(ws), name="trade-subscriptions")
                    try:
                        async for raw in ws:
                            self._handle(raw)
                            if stop.is_set():
                                break
                    finally:
                        if pump is not None:
                            pump.cancel()
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
                 *, max_attempts: int = 3, sleep=asyncio.sleep, analysis_queue: asyncio.Queue | None = None,
                 store_events: bool = True, store_jobs: bool = True, max_new_per_day: int = 0) -> None:
        self.repo, self.queue, self.health, self.metrics = repo, queue, health, metrics
        self.max_attempts, self._sleep = max_attempts, sleep
        self.analysis_queue = analysis_queue
        self.store_events, self.store_jobs, self.max_new_per_day = store_events, store_jobs, max_new_per_day
        self._day, self._day_count = utcnow().date(), 0

    async def handle(self, event: LaunchEvent) -> bool:
        """Idempotent: safe to retry and safe for duplicate / delayed / out-of-order events."""
        today = utcnow().date()
        if today != self._day:
            self._day, self._day_count = today, 0
        if self.max_new_per_day and self._day_count >= self.max_new_per_day:
            self.metrics.throttled += 1  # daily limit reached: protects the free database quota
            return False
        if self.store_events:
            await self.repo.record_launch_event(event)
        created = await self.repo.create_token_if_absent(TokenRecord.from_event(event))
        if self.store_jobs:
            await self.repo.create_analysis_job(event.mint)  # always attempted so a retry can finish a half-done earlier attempt
        if created:
            self._day_count += 1
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


SESSION_COOKIE = "tosh_session"
SESSION_TTL = 30 * 24 * 3600          # stay signed in for 30 days
LOGIN_MAX_FAILS, LOGIN_WINDOW = 8, 600  # 8 wrong tries per 10 minutes per address, then a cool-down
_LOGIN_FAILS: dict[str, list[float]] = {}


def _session_key(cfg: Settings) -> bytes:
    # Derived from the password, so changing DASHBOARD_PASSWORD signs every device out.
    return hashlib.sha256(f"toshbot-session:{cfg.dashboard_password}:{cfg.api_key}".encode()).digest()


def _session_sig(cfg: Settings, exp: int) -> str:
    return hmac.new(_session_key(cfg), f"{cfg.dashboard_user}|{exp}".encode(), hashlib.sha256).hexdigest()


def make_session(cfg: Settings, now: float | None = None) -> str:
    exp = int((time.time() if now is None else now) + SESSION_TTL)
    return f"{exp}.{_session_sig(cfg, exp)}"


def session_valid(cfg: Settings, token: str, now: float | None = None) -> bool:
    try:
        exp_s, _, sig = token.partition(".")
        exp = int(exp_s)
    except ValueError:
        return False
    if exp < (time.time() if now is None else now) or not sig:
        return False
    return hmac.compare_digest(sig, _session_sig(cfg, exp))


def _client_ip(request: Request) -> str:
    # Rightmost X-Forwarded-For entry = the address the platform's own proxy saw; the left side can be faked by the client.
    fwd = request.headers.get("x-forwarded-for", "")
    last = fwd.split(",")[-1].strip() if fwd else ""
    return last or (request.client.host if request.client else "unknown")


def _login_locked(ip: str, now: float | None = None) -> bool:
    now = time.time() if now is None else now
    recent = [t for t in _LOGIN_FAILS.get(ip, []) if now - t < LOGIN_WINDOW]
    if recent:
        _LOGIN_FAILS[ip] = recent
    else:
        _LOGIN_FAILS.pop(ip, None)
    return len(recent) >= LOGIN_MAX_FAILS


def _login_failed(ip: str, now: float | None = None) -> None:
    if len(_LOGIN_FAILS) > 1000:   # bound memory
        _LOGIN_FAILS.clear()
    _LOGIN_FAILS.setdefault(ip, []).append(time.time() if now is None else now)


async def require_dashboard_auth(request: Request) -> None:
    """Dashboard door: a signed session cookie (from the branded /login page) or HTTP Basic Auth (for scripts).
    Fails closed in production when no password is set."""
    cfg = request.app.state.settings
    if not cfg.dashboard_password:
        if cfg.environment == "production":
            raise HTTPException(status_code=503, detail="Dashboard is disabled: set DASHBOARD_PASSWORD")
        return
    token = request.cookies.get(SESSION_COOKIE, "")
    if token and session_valid(cfg, token):
        return
    header = request.headers.get("authorization", "")
    if header.lower().startswith("basic "):
        try:
            user, _, pw = base64.b64decode(header[6:]).decode("utf-8").partition(":")
            ok_user = hmac.compare_digest(user.encode(), cfg.dashboard_user.encode())
            ok_pw = hmac.compare_digest(pw.encode(), cfg.dashboard_password.encode())
            if ok_user and ok_pw:
                return
        except Exception:
            pass
    accept = request.headers.get("accept", "")
    is_browser = "text/html" in accept or "sec-fetch-dest" in request.headers
    if is_browser and "text/html" in accept and request.url.path.rstrip("/") in ("/dashboard", "/cupsey"):
        raise HTTPException(status_code=303, detail="Sign in required", headers={"Location": "/login"})
    if is_browser:   # a background fetch from the page: plain 401 so the browser never shows its own ugly popup
        raise HTTPException(status_code=401, detail="Authentication required")
    raise HTTPException(status_code=401, detail="Authentication required", headers={"WWW-Authenticate": 'Basic realm="Tosh Bot"'})


def policy_from_settings(s: Settings) -> SecurityPolicy:
    return SecurityPolicy(
        evidence_max_age_seconds=s.evidence_max_age_seconds, single_holder_reject_pct=s.single_holder_reject_pct,
        top10_hold_pct=s.top10_hold_pct, creator_hold_pct=s.creator_hold_pct, max_transfer_fee_bps=s.max_transfer_fee_bps,
        creator_max_launches_24h=s.creator_max_launches_24h, min_curve_reserve_sol=s.min_curve_reserve_sol,
        **{k: getattr(s, k) for k in SIGNAL_POLICY_FIELDS})


SIGNAL_POLICY_FIELDS = [
    "signal_review_seconds", "signal_min_observe_seconds", "signal_min_trades", "signal_min_unique_buyers",
    "signal_min_distinct_sellers", "signal_max_top_trader_pct", "signal_max_wash_pct", "signal_momentum_window_seconds",
    "signal_min_mcap_change_pct", "signal_max_mcap_change_pct", "signal_max_drawdown_pct", "signal_min_buy_pressure_pct",
    "signal_min_new_buyers", "signal_max_trade_age_seconds", "signal_max_mcap_sol", "signal_min_curve_sol", "qualify_min_score"]


def _valid_mint(mint: str) -> str:
    if not BASE58_ADDRESS.match(mint):
        raise HTTPException(status_code=422, detail="Invalid mint address")
    return mint


LOGO_ICON_URI = "data:image/jpeg;base64,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"
LOGO_FULL_URI = "data:image/jpeg;base64,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"


LOGIN_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#000000" media="(prefers-color-scheme: dark)">
<meta name="theme-color" content="#eaf0fb" media="(prefers-color-scheme: light)">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-title" content="Tosh Bot">
<link rel="icon" href="__LOGO_ICON__">
<link rel="apple-touch-icon" href="__LOGO_ICON__">
<meta name="robots" content="noindex">
<title>Sign in · Tosh Bot</title>
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
  --bg:#000;--text:#f5f7fb;--mute:rgba(235,240,250,.68);--dim:rgba(235,240,250,.45);
  --blue:#4c9bff;--cyan:#3de0f5;--red:#ff5d73;
  --g-bg:rgba(255,255,255,.075);--g-line:rgba(255,255,255,.16);--g-hi:rgba(255,255,255,.36);--g-lo:rgba(255,255,255,.05);
  --g-shadow:0 14px 44px rgba(0,0,0,.45);--field:rgba(255,255,255,.07);--field-line:rgba(255,255,255,.14);
  --o1:rgba(59,130,246,.30);--o2:rgba(34,211,238,.20);--o3:rgba(120,90,255,.16);
  --font:-apple-system,BlinkMacSystemFont,"SF Pro Display","SF Pro Text","Segoe UI",Roboto,Inter,Helvetica,Arial,sans-serif;
  --spring:cubic-bezier(.34,1.56,.64,1);
}
[data-theme="light"]{
  color-scheme:light;
  --bg:#eaf0fb;--text:#0a0f1a;--mute:rgba(10,15,26,.66);--dim:rgba(10,15,26,.42);
  --blue:#0a74ff;--red:#ec3b55;
  --g-bg:rgba(255,255,255,.62);--g-line:rgba(255,255,255,.9);--g-hi:rgba(255,255,255,1);--g-lo:rgba(255,255,255,.4);
  --g-shadow:0 12px 40px rgba(40,60,110,.18);--field:rgba(10,15,26,.045);--field-line:rgba(10,15,26,.12);
  --o1:rgba(59,130,246,.45);--o2:rgba(167,139,250,.40);--o3:rgba(34,211,238,.38);
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{background:var(--bg);-webkit-text-size-adjust:100%}
body{margin:0;min-height:100vh;min-height:100dvh;color:var(--text);font:15px/1.45 var(--font);-webkit-font-smoothing:antialiased;
  display:grid;place-items:center;padding:calc(24px + env(safe-area-inset-top)) 18px calc(24px + env(safe-area-inset-bottom));overflow-x:hidden}
body::before{content:"";position:fixed;inset:-20%;z-index:-1;pointer-events:none;will-change:transform;
  background:radial-gradient(46% 40% at 18% 14%,var(--o1),transparent 72%),radial-gradient(42% 36% at 86% 22%,var(--o2),transparent 72%),
             radial-gradient(46% 40% at 72% 88%,var(--o3),transparent 72%);
  animation:drift 28s ease-in-out infinite alternate}
@keyframes drift{from{transform:translate3d(-2%,-1%,0) scale(1)}to{transform:translate3d(3%,2%,0) scale(1.12)}}
@media(prefers-reduced-motion:reduce){body::before{animation:none}*{transition:none!important;animation:none!important}}
svg{display:block}
button{font:inherit;color:inherit;cursor:pointer}
.card{position:relative;width:100%;max-width:400px;padding:34px 26px 26px;border-radius:34px;
  background:var(--g-bg);-webkit-backdrop-filter:blur(28px) saturate(190%);backdrop-filter:blur(28px) saturate(190%);
  border:1px solid var(--g-line);
  box-shadow:var(--g-shadow),inset 0 1.5px 0 var(--g-hi),inset 0 -1px 0 var(--g-lo);
  animation:rise .7s var(--spring) both}
@keyframes rise{from{opacity:0;transform:translateY(18px) scale(.97)}to{opacity:1;transform:none}}
.card::before{content:"";position:absolute;inset:0;border-radius:inherit;pointer-events:none;
  background:linear-gradient(135deg,rgba(255,255,255,.22) 0%,rgba(255,255,255,0) 32%,rgba(255,255,255,0) 68%,rgba(255,255,255,.08) 100%);mix-blend-mode:overlay}
.logo{width:220px;max-width:100%;margin:0 auto 16px;border-radius:18px;box-shadow:0 10px 34px rgba(59,130,246,.45)}
h1{margin:0;text-align:center;font-size:27px;font-weight:800;letter-spacing:-.03em}
.sub{margin:6px 0 26px;text-align:center;color:var(--mute);font-size:14px}
label{display:block;margin-bottom:14px}
label>span{display:block;margin:0 4px 7px;font-size:12px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--dim)}
.in{position:relative}
input{width:100%;height:52px;padding:0 16px;border-radius:17px;border:1px solid var(--field-line);background:var(--field);
  color:var(--text);font:inherit;font-size:16px;outline:none;transition:border-color .2s,box-shadow .2s}
input::placeholder{color:var(--dim)}
input:focus{border-color:var(--blue);box-shadow:0 0 0 4px rgba(76,155,255,.22)}
.in input{padding-right:52px}
.eye{position:absolute;right:6px;top:6px;width:40px;height:40px;display:grid;place-items:center;border:0;border-radius:13px;background:transparent;color:var(--mute)}
.eye:active{transform:scale(.9)}
.err{display:flex;gap:9px;align-items:flex-start;margin:2px 0 14px;padding:11px 13px;border-radius:15px;font-size:13.5px;font-weight:600;
  color:var(--red);background:rgba(255,93,115,.12);border:1px solid rgba(255,93,115,.30)}
.err svg{flex:none;margin-top:1px}
.go{width:100%;height:54px;margin-top:6px;border:0;border-radius:999px;font-size:16px;font-weight:750;letter-spacing:-.01em;color:#fff;
  background:linear-gradient(135deg,#3b82f6,#22d3ee);box-shadow:0 10px 30px rgba(59,130,246,.42),inset 0 1px 0 rgba(255,255,255,.45);
  transition:transform .35s var(--spring),opacity .2s}
.go:active{transform:scale(.96)}
.go[disabled]{opacity:.65}
.foot{margin:22px 4px 0;text-align:center;color:var(--dim);font-size:12px;line-height:1.5}
</style>
</head>
<body>
<main class="card">
  <div class="logo"><img src="__LOGO_FULL__" alt="Tosh Bot" width="220" style="display:block;width:220px;max-width:100%;height:auto;border-radius:18px"></div>
  <h1 style="position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0)">Tosh Bot</h1>
  <p class="sub">Live security analysis for new Solana launches</p>
  <form method="post" action="/login" id="f">
    <label><span>Username</span>
      <input name="username" type="text" value="__USER__" placeholder="Your name" autocomplete="username" autocapitalize="none" autocorrect="off" spellcheck="false" required __USERFOCUS__>
    </label>
    <label><span>Password</span>
      <div class="in">
        <input name="password" id="pw" type="password" placeholder="Your password" autocomplete="current-password" required __PWFOCUS__>
        <button type="button" class="eye" id="eye" aria-label="Show password"><svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/></svg></button>
      </div>
    </label>
    __ERROR__
    <button class="go" type="submit" id="go">Sign in</button>
  </form>
  <p class="foot">Detection and analysis only. Nothing here is financial advice or a guarantee that a token is safe.</p>
</main>
<script>
(function () {
  var pw = document.getElementById("pw"), eye = document.getElementById("eye");
  eye.addEventListener("click", function () {
    var show = pw.type === "password";
    pw.type = show ? "text" : "password";
    eye.setAttribute("aria-label", show ? "Hide password" : "Show password");
    eye.style.color = show ? "var(--blue)" : "";
  });
  document.getElementById("f").addEventListener("submit", function () {
    var b = document.getElementById("go"); b.disabled = true; b.textContent = "Signing in…";
  });
  window.addEventListener("pageshow", function () { var b = document.getElementById("go"); b.disabled = false; b.textContent = "Sign in"; });
})();
</script>
</body>
</html>'''


def render_login_page(error: str = "", user: str = "") -> str:
    err = ('<div class="err" role="alert"><svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 7.5v5M12 16h.01"/></svg><span>'
           + _html_escape(error) + "</span></div>") if error else ""
    return (LOGIN_HTML.replace("__LOGO_ICON__", LOGO_ICON_URI).replace("__LOGO_FULL__", LOGO_FULL_URI).replace("__ERROR__", err).replace("__USER__", _html_escape(user, quote=True))
            .replace("__USERFOCUS__", "" if user else "").replace("__PWFOCUS__", "autofocus" if user else ""))


LOGIN_HEADERS = {"Cache-Control": "no-store", "X-Frame-Options": "DENY", "Referrer-Policy": "no-referrer"}


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
<link rel="icon" href="__LOGO_ICON__">
<link rel="apple-touch-icon" href="__LOGO_ICON__">
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
  --bg:#000000;--text:#f5f7fb;--mute:rgba(235,240,250,.68);--dim:rgba(235,240,250,.45);
  --blue:#4c9bff;--cyan:#3de0f5;--gold:#f0b90b;--green:#34d399;--amber:#ffb340;--red:#ff5d73;--gray:#8b97aa;
  --g-bg:rgba(255,255,255,.075);--g-bg2:rgba(255,255,255,.12);--g-line:rgba(255,255,255,.16);--g-hi:rgba(255,255,255,.36);--g-lo:rgba(255,255,255,.05);
  --g-shadow:0 14px 44px rgba(0,0,0,.38);--line:rgba(255,255,255,.10);--card2:rgba(255,255,255,.06);--r:28px;
  --o1:transparent;--o2:transparent;--o3:transparent;--o4:transparent;--o5:transparent;
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
  background:linear-gradient(180deg,rgba(255,255,255,.10),rgba(255,255,255,.04))}
[data-theme="light"] .hero{background:radial-gradient(120% 150% at 0% 0%,rgba(10,116,255,.18),transparent 58%),radial-gradient(90% 120% at 100% 0%,rgba(250,200,60,.22),transparent 52%),var(--g-bg)}
.hero-row{position:relative;display:flex;flex-wrap:nowrap;align-items:center;justify-content:space-between;gap:12px}
.hero-row>div:first-child{min-width:0}
@media (max-width:480px){.donut{width:104px;height:104px}.donutbox{gap:12px}.legend{font-size:12.5px;gap:5px}.legend b{padding-left:10px}.bignum{font-size:54px}}
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
.av{position:relative;overflow:hidden}
.av .avimg{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;opacity:0;transition:opacity .2s}
.av.has-img{color:transparent;text-shadow:none;background:#1a2230!important}
.av.has-img .avimg{opacity:1}
.tmain{flex:1;min-width:0}
.tt{display:flex;gap:8px;align-items:baseline;min-width:0}
.tt b{font-size:16px;font-weight:750;white-space:nowrap}
.tt span{color:var(--mute);font-size:13.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ts{font-size:12px;color:var(--dim);font-family:var(--mono);margin-top:1px}
.why{font-size:12.5px;color:var(--mute);margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tr{display:flex;flex-direction:column;align-items:flex-end;gap:6px;color:var(--dim);flex:none}
.trr{display:flex;align-items:center;gap:8px}
.score{--c:var(--blue);display:inline-grid;place-items:center;min-width:32px;height:24px;padding:0 8px;border-radius:999px;font-size:12px;font-weight:800;color:var(--c);background:rgba(128,128,128,.18);background:color-mix(in srgb,var(--c) 18%,transparent);box-shadow:inset 0 1px 0 rgba(255,255,255,.25),inset 0 0 0 .5px color-mix(in srgb,var(--c) 45%,transparent)}
.tok.pick::after{content:"";position:absolute;left:0;top:12px;bottom:12px;width:3px;border-radius:0 3px 3px 0;background:var(--green);box-shadow:0 0 12px var(--green)}
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
  background:radial-gradient(130% 120% at 0% 0%,color-mix(in srgb,var(--c) 16%,transparent),transparent 62%),var(--g-bg)}
.verdict .big{position:relative;display:flex;align-items:center;gap:12px}
.verdict h2{position:relative;margin:0;font-size:26px;font-weight:800;letter-spacing:-.035em;color:var(--c)}
.verdict p{position:relative;margin:10px 0 0}
.axbtn{margin-left:auto;flex:none;align-self:flex-start;background:linear-gradient(135deg,#3b82f6,#2563eb);color:#fff!important;box-shadow:0 6px 18px rgba(59,130,246,.35)}
.axbtn::before{display:none}
.action{padding:18px 22px;border-radius:30px;margin-top:12px;background:radial-gradient(130% 120% at 0% 0%,color-mix(in srgb,var(--c) 18%,transparent),transparent 62%),var(--g-bg)}
.action .k{position:relative;font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:var(--dim);font-weight:700}
.action .a{position:relative;margin-top:4px;font-size:22px;font-weight:800;letter-spacing:-.03em;color:var(--c)}
.action p{position:relative;margin:6px 0 0;font-size:13.5px}
.board{position:relative;display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:12px 0 4px}
.board .lt{padding:12px 0;border-radius:18px;text-align:center;font-weight:800;font-size:15px;letter-spacing:.04em;color:var(--dim);border:1px solid var(--line);opacity:.55}
.board .lt.on{opacity:1;color:#fff;border-color:transparent}
.board .lt.on.buy{background:#2ebd85;animation:pbuy 1.2s ease-in-out infinite}
.board .lt.on.sell{background:#f6465d;animation:psell 1.2s ease-in-out infinite}
.board .lt.on.wait{background:#6b7686}
@keyframes pbuy{0%,100%{box-shadow:0 0 0 0 rgba(46,189,133,.7)}50%{box-shadow:0 0 0 12px rgba(46,189,133,0)}}
@keyframes psell{0%,100%{box-shadow:0 0 0 0 rgba(246,70,93,.7)}50%{box-shadow:0 0 0 12px rgba(246,70,93,0)}}
.buzz{display:inline-flex;align-items:center;gap:4px;margin-right:6px;padding:3px 9px;border-radius:999px;font-size:11px;font-weight:800;letter-spacing:.02em;color:var(--c);background:rgba(128,128,128,.14)}
.buzz.hot{--c:#ff7a45;background:rgba(255,122,69,.16)}.buzz.warm{--c:#f5a524}.buzz.quiet{--c:var(--gray)}
.buychip{display:inline-block;margin-right:6px;padding:3px 9px;border-radius:999px;background:#2ebd85;color:#fff;font-weight:800;font-size:11px;letter-spacing:.05em;animation:pbuy 1.2s ease-in-out infinite}
@media (prefers-reduced-motion:reduce){.board .lt.on,.buychip{animation:none}}
.action .sell{margin-top:10px;padding-top:10px;border-top:1px solid var(--line)}
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
.fold{margin-top:12px;overflow:hidden}
.fold>summary{list-style:none;cursor:pointer;display:flex;align-items:center;gap:10px;padding:16px 18px;font-weight:700;font-size:15px}
.fold>summary::-webkit-details-marker{display:none}
.fold>summary::after{content:"";flex:none;width:8px;height:8px;border-right:2px solid var(--dim);border-bottom:2px solid var(--dim);transform:rotate(45deg);transition:transform .25s}
.fold[open]>summary::after{transform:rotate(-135deg)}
.fold .fbadge{margin-left:auto;font-weight:500;font-size:13px}
.fold>.fb{padding:0 18px 18px}
.stat3{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:14px}
.stat3>div{padding:10px 6px;border-radius:16px;background:rgba(128,128,128,.10);text-align:center;min-width:0}
.stat3 .k{font-size:10.5px;text-transform:uppercase;letter-spacing:.06em;color:var(--dim);font-weight:700}
.stat3 .v{font-size:16px;font-weight:800;margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.bigbtn{display:flex;justify-content:center;width:100%;padding:15px 18px;font-size:16px;border-radius:18px;margin-top:14px;font-weight:800;color:#fff}
.cdn{font-size:22px;font-weight:800;font-variant-numeric:tabular-nums}
.minirow{display:flex;gap:8px;margin-top:10px}.minirow .btn{flex:1;justify-content:center;padding:11px 8px}
.csmore{display:block;width:100%;margin-top:10px;padding:12px;text-align:center;color:var(--blue);font-weight:650;font-size:14px}
.kv{position:relative;display:grid;grid-template-columns:1fr auto;gap:8px 14px;font-size:14px}
.kv dt{color:var(--mute)}.kv dd{margin:0;text-align:right;font-weight:650}
.tile{padding:16px;border-radius:28px}
.tile .l{position:relative;font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:var(--dim);font-weight:700}
.tile .v{position:relative;font-size:26px;font-weight:800;letter-spacing:-.035em;margin-top:4px}
.tile .h{position:relative;font-size:12px;color:var(--dim)}
.prow{position:relative;display:flex;align-items:center;gap:10px;padding:12px 0;border-top:1px solid var(--line)}
.prow:first-child{border-top:0}
.card.pad>*,.hero>*{position:relative}
.chartbody{position:relative;overflow:hidden;border-radius:var(--r)}
.chartframe{display:block;width:100%;height:460px;border:0;background:#000}
.chartwrap.full{position:fixed;inset:0;z-index:100;display:flex;flex-direction:column;background:var(--bg);padding:env(safe-area-inset-top) 10px calc(10px + env(safe-area-inset-bottom))}
.chartwrap.full .sec{margin:8px 6px}
.chartwrap.full>.card{flex:1;min-height:0;display:flex;flex-direction:column}
.chartwrap.full .chartbody{flex:1;min-height:0;display:flex;flex-direction:column}
.chartwrap.full .chartframe{flex:1;height:auto;min-height:0}
body.chartfull{overflow:hidden}
.chartempty{padding:34px 20px;text-align:center;color:var(--mute)}
.chartempty .eic{display:inline-grid;place-items:center;width:60px;height:60px;border-radius:50%;background:var(--card2);color:var(--dim);margin-bottom:12px;box-shadow:inset 0 1px 0 var(--g-hi)}
.chartempty b{display:block;color:var(--text);font-size:17px}
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
        <a class="ibtn" href="/logout" aria-label="Sign out" id="outbtn"></a>
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
  WATCH:"Security checks passed. The bot is now watching live trades; BUY only lights when all buy conditions are met.",
  QUALIFIED:"Security passed and live buy conditions are met right now. Re-checked every 30 seconds.",
  REVOKED:"This token no longer meets the requirements."
};
const CHECK_TITLE = {mint_permissions:"Mint & freeze permissions",token_extensions:"Token rules & extensions",holder_concentration:"Holder concentration",creator_wallet:"Creator wallet",liquidity:"Liquidity",sell_evidence:"Sell evidence",trading_manipulation:"Trading manipulation",momentum:"Momentum"};
const CHECK_HELP = {
  mint_permissions:"Can anyone create more tokens, or freeze holders' balances?",
  token_extensions:"Does the token have special rules like hidden fees or blocked transfers?",
  holder_concentration:"Do just a few wallets control most of the supply?",
  creator_wallet:"What has the wallet that launched this token been doing?",
  liquidity:"Is there real money in the pool to trade against?",
  sell_evidence:"Have other wallets actually sold on the curve, so exits work? (Observed on the live feed, not simulated.)",
  trading_manipulation:"Is the trading organic, or one wallet / round-trip wash trading / a creator dump?",
  momentum:"Is the price rising now, with real buyers, without being a vertical spike or already falling?"
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
  search:'<circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/>',
  expand:'<path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/>',
  shrink:'<path d="M9 4v5H4M15 4v5h5M9 20v-5H4M15 20v-5h5"/>',
  logout:'<path d="M9 4H5a1 1 0 0 0-1 1v14a1 1 0 0 0 1 1h4"/><path d="m16 8 4 4-4 4M20 12H9"/>',
  wallet:'<path d="M4 7.5A2.5 2.5 0 0 1 6.5 5H18v3"/><path d="M4 7.5V17a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7a2 2 0 0 0-2-2H6.5A2.5 2.5 0 0 1 4 7.5z"/><path d="M16 13.5h.01"/>'
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
  const i = document.createElement("img");
  i.src = "__LOGO_ICON__"; i.alt = "Tosh Bot"; i.width = size; i.height = size; i.decoding = "async";
  i.style.cssText = "display:block;object-fit:cover;border-radius:" + Math.round(size * 0.3) + "px";
  return i;
}
// Last good answer for every request, so a screen can be painted instantly when you come back to it.
const CACHE = new Map();
let cacheOnly = false;
async function api(path) {
  if (cacheOnly) {
    if (CACHE.has(path)) return CACHE.get(path);
    const miss = new Error("not cached yet"); miss.noCache = true; throw miss;
  }
  const ctl = new AbortController(), to = setTimeout(() => ctl.abort(), 20000);
  try {
    const r = await fetch(API + path, {cache: "no-store", signal: ctl.signal});
    if (r.status === 401) { location.href = "/login"; throw new Error("Signed out"); }
    if (!r.ok) {
      let d = "HTTP " + r.status;
      try { const j = await r.json(); if (j.detail) d = j.detail; } catch (e) {}
      const err = new Error(d); err.status = r.status; throw err;
    }
    const j = await r.json();
    CACHE.delete(path); CACHE.set(path, j);
    if (CACHE.size > 80) CACHE.delete(CACHE.keys().next().value);
    return j;
  } catch (e) {
    if (e.name === "AbortError") throw new Error("the server took too long to answer");
    throw e;
  } finally { clearTimeout(to); }
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
  if (el._v === undefined) { el._v = n; el.textContent = Number(n).toLocaleString(); return; }
  const from = el._v; el._v = n;
  if (from === n) { el.textContent = Number(n).toLocaleString(); return; }
  const t0 = performance.now();
  (function step(t) {
    const p = Math.min(1, (t - t0) / 550);
    el.textContent = Math.round(from + (n - from) * (1 - Math.pow(1 - p, 3))).toLocaleString();
    if (p < 1) requestAnimationFrame(step);
  })(t0);
}
let toastTimer;
function toast(msg, ms) {
  const t = document.getElementById("toast"); t.textContent = msg; t.classList.add("show");
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.classList.remove("show"), ms || 1600);
}
async function copyText(txt) {
  try { await navigator.clipboard.writeText(txt); toast("Copied"); }
  catch (e) { window.prompt("Copy this:", txt); }
}
const pill = (s) => h("span", {class: "pill p-" + s}, h("i"), LABEL[s] || s);
// Pictures: one download per token (de-duplicated), at most 6 at a time, kept in memory so list refreshes are instant.
const AVQ = {cache: new Map(), fail: new Map(), waiters: new Map(), queue: [], active: 0, MAX: 6};
function initialsAvatar(t, size) {
  const sym = ((t.symbol || t.name || "?") + "").replace(/[^A-Za-z0-9]/g, "").slice(0, 2).toUpperCase() || "?";
  let n = 0; for (const ch of t.mint) n = (n * 31 + ch.charCodeAt(0)) >>> 0;
  const hue = n % 360;
  return h("span", {class: "av", style: "width:" + size + "px;height:" + size + "px;font-size:" + Math.round(size * 0.36) + "px;background:linear-gradient(135deg,hsl(" + hue + " 68% 56%),hsl(" + ((hue + 45) % 360) + " 68% 38%))"}, sym);
}
function avatar(t, size) {
  // Real picture first; the coloured initials are only the fallback when it can't be fetched.
  const box = initialsAvatar(t, size);
  if (!t.uri) return box;
  const img = document.createElement("img");
  img.className = "avimg"; img.alt = ""; img.decoding = "async";
  img.addEventListener("load", () => box.classList.add("has-img"));
  img.addEventListener("error", () => { img.remove(); box.classList.remove("has-img"); });
  box.appendChild(img);
  const cached = AVQ.cache.get(t.mint);
  if (cached) { box.classList.add("has-img"); img.src = cached; return box; }
  const f = AVQ.fail.get(t.mint);
  if (f && Date.now() < f.until) return box;
  let waiting = AVQ.waiters.get(t.mint);
  if (!waiting) { waiting = new Set(); AVQ.waiters.set(t.mint, waiting); AVQ.queue.push(t.mint); setTimeout(avPump, 0); }
  waiting.add(img);
  return box;
}
function avPump() {
  while (AVQ.active < AVQ.MAX && AVQ.queue.length) {
    const mint = AVQ.queue.shift();
    const waiting = AVQ.waiters.get(mint);
    if (!waiting) continue;
    for (const im of waiting) if (!im.isConnected) waiting.delete(im);   // scrolled away / re-rendered
    if (!waiting.size) { AVQ.waiters.delete(mint); continue; }
    AVQ.active++;
    avLoad(mint).finally(() => { AVQ.active--; avPump(); });
  }
}
async function avLoad(mint) {
  try {
    const r = await fetch(API + "/tokens/" + encodeURIComponent(mint) + "/image");
    if (!r.ok) throw new Error("no image " + r.status);
    const url = URL.createObjectURL(await r.blob());
    AVQ.cache.set(mint, url); AVQ.fail.delete(mint);
    if (AVQ.cache.size > 1000) { const [k, u] = AVQ.cache.entries().next().value; AVQ.cache.delete(k); setTimeout(() => URL.revokeObjectURL(u), 30000); }
    for (const im of AVQ.waiters.get(mint) || []) if (im.isConnected) im.src = url;
  } catch (e) {
    const n = ((AVQ.fail.get(mint) || {}).n || 0) + 1;
    AVQ.fail.set(mint, {n, until: Date.now() + [15000, 45000, 120000, 300000][Math.min(n - 1, 3)]});
  } finally { AVQ.waiters.delete(mint); }
}
function scoreChip(sc) {
  if (sc == null) return null;
  const c = sc >= 70 ? "#34d399" : sc >= 50 ? "#4c9bff" : "#ffb340";
  return h("span", {class: "score", style: "--c:" + c, title: "Partial evidence score, not a prediction"}, Math.round(sc));
}
function buzzChip(t) {
  const b = t && t.buzz;
  if (!b || b.score == null || b.level === "quiet") return null;
  return h("span", {class: "buzz " + b.level, title: "Popularity " + Math.round(b.score) + "/100. Attention, not safety and not a prediction."}, b.level === "hot" ? "Hot " + Math.round(b.score) : "Buzz " + Math.round(b.score));
}
function tokRow(t, withReason, extra) {
  return h("a", {class: "tok" + (extra ? " " + extra : ""), href: "#/token/" + t.mint}, avatar(t, 44),
    h("div", {class: "tmain"},
      h("div", {class: "tt"}, h("b", {}, t.symbol || "—"), h("span", {}, t.name || "Unnamed")),
      h("div", {class: "ts"}, short(t.mint, 5) + " · " + ago(t.detected_at)),
      withReason && t.status_reason ? h("div", {class: "why"}, t.status_reason) : null),
    h("div", {class: "tr"}, h("div", {class: "trr"}, t.status === "QUALIFIED" && t.status_updated_at && (Date.now() - new Date(t.status_updated_at).getTime()) < 180000 ? h("span", {class: "buychip"}, "BUY") : null, buzzChip(t), scoreChip(t.opportunity_score), pill(t.status)), icon("chev", 16)));
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
  const cPassed = mkChip("Top picks", "passed", "#3b82f6", "shield"), cReview = mkChip("In review", "review", "#f5a524", "eye"), cRej = mkChip("Rejected", "rejected", "#f6465d", "ban");
  const chart = h("div"), latest = h("div"), sys = h("div"), picks = h("div"), cup = h("div");
  latest.append(skeleton(5)); picks.append(skeleton(2)); cup.append(skeleton(2));
  const root = h("div", {},
    h("section", {class: "hero"},
      h("div", {class: "hero-row"},
        h("div", {}, h("span", {class: "live"}, h("span", {class: "dot ok"}), "Live scan · Solana"), num, h("div", {style: "font-weight:600"}, "tokens detected"), rate),
        h("div", {class: "donutbox"}, donutBox, legend))),
    h("div", {class: "grid3", style: "margin-top:14px"}, cPassed, cReview, cRej),
    sec("Cupsey wallet", "Open", "#/cupsey"), cup,
    sec("Top picks right now", "See all", "#/passed"), picks,
    h("div", {class: "grid2", style: "margin-top:2px"},
      h("div", {}, sec("Launches per minute"), h("div", {class: "card pad"}, chart)),
      h("div", {}, sec("System", "Details", "#/system"), h("div", {class: "card pad"}, sys))),
    sec("Latest launches", "See all", "#/scanner"), latest);
  async function refresh() {
    api("/cupsey").then((d) => cup.replaceChildren(cupseyStrip(d))).catch((e) => { if (!e.noCache && !cup.querySelector(".tok")) cup.replaceChildren(empty("wallet", "Cupsey monitor unavailable", "Open the Cupsey tab for details.")); });   // never blocks the rest of Home
    const [s, recent, hl, pk] = await Promise.all([api("/stats"), api("/tokens/recent?limit=200"), api("/system/health"), api("/tokens/recent?limit=50&status=WATCH,QUALIFIED&sort=hot")]);
    const fresh = pk.filter((t) => Date.now() - new Date(t.detected_at).getTime() < 2 * 3600 * 1000).slice(0, 3);
    picks.replaceChildren(fresh.length ? h("div", {class: "card list"}, fresh.map((t) => tokRow(t, false, "pick"))) : empty("shield", "No picks in the last 2 hours", "A pick is a token that passed every security check built so far, ranked by evidence score plus popularity. Not a buy signal."));
    const c = s.counts;
    setNum(num, s.total);
    setNum(cPassed._n, c.WATCH + c.QUALIFIED); setNum(cReview._n, c.INSUFFICIENT_DATA + c.HOLD); setNum(cRej._n, c.REJECTED + c.REVOKED);
    rate.textContent = recent.length ? "Last launch " + ago(recent[0].detected_at) : "Waiting for the first launch…";
    donutBox.replaceChildren(donut(c, s.total));
    legend.replaceChildren(...ORDER.filter((k) => c[k] > 0).map((k) => h("div", {}, h("i", {style: "background:" + COLOR[k]}), (k === "INSUFFICIENT_DATA" ? "Low data" : LABEL[k]), h("b", {}, c[k].toLocaleString()))));
    chart.replaceChildren(recent.length ? areaChart(recent) : h("div", {class: "dim", style: "padding:40px 0;text-align:center"}, "No launches yet"));
    latest.replaceChildren(recent.length ? h("div", {class: "card list"}, recent.slice(0, 8).map((t) => tokRow(t, false))) : empty("radar", "Nothing detected yet", "New launches appear here within seconds once the feed is connected."));
    const rows = Object.values(hl.providers).map((p) => h("div", {class: "prow"}, h("span", {class: "dot " + dotState(p.effective_status)}), h("div", {style: "flex:1;font-weight:600"}, p.name), h("span", {class: "dim", style: "font-size:12.5px"}, p.effective_status + " · " + ago(p.last_success_at))));
    rows.push(h("div", {class: "prow"}, h("span", {class: "dot " + (hl.database_reachable ? "ok" : "bad")}), h("div", {style: "flex:1;font-weight:600"}, "database"), h("span", {class: "dim", style: "font-size:12.5px"}, hl.database_reachable ? "reachable" : "unreachable")));
    sys.replaceChildren(...rows);
  }
  return {root, refresh, every: 5000};
}

function listView(o) {
  let all = [], q = "", group = o.group || "all", win = o.windows ? 120 : 0;
  const list = h("div"), chips = h("div", {class: "filters"});
  list.append(skeleton(7));
  const input = h("input", {type: "search", placeholder: "Search name, symbol, mint or creator", autocomplete: "off", autocapitalize: "off", spellcheck: "false", oninput: (e) => { q = e.target.value.trim().toLowerCase(); draw(); }});
  const bar = (o.search || o.windows) ? h("div", {class: "searchbar"}, o.search ? h("label", {class: "search"}, icon("search", 18), input) : null, chips) : null;
  const count = (g) => GROUPS[g] ? all.filter((t) => GROUPS[g].includes(t.status)).length : all.length;
  function drawChips() {
    if (o.windows) { chips.replaceChildren(...[[30, "30 min"], [120, "2 hours"], [360, "6 hours"], [0, "All"]].map(([m, lbl]) => h("button", {class: "fchip" + (win === m ? " on" : ""), onclick: () => { win = m; draw(); }}, lbl))); return; }
    if (!o.search) return;
    chips.replaceChildren(...[["all", "All"], ["review", "In review"], ["passed", "Passed"], ["rejected", "Rejected"]].map(([g, lbl]) =>
      h("button", {class: "fchip" + (group === g ? " on" : ""), onclick: () => { group = g; draw(); }}, lbl, h("em", {}, count(g)))));
  }
  function draw() {
    drawChips();
    const rows = all.filter((t) => (!GROUPS[group] || GROUPS[group].includes(t.status)) && (!q || [t.name, t.symbol, t.mint, t.creator].some((v) => v && v.toLowerCase().includes(q))) && (!win || Date.now() - new Date(t.detected_at).getTime() < win * 60000));
    list.replaceChildren(rows.length ? h("div", {class: "card list"}, rows.map((t) => tokRow(t, !o.windows, o.windows ? "pick" : null))) : (all.length ? empty("search", "No matches", "Try a different search or filter.") : empty(o.icon, o.emptyTitle, o.emptyText)));
  }
  const root = h("div", {}, h("h1", {class: "large"}, o.title), h("p", {class: "sub"}, o.subtitle), bar, list);
  async function refresh() { all = await api(o.url); draw(); }
  return {root, refresh, every: 5000};
}

function systemView() {
  const tiles = h("div", {class: "grid3"}), provs = h("div"), counters = h("div");
  const root = h("div", {}, h("h1", {class: "large"}, "System health"), h("p", {class: "sub"}, "Connections, queues and counters since the backend last started."), tiles,
    sec("Providers"), h("div", {class: "card pad"}, provs), sec("Counters"), h("div", {class: "card pad"}, counters));
  const NAMES = {received:"Messages received",parsed:"Launches parsed",ignored:"Ignored (non-launch)",parse_errors:"Parse errors",dropped_queue_full:"Dropped (queue full)",stored:"New tokens stored",duplicates:"Duplicates skipped",failed_jobs:"Failed storage jobs",analyses:"Analyses run",failed_analyses:"Failed analyses",throttled:"Skipped (daily limit)"};
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

function evidenceCard(o) {
  const c = o.score >= 70 ? "#34d399" : o.score >= 50 ? "#4c9bff" : "#ffb340";
  return h("div", {class: "card pad", style: "margin-top:12px"},
    h("div", {style: "display:flex;align-items:baseline;justify-content:space-between"}, h("b", {}, "Evidence score"), h("span", {class: "dim", style: "font-size:12px"}, (o.complete ? "complete · includes live trade data" : "partial · " + o.available_weight + "% of planned inputs"))),
    h("div", {style: "font-size:44px;font-weight:800;letter-spacing:-.04em;color:" + c}, o.score.toFixed(1), h("span", {class: "dim", style: "font-size:16px;font-weight:600"}, " / 100")),
    o.components.map((x) => h("div", {style: "margin:12px 0"},
      h("div", {class: "hrow"}, h("span", {}, x.name.replace(/_/g, " ") + " (weight " + x.weight + ")"), h("b", {style: "color:var(--text)"}, x.score.toFixed(0))),
      h("div", {class: "stack", style: "height:8px;margin:4px 0"}, h("span", {style: "width:" + Math.max(1, x.score) + "%;background:" + c})),
      h("div", {class: "dim", style: "font-size:12px"}, x.detail))),
    h("p", {class: "dim", style: "font-size:12px;margin:10px 0 0"}, (o.missing.length ? "Not scored yet (never guessed): " + o.missing.join(", ") + ". " : "") + o.note));
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

function chartCard(mint) {
  const body = h("div", {class: "chartbody"});
  function load() {
    const th = document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
    const url = "https://dexscreener.com/solana/" + encodeURIComponent(mint) + "?embed=1&loadChartSettings=0&trades=0&chartStyle=1&chartLeftToolbar=0&chartType=usd&interval=5&chartDefaultOnMobile=1&theme=" + th + "&chartTheme=" + th;
    body.replaceChildren(
      h("iframe", {class: "chartframe", src: url, title: "Price chart", loading: "eager", referrerpolicy: "no-referrer", sandbox: "allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"}),
      h("div", {style: "display:flex;align-items:center;justify-content:space-between;gap:10px;margin:10px 16px 14px"},
        h("p", {class: "dim", style: "font-size:12px;margin:0"}, "Chart by DEX Screener. Brand-new tokens can take a minute or two to appear. If it says not found, the token has no market yet."),
        h("button", {class: "btn", onclick: load, style: "flex:none"}, icon("pulse", 16), "Reload")));
  }
  load();  // loads automatically as soon as the token screen opens
  const wrap = h("div", {class: "chartwrap"});
  const btn = h("button", {class: "ibtn", "aria-label": "Fullscreen chart", title: "Fullscreen"}, icon("expand", 20));
  function setFull(on) {
    wrap.classList.toggle("full", on);        // CSS only, so the iframe is never reloaded
    document.body.classList.toggle("chartfull", on);
    btn.replaceChildren(icon(on ? "shrink" : "expand", 20));
    btn.setAttribute("aria-label", on ? "Exit fullscreen" : "Fullscreen chart");
    if (on) window.scrollTo(0, 0);
  }
  btn.addEventListener("click", () => setFull(!wrap.classList.contains("full")));
  const onKey = (e) => { if (e.key === "Escape" && wrap.classList.contains("full")) setFull(false); };
  document.addEventListener("keydown", onKey);
  window.addEventListener("hashchange", () => { document.body.classList.remove("chartfull"); document.removeEventListener("keydown", onKey); }, {once: true});
  wrap.append(h("div", {class: "sec"}, h("h2", {}, "Price chart"), btn), h("div", {class: "card"}, body));
  return wrap;
}

const axiomUrl = (t) => t.bonding_curve ? "https://axiom.trade/meme/" + t.bonding_curve : "https://axiom.trade/t/" + t.mint;

// Plain-language action derived ONLY from the verdict. The bot has no price/momentum/sell-simulation data yet,
// so it never says "buy now" for WATCH; only QUALIFIED is a buy candidate. It does not track your positions.
function actionFor(status, hist, rep) {
  const r = actionBase(status, hist);
  if (status === "QUALIFIED" && !(rep && (Date.now() - new Date(rep.analyzed_at).getTime()) < 180000)) {
    return {light: "wait", c: "#6b7686", a: "WAIT: signal expired", why: "The last live check of this token is more than 3 minutes old, so the BUY signal is no longer trusted. Re-check on Axiom.", sell: null};
  }
  const prev = hist && hist.length > 1 ? hist[1].status : null;
  if (prev && ["WATCH", "QUALIFIED"].includes(prev) && ["REJECTED", "REVOKED", "HOLD"].includes(status)) r.light = "sell";
  return r;
}
function actionBase(status, hist) {
  const prev = hist && hist.length > 1 ? hist[1].status : null;
  const downgraded = prev && ["WATCH", "QUALIFIED"].includes(prev) && ["REJECTED", "REVOKED", "HOLD"].includes(status);
  const SELL_BAD = downgraded
    ? "Downgraded: this token was " + LABEL[prev] + " earlier and is now " + LABEL[status] + ". Do not buy."
    : null;
  switch (status) {
    case "QUALIFIED": return {light: "buy", c: "#2ebd85", a: "BUY CANDIDATE", why: "Security passed and the live buy conditions are met right now (real buyers, sells working, price rising, no manipulation seen). Check the chart on Axiom before you buy. This is a signal, not a guarantee.", sell: SELL_BAD};
    case "WATCH": return {light: "wait", c: "#3b82f6", a: "WAIT: not a buy yet", why: prev === "QUALIFIED" ? "The buy window has closed: it was a BUY signal earlier but the live conditions no longer all hold. Do not chase it." : "Security checks passed. The bot is watching live trades and lights BUY only when momentum, real buyers and sell evidence all line up. See Security checks below for what is still missing.", sell: SELL_BAD};
    case "HOLD": return {light: "wait", c: "#f5a524", a: "WAIT: do not buy", why: "Something suspicious is unresolved.", sell: SELL_BAD};
    case "REJECTED": case "REVOKED": return {light: "sell", c: "#f6465d", a: "AVOID: do not buy", why: "A verified serious problem was found.", sell: SELL_BAD};
    default: return {light: "wait", c: "#6b7686", a: "WAIT: not enough data", why: "The security engine has not finished. Do not buy yet.", sell: null};
  }
}

function buzzCard(b) {
  const link = (k, label) => b.links && b.links[k] ? h("a", {class: "btn", href: b.links[k], target: "_blank", rel: "noopener noreferrer"}, label, icon("ext", 14)) : null;
  return h("section", {class: "card pad", style: "margin-top:14px"},
    h("div", {style: "display:flex;align-items:center;gap:10px;flex-wrap:wrap"}, h("b", {}, "Popularity"), buzzChip({buzz: b}) || h("span", {class: "buzz quiet"}, "Quiet " + Math.round(b.score)),
      h("span", {class: "dim", style: "font-size:12.5px"}, "data coverage " + Math.round((b.coverage || 0) * 100) + "%")),
    h("div", {style: "margin-top:10px"}, (b.components || []).map((c) => h("div", {style: "margin:8px 0"},
      h("div", {style: "display:flex;justify-content:space-between;font-size:13px"}, h("span", {}, c.name), h("b", {}, Math.round(c.score))),
      h("div", {class: "dim", style: "font-size:12px"}, c.detail)))),
    (b.warnings || []).length ? h("ul", {class: "warns"}, b.warnings.map((x) => h("li", {}, x))) : null,
    h("div", {style: "display:flex;gap:8px;flex-wrap:wrap;margin-top:8px"}, link("x", "X"), link("telegram", "Telegram"), link("website", "Website")),
    h("p", {class: "dim", style: "font-size:12px;margin:10px 0 0"}, b.note || ""));
}
function tokenView(mint) {
  const top = h("div"), bottom = h("div");
  top.append(skeleton(3));
  const chart = chartCard(mint);   // built once and never re-rendered, so the chart does not reload every refresh
  const root = h("div", {}, h("a", {href: "#/scanner", class: "back", onclick: (e) => { if (navDepth > 1) { e.preventDefault(); history.back(); } }}, icon("back", 18), "Back"), top, chart, bottom);
  let lastKey = "";
  async function refresh() {
    const t = await api("/tokens/" + encodeURIComponent(mint));
    let r = null, hist = [], bz = null;
    try { bz = (await api("/tokens/" + encodeURIComponent(mint) + "/buzz")).buzz; } catch (e) { /* popularity is optional */ }
    try { r = await api("/tokens/" + encodeURIComponent(mint) + "/security"); hist = await api("/tokens/" + encodeURIComponent(mint) + "/security/history?limit=20"); }
    catch (e) { if (e.status !== 404) throw e; }
    const key = t.status + "|" + (r ? r.analyzed_at : "none") + "|" + hist.length + "|" + (bz ? bz.updated_at : "none");
    if (key === lastKey) return;   // nothing new: leave the page (and any opened sections) untouched
    lastKey = key;
    const fact = (k, v, copy) => h("div", {class: "fact"}, h("div", {class: "k"}, k), h("div", {class: "v" + (copy ? " mono" : "")}, v));
    top.replaceChildren(...[
      h("div", {class: "thead"}, avatar(t, 58), h("div", {style: "min-width:0"}, h("h1", {}, t.symbol || "Token"), h("div", {class: "mute"}, t.name || "Unnamed"),
        h("button", {class: "copy", onclick: () => copyText(t.mint)}, short(t.mint, 8), icon("copy", 13))),
        h("a", {class: "btn axbtn", href: axiomUrl(t), target: "_blank", rel: "noopener noreferrer"}, "Axiom", icon("ext", 15))),
      h("section", {class: "verdict", style: "--c:" + COLOR[t.status]},
        h("div", {class: "big"}, pill(t.status), h("span", {class: "dim", style: "font-size:12.5px"}, "Security verdict")),
        h("h2", {style: "margin-top:12px"}, LABEL[t.status]), h("p", {class: "mute", style: "margin-top:4px"}, STATUS_HELP[t.status] || ""),
        h("p", {}, r ? r.reason : t.status_reason),
        r && r.warnings.length ? h("ul", {class: "warns"}, r.warnings.map((x) => h("li", {}, x))) : null,
        r ? h("p", {class: "dim", style: "font-size:12px"}, "Analysed " + ago(r.analyzed_at) + " · logic " + r.analysis_version + (r.next_review_at ? " · next review " + fmt(r.next_review_at) : "")) : null),
      (() => { const a = actionFor(t.status, hist, r); return h("section", {class: "action", style: "--c:" + a.c},
        h("div", {class: "k"}, "Signal board"),
        h("div", {class: "board"}, [["buy", "BUY"], ["wait", "WAIT"], ["sell", "AVOID"]].map(([k, l]) => h("div", {class: "lt " + k + (a.light === k ? " on" : "")}, l))),
        h("div", {class: "a"}, a.a), h("p", {class: "mute"}, a.why),
        a.sell ? h("p", {class: "sell", style: "font-weight:650"}, a.sell) : null,
        h("p", {class: "dim", style: "font-size:12px"}, "BUY lights only when every live condition holds right now and goes off the moment one fails. AVOID lights for rejected or suspicious tokens. It is a signal from public trade data, not a guarantee. Not financial advice.")); })(),
      r && r.opportunity ? evidenceCard(r.opportunity) : null,
      bz ? buzzCard(bz) : null,
      h("div", {class: "facts"}, fact("Detected", fmt(t.detected_at)), fact("Launch time", t.launch_timestamp ? fmt(t.launch_timestamp) : "Not supplied"), fact("Creator", short(t.creator, 6), true), fact("Bonding curve", short(t.bonding_curve, 6), true)),
      h("div", {class: "btns"},
        h("a", {class: "btn", href: axiomUrl(t), target: "_blank", rel: "noopener noreferrer"}, "Axiom", icon("ext", 15)),
        h("a", {class: "btn", href: "https://solscan.io/token/" + t.mint, target: "_blank", rel: "noopener noreferrer"}, "Solscan", icon("ext", 15)),
        h("a", {class: "btn", href: "https://pump.fun/coin/" + t.mint, target: "_blank", rel: "noopener noreferrer"}, "Pump.fun", icon("ext", 15))),
    ].filter(Boolean));
    bottom.replaceChildren(...[
      sec("Security checks"),
      r ? h("div", {class: "card list"}, r.checks.map(checkRow)) : empty("pulse", "Analysis has not completed yet", "The security engine runs shortly after detection. This page refreshes automatically."),
      r && r.not_evaluated.length ? [sec("Not checked yet"), h("div", {class: "card pad"}, h("p", {class: "dim", style: "margin:0 0 8px;font-size:13px"}, "These checks do not exist yet. “Not checked” is never the same as “passed”."), h("ul", {class: "mute", style: "margin:0;padding-left:18px;font-size:13.5px"}, r.not_evaluated.map((x) => h("li", {}, x))))] : null,
      hist.length > 1 ? [sec("Analysis history"), h("div", {class: "card list"}, hist.map((x) => h("div", {class: "tok"}, h("div", {class: "tmain mute"}, fmt(x.analyzed_at)), pill(x.status))))] : null,
    ].flat().filter(Boolean));
  }
  return {root, refresh, every: 8000};
}

/* ---------- Cupsey wallet monitor (read-only; data from /dashboard/api/cupsey) ---------- */
const CS_STATUS = {UNVERIFIED: ["Unverified", "var(--gray)"], POSSIBLY_ASSOCIATED: ["Possibly associated", "var(--amber)"], STRONGLY_SUPPORTED: ["Strongly supported", "var(--blue)"], CONFIRMED: ["Confirmed", "var(--green)"], RETIRED: ["Retired", "var(--red)"]};
const CS_REASON = {referenced_only: "only listed inside other people's transactions", sol_received_unsigned: "SOL received from others (dust)", token_in_unsigned: "tokens received without paying (airdrops)", token_out_unsigned: "tokens moved out by someone else", no_value_movement: "nothing moved", sol_received: "SOL received", sol_sent: "SOL sent, no trade", wsol_wrap_unwrap: "SOL wrapped or unwrapped", token_to_token_swap: "token-to-token swaps", unsigned_swap_shape: "swap-like moves the wallet did not sign", ambiguous_sol_movement: "unclear SOL movement", swap_sol_to_token: "buys", swap_token_to_sol: "sells", token_in: "tokens received", token_out: "tokens sent", failed_tx: "failed transactions", wallet_not_involved: "unrelated transactions"};
function csAgo(ts, now) {
  if (!ts) return "never";
  const s = Math.max(0, Math.round(now - ts));
  if (s < 90) return s + "s ago"; if (s < 5400) return Math.floor(s / 60) + "m ago";
  if (s < 172800) return Math.floor(s / 3600) + "h ago"; return Math.floor(s / 86400) + "d ago";
}
const csSol = (v) => (Number(v) || 0).toFixed(3);
const csLink = (kind, id) => "https://solscan.io/" + kind + "/" + encodeURIComponent(id || "");
const csTile = (l, v, hint) => h("div", {class: "tile"}, h("div", {class: "l"}, l), h("div", {class: "v"}, v), hint ? h("div", {class: "h"}, hint) : null);
const csPrice = (v) => (v == null || !isFinite(v)) ? "n/a" : Number(v).toPrecision(3);
const csCoin = (x) => x.symbol ? "$" + x.symbol : short(x.mint, 4);
const csClock = (ts) => new Date(ts * 1000).toLocaleTimeString([], {hour: "2-digit", minute: "2-digit", second: "2-digit"});
function csDur(sec) { sec = Math.max(0, Math.round(sec)); return sec < 60 ? sec + "s" : sec < 3600 ? Math.floor(sec / 60) + "m " + String(sec % 60).padStart(2, "0") + "s" : Math.floor(sec / 3600) + "h " + String(Math.floor(sec % 3600 / 60)).padStart(2, "0") + "m"; }
function csPlanTable(rows, withPrices) {
  return h("dl", {class: "kv"}, rows.flatMap((r) => [h("dt", {}, r.step), h("dd", {}, r.rule + (withPrices && r.price ? "  (around " + csPrice(r.price) + " SOL per token)" : ""))]));
}
const csOpen = {}, csShow = {trades: false};
const csStat = (k, v, color) => h("div", {}, h("div", {class: "k"}, k), h("div", {class: "v", style: color ? "color:" + color : null}, v));
function csFold(key, title, badge, body, openDefault) {
  const open = key in csOpen ? csOpen[key] : !!openDefault;
  return h("details", {class: "card fold", open: open, ontoggle: (e) => { csOpen[key] = e.target.open; }},
    h("summary", {}, h("span", {}, title), badge ? h("span", {class: "dim fbadge"}, badge) : null), h("div", {class: "fb"}, body));
}
const csExt = (cls, href, label, style) => h("a", {class: cls, style: style || null, href, target: "_blank", rel: "noopener noreferrer"}, label, icon("ext", 15));
function csExitCard(x, now) {
  const good = x.kind === "TAKING PROFIT", col = good ? "var(--green)" : "var(--red)";
  return h("div", {class: "card pad", style: "border-left:4px solid " + col},
    h("div", {style: "display:flex;align-items:center;justify-content:space-between;gap:10px"},
      h("div", {style: "min-width:0"}, h("div", {style: "font-weight:800;font-size:20px"}, csCoin(x)), x.name ? h("div", {class: "mute", style: "font-size:13px"}, x.name) : null),
      h("span", {class: "pill", style: "--c:" + col}, h("i"), x.kind)),
    h("div", {style: "margin-top:12px;font-weight:800;font-size:17px;line-height:1.3"}, x.action),
    h("div", {class: "stat3"}, csStat("He sold", csSol(x.sol) + " SOL"), csStat("His gain", x.gain_pct != null ? (x.gain_pct >= 0 ? "+" : "") + x.gain_pct + "%" : "n/a", col), csStat("When", csAgo(x.time, now))),
    csExt("btn bigbtn", "https://pump.fun/coin/" + x.mint, "Sell on Pump.fun", "background:" + col),
    h("div", {class: "minirow"}, csExt("btn", "https://dexscreener.com/solana/" + x.mint, "Chart"), h("button", {class: "btn", onclick: () => { copyText(x.mint); toast("Coin address copied", 1600); }}, "Copy address", icon("copy", 15))),
    h("p", {class: "dim", style: "font-size:12px;margin:10px 0 0;text-align:center"}, "Do it fast. His selling pushes the price down."));
}
function csEntryCard(d, skew) {
  const nc = d.now_card, e = nc.entry, closes = e.closes_at;
  const cd = h("span", {class: "cdn"}, "");
  const tick = () => { const left = closes - (Date.now() + skew) / 1000; cd.textContent = left > 0 ? csDur(left) : "CLOSED"; cd.style.color = left > 0 ? "var(--green)" : "var(--red)"; return left; };
  tick();
  const card = h("div", {class: "card pad", style: "border-left:4px solid var(--green)"},
    h("div", {style: "display:flex;align-items:center;justify-content:space-between;gap:10px"},
      h("div", {style: "min-width:0"}, h("div", {style: "font-weight:800;font-size:20px"}, csCoin(nc)), nc.name ? h("div", {class: "mute", style: "font-size:13px"}, nc.name) : null),
      h("div", {style: "text-align:right"}, h("span", {class: "pill", style: "--c:var(--green)"}, h("i"), "ENTRY OPEN"), h("div", {style: "margin-top:4px"}, cd))),
    h("div", {class: "stat3"}, csStat("He bought", csSol(nc.sol) + " SOL"), csStat("Price vs his", e.price_known ? (e.move_pct >= 0 ? "+" : "") + e.move_pct + "%" : "n/a"), csStat("Your size", nc.size ? nc.size + " SOL" : "see plan")),
    csExt("btn bigbtn", "https://pump.fun/coin/" + nc.mint, "Open on Pump.fun", "background:var(--green)"),
    h("div", {class: "minirow"}, csExt("btn", "https://dexscreener.com/solana/" + nc.mint, "Chart"), csExt("btn", axiomUrl({mint: nc.mint}), "Axiom"),
      h("button", {class: "btn", onclick: () => { copyText(nc.mint); toast("Coin address copied", 1600); }}, "Copy", icon("copy", 15))),
    h("p", {class: "dim", style: "font-size:12.5px;margin:12px 0 0"}, "Enter between " + csClock(nc.time) + " and " + csClock(closes) + " only" + (e.price_known ? "." : ". Live price unavailable, go by the clock.")),
    csFold("plan", "Plan: stop, take profit, exits", "", csPlanTable(nc.plan, true), false));
  const iv = setInterval(() => { if (!card.isConnected) { clearInterval(iv); return; } if (tick() <= 0) clearInterval(iv); }, 1000);
  return card;
}
function csIdleCard(d) {
  const last = d.stats.last_trade;
  return h("div", {class: "card pad", style: "display:flex;align-items:center;gap:12px"}, h("span", {class: "dot"}),
    h("div", {}, h("div", {style: "font-weight:750"}, "No entry window open"),
      h("div", {class: "dim", style: "font-size:13px;margin-top:2px"}, last ? "His last trade was " + csAgo(last, d.now) + ". Don't chase old trades. Wait for the next alert." : "Waiting for his next trade.")));
}
function csRightNow(d, skew) {
  const cards = (d.exit_cards || []).map((x) => csExitCard(x, d.now));
  if (d.now_card) cards.push(csEntryCard(d, skew));
  if (!cards.length) cards.push(csIdleCard(d));
  return h("div", {style: "display:grid;gap:12px"}, cards);
}
function csPlaybook(pb) {
  const c = pb.cfg;
  return h("div", {},
    csPlanTable(pb.plan, false),
    h("div", {style: "margin:16px 0 8px;font-weight:700"}, "Buy size by bankroll"),
    h("dl", {class: "kv"}, pb.sizes.flatMap((r) => [h("dt", {}, r.bankroll + " SOL"), h("dd", {}, "buy " + r.buy + " · max loss " + r.max_loss)])),
    h("p", {class: "dim", style: "font-size:12.5px;margin:12px 0 0"}, "Risk " + c.risk_pct + "% per trade, stop " + c.stop_pct + "%, max " + c.max_position_sol + " SOL. " + (pb.hold && pb.hold.median_s ? "His typical hold so far: " + csDur(pb.hold.median_s) + " (" + pb.hold.n + " closed trades)." : "No closed trades of his yet, so the 30 minute time stop is a default.")),
    h("p", {class: "dim", style: "font-size:12px;margin:8px 0 0"}, "Change these in Render: CUPSEY_ENTRY_WINDOW_SECONDS, CUPSEY_MAX_CHASE_PCT, CUPSEY_STOP_PCT, CUPSEY_TP1_PCT, CUPSEY_TP2_PCT, CUPSEY_TIME_STOP_MINUTES, CUPSEY_BANKROLL_SOL, CUPSEY_RISK_PCT, CUPSEY_MAX_POSITION_SOL. Start tiny. Not financial advice."));
}
function csTradeRow(x, now) {
  const sell = String(x.cls).indexOf("SELL") >= 0;
  const label = String(x.cls).toLowerCase().replace(/\b\w/g, (m) => m.toUpperCase());
  const more = [];
  if (sell && x.pct != null) more.push(Math.round(x.pct) + "% of position");
  if (sell && x.pnl != null) more.push((x.pnl >= 0 ? "+" : "") + Number(x.pnl).toFixed(3) + " SOL result");
  return h("a", {class: "tok", href: csLink("tx", x.sig), target: "_blank", rel: "noopener noreferrer"},
    h("span", {class: "pill", style: "--c:" + (sell ? "var(--red)" : "var(--green)")}, h("i"), label),
    h("div", {class: "tmain"},
      h("div", {class: "tt"}, h("b", {}, csCoin(x)), h("span", {}, csSol(x.sol) + " SOL" + (x.big ? " ★" : ""))),
      h("div", {class: "ts"}, csAgo(x.time, now) + (more.length ? " · " + more.join(" · ") : "")),
      x.entry ? h("div", {class: "ts", style: "color:" + (x.entry.state === "OPEN" ? "var(--green)" : "var(--gray)")}, x.entry.state === "OPEN" ? "Entry window open (" + csDur(x.entry.closes_at - now) + " left)" : "Entry closed: " + x.entry.reason) : (sell && x.exit ? h("div", {class: "ts", style: "color:" + (x.exit.kind === "TAKING PROFIT" ? "var(--green)" : "var(--red)")}, x.exit.kind + (x.exit.gain_pct != null ? " (" + (x.exit.gain_pct >= 0 ? "+" : "") + x.exit.gain_pct + "% vs his cost)" : "") + ". " + x.exit.action) : null)),
    h("div", {class: "tr"}, icon("ext", 15)));
}
function csPosRow(p, now) {
  const n = (c, w) => c + " " + w + (c === 1 ? "" : "s");
  return h("a", {class: "tok", href: csLink("token", p.mint), target: "_blank", rel: "noopener noreferrer"},
    h("div", {class: "tmain"},
      h("div", {class: "tt"}, h("b", {}, csCoin(p)), h("span", {}, "cost " + csSol(p.cost_sol) + " SOL")),
      h("div", {class: "ts"}, "bought " + csSol(p.bought_sol) + " SOL · " + n(p.buys, "buy") + (p.sells ? " · " + n(p.sells, "sell") : "") + " · opened " + csAgo(p.opened_at, now)),
      h("div", {class: "ts", style: "color:" + (p.entry_open ? "var(--green)" : "var(--gray)")}, (p.entry_open ? "Entry window open" : "He is holding. Entry closed. Exit when he sells.")
        + (p.price_now && p.avg_price ? " · price vs his avg " + (p.price_now / p.avg_price >= 1 ? "+" : "") + ((p.price_now / p.avg_price - 1) * 100).toFixed(0) + "%" : ""))),
    h("div", {class: "tr"}, icon("ext", 15)));
}
function cupseyView() {
  const headRow = h("div", {style: "display:flex;align-items:center;justify-content:space-between;gap:12px"}), walletRow = h("div", {style: "margin-top:6px"});
  const hero = h("div", {style: "margin-top:16px"}), tiles = h("div", {class: "grid3", style: "margin-top:14px"}), tradeBox = h("div"), posBox = h("div"), foldBox = h("div");
  const tradeSec = h("div", {}, sec("Latest trades"), tradeBox), posSec = h("div", {}, sec("He is holding"), posBox);
  hero.append(skeleton(1)); tradeBox.append(skeleton(3));
  const root = h("div", {}, headRow, walletRow, hero, tiles, tradeSec, posSec, foldBox);
  async function refresh() {
    const d = await api("/cupsey"), w = d.wallet, now = d.now;
    headRow.replaceChildren(h("h1", {class: "large", style: "margin:0"}, "Cupsey"),
      h("span", {class: "pill", style: "--c:" + (d.running && d.live ? "var(--green)" : "var(--amber)")}, h("i"), d.running ? (d.live ? "Live · " + csAgo(d.poll.last_ok, now).replace(" ago", "") : "Not reading") : "Off"));
    walletRow.replaceChildren(h("div", {style: "display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:13px;color:var(--mute)"}, "Tracked wallet", h("code", {}, short(w.address, 6)),
      h("button", {class: "ibtn", "aria-label": "Copy wallet address", onclick: () => { copyText(w.address); toast("Address copied", 1600); }}, icon("copy", 16)),
      h("a", {href: csLink("account", w.address), target: "_blank", rel: "noopener noreferrer", style: "color:var(--blue);font-weight:650"}, "Solscan")),
      d.running && d.poll.error ? h("p", {style: "color:var(--red);font-size:13px;margin:8px 0 0"}, "Problem reading the blockchain: " + d.poll.error) : null,
      !d.running ? h("p", {class: "dim", style: "font-size:13px;margin:8px 0 0"}, "Tracker is not running. Check that CUPSEY_TRACKING is not set to false, then redeploy.") : null);
    for (const x of [tiles, tradeSec, posSec, foldBox]) x.style.display = d.running ? "" : "none";
    hero.replaceChildren();
    if (!d.running) return;
    const s = d.stats;
    hero.replaceChildren(csRightNow(d, d.now * 1000 - Date.now()));
    tiles.replaceChildren(csTile("Buys / sells", String(s.trades), "since start"), csTile("Last trade", s.last_trade ? csAgo(s.last_trade, now) : (s.lookback && s.lookback.state === "searching" ? "searching…" : "none found"), s.last_trade ? "activity " + csAgo(s.newest, now) : (s.lookback && s.lookback.state === "done" ? "none in last " + s.lookback.scanned + " txs" : "activity " + csAgo(s.newest, now))),
      csTile("Holding", String(d.open.length), s.airdropped ? "+" + s.airdropped + " airdrops hidden" : "open positions"));
    const lf = s.last_trade_found, many = d.events.length > 5, rows = csShow.trades ? d.events.slice(0, 25) : d.events.slice(0, 5);
    tradeBox.replaceChildren(d.events.length ? h("div", {}, h("div", {class: "card list"}, rows.map((x) => csTradeRow(x, now))), many ? h("button", {class: "csmore", onclick: () => { csShow.trades = !csShow.trades; refresh(); }}, csShow.trades ? "Show fewer" : "Show all " + Math.min(d.events.length, 25) + " trades") : null)
      : h("div", {}, empty("wallet", "No buys or sells seen yet", "A trade shows here the moment tokens are swapped for SOL."),
        lf ? h("p", {class: "dim", style: "font-size:13px;margin:10px 4px 0"}, "Last real trade in history: " + lf.kind.toLowerCase() + " ~" + csSol(lf.sol) + " SOL, " + csAgo(lf.time, now) + ". ", h("a", {href: csLink("tx", lf.sig), target: "_blank", rel: "noopener noreferrer", style: "color:var(--blue)"}, "View")) : null));
    posSec.style.display = d.open.length ? "" : "none";
    posBox.replaceChildren(d.open.length ? h("div", {class: "card list"}, d.open.map((p) => csPosRow(p, now))) : "");
    const al = d.alerts || {}, codes = (s.reason_codes || []).map(([c, n]) => n + " × " + (CS_REASON[c.split("/")[1]] || c)).join(" · ");
    const alertsBody = h("div", {}, h("p", {class: "dim", style: "font-size:13.5px;margin:0"}, al.on ? al.subscribers + " subscriber(s), " + al.sent + " alerts sent. " + (al.persistent ? "List saved (" + al.store + ")." : "List is NOT saved across restarts.") + (al.error ? " Problem: " + al.error : "") : "Telegram alerts are off. Add TELEGRAM_BOT_TOKEN in Render to switch them on."),
      al.link ? h("div", {style: "margin-top:12px"}, h("button", {class: "btn", onclick: () => { copyText(al.link); toast("Alert bot link copied", 1600); }}, "Copy alert bot link", icon("copy", 15))) : null);
    const infoBody = h("div", {}, h("dl", {class: "kv"},
      h("dt", {}, "Transactions read"), h("dd", {}, Number(s.checked).toLocaleString()), h("dt", {}, "Signed by this wallet"), h("dd", {}, Number(s.signed).toLocaleString()),
      h("dt", {}, "Started by someone else"), h("dd", {}, Number(s.foreign).toLocaleString()), h("dt", {}, "Transfers without a trade"), h("dd", {}, Number(s.transfers).toLocaleString()),
      h("dt", {}, "Closed positions"), h("dd", {}, s.closed + (s.closed ? " (" + (s.closed_pnl >= 0 ? "+" : "") + csSol(s.closed_pnl) + " SOL est.)" : "")), h("dt", {}, "Read method"), h("dd", {}, d.poll.mode)),
      codes ? h("p", {class: "dim", style: "font-size:12.5px;margin:12px 0 0"}, "Most common: " + codes) : null,
      h("p", {class: "dim", style: "font-size:12px;margin:12px 0 0"}, "SOL sizes are estimates (fees and tips included). ", h("a", {href: "/cupsey/verify?download=1", style: "color:var(--blue)"}, "Download full verification (JSON)")));
    const evBody = h("div", {}, ...(w.evidence.length ? w.evidence.map((e) => h("div", {class: "prow", style: "align-items:flex-start"}, h("span", {class: "pill", style: "--c:var(--gray)"}, h("i"), String(e.kind).replace(/_/g, " ")),
      h("div", {style: "flex:1;min-width:0"}, h("div", {style: "font-weight:650"}, e.source), h("div", {class: "dim", style: "font-size:12.5px"}, e.summary + (e.observed ? " (" + e.observed + ")" : ""))))) : [h("span", {class: "dim"}, "No evidence on file.")]),
      w.notes ? h("p", {class: "dim", style: "font-size:12.5px;margin:12px 0 0"}, w.notes) : null);
    foldBox.replaceChildren(csFold("playbook", "Your playbook and buy sizes", "stop -" + d.playbook.cfg.stop_pct + "%", csPlaybook(d.playbook), false),
      csFold("alerts", "Telegram alerts", al.on ? al.subscribers + " subscribers" : "off", alertsBody, false),
      csFold("evidence", "Wallet evidence", "", evBody, false), csFold("info", "How the tracker sees it", "", infoBody, false));
  }
  return {root, refresh, every: 5000};
}
function cupseyStrip(d) {
  if (!d.running) return empty("wallet", "Cupsey monitor is off", "Open the Cupsey tab for details.");
  const now = d.now, last = d.events[0];
  const top = h("a", {class: "tok", href: "#/cupsey"},
    h("span", {class: "dot " + (d.live ? "ok" : "warn")}),
    h("div", {class: "tmain"}, h("div", {class: "tt"}, h("b", {}, d.live ? "Live" : "Not reading"), h("span", {}, d.stats.trades + " trades seen · " + d.open.length + " open")),
      h("div", {class: "ts"}, "last trade " + csAgo(d.stats.last_trade, now))),
    h("div", {class: "tr"}, icon("chev", 16)));
  return h("div", {class: "card list"}, top, last ? csTradeRow(last, now) : null);
}

/* ---------- router ---------- */
const NAV = [["", "Home", "home"], ["cupsey", "Cupsey", "wallet"], ["scanner", "Scanner", "radar"], ["passed", "Picks", "shield"], ["review", "Review", "eye"], ["rejected", "Rejected", "ban"]];
function buildNav() {
  const mk = (cls) => (r, l, ic) => h("a", {href: "#/" + r, "data-r": r}, icon(ic, cls === "rail" ? 20 : 22), cls === "rail" ? l : h("span", {}, l));
  document.getElementById("railnav").replaceChildren(...NAV.map(([r, l, ic]) => mk("rail")(r, l, ic)));
  document.getElementById("railsys").replaceChildren(mk("rail")("system", "System health", "pulse"));
  document.getElementById("tabbar").replaceChildren(...NAV.map(([r, l, ic]) => mk("tab")(r, l, ic)));
  document.getElementById("logo1").replaceChildren(logo(36));
  document.getElementById("logo2").replaceChildren(logo(30));
  document.getElementById("sysbtn").replaceChildren(icon("pulse", 19));
  document.getElementById("outbtn").replaceChildren(icon("logout", 19));
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
    case "passed": return listView({title: "Top picks", subtitle: "Tokens that passed every security check, ranked by evidence score plus popularity (Hot / Buzz chips). Popularity never lowers a security rule. Only a pulsing BUY chip means live buy conditions are met right now. Never a guarantee.", url: "/tokens/recent?limit=200&status=QUALIFIED,WATCH&sort=hot", windows: true, icon: "shield", emptyTitle: "No picks yet", emptyText: "A token appears here once it passes the security checks. BUY needs live momentum, real buyers and sell evidence on top."});
    case "review": case "investigating": return listView({title: "In review", subtitle: "Required evidence is missing, or something suspicious is unresolved.", url: "/tokens/recent?limit=200&status=INSUFFICIENT_DATA,HOLD", icon: "eye", emptyTitle: "Nothing in review", emptyText: "Tokens land here while data is being collected, or when something looks suspicious but is not proven."});
    case "rejected": return listView({title: "Rejected", subtitle: "A verified, serious security problem was found. Open a token to see the exact evidence.", url: "/tokens/recent?limit=200&status=REJECTED,REVOKED", icon: "ban", emptyTitle: "No rejected tokens", emptyText: "Rejections appear here with the on-chain evidence behind them."});
    case "cupsey": return cupseyView();
    case "system": return systemView();
    case "token": return tokenView(arg || "");
    default: return homeView();
  }
}
let seenPicks = null, newPicks = 0;
function updateTitle() { document.title = (newPicks ? "(" + newPicks + ") " : "") + "Tosh Bot"; }
async function pickWatcher() {
  try {
    const rows = await api("/tokens/recent?limit=50&status=WATCH,QUALIFIED&sort=hot");
    if (seenPicks === null) { seenPicks = new Set(rows.map((t) => t.mint)); return; }
    const fresh = rows.filter((t) => !seenPicks.has(t.mint));
    fresh.forEach((t) => seenPicks.add(t.mint));
    if (fresh.length) { newPicks += fresh.length; toast("New pick: " + (fresh[0].symbol || "token") + (fresh.length > 1 ? " +" + (fresh.length - 1) + " more" : ""), 5000); updateTitle(); }
  } catch (e) { /* the banner already reports connection problems */ }
}
let prevHash = null, navDepth = 0, currentRun = null;
const SCROLL = new Map();
document.addEventListener("visibilitychange", () => { if (!document.hidden && currentRun) currentRun(); });
async function route() {
  clearInterval(timer);
  navDepth++;
  if (prevHash !== null) SCROLL.set(prevHash, window.scrollY);
  prevHash = location.hash;
  const parts = location.hash.replace(/^#\/?/, "").split("/");
  let name = parts[0] || "", arg = decodeURIComponent(parts[1] || "");
  if (name === "investigating") name = "review";
  if (name === "passed") { newPicks = 0; updateTitle(); }
  document.querySelectorAll("[data-r]").forEach((a) => a.classList.toggle("on", a.dataset.r === name || (name === "token" && a.dataset.r === "scanner")));
  const view = build(name, arg), my = ++gen;
  app.replaceChildren(view.root); banner.style.display = "none";
  let painted = false;
  cacheOnly = true;  // instant paint from the last data received: no placeholders, no zeros
  try { await view.refresh(); painted = true; } catch (e) { /* first visit: placeholders stay until data arrives */ } finally { cacheOnly = false; }
  if (my !== gen) return;
  window.scrollTo(0, painted ? (SCROLL.get(location.hash) || 0) : 0);
  let busy = false;
  const run = async () => {
    if (busy || document.hidden || my !== gen) return;
    busy = true;
    try { await view.refresh(); if (my === gen) banner.style.display = "none"; }
    catch (e) { if (my === gen) { banner.textContent = "Can't reach live data (" + e.message + "). Showing the last data received. Retrying automatically."; banner.style.display = "block"; } }
    finally { busy = false; }
  };
  currentRun = run;
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
route(); pill2(); setInterval(pill2, 10000); pickWatcher(); setInterval(pickWatcher, 15000);
</script>
</body>
</html>
'''


# ============================================================================
# app/token_images.py  (real token pictures, with the dashboard falling back to initials)
# ============================================================================
IPFS_GATEWAYS = ("https://ipfs.io", "https://dweb.link", "https://gateway.pinata.cloud", "https://nftstorage.link")
IMAGE_MAX_BYTES = 1_500_000
METADATA_MAX_BYTES = 200_000


def sniff_image_type(data: bytes) -> str | None:
    """Identify an image by its magic bytes (never trust the remote Content-Type). SVG is deliberately NOT allowed."""
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[4:12] in (b"ftypavif", b"ftypavis"):
        return "image/avif"
    return None


def ipfs_candidates(url: str) -> list[str]:
    """ipfs:// and any /ipfs/<cid> URL become a list of public gateways to try; other URLs are used as-is."""
    from urllib.parse import urlparse

    url = (url or "").strip()
    if not url:
        return []
    if url.startswith("ipfs://"):
        path = url[len("ipfs://"):].lstrip("/")
        path = path[len("ipfs/"):] if path.startswith("ipfs/") else path
        return [f"{gw}/ipfs/{path}" for gw in IPFS_GATEWAYS]
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return []
    out = [url]
    if parsed.path.startswith("/ipfs/"):
        tail = parsed.path + (f"?{parsed.query}" if parsed.query else "")
        out += [gw + tail for gw in IPFS_GATEWAYS if urlparse(gw).hostname != parsed.hostname]
    return out[:5]


try:  # optional: resizes pictures to small thumbnails (saves bandwidth + memory). Without Pillow, originals are used.
    from PIL import Image as PILImage
except Exception:  # pragma: no cover
    PILImage = None
THUMB_PX = 128
MAX_PIXELS = 25_000_000
RAW_FALLBACK_MAX = 300_000


def make_thumbnail(data: bytes) -> tuple[bytes, str] | None:
    """Shrinks any picture to a <=128px WebP (first frame only for animations). Runs in a worker thread."""
    if PILImage is None:
        return None
    import io

    with PILImage.open(io.BytesIO(data)) as im:
        if im.width * im.height > MAX_PIXELS:  # decompression-bomb guard
            raise ValueError("image dimensions too large")
        im.seek(0)
        im = im.convert("RGBA")
        im.thumbnail((THUMB_PX, THUMB_PX))
        out = io.BytesIO()
        im.save(out, "WEBP", quality=80, method=4)
        return out.getvalue(), "image/webp"


class TokenImageService:
    """Fetches a token's real picture: token uri -> metadata JSON -> "image" -> small thumbnail. Done server-side because
    the token creator controls the uri, so it is treated as untrusted (no private/internal addresses, size caps, only real
    raster images). Protections for the "load everything" mode: thumbnails (small), request de-duplication, a global
    fetch limit, per-host limits, a circuit-breaker for failing gateways, hedged gateway requests and failure back-off."""

    def __init__(self, *, max_cached: int = 1500, concurrency: int = 6, max_bytes_total: int = 60_000_000,
                 per_host: int = 3, max_waiting: int = 400) -> None:
        from collections import OrderedDict

        self._client = httpx.AsyncClient(timeout=httpx.Timeout(6.0, connect=4.0), follow_redirects=False,
                                         headers={"User-Agent": "ToshBot/0.2 (+token image fetch)"})
        self._ok: "OrderedDict[str, tuple[bytes, str]]" = OrderedDict()
        self._max_cached, self._max_bytes_total, self._bytes = max_cached, max_bytes_total, 0
        self._fail: dict[str, tuple[float, int]] = {}  # mint -> (retry_not_before, attempts)
        self._inflight: dict[str, asyncio.Task] = {}
        self._sem = asyncio.Semaphore(concurrency)
        self._per_host, self._max_waiting = per_host, max_waiting
        self._host_sems: dict[str, asyncio.Semaphore] = {}
        self._host_fails: dict[str, int] = {}
        self._host_down: dict[str, float] = {}  # host -> skip until (monotonic)
        self.counters = {"served": 0, "fetched": 0, "failed": 0, "shed": 0}

    async def aclose(self) -> None:
        await self._client.aclose()

    def stats(self) -> dict:
        now = time.monotonic()
        return {"cached": len(self._ok), "cached_mb": round(self._bytes / 1e6, 1), "in_flight": len(self._inflight),
                "backing_off": sum(1 for v in self._fail.values() if v[0] > now),
                "hosts_paused": sorted(h for h, until in self._host_down.items() if until > now),
                "thumbnails": PILImage is not None, **self.counters}

    # ---- network ------------------------------------------------------------
    @staticmethod
    async def _assert_public(host: str) -> None:
        import ipaddress
        import socket

        infos = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
        if not infos:
            raise ValueError("host did not resolve")
        for info in infos:
            if not ipaddress.ip_address(info[4][0].split("%")[0]).is_global:
                raise ValueError("refusing to fetch a non-public address")

    def _host_failed(self, host: str, exc: BaseException) -> None:
        text = str(exc)
        # a 404 means "this picture doesn't exist"; only count problems that mean the *host* is struggling
        if isinstance(exc, httpx.HTTPError) or text.startswith(("http 429", "http 5")):
            n = self._host_fails.get(host, 0) + 1
            self._host_fails[host] = n
            if n >= 3:
                self._host_down[host] = time.monotonic() + 60
                self._host_fails[host] = 0
                log.warning("image host paused for 60s", extra={"host": host})

    async def _get(self, url: str, max_bytes: int) -> bytes:
        from urllib.parse import urljoin, urlparse

        for _ in range(4):  # follow a few redirects manually so every hop is re-validated
            parsed = urlparse(url)
            host = parsed.hostname
            if parsed.scheme not in ("http", "https") or not host:
                raise ValueError("unsupported url")
            if self._host_down.get(host, 0) > time.monotonic():
                raise ValueError("host temporarily paused")
            await self._assert_public(host)
            sem = self._host_sems.setdefault(host, asyncio.Semaphore(self._per_host))
            redirect, data = None, b""
            try:
                async with sem:
                    async with self._client.stream("GET", url) as resp:
                        if resp.status_code in (301, 302, 303, 307, 308) and resp.headers.get("location"):
                            redirect = urljoin(url, resp.headers["location"])
                        else:
                            if resp.status_code != 200:
                                raise ValueError(f"http {resp.status_code}")
                            buf = bytearray()
                            async for chunk in resp.aiter_bytes():
                                buf += chunk
                                if len(buf) > max_bytes:
                                    raise ValueError("response too large")
                            data = bytes(buf)
            except Exception as exc:
                self._host_failed(host, exc)
                raise
            self._host_fails.pop(host, None)
            if redirect:
                url = redirect
                continue
            return data
        raise ValueError("too many redirects")

    async def _first_ok(self, candidates: list[str], max_bytes: int) -> bytes:
        """Tries gateways in order, but if one is slow (>2.5s) the next is started in parallel (first success wins)."""
        from urllib.parse import urlparse

        now = time.monotonic()
        queue = [u for u in candidates if self._host_down.get(urlparse(u).hostname, 0) <= now] or list(candidates)
        if not queue:
            raise ValueError("no candidates")
        pending: set[asyncio.Task] = set()
        last: Exception = ValueError("all sources failed")
        try:
            while queue or pending:
                if queue and not pending:
                    pending.add(asyncio.create_task(self._get(queue.pop(0), max_bytes)))
                done, pending = await asyncio.wait(pending, timeout=2.5 if queue else None,
                                                   return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    try:
                        return task.result()
                    except Exception as exc:
                        last = exc
                if not done and queue:
                    pending.add(asyncio.create_task(self._get(queue.pop(0), max_bytes)))
            raise last
        finally:
            for task in pending:
                task.cancel()

    async def get_metadata(self, uri: str) -> dict | None:
        """The token's metadata JSON (name, links...) through the same hardened fetcher as pictures. None if it is not JSON."""
        try:
            async with self._sem:
                raw = await asyncio.wait_for(self._first_ok(ipfs_candidates(uri), METADATA_MAX_BYTES), timeout=15)
            meta = json.loads(raw[:METADATA_MAX_BYTES].decode("utf-8", errors="strict"))
            return meta if isinstance(meta, dict) else None
        except Exception:
            return None

    # ---- picture handling -----------------------------------------------------
    @staticmethod
    async def _finalize(data: bytes, kind: str) -> tuple[bytes, str]:
        try:
            thumb = await asyncio.to_thread(make_thumbnail, data)
            if thumb:
                return thumb
        except Exception:
            pass
        if len(data) <= RAW_FALLBACK_MAX:  # no Pillow / unsupported format: serve the original only if it is small
            return data, kind
        raise ValueError("image too large and could not be resized")

    async def _resolve(self, uri: str) -> tuple[bytes, str]:
        raw = await self._first_ok(ipfs_candidates(uri), IMAGE_MAX_BYTES)
        kind = sniff_image_type(raw)
        if kind:  # the uri pointed straight at an image
            return await self._finalize(raw, kind)
        meta = json.loads(raw[:METADATA_MAX_BYTES].decode("utf-8", errors="strict"))
        image_url = next((meta.get(k) for k in ("image", "image_uri", "imageUrl", "logo", "icon")
                          if isinstance(meta, dict) and isinstance(meta.get(k), str) and meta.get(k)), None)
        if not image_url:
            raise ValueError("metadata has no image")
        data = await self._first_ok(ipfs_candidates(image_url), IMAGE_MAX_BYTES)
        kind = sniff_image_type(data)
        if not kind:
            raise ValueError("not a supported raster image")
        return await self._finalize(data, kind)

    async def _fetch_and_store(self, mint: str, uri: str) -> tuple[bytes, str] | None:
        async with self._sem:
            try:
                data, kind = await asyncio.wait_for(self._resolve(uri), timeout=25)
            except Exception as exc:
                attempts = self._fail.get(mint, (0.0, 0))[1] + 1
                self._fail[mint] = (time.monotonic() + (30, 120, 600, 3600)[min(attempts - 1, 3)], attempts)
                if len(self._fail) > 5000:
                    self._fail.clear()
                self.counters["failed"] += 1
                log.info("token image unavailable", extra={"mint": mint, "reason": f"{type(exc).__name__}: {exc}"[:120]})
                return None
        self.counters["fetched"] += 1
        self._fail.pop(mint, None)
        if mint in self._ok:
            self._bytes -= len(self._ok[mint][0])
        self._ok[mint] = (data, kind)
        self._ok.move_to_end(mint)
        self._bytes += len(data)
        while self._ok and (len(self._ok) > self._max_cached or self._bytes > self._max_bytes_total):
            _, (old, _k) = self._ok.popitem(last=False)  # drop the oldest picture first
            self._bytes -= len(old)
        return data, kind

    async def get(self, mint: str, uri: str | None) -> tuple[bytes, str] | None:
        """Returns (bytes, content_type) or None when there is no real picture (caller falls back to initials)."""
        if not uri:
            return None
        hit = self._ok.get(mint)
        if hit:
            self._ok.move_to_end(mint)
            self.counters["served"] += 1
            return hit
        failed = self._fail.get(mint)
        if failed and time.monotonic() < failed[0]:
            return None
        task = self._inflight.get(mint)
        if task is None:
            if len(self._inflight) >= self._max_waiting:  # overloaded: shed load, the dashboard retries shortly
                self.counters["shed"] += 1
                return None
            task = asyncio.create_task(self._fetch_and_store(mint, uri))
            self._inflight[mint] = task
            task.add_done_callback(lambda _t, m=mint: self._inflight.pop(m, None))
        return await asyncio.shield(task)


# ============================================================================
# POPULARITY ("buzz"): how much real attention a token that already passed security is getting
# ============================================================================
BUZZ_WEIGHTS = {"crowd": 40, "community": 25, "market": 20, "chatter": 15}
_TG_HANDLE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")
_X_HANDLE = re.compile(r"^[A-Za-z0-9_]{1,15}$")


def _clean_url(value: Any) -> str | None:
    """Only plain https links are kept. The app never opens them (links are shown, and Telegram is asked by handle)."""
    if not isinstance(value, str):
        return None
    v = value.strip()
    if v and "://" not in v and "." in v:
        v = "https://" + v.lstrip("/")
    if len(v) > 200 or not v.lower().startswith("https://") or any(c in v for c in " \n\r\t<>\"'"):
        return None
    return v


def telegram_handle(url: str | None) -> str | None:
    """t.me/<public name> -> name. Invite links (joinchat, +abc) have no public name and return None."""
    from urllib.parse import urlsplit
    if not url:
        return None
    p = urlsplit(url)
    if (p.hostname or "").lower() not in ("t.me", "telegram.me", "www.t.me"):
        return None
    seg = [x for x in p.path.split("/") if x]
    return seg[0] if seg and _TG_HANDLE.match(seg[0]) and seg[0].lower() not in ("joinchat", "addstickers") else None


def x_link_ok(url: str | None) -> bool:
    from urllib.parse import urlsplit
    if not url:
        return False
    p = urlsplit(url)
    if (p.hostname or "").lower() not in ("x.com", "twitter.com", "www.x.com", "www.twitter.com", "mobile.twitter.com"):
        return False
    seg = [x for x in p.path.split("/") if x]
    return bool(seg) and (bool(_X_HANDLE.match(seg[0])) or (seg[0] == "i" and len(seg) >= 3 and seg[1] == "communities"))


def extract_links(*sources: dict | None) -> dict:
    """Pick the X / Telegram / website links out of the token metadata and DEX Screener profile. Missing = None."""
    out: dict[str, str | None] = {"x": None, "telegram": None, "website": None}
    for src in sources:
        if not isinstance(src, dict):
            continue
        cands = {"x": [src.get("twitter"), src.get("x")], "telegram": [src.get("telegram")], "website": [src.get("website")]}
        for s in src.get("socials") or []:
            if isinstance(s, dict):
                kind = str(s.get("type") or "").lower()
                cands["x" if kind in ("twitter", "x") else "telegram" if kind == "telegram" else "website"].append(s.get("url"))
        for w in src.get("websites") or []:
            if isinstance(w, dict):
                cands["website"].append(w.get("url"))
        for kind, vals in cands.items():
            for v in vals:
                u = _clean_url(v)
                if not out[kind] and u and (kind != "x" or x_link_ok(u)) and (kind != "telegram" or telegram_handle(u)):
                    out[kind] = u
    return out


def buzz_crowd(stats: "TradeStats | None") -> dict | None:
    """Who is trading it right now (from the live trade feed): independent buyers, new buyers, buy pressure, discounted for round-trip or one-wallet volume."""
    if stats is None or stats.n_trades < 5:
        return None
    ub, nb = min(1.0, stats.unique_buyers / 40), min(1.0, stats.new_buyers / 10)
    pressure = _clamp(((stats.buy_pressure_pct if stats.buy_pressure_pct is not None else 50.0) - 40) / 40 * 100) / 100
    base = 100 * (0.45 * ub + 0.30 * nb + 0.25 * pressure)
    auth = _clamp(100 - 1.5 * stats.top_trader_pct - stats.wash_pct) / 100
    return {"name": "crowd", "weight": BUZZ_WEIGHTS["crowd"], "score": round(base * (0.5 + 0.5 * auth), 1), "organic": auth >= 0.5,
            "detail": f"{stats.unique_buyers} independent buyers, {stats.new_buyers} new in {stats.window_seconds}s, "
                      f"buy pressure {stats.buy_pressure_pct if stats.buy_pressure_pct is not None else 0:.0f}%"}


def buzz_community(links: dict | None, tg_members: int | None) -> dict | None:
    """Real social presence: X, Telegram and website links, plus the Telegram group's size. None until the metadata has been read."""
    if links is None:
        return None
    pts = (25 if links.get("x") else 0) + (15 if links.get("telegram") else 0) + (10 if links.get("website") else 0)
    if tg_members:
        pts += 50 * max(0.0, min(1.0, (math.log10(max(tg_members, 1)) - 1.7) / 2.0))   # 50 members = 0, ~5,000 = full
    have = [k for k in ("x", "telegram", "website") if links.get(k)]
    return {"name": "community", "weight": BUZZ_WEIGHTS["community"], "score": round(min(100.0, pts), 1),
            "detail": ("links: " + ", ".join(have) if have else "no X, Telegram or website link") + (f", Telegram group {tg_members:,} members" if tg_members else "")}


def buzz_market(dex: dict | None) -> dict | None:
    """DEX Screener view: recent trade count and buy share, and whether the token has a profile. Paid boosts count a little and are flagged."""
    if not dex:
        return None
    b5, s5, b1, s1 = (int(dex.get(k) or 0) for k in ("buys_m5", "sells_m5", "buys_h1", "sells_h1"))
    ratio = b1 / (b1 + s1) if (b1 + s1) else 0.5
    pts = 100 * (0.40 * min(1.0, (b1 + s1) / 300) + 0.25 * min(1.0, (b5 + s5) / 60) + 0.20 * _clamp((ratio - 0.4) / 0.4 * 100) / 100)
    pts += (10 if dex.get("profile") else 0) + min(5, 2 * int(dex.get("boosts") or 0))
    return {"name": "market", "weight": BUZZ_WEIGHTS["market"], "score": round(min(100.0, pts), 1), "boosted": int(dex.get("boosts") or 0) > 0,
            "detail": f"{b1 + s1} trades in 1h ({ratio * 100:.0f}% buys), {b5 + s5} in 5m" + (", has a DEX Screener profile" if dex.get("profile") else "")
                      + (f", {int(dex['boosts'])} paid boost(s)" if dex.get("boosts") else "")}


def buzz_chatter(comments: int | None) -> dict | None:
    if comments is None:
        return None
    return {"name": "chatter", "weight": BUZZ_WEIGHTS["chatter"], "score": round(100 * min(1.0, comments / 40), 1), "detail": f"{comments} recent comments on pump.fun"}


def combine_buzz(parts: list[dict | None], links: dict | None, now: datetime | None = None) -> dict | None:
    """Weighted average over the sources that exist. A missing source is never guessed: its weight is dropped and `coverage` says how much of the picture we have."""
    comps = [p for p in parts if p]
    if not comps:
        return None
    total = sum(c["weight"] for c in comps)
    score = sum(c["weight"] * c["score"] for c in comps) / total
    warnings: list[str] = []
    crowd = next((c for c in comps if c["name"] == "crowd"), None)
    if crowd and not crowd.get("organic", True):
        score = min(score, 40.0)
        warnings.append("Trading looks one-sided or round-trip, so the popularity score is capped. Hype can be manufactured.")
    if any(c.get("boosted") for c in comps):
        warnings.append("This token has paid DEX Screener boosts. Paid attention is not organic interest.")
    coverage = total / sum(BUZZ_WEIGHTS.values())
    if coverage < 0.4:
        warnings.append("Limited data so far, treat the popularity score loosely.")
    score = round(score, 1)
    return {"score": score, "level": "hot" if score >= 70 else "warm" if score >= 50 else "quiet", "coverage": round(coverage, 2),
            "components": [{k: v for k, v in c.items() if k not in ("organic", "boosted")} for c in comps], "links": links or {}, "warnings": warnings,
            "updated_at": (now or utcnow()).isoformat(),
            "note": "Popularity is attention, not safety and not a prediction. It only ranks tokens that already passed the security checks."}


def rank_score(evidence: float | None, buzz: dict | None, weight: float) -> float | None:
    """Top picks order: the evidence score, nudged by popularity. Popularity needs enough coverage to count and can never create a pick on its own."""
    if evidence is None:
        return None
    if not buzz or buzz.get("coverage", 0) < 0.4 or weight <= 0:
        return evidence
    w = max(0.0, min(0.6, weight))
    return round((1 - w) * evidence + w * buzz["score"], 1)


class BuzzEngine:
    """Background popularity scoring for the Top picks candidates (WATCH / QUALIFIED). Memory only. Every outside call is capped, cached,
    time-limited and fails quietly: a source being down only lowers `coverage`, it never breaks the dashboard."""

    def __init__(self, repo: "Repository", tracker: "TradeTracker | None", images: "TokenImageService | None", settings: "Settings",
                 tg_token: str = "", client=None, clock=time.time) -> None:
        self.repo, self.tracker, self.images, self.cfg, self._clock = repo, tracker, images, settings, clock
        self.tg_token = tg_token
        self.client = client
        self.cache: dict[str, dict] = {}
        self._links: dict[str, dict | None] = {}
        self._dex: dict[str, tuple[float, dict | None]] = {}
        self._tg: dict[str, tuple[float, int | None]] = {}
        self._cm: dict[str, tuple[float, int | None]] = {}
        self._sem = asyncio.Semaphore(3)
        self._dex_blocked_until = 0.0
        self.rounds = 0
        self.last_round: float | None = None
        self.errors: Counter = Counter()

    def get(self, mint: str) -> dict | None:
        return self.cache.get(mint)

    def _http(self):
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=httpx.Timeout(6.0, connect=4.0), follow_redirects=False, headers={"User-Agent": "ToshBot/0.3 (+popularity)"})
        return self.client

    async def aclose(self) -> None:
        if self.client is not None:
            await self.client.aclose()

    @staticmethod
    def _bound(d: dict, n: int = 3000) -> None:
        if len(d) > n:
            for k in list(d)[: n // 3]:
                d.pop(k, None)

    async def _meta_links(self, token: "TokenRecord") -> dict | None:
        if token.mint in self._links:
            return self._links[token.mint]
        if not token.uri or self.images is None:
            return None
        meta = await self.images.get_metadata(token.uri)
        if meta is None:
            return None   # not readable (yet): stay "unknown", never "no links"
        self._bound(self._links)
        self._links[token.mint] = extract_links(meta)
        return self._links[token.mint]

    async def _dex_info(self, mint: str) -> dict | None:
        now = self._clock()
        hit = self._dex.get(mint)
        if hit and now - hit[0] < 90:
            return hit[1]
        if not self.cfg.buzz_dexscreener or now < self._dex_blocked_until or not BASE58_ADDRESS.match(mint):
            return hit[1] if hit else None
        info = None
        try:
            r = await self._http().get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
            if r.status_code == 429:
                self._dex_blocked_until = now + 60
                self.errors["dex_rate_limited"] += 1
            elif r.status_code == 200:
                pairs = [p for p in ((r.json() or {}).get("pairs") or []) if isinstance(p, dict) and p.get("chainId") == "solana"]
                if pairs:
                    p = max(pairs, key=lambda x: float(((x.get("liquidity") or {}).get("usd")) or 0))
                    tx = p.get("txns") or {}
                    prof = p.get("info") if isinstance(p.get("info"), dict) else {}
                    info = {"buys_m5": (tx.get("m5") or {}).get("buys", 0), "sells_m5": (tx.get("m5") or {}).get("sells", 0),
                            "buys_h1": (tx.get("h1") or {}).get("buys", 0), "sells_h1": (tx.get("h1") or {}).get("sells", 0),
                            "boosts": ((p.get("boosts") or {}).get("active", 0) if isinstance(p.get("boosts"), dict) else 0), "profile": bool(prof),
                            "links": extract_links(prof)}
        except Exception as exc:
            self.errors[f"dex_{type(exc).__name__}"] += 1
        self._bound(self._dex)
        self._dex[mint] = (now, info)
        return info

    async def _tg_members(self, handle: str | None) -> int | None:
        if not handle or not self.tg_token or not self.cfg.buzz_telegram_members:
            return None
        now = self._clock()
        hit = self._tg.get(handle.lower())
        if hit and now - hit[0] < 1800:
            return hit[1]
        n = None
        try:
            r = await self._http().post(f"https://api.telegram.org/bot{self.tg_token}/getChatMemberCount", json={"chat_id": "@" + handle})
            if r.status_code == 200 and (r.json() or {}).get("ok"):
                n = int(r.json()["result"])
        except Exception as exc:   # the URL holds the bot token: only the error type is kept
            self.errors[f"tg_{type(exc).__name__}"] += 1
        self._bound(self._tg)
        self._tg[handle.lower()] = (now, n)
        return n

    async def _comments(self, mint: str) -> int | None:
        if not self.cfg.buzz_pumpfun_comments or not BASE58_ADDRESS.match(mint):
            return None
        now = self._clock()
        hit = self._cm.get(mint)
        if hit and now - hit[0] < 120:
            return hit[1]
        n = None
        try:
            r = await self._http().get(self.cfg.buzz_comments_url.replace("{mint}", mint))
            if r.status_code == 200:
                data = r.json()
                data = data.get("replies") if isinstance(data, dict) else data
                n = len(data) if isinstance(data, list) else None
        except Exception as exc:
            self.errors[f"comments_{type(exc).__name__}"] += 1
        self._bound(self._cm)
        self._cm[mint] = (now, n)
        return n

    async def score(self, token: "TokenRecord") -> dict | None:
        stats = self.tracker.snapshot(token.mint) if self.tracker is not None else None
        async with self._sem:
            links = await self._meta_links(token)
            dex = await self._dex_info(token.mint)
            if links is not None and dex and dex.get("links"):
                for k, v in dex["links"].items():   # DEX Screener's profile fills gaps in the token's own metadata
                    links = {**links, k: links.get(k) or v}
            elif links is None and dex and dex.get("links"):
                links = dex["links"]
            members = await self._tg_members(telegram_handle((links or {}).get("telegram")))
            comments = await self._comments(token.mint)
        return combine_buzz([buzz_crowd(stats), buzz_community(links, members), buzz_market(dex), buzz_chatter(comments)], links)

    async def refresh_once(self) -> int:
        rows = await self.repo.list_recent_tokens(200, ["WATCH", "QUALIFIED"])
        rows = sorted(rows, key=lambda t: (t.opportunity_score if t.opportunity_score is not None else -1.0, t.detected_at), reverse=True)[: self.cfg.buzz_max_tokens]
        done = 0
        for t in rows:
            try:
                b = await self.score(t)
            except Exception as exc:
                self.errors[f"score_{type(exc).__name__}"] += 1
                continue
            if b:
                self.cache[t.mint] = b
                done += 1
        keep = {t.mint for t in rows}
        for mint in [m for m in self.cache if m not in keep and self._clock() - self._age(self.cache[m]) > 3600]:
            self.cache.pop(mint, None)
        self.rounds += 1
        self.last_round = self._clock()
        return done

    @staticmethod
    def _age(b: dict) -> float:
        try:
            return datetime.fromisoformat(b["updated_at"]).timestamp()
        except Exception:
            return 0.0

    async def run(self) -> None:
        while True:
            try:
                await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.errors[f"round_{type(exc).__name__}"] += 1
                log.warning("popularity round failed", extra={"error": type(exc).__name__})
            await asyncio.sleep(max(15.0, self.cfg.buzz_interval_seconds))


def build_api_router(auth) -> APIRouter:
    router = APIRouter(dependencies=[Depends(auth)])

    @router.get("/tokens/recent", response_model=list[HotToken])
    async def recent_tokens(request: Request, limit: int = Query(50, ge=1, le=200),
                            status: str | None = Query(None, description="Comma-separated statuses"),
                            sort: str | None = Query(None, description="'score' = best evidence score first, 'hot' = evidence score plus popularity")):
        statuses = [x.strip().upper() for x in status.split(",") if x.strip()] if status else None
        if statuses and any(x not in VALID_STATUSES for x in statuses):
            raise HTTPException(status_code=422, detail="Unknown status")
        rows = await request.app.state.repo.list_recent_tokens(limit, statuses)
        engine = getattr(request.app.state, "buzz", None)
        weight = request.app.state.settings.buzz_rank_weight
        out = []
        for t in rows:
            b = engine.get(t.mint) if engine is not None and t.status in ("WATCH", "QUALIFIED") else None
            out.append(HotToken(**t.model_dump(), buzz=b, rank=rank_score(t.opportunity_score, b, weight) if b else t.opportunity_score))
        if sort in ("score", "hot"):
            key = (lambda x: x.rank) if sort == "hot" else (lambda x: x.opportunity_score)
            out.sort(key=lambda x: (key(x) if key(x) is not None else -1.0, x.detected_at), reverse=True)
        return out

    @router.get("/tokens/{mint}/buzz")
    async def token_buzz(request: Request, mint: str) -> dict:
        """Popularity of one token that passed security. `buzz` is null when it has not been scored (yet) or the token is not a candidate."""
        mint = _valid_mint(mint)
        engine = getattr(request.app.state, "buzz", None)
        return {"enabled": engine is not None, "buzz": engine.get(mint) if engine is not None else None}

    @router.get("/tokens/{mint}/image")
    async def token_image(request: Request, mint: str):
        """The token's real picture (from its on-chain metadata). 404 means "none": the dashboard shows initials."""
        mint = _valid_mint(mint)
        token = await request.app.state.repo.get_token(mint)
        if token is None or not token.uri:
            raise HTTPException(status_code=404, detail="No image")
        svc = getattr(request.app.state, "images", None)
        if svc is None:
            svc = request.app.state.images = TokenImageService()
        found = await svc.get(mint, token.uri)
        if found is None:
            return Response(status_code=404, headers={"Cache-Control": "no-store"})
        data, kind = found
        return Response(content=data, media_type=kind, headers={
            "Cache-Control": "private, max-age=86400", "X-Content-Type-Options": "nosniff",
            "Content-Security-Policy": "default-src 'none'; sandbox"})

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
            "token_images": st.images.stats() if getattr(st, "images", None) else None,
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
        if repository is None and settings.storage_backend != "memory" and settings.cache_recent_tokens > 0:
            repo = CachedRepository(repo, settings.cache_recent_tokens, settings.persist_status_set)
            try:
                await repo.warm()
                log.info("memory cache ready", extra={"tokens": len(repo.recent)})
            except Exception as exc:  # database unreachable or over quota: fall back to direct reads
                log.warning("cache warm-up failed", extra={"error": f"{type(exc).__name__}: {str(exc)[:200]}"})
        if settings.storage_backend == "memory" and repository is None:
            log.warning("STORAGE_BACKEND=memory: data is NOT persistent")
        health, metrics = HealthRegistry(), Metrics()
        queue: asyncio.Queue = asyncio.Queue(maxsize=settings.queue_maxsize)
        analysis_queue: asyncio.Queue = asyncio.Queue(maxsize=settings.analysis_queue_maxsize)
        stop = asyncio.Event()
        app.state.repo, app.state.health, app.state.metrics = repo, health, metrics
        app.state.images = TokenImageService()
        app.state.queue, app.state.analysis_queue = queue, analysis_queue

        solana = rpc or SolanaRPC(settings.solana_rpc_url, health, timeout=settings.rpc_timeout_seconds,
                                  max_concurrency=settings.rpc_max_concurrency)
        processor = TokenProcessor(repo, queue, health, metrics, max_attempts=settings.processor_max_attempts,
                                   analysis_queue=analysis_queue if settings.analysis_enabled else None,
                                   store_events=settings.store_launch_events, store_jobs=settings.store_jobs,
                                   max_new_per_day=settings.max_new_tokens_per_day)
        tasks = [asyncio.create_task(processor.run(), name="processor")]
        analysis_tasks: list[asyncio.Task] = []
        tracker = None
        if settings.analysis_enabled and settings.listener_enabled and settings.signals_enabled:
            tracker = TradeTracker(settings.signal_window_minutes * 60, settings.signal_max_tracked,
                                   momentum_window=settings.signal_momentum_window_seconds)
        app.state.tracker = tracker
        if settings.analysis_enabled:
            analyzer = SecurityAnalyzer(solana, repo, policy_from_settings(settings), tracker=tracker,
                                        security_cache_seconds=settings.security_recheck_seconds)
            for i in range(settings.analysis_workers):
                worker = AnalysisWorker(repo, analyzer, analysis_queue, health, metrics, retry_delays=settings.retry_delay_list,
                                        save_all_attempts=settings.save_all_attempts, store_jobs=settings.store_jobs,
                                        review_save_seconds=settings.review_save_seconds if tracker else 0.0)
                analysis_tasks.append(asyncio.create_task(worker.run(), name=f"analysis-{i}"))
            if tracker is not None:
                analysis_tasks.append(asyncio.create_task(
                    ReviewScheduler(tracker, analysis_queue, interval=settings.signal_review_seconds).run(), name="signal-reviews"))
            try:  # resume jobs left pending by a restart (only possible when jobs are stored)
                for mint in (await repo.list_pending_jobs(500) if settings.store_jobs else []):
                    analysis_queue.put_nowait(mint)
            except Exception as exc:
                log.warning("could not re-queue pending jobs", extra={"error": str(exc)})
        else:
            log.warning("analysis disabled by configuration")
        if settings.buzz_enabled and settings.analysis_enabled:
            app.state.buzz = BuzzEngine(repo, tracker, app.state.images, settings, tg_token=(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip())
            analysis_tasks.append(asyncio.create_task(app.state.buzz.run(), name="popularity"))
        if settings.listener_enabled:
            listener = PumpPortalListener(
                settings.pumpportal_ws_url, queue, health, metrics,
                base_delay=settings.reconnect_base_seconds, max_delay=settings.reconnect_max_seconds,
                stable_after=settings.stable_connection_seconds, tracker=tracker)
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
            await app.state.images.aclose()
            if getattr(app.state, "buzz", None) is not None:
                await app.state.buzz.aclose()
            await repo.aclose()

    app = FastAPI(title="Tosh Bot API", version="0.2.0", lifespan=lifespan)
    app.state.settings = settings

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception):
        log.error("unhandled error", extra={"path": request.url.path, "error": f"{type(exc).__name__}: {str(exc)[:300]}"})
        return JSONResponse(status_code=500, content={"detail": f"{type(exc).__name__}: {str(exc)[:200]}"})
    app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origin_list, allow_methods=["GET"], allow_headers=["X-API-Key"])

    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root() -> dict:
        return {"service": "toshbot", "status": "ok", "health": "/health", "cupsey": "/cupsey", "dashboard": "/dashboard"}

    @app.api_route("/health", methods=["GET", "HEAD"])
    async def liveness() -> dict:
        return {"status": "ok"}

    # Same JSON API, two doors: /api (X-API-Key, for programs) and /dashboard/api (Basic Auth, for the built-in page).
    app.include_router(build_api_router(require_api_key), prefix="/api")
    app.include_router(build_api_router(require_dashboard_auth), prefix="/dashboard/api")

    @app.get("/dashboard", include_in_schema=False, dependencies=[Depends(require_dashboard_auth)])
    async def dashboard_page():
        return HTMLResponse(DASHBOARD_HTML.replace("__LOGO_ICON__", LOGO_ICON_URI), headers={"Cache-Control": "no-store"})

    @app.get("/login", include_in_schema=False)
    async def login_page(request: Request):
        cfg = request.app.state.settings
        token = request.cookies.get(SESSION_COOKIE, "")
        if not cfg.dashboard_password or (token and session_valid(cfg, token)):
            return RedirectResponse("/dashboard", status_code=303)
        return HTMLResponse(render_login_page(), headers=LOGIN_HEADERS)

    @app.post("/login", include_in_schema=False)
    async def login_submit(request: Request):
        cfg = request.app.state.settings
        if not cfg.dashboard_password:
            return RedirectResponse("/dashboard", status_code=303)
        ip = _client_ip(request)
        if _login_locked(ip):
            return HTMLResponse(render_login_page("Too many attempts. Wait a few minutes and try again."), status_code=429, headers=LOGIN_HEADERS)
        form = parse_qs((await request.body())[:4096].decode("utf-8", "replace"), keep_blank_values=True)
        user = form.get("username", [""])[0].strip()
        pw = form.get("password", [""])[0]
        ok_user = hmac.compare_digest(user.encode(), cfg.dashboard_user.encode())
        ok_pw = hmac.compare_digest(pw.encode(), cfg.dashboard_password.encode())
        if not (ok_user and ok_pw):
            _login_failed(ip)
            await asyncio.sleep(0.4)   # slows down guessing
            return HTMLResponse(render_login_page("Wrong username or password.", user), status_code=401, headers=LOGIN_HEADERS)
        _LOGIN_FAILS.pop(ip, None)
        resp = RedirectResponse("/dashboard", status_code=303)
        resp.set_cookie(SESSION_COOKIE, make_session(cfg), max_age=SESSION_TTL, httponly=True,
                        secure=cfg.environment == "production", samesite="lax", path="/")
        return resp

    @app.get("/logout", include_in_schema=False)
    async def logout():
        resp = RedirectResponse("/login", status_code=303)
        resp.delete_cookie(SESSION_COOKIE, path="/")
        return resp

    # Cupsey monitoring add-on (cupsey.py). Optional: if the file is missing or fails, the rest of the app is unaffected.
    try:
        from cupsey import register_cupsey_routes
        register_cupsey_routes(app, require_dashboard_auth)
    except ModuleNotFoundError as exc:
        if exc.name != "cupsey":
            raise
        log.info("cupsey.py not found; Cupsey routes disabled")
    except Exception as exc:
        log.error("cupsey routes failed to load", extra={"error": f"{type(exc).__name__}: {str(exc)[:200]}"})

    return app


app = create_app()
