from dataclasses import asdict, dataclass, field
from datetime import datetime

from app.models import utcnow


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
