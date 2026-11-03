import inspect
import os
from abc import ABC, abstractmethod

from app.config import Settings
from datetime import datetime

from app.models import LaunchEvent, TokenRecord, utcnow
from app.security.models import SecurityReport

ANALYSIS_VERSION = "phase1-detection-0"


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
        self.jobs[key] = {"mint": mint, "status": "pending", "analysis_version": ANALYSIS_VERSION, "created_at": utcnow()}
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
        data = {"mint": mint, "status": "pending", "analysis_version": ANALYSIS_VERSION, "created_at": utcnow()}
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
