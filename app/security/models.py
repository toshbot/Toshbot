from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.models import TokenStatus, utcnow

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
