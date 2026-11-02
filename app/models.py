import re
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

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
    source: str = "pumpportal"
    launch_timestamp: datetime | None = None
    detected_at: datetime = Field(default_factory=utcnow)
    # Fail closed: nothing has been verified yet, so the status is never anything but INSUFFICIENT_DATA in Phase 1.
    status: TokenStatus = "INSUFFICIENT_DATA"
    status_reason: str = "Security analysis has not been performed yet (Phase 1 detection only)."
    status_updated_at: datetime = Field(default_factory=utcnow)
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
            launch_timestamp=event.event_timestamp,
            detected_at=event.received_at,
            status_updated_at=event.received_at,
        )
