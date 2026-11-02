import json
import math
from datetime import datetime, timezone
from typing import Any

from app.models import BASE58_ADDRESS, LaunchEvent, utcnow

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
