from pydantic import BaseModel

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
