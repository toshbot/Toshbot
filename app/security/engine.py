import asyncio
import logging
from datetime import datetime, timedelta

from app.models import TokenRecord, utcnow
from app.repository import Repository
from app.rpc import RpcError, SolanaRPC
from app.security import checks as c
from app.security.models import CheckResult, SecurityReport
from app.security.policy import ANALYSIS_VERSION, NOT_EVALUATED, REQUIRED_CHECKS, SecurityPolicy

log = logging.getLogger("tosh.security")


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
            return c.unavailable(name, f"data provider problem ({exc})")
        except (KeyError, TypeError, ValueError, IndexError, AttributeError) as exc:
            return c.unavailable(name, f"unexpected data shape ({type(exc).__name__}: {exc})")
        except Exception as exc:  # never convert an internal error into a pass OR a failure
            log.exception("check crashed", extra={"check": name})
            return c.unavailable(name, f"internal error ({type(exc).__name__})")

    async def _load_mint(self, mint: str) -> tuple[c.MintData | None, str | None]:
        try:
            account = await self.rpc.get_account_info(mint)
            if account is None:
                return None, "mint account not visible yet (token may be too new)"
            return c.parse_mint(account), None
        except RpcError as exc:
            return None, f"data provider problem ({exc})"
        except (KeyError, TypeError, ValueError) as exc:
            return None, f"unexpected mint data ({type(exc).__name__}: {exc})"

    async def analyze(self, token: TokenRecord) -> SecurityReport:
        md, err = await self._load_mint(token.mint)
        if md is None:
            mint_checks = [c.unavailable(n, err) for n in ("mint_permissions", "token_extensions", "holder_concentration")]
            holders = None
        else:
            mint_checks = [await self._safe("mint_permissions", _wrap(c.check_mint_permissions, md)),
                           await self._safe("token_extensions", _wrap(c.check_token_extensions, md, self.policy))]
            holders = self._safe("holder_concentration", c.check_holders(self.rpc, token, md, self.policy))
        others = [self._safe("creator_wallet", c.check_creator(self.rpc, self.repo, token, self.policy)),
                  self._safe("liquidity", c.check_liquidity(self.rpc, token, self.policy))]
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
