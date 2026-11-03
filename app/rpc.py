import asyncio
import logging
from typing import Any

import httpx

from app.health import HealthRegistry

log = logging.getLogger("tosh.rpc")
PROVIDER = "solana_rpc"


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
                    self.health.mark_failure(PROVIDER, f"HTTP {resp.status_code}")
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
                            self.health.mark_success(PROVIDER)  # the provider is up; the request was rejected
                            raise RpcError(str(err)[:300])
                    elif "result" in body:
                        self.health.mark_success(PROVIDER)
                        return body["result"]
                    else:
                        last = "malformed response"
            if attempt < self.max_retries:
                await self._sleep(delay)
        self.health.mark_failure(PROVIDER, last)
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
