"""TEST FIXTURES ONLY: synthetic on-chain data shaped like Solana jsonParsed responses. Never live data."""
from app.models import TokenRecord
from app.rpc import RpcUnavailable
from app.security.policy import TOKEN_2022_PROGRAM, TOKEN_PROGRAM
from tests.helpers import FIXTURE_CREATOR, fixture_mint

SUPPLY = 10**15  # 1B tokens, 6 decimals
CURVE, OTHER1, OTHER2 = fixture_mint(9), fixture_mint(10), fixture_mint(11)


def mint_account(owner=TOKEN_PROGRAM, mint_auth=None, freeze=None, supply=SUPPLY, extensions=None) -> dict:
    info = {"mintAuthority": mint_auth, "freezeAuthority": freeze, "supply": str(supply), "decimals": 6}
    if extensions is not None:
        info["extensions"] = extensions
    return {"owner": owner, "data": {"program": "spl-token", "parsed": {"type": "mint", "info": info}}}


def token_account(owner: str) -> dict:
    return {"data": {"parsed": {"type": "account", "info": {"owner": owner}}}}


def fixture_token(i=1, **kw) -> TokenRecord:
    base = dict(mint=fixture_mint(i), creator=FIXTURE_CREATOR, bonding_curve=CURVE, name="Fixture", symbol="FX")
    base.update(kw)
    return TokenRecord(**base)


# (token account address, owner, percent of supply)
HEALTHY_HOLDERS = [("TA1", CURVE, 80.0), ("TA2", FIXTURE_CREATOR, 5.0), ("TA3", OTHER1, 3.0), ("TA4", OTHER2, 2.0)]


class FakeRPC:
    def __init__(self, mint=None, holders=HEALTHY_HOLDERS, curve_lamports=2_000_000_000, sigs=None, fail=(), use_default_mint=True):
        self.mint = mint_account() if (mint is None and use_default_mint) else mint
        self.holders, self.curve_lamports, self.fail = holders, curve_lamports, set(fail)
        self.sigs = [{"signature": "FIXTURE", "blockTime": 1_700_000_000}] if sigs is None else sigs

    def _maybe_fail(self, name):
        if name in self.fail:
            raise RpcUnavailable(f"fixture outage in {name}")

    async def get_account_info(self, address):
        self._maybe_fail("get_account_info")
        return self.mint

    async def get_token_largest_accounts(self, mint):
        self._maybe_fail("get_token_largest_accounts")
        return [{"address": a, "amount": str(int(SUPPLY * pct / 100))} for a, _, pct in self.holders]

    async def get_multiple_accounts(self, addresses):
        self._maybe_fail("get_multiple_accounts")
        owners = {a: o for a, o, _ in self.holders}
        return [token_account(owners[a]) for a in addresses]

    async def get_balance(self, address):
        self._maybe_fail("get_balance")
        return self.curve_lamports

    async def get_signatures_for_address(self, address, limit=100):
        self._maybe_fail("get_signatures_for_address")
        return self.sigs

    async def aclose(self):
        pass
