"""TEST FIXTURES ONLY. Everything here is synthetic and must never be shown as live data."""
import json

ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZ"  # valid base58 characters
FIXTURE_CREATOR = "1" * 32


def fixture_mint(i: int = 0) -> str:
    return ALPHABET[i % len(ALPHABET)] * 44


def create_message(i: int = 0, **overrides) -> str:
    data = {
        "signature": f"FIXTURE-SIG-{i}",
        "mint": fixture_mint(i),
        "traderPublicKey": FIXTURE_CREATOR,
        "txType": "create",
        "initialBuy": 1000.0,
        "solAmount": 1.5,
        "name": f"Fixture Token {i}",
        "symbol": f"FX{i}",
        "uri": "https://example.invalid/meta.json",
        "pool": "pump",
        "marketCapSol": 30.0,
    }
    data.update(overrides)
    return json.dumps(data)
