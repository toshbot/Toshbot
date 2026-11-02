"""Runs only against the Firestore emulator (set FIRESTORE_EMULATOR_HOST). Skipped otherwise."""
import asyncio
import os

import pytest

from app.config import Settings
from app.models import TokenRecord
from app.parser import parse_message
from app.repository import build_repository
from tests.helpers import create_message

pytestmark = pytest.mark.skipif(not os.environ.get("FIRESTORE_EMULATOR_HOST"), reason="Firestore emulator not configured")


async def test_firestore_dedupes_and_lists():
    repo = build_repository(Settings(_env_file=None, storage_backend="firestore", firestore_collection_prefix="test_"))
    token = TokenRecord.from_event(parse_message(create_message(7)))
    results = await asyncio.gather(*[repo.create_token_if_absent(token) for _ in range(5)])
    assert results.count(True) <= 1  # 0 if a previous run already created it
    assert any(t.mint == token.mint for t in await repo.list_recent_tokens(50))
    assert await repo.ping()
    await repo.aclose()
