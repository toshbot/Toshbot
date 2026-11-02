import json

import pytest

from app.parser import ParseError, parse_message
from tests.helpers import FIXTURE_CREATOR, create_message, fixture_mint


def test_valid_create_event_is_parsed():
    event = parse_message(create_message(1))
    assert event.mint == fixture_mint(1)
    assert event.creator == FIXTURE_CREATOR
    assert event.symbol == "FX1"
    assert event.received_at is not None  # local receipt time always recorded


def test_missing_optional_metadata_does_not_discard_event():
    msg = json.dumps({"mint": fixture_mint(2), "txType": "create"})
    event = parse_message(msg)
    assert event.mint == fixture_mint(2)
    assert event.name is None and event.creator is None


def test_subscription_ack_is_ignored():
    assert parse_message(json.dumps({"message": "Successfully subscribed to keys."})) is None


def test_trade_event_is_ignored():
    assert parse_message(create_message(3, txType="buy")) is None


def test_invalid_mint_raises():
    with pytest.raises(ParseError):
        parse_message(create_message(4, mint="not-a-real-address"))


def test_malformed_json_raises():
    with pytest.raises(ParseError):
        parse_message("{not json")


def test_provider_error_message_is_not_silently_ignored():
    with pytest.raises(ParseError):
        parse_message(json.dumps({"errors": ["rate limited"]}))


def test_garbage_numbers_become_none_not_crash():
    event = parse_message(create_message(5, solAmount="NaN", initialBuy="abc"))
    assert event.sol_amount is None and event.initial_buy_tokens is None
