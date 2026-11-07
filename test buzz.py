"""Popularity ("buzz") tests. Synthetic FIXTURE data only, no network. Run next to main.py:  pytest -q test_buzz.py   (or: python test_buzz.py)"""
import asyncio
import time
from types import SimpleNamespace

from main import (BASE58_ADDRESS, BuzzEngine, TradeStats, buzz_chatter, buzz_community, buzz_crowd, buzz_market, combine_buzz,  # noqa: F401
                  extract_links, rank_score, telegram_handle, x_link_ok)

MINT = "A" * 44


def stats(**kw):
    base = dict(observed_seconds=300, n_trades=60, last_trade_age=2, unique_buyers=30, distinct_sellers=6, creator_sold_sol=0, volume_sol=40,
                top_trader_pct=8, wash_pct=10, mcap_now=30, mcap_ref=25, mcap_change_pct=20, drawdown_pct=3, buy_pressure_pct=70, new_buyers=8, window_seconds=120)
    base.update(kw)
    return TradeStats(**base)


def test_links_are_validated_and_never_opened():
    ln = extract_links({"twitter": "https://x.com/somecoin", "telegram": "t.me/somecoin_chat", "website": "https://coin.example"},
                       {"socials": [{"type": "twitter", "url": "https://x.com/other"}], "websites": [{"url": "https://other.example"}]})
    assert ln == {"x": "https://x.com/somecoin", "telegram": "https://t.me/somecoin_chat", "website": "https://coin.example"}
    bad = extract_links({"twitter": "javascript:alert(1)", "telegram": "https://evil.example/somecoin", "website": "http://plain.example"})
    assert bad == {"x": None, "telegram": None, "website": None}
    assert telegram_handle("https://t.me/+AbCdEf123") is None and telegram_handle("https://t.me/joinchat/xyz") is None and telegram_handle("https://t.me/abc") is None
    assert telegram_handle("https://t.me/real_group") == "real_group"
    assert x_link_ok("https://x.com/i/communities/123456") and not x_link_ok("https://x.com/") and not x_link_ok("https://notx.com/abc")


def test_crowd_rewards_independent_buyers_and_discounts_wash():
    organic = buzz_crowd(stats())
    washed = buzz_crowd(stats(wash_pct=80, top_trader_pct=40))
    assert organic["score"] > 60 and organic["organic"] and washed["score"] < organic["score"] and not washed["organic"]
    assert buzz_crowd(None) is None and buzz_crowd(stats(n_trades=2)) is None   # not enough trades: no guess


def test_community_scores_links_and_group_size_and_unknown_is_not_zero():
    full = buzz_community({"x": "u", "telegram": "u", "website": "u"}, 5000)
    assert full["score"] == 100.0
    assert buzz_community({"x": None, "telegram": None, "website": None}, None)["score"] == 0
    assert buzz_community(None, None) is None   # metadata not read yet = unknown, never "no links"
    assert 50 < buzz_community({"x": "u", "telegram": "u", "website": None}, 500)["score"] < 100


def test_market_and_chatter():
    hot = buzz_market({"buys_m5": 40, "sells_m5": 10, "buys_h1": 250, "sells_h1": 100, "boosts": 0, "profile": True})
    cold = buzz_market({"buys_m5": 0, "sells_m5": 1, "buys_h1": 3, "sells_h1": 9})
    assert hot["score"] > 70 > cold["score"] and buzz_market(None) is None and buzz_market({}) is None
    assert buzz_market({"buys_h1": 1, "boosts": 3})["boosted"]
    assert buzz_chatter(40)["score"] == 100 and buzz_chatter(None) is None


def test_combine_drops_missing_sources_flags_hype_and_caps_inorganic():
    b = combine_buzz([buzz_crowd(stats()), None, None, None], None)
    assert b["coverage"] == 0.4 and b["level"] in ("warm", "hot") and not b["warnings"]
    fake = combine_buzz([buzz_crowd(stats(wash_pct=90)), buzz_community({"x": "u", "telegram": "u", "website": "u"}, 9000)], {})
    assert fake["score"] <= 40 and any("manufactured" in w for w in fake["warnings"])
    paid = combine_buzz([buzz_market({"buys_h1": 100, "sells_h1": 10, "boosts": 2})], {})
    assert any("paid" in w.lower() for w in paid["warnings"]) and any("Limited" in w for w in paid["warnings"])
    assert combine_buzz([None, None], None) is None


def test_rank_only_nudges_and_never_creates_a_pick():
    hot = {"score": 90, "coverage": 1.0}
    assert rank_score(60, hot, 0.25) == 67.5 and rank_score(60, None, 0.25) == 60 and rank_score(None, hot, 0.25) is None
    assert rank_score(60, {"score": 90, "coverage": 0.2}, 0.25) == 60 and rank_score(60, hot, 0) == 60
    assert rank_score(60, hot, 0.99) == round(0.4 * 60 + 0.6 * 90, 1)   # weight is capped at 60%


class Resp:
    def __init__(self, code=200, data=None): self.status_code, self._d = code, data
    def json(self): return self._d


class FakeHttp:
    def __init__(self, dex=None, tg=None, fail=False): self.dex, self.tg, self.fail, self.calls = dex, tg, fail, []
    async def get(self, url):
        self.calls.append(url)
        if self.fail: raise RuntimeError("boom " + url)
        return self.dex or Resp(200, {"pairs": []})
    async def post(self, url, json=None):
        self.calls.append(url)
        return self.tg or Resp(200, {"ok": True, "result": 1200})


class FakeImages:
    def __init__(self, meta): self.meta, self.n = meta, 0
    async def get_metadata(self, uri): self.n += 1; return self.meta


class FakeTracker:
    def __init__(self, st): self.st = st
    def snapshot(self, mint): return self.st


def make_engine(http, meta=None, st=None, **cfg):
    c = SimpleNamespace(buzz_dexscreener=True, buzz_telegram_members=True, buzz_pumpfun_comments=False, buzz_comments_url="https://c.example/{mint}",
                        buzz_max_tokens=10, buzz_interval_seconds=45, **cfg)
    return BuzzEngine(None, FakeTracker(st), FakeImages(meta), c, tg_token="123:SECRET-TOKEN", client=http)


TOKEN = SimpleNamespace(mint=MINT, uri="ipfs://x")
DEX = Resp(200, {"pairs": [{"chainId": "solana", "liquidity": {"usd": 9000}, "txns": {"m5": {"buys": 20, "sells": 5}, "h1": {"buys": 120, "sells": 40}},
                            "boosts": {"active": 1}, "info": {"socials": [{"type": "twitter", "url": "https://x.com/dexcoin"}], "websites": []}}]})


def test_engine_combines_all_sources_and_caches_calls():
    http = FakeHttp(dex=DEX)
    e = make_engine(http, meta={"twitter": "https://x.com/coin", "telegram": "https://t.me/coin_chat"}, st=stats())
    b = asyncio.run(e.score(TOKEN))
    assert b["coverage"] == 0.85 and {c["name"] for c in b["components"]} == {"crowd", "community", "market"}
    assert b["links"]["x"] == "https://x.com/coin" and any("paid" in w.lower() for w in b["warnings"])
    n = len(http.calls); asyncio.run(e.score(TOKEN))
    assert len(http.calls) == n and e.images.n == 1   # DEX Screener, Telegram and metadata are all cached


def test_engine_survives_every_source_failing_and_never_leaks_the_bot_token():
    e = make_engine(FakeHttp(fail=True), meta=None, st=None)
    assert asyncio.run(e.score(TOKEN)) is None   # nothing known: no score, not a guess
    assert "SECRET-TOKEN" not in str(dict(e.errors))
    e2 = make_engine(FakeHttp(fail=True), meta={"telegram": "https://t.me/coin_chat"}, st=stats())
    b = asyncio.run(e2.score(TOKEN))
    assert b and "SECRET-TOKEN" not in str(dict(e2.errors)) and "SECRET-TOKEN" not in str(b)


def test_dex_rate_limit_backs_off():
    http = FakeHttp(dex=Resp(429, None))
    e = make_engine(http, meta={}, st=stats())
    asyncio.run(e.score(TOKEN)); n = len(http.calls)
    e._dex.clear(); asyncio.run(e.score(TOKEN))
    assert len([c for c in http.calls if "dexscreener" in c]) == 1 and e.errors["dex_rate_limited"] == 1 and len(http.calls) >= n


def test_comments_are_off_by_default_and_parse_both_shapes_when_on():
    http = FakeHttp(); e = make_engine(http, meta={}, st=stats()); assert asyncio.run(e._comments(MINT)) is None and not any("c.example" in c for c in http.calls)
    on = make_engine(FakeHttp(dex=Resp(200, {"replies": [{}] * 12})), meta={}, st=stats()); on.cfg.buzz_pumpfun_comments = True
    assert asyncio.run(on._comments(MINT)) == 12
    on2 = make_engine(FakeHttp(dex=Resp(200, [{}] * 7)), meta={}, st=stats()); on2.cfg.buzz_pumpfun_comments = True
    assert asyncio.run(on2._comments(MINT)) == 7


def test_refresh_scores_best_candidates_first_and_respects_the_cap():
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    rows = [SimpleNamespace(mint=chr(65 + i) * 44, uri=None, opportunity_score=float(i), detected_at=now) for i in range(5)]

    class Repo:
        async def list_recent_tokens(self, limit, statuses): return rows
    e = make_engine(FakeHttp(), meta={}, st=stats()); e.repo = Repo(); e.cfg.buzz_max_tokens = 2
    asyncio.run(e.refresh_once())
    assert set(e.cache) == {"E" * 44, "D" * 44} and e.rounds == 1


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn(); print("ok", name)
