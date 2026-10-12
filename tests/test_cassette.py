"""A replay must reproduce processing without ever reaching the network."""
import sys
from dataclasses import dataclass, field
from types import ModuleType, SimpleNamespace as NS

import pytest

from fastsearch.cassette import (CASSETTE, build_page, freeze_page,
                                 make_cassette, mask_contexts, mask_torrent)


@dataclass
class FakeTorrent:
    data: dict = field(default_factory=dict)

    def to_dict(self):
        return dict(self.data)

    def from_dict(self, values):
        self.data.update(values)


@dataclass
class FakeObservation:
    attempted: bool = False
    outcome: str = "skipped"
    error: object = None
    raw_count: object = None
    has_more: object = None


@dataclass
class FakeScanPage:
    torrents: list
    observation: FakeObservation
    retry_at: object = None
    wait_reason: object = None


@pytest.fixture(autouse=True)
def fake_app(monkeypatch):
    """Substitute only the three native types build_page reconstructs."""
    scan = ModuleType("app.chain.search.scan")
    scan.ScanPage = FakeScanPage
    observation = ModuleType("app.application.site.observation")
    observation.SiteSearchObservation = FakeObservation
    context = ModuleType("app.domain.context")
    context.TorrentInfo = FakeTorrent
    for name, module in (("app", ModuleType("app")), ("app.chain", ModuleType("app.chain")),
                         ("app.chain.search", ModuleType("app.chain.search")),
                         ("app.application", ModuleType("app.application")),
                         ("app.application.site", ModuleType("app.application.site")),
                         ("app.domain", ModuleType("app.domain")),
                         ("app.chain.search.scan", scan),
                         ("app.application.site.observation", observation),
                         ("app.domain.context", context)):
        monkeypatch.setitem(sys.modules, name, module)
    CASSETTE.stop()
    yield
    CASSETTE.stop()


def torrent(**values):
    return FakeTorrent({"site_name": "示例站", "title": "Example.2026.1080p", "description": "",
                        "enclosure": "https://example.invalid/a.torrent", "size": 1024.0,
                        "site_cookie": "uid=1; pass=secret", "site_ua": "UA", **values})


def page(torrents=None, **observation):
    return FakeScanPage(torrents=torrents if torrents is not None else [torrent()],
                        observation=FakeObservation(**{"attempted": True, "outcome": "success",
                                                       "raw_count": 1, "has_more": True, **observation}))


def sources(media_id="282326"):
    return NS(media=NS(media_id=media_id), queries={})


def test_credentials_never_reach_the_corpus():
    frozen = freeze_page(page())
    assert "site_cookie" not in frozen["torrents"][0]
    assert "site_ua" not in frozen["torrents"][0]
    assert frozen["torrents"][0]["enclosure"] == "https://example.invalid/a.torrent"
    item = {"torrent_info": {"site_cookie": "secret", "title": "x"}, "media_info": {"title": "x"}}
    assert mask_contexts([item])[0]["torrent_info"] == {"title": "x"}
    assert mask_torrent({"title": "x"}) == {"title": "x"}


def test_recorded_page_rebuilds_the_native_scan_page():
    record = {"key": "3:Example", "page": 0, **freeze_page(page([torrent(), torrent(title="B")]))}
    rebuilt = build_page(record)
    assert [item.data["title"] for item in rebuilt.torrents] == ["Example.2026.1080p", "B"]
    assert rebuilt.observation.outcome == "success"
    assert rebuilt.observation.has_more is True
    assert rebuilt.retry_at is None


def test_armed_ignores_other_searches():
    CASSETTE.start("record", "282326")
    assert CASSETTE.armed(sources("282326")) is True
    assert CASSETTE.armed(sources(282326)) is True
    # A concurrent unrelated search must pass straight through to the network.
    assert CASSETTE.armed(sources("999")) is False
    CASSETTE.stop()
    assert CASSETTE.armed(sources("282326")) is False


def test_recording_then_replay_never_calls_the_original():
    calls = []

    def original(self, key, page_number, *, budget=None):
        calls.append((key, page_number))
        return page([torrent(title=f"{key}:{page_number}")])

    fetch = make_cassette(original, lambda: True)
    CASSETTE.mode, CASSETTE.media_id = "record", "282326"
    try:
        recorded = fetch(sources(), "3:Example", 0)
        assert recorded.torrents[0].data["title"] == "3:Example:0"
        assert len(CASSETTE.dump()) == 1
    finally:
        CASSETTE.mode = None
    CASSETTE.mode, CASSETTE.media_id = "replay", "282326"
    CASSETTE.load(CASSETTE.dump())
    try:
        replayed = fetch(sources(), "3:Example", 0)
        assert calls == [("3:Example", 0)]          # the original ran once, during recording
        assert replayed.torrents[0].data["title"] == "3:Example:0"
        assert replayed.observation.outcome == "success"
    finally:
        CASSETTE.mode = None


def test_replay_refuses_to_fall_back_to_the_network():
    def original(self, key, page_number, *, budget=None):
        raise AssertionError("回放不得打站点")

    CASSETTE.start("replay", "282326")
    CASSETTE.load([])
    fetch = make_cassette(original, lambda: True)
    with pytest.raises(RuntimeError):
        fetch(sources(), "3:Example", 0)
    assert CASSETTE.missing == [{"key": "3:Example", "page": 0}]


def test_inactive_cassette_is_a_pure_passthrough():
    calls = []

    def original(self, key, page_number, *, budget=None):
        calls.append(budget)
        return page()

    fetch = make_cassette(original, lambda: True)
    fetch(sources(), "3:Example", 0, budget="claim")
    fetch = make_cassette(original, lambda: False)
    CASSETTE.mode, CASSETTE.media_id = "record", "282326"
    try:
        fetch(sources(), "3:Example", 0, budget="claim")
    finally:
        CASSETTE.mode = None
    assert calls == ["claim", "claim"]
    assert CASSETTE.dump() == []
