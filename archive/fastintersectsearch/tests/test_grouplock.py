"""Keep a season inside one release group, without touching MoviePilot's order."""
import sys
from types import ModuleType, SimpleNamespace as NS

import pytest

from fastsearch.grouplock import (choose_team, make_group_lock, narrow, team_of,
                                  used_team_of)


def candidate(team, episodes=(), pri=0, seeders=0, title=None):
    return NS(meta_info=NS(resource_team=team, episode_list=list(episodes)),
              torrent_info=NS(pri_order=pri, seeders=seeders,
                              title=title or f"{team or 'unknown'}-{pri}"))


def order(contexts):
    """Mirror MoviePilot's ordering: priority first, then seeders — episode count
    only breaks a tie at the very end (``TorrentHelper.sort_torrents``)."""
    def key(context):
        episodes = context.meta_info.episode_list or []
        torrent = context.torrent_info
        return (torrent.pri_order, 1 if not episodes else 0, torrent.seeders, len(episodes))
    return sorted(contexts, key=key, reverse=True)


def test_team_of_trims_and_treats_blank_as_unknown():
    assert team_of(candidate(" QHstudIo ")) == "QHstudIo"
    assert team_of(candidate("")) is None
    assert team_of(candidate(None)) is None
    assert team_of(NS(meta_info=None)) is None


def test_first_round_ignores_a_lower_priority_group_however_much_it_covers():
    """用户举过的反例：层级18 覆盖多，也不能压过层级52 的头名。"""
    contexts = [candidate("LowTier", range(1, 11), pri=18),
                candidate("HighTier", [1, 2], pri=52)]
    assert choose_team(contexts, None, order) == "HighTier"


def test_first_round_prefers_the_group_covering_more_inside_the_priority_floor():
    """同层级内改看覆盖：MP 会因为做种多先选 Head，我们改选能一次补最多的。"""
    contexts = [candidate("Head", [1], pri=52, seeders=500),
                candidate("Wide", range(1, 7), pri=52, seeders=5)]
    assert order(contexts)[0].meta_info.resource_team == "Head"
    assert choose_team(contexts, None, order) == "Wide"


def test_first_round_breaks_a_coverage_tie_by_moviepilot_order():
    contexts = [candidate("Earlier", [1, 2], pri=52),
                candidate("Later", [1, 2], pri=52)]
    assert choose_team(contexts, None, order) == "Earlier"


def test_first_round_takes_the_pack_group_without_a_coverage_comparison():
    contexts = [candidate("Pack", [], pri=60),
                candidate("Episodes", range(1, 21), pri=60)]
    assert choose_team(contexts, None, order) == "Pack"


def test_unknown_head_group_stays_out():
    contexts = [candidate("Known", [1, 2], pri=10),
                candidate(None, [1, 2, 3], pri=90)]
    assert choose_team(contexts, None, order) is None


def test_the_group_already_in_use_wins_over_this_round_s_head():
    contexts = [candidate("MTeam", [1, 2], pri=90),
                candidate("HDSky", [1, 2], pri=10)]
    assert choose_team(contexts, "HDSky", order) == "HDSky"


def test_narrow_keeps_the_group_and_every_candidate_whose_group_is_unknown():
    known, other, unknown = candidate("HDSky"), candidate("MTeam"), candidate(None)
    contexts = [known, known, unknown, other]
    assert narrow(contexts, "HDSky") == [known, known, unknown]


def test_narrow_declines_when_nothing_would_change_or_nothing_would_remain():
    same = [candidate("HDSky"), candidate("HDSky")]
    assert narrow(same, "HDSky") is None
    assert narrow(same, "MTeam") is None
    assert narrow(same, None) is None
    assert narrow([], "HDSky") is None


def lock(monkeypatch, enabled=True, used=None, raise_on_lookup=False):
    def history_lookup(source):
        if raise_on_lookup:
            raise RuntimeError("history unavailable")
        return used

    def parse(origin):
        return origin if isinstance(origin, dict) else None

    apply = make_group_lock(lambda: enabled, order, parse, "电视剧",
                            history_lookup=history_lookup)
    return apply


def test_handler_narrows_only_subscription_tv_batches(monkeypatch):
    apply = lock(monkeypatch, used="HDSky")
    batch = [candidate("HDSky"), candidate("MTeam"), candidate(None)]
    assert apply({"type": "电视剧"}, batch) == batch[:1] + batch[2:]
    assert apply({"type": "电影"}, batch) is None
    assert apply(None, batch) is None
    assert apply({"type": "电视剧"}, []) is None
    assert lock(monkeypatch, used="HDSky", enabled=False)({"type": "电视剧"}, batch) is None


def test_handler_declines_when_the_history_lookup_fails(monkeypatch):
    apply = lock(monkeypatch, raise_on_lookup=True)
    assert apply({"type": "电视剧"}, [candidate("HDSky"), candidate("MTeam")]) is None


def test_handler_says_why_in_the_log_either_way():
    lines = []
    log = NS(debug=lines.append, info=lines.append, warning=lines.append)

    def parse(origin):
        return origin if isinstance(origin, dict) else None

    apply = make_group_lock(lambda: True, order, parse, "电视剧",
                            history_lookup=lambda source: "HDSky", log=log)
    batch = [candidate("HDSky"), candidate("MTeam")]
    assert apply({"type": "电视剧"}, batch) == batch[:1]
    assert lines == ["一季同组：锁定 HDSky，候选 2 → 1"]
    lines.clear()
    assert apply({"type": "电影"}, batch) is None
    assert lines == ["一季同组：不是电视剧，不干预"]


def host_module(monkeypatch, name):
    result = ModuleType(name)
    result.__path__ = []
    monkeypatch.setitem(sys.modules, name, result)
    if "." in name:
        parent, _, child = name.rpartition(".")
        if parent not in sys.modules:
            host_module(monkeypatch, parent)
        monkeypatch.setattr(sys.modules[parent], child, result, raising=False)
    return result


def history_env(monkeypatch, rows, teams):
    def match(title):
        return teams.get(title, "")
    oper = host_module(monkeypatch, "app.db.oper.downloadhistory")
    oper.DownloadHistoryOper = lambda: NS(get_by_media_identity=lambda source, media_id: rows)
    matcher = host_module(monkeypatch, "app.domain.meta.releasegroup")
    matcher.ReleaseGroupsMatcher = lambda: NS(match=lambda title: match(title))
    return rows


def row(identifier, date, torrent, seasons="1"):
    return NS(id=identifier, date=date, torrent_name=torrent, seasons=seasons)


def test_used_team_comes_from_the_newest_matching_season(monkeypatch):
    rows = [row(1, "2026-10-13 00:20:00", "old-MTeam", seasons="2"),
            row(2, "2026-10-13 00:19:00", "pack-HDSky", seasons="1"),
            row(3, "2026-10-13 00:25:00", "newer-QHstudIo", seasons="1")]
    history_env(monkeypatch, rows, {"pack-HDSky": "HDSky", "newer-QHstudIo": "QHstudIo"})
    source = {"media_source": "themoviedb", "media_id": "297253", "season": 1}
    assert used_team_of(source) == "QHstudIo"


def test_used_team_is_absent_without_identity_or_history(monkeypatch):
    history_env(monkeypatch, [], {})
    assert used_team_of({"media_source": "themoviedb", "media_id": "1"}) is None
    assert used_team_of({"media_source": None, "media_id": None}) is None


def test_used_team_looks_past_a_row_it_cannot_attribute(monkeypatch):
    rows = [row(1, "2026-10-13 00:19:00", "recognised-HDSky"),
            row(2, "2026-10-13 00:25:00", "obscure-group-nobody-knows")]
    history_env(monkeypatch, rows, {"recognised-HDSky": "HDSky"})
    assert used_team_of({"media_source": "themoviedb", "media_id": "1"}) == "HDSky"
