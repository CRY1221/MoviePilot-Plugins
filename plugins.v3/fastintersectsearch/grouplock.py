"""Keep one season's download candidates inside a single release group.

MoviePilot walks the sorted candidate list and takes whatever covers the
episodes still missing (``batch.py`` 按集包/拆包两条路都是这么写的）， so a
season can end up assembled from several groups when no single group covers
everything. Nothing in ``app/chain/download/`` ever reads ``resource_team``.

This module narrows the candidate list handed to the download chain down to the
group this season should use, through MoviePilot's own ``ResourceSelection``
extension point (``app/chain/download/selection.py`` 会回读 ``updated_contexts``）。
"""


def team_of(context):
    """Read the release group MoviePilot already parsed for this candidate."""
    meta = getattr(context, "meta_info", None)
    team = getattr(meta, "resource_team", None)
    return str(team).strip() or None if team else None


def _pri_order(context):
    value = getattr(getattr(context, "torrent_info", None), "pri_order", None)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _episodes(context):
    values = getattr(getattr(context, "meta_info", None), "episode_list", None) or []
    episodes = set()
    for value in values:
        try:
            episodes.add(int(value))
        except (TypeError, ValueError):
            continue
    return episodes


def _is_pack(context):
    """A candidate without episode numbers covers the season as a whole."""
    return not _episodes(context)


def choose_team(contexts, used_team, sort_torrents):
    """Return the group this round should use; ``None`` means stay out.

    ``used_team`` wins whenever the season already has a group: the point is to
    follow the group in use, not whichever candidate happens to sort first now.
    On a genuinely fresh season the choice is MoviePilot's own head candidate —
    its ``pri_order`` becomes a floor, and inside that band the group covering
    the most episodes wins.
    """
    if used_team:
        return used_team
    ordered = sort_torrents(contexts)
    if not ordered:
        return None
    head = ordered[0]
    head_team = team_of(head)
    if not head_team:
        # 认不出组就不干预：不猜。
        return None
    if _is_pack(head):
        # 整包天然只有一个来源，MP 的排序也把它排在最前，直接认它的组。
        return head_team
    floor = _pri_order(head)
    covered, first_seen = {}, {}
    for rank, context in enumerate(ordered):
        team = team_of(context)
        if not team or _pri_order(context) < floor:
            continue
        covered.setdefault(team, set()).update(_episodes(context))
        first_seen.setdefault(team, rank)
    if not covered:
        return None
    return max(covered, key=lambda team: (len(covered[team]), -first_seen[team]))


def narrow(contexts, team):
    """Keep the chosen group plus every candidate whose group is unknown.

    Returns ``None`` when there is nothing to change, so the caller leaves the
    batch exactly as MoviePilot built it.
    """
    if not team:
        return None
    kept = [context for context in contexts if team_of(context) in (None, team)]
    if not kept or len(kept) == len(contexts):
        return None
    return kept


def used_team_of(source):
    """Read the group already in use for this media from download history.

    ``source`` is the dict MoviePilot itself put inside the ``Subscribe|{...}``
    origin string. Returns ``None`` when nothing was downloaded yet; raises when
    the lookup itself fails, so the caller can decline to guess.
    """
    from app.db.oper.downloadhistory import DownloadHistoryOper
    from app.domain.meta.releasegroup import ReleaseGroupsMatcher

    media_source, media_id = source.get("media_source"), source.get("media_id")
    if not media_source or not media_id:
        return None
    rows = DownloadHistoryOper().get_by_media_identity(media_source, str(media_id))
    if not rows:
        return None
    season = source.get("season")
    if season is not None:
        matching = [row for row in rows if str(getattr(row, "seasons", "")) == str(season)]
        if matching:
            rows = matching
    # 查询口不带排序，这里自己按时间取最新的一条。
    rows = sorted(rows, key=lambda row: (str(getattr(row, "date", "") or ""),
                                         int(getattr(row, "id", 0) or 0)))
    matcher = ReleaseGroupsMatcher()
    for row in reversed(rows):
        team = matcher.match(title=getattr(row, "torrent_name", None) or "")
        if team:
            return team
    return None


class _Silent:
    """Log target used when the caller injects none."""

    @staticmethod
    def debug(*_args, **_kwargs):
        pass

    @staticmethod
    def info(*_args, **_kwargs):
        pass

    @staticmethod
    def warning(*_args, **_kwargs):
        pass


def make_group_lock(enabled, sort_torrents, parse_subscribe, tv_type,
                    history_lookup=used_team_of, log=None):
    """Build the ``ResourceSelection`` handler body.

    Returns ``None`` whenever the plugin is off, the batch is not a subscription
    download, or the candidates need no change. Every exit says why in the log,
    because otherwise a silent no-op is indistinguishable from a dead handler.
    """
    log = log or _Silent()

    def apply(origin, contexts):
        if not enabled() or not contexts:
            return None
        source = parse_subscribe(origin)
        if not source:
            log.debug("一季同组：不是订阅下载，不干预")
            return None
        if source.get("type") != tv_type:
            log.debug("一季同组：不是电视剧，不干预")
            return None
        try:
            used_team = history_lookup(source)
        except Exception as error:
            # 查不到已用的组就不猜，交给 MP 原样处理。
            log.warning(f"一季同组：读取已用制作组失败，本轮不干预（{error}）")
            return None
        team = choose_team(contexts, used_team, sort_torrents)
        if not team:
            log.debug("一季同组：这一轮认不出制作组，不干预")
            return None
        narrowed = narrow(contexts, team)
        if narrowed is None:
            log.debug(f"一季同组：{team} 无需收敛（候选 {len(contexts)} 条）")
            return None
        log.info(f"一季同组：锁定 {team}，候选 {len(contexts)} → {len(narrowed)}")
        return narrowed

    return apply
