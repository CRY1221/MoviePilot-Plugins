"""Limit the search keywords to original / Chinese / English names.

MoviePilot expands aliases up to its configured name limit. This adapter keeps
the names that actually matter
for finding releases: the original title, one Chinese name and one English
name. The cast is taken from the media info MoviePilot already recognised, so
no extra metadata request is made.
"""
import re
from functools import wraps

from .scheduling import is_music

_CJK = re.compile(r"[\u3400-\u9fff\uf900-\ufaff\u3040-\u30ff]")


def _clean(value):
    return str(value or "").strip()


def has_cjk(value):
    return bool(_CJK.search(_clean(value)))


def select_search_names(mediainfo, keywords, limit=3):
    """Pick original + Chinese + English names, preserving keyword order."""
    names = [_clean(name) for name in (getattr(mediainfo, "names", None) or [])]
    title = _clean(getattr(mediainfo, "title", None))
    original = _clean(getattr(mediainfo, "original_title", None))
    english = _clean(getattr(mediainfo, "en_title", None))
    regional = [_clean(getattr(mediainfo, field, None))
                for field in ("hk_title", "tw_title", "sg_title")]

    chinese_pool = [title, *regional, *names]
    latin_pool = [english, *names, original]
    chinese = next((name for name in chinese_pool if name and has_cjk(name)), "")
    latin = next((name for name in latin_pool if name and not has_cjk(name)), "")

    picked = []
    for candidate in (original or title, chinese, latin or (original if not has_cjk(original) else "")):
        candidate = _clean(candidate)
        if candidate and candidate not in picked:
            picked.append(candidate)
    if not picked:
        return list(keywords)
    # Keep whatever MoviePilot already had (spacing, punctuation) and stay inside
    # the recognised keyword set whenever those names are present there.
    known = {_clean(keyword).casefold(): keyword for keyword in keywords}
    ordered = []
    for name in picked:
        keyword = known.get(name.casefold(), name)
        if keyword not in ordered:
            ordered.append(keyword)
    return ordered[:max(1, limit)]


def make_prepare_params(original, enabled):
    """Trim expanded keywords unless the caller passed an explicit keyword."""

    @wraps(original)
    def prepare(mediainfo, keyword=None, no_exists=None, include_candidates=False):
        season_episodes, keywords = original(
            mediainfo, keyword=keyword, no_exists=no_exists,
            include_candidates=include_candidates)
        if not enabled() or keyword or not keywords or is_music(getattr(mediainfo, "type", None)):
            return season_episodes, keywords
        return season_episodes, select_search_names(mediainfo, keywords)

    return prepare
