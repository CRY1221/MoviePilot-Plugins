"""Normalize Python literal brackets for Rust without editing the rule repository.

Python treats an unescaped '[' inside a character class as a literal. Rust's
regex grammar interprets it as a nested class. Accept a rewrite only when
Python's complete parsed expression is identical, including flags and ranges.
"""
import re
from contextvars import ContextVar
from copy import deepcopy
from functools import lru_cache, wraps

from .recognition import ACTIVE
from .subscription import SUBSCRIPTION_SCOPE
from .scheduling import is_music

VALIDATION = ContextVar("fastsearch_rule_validation", default=None)
VERIFY_FILTER = ContextVar("fastsearch_verify_filter", default=True)


@lru_cache(maxsize=512)
def compatible_pattern(pattern):
    if not isinstance(pattern, str):
        return pattern
    out, inside, first, escaped = [], False, False, False
    for char in pattern:
        if escaped:
            out.append(char)
            escaped = False
            if inside:
                first = False
        elif char == "\\":
            out.append(char)
            escaped = True
        elif inside:
            if char == "[":
                out.append("\\[")
                first = False
            else:
                out.append(char)
                if char == "]" and not first:
                    inside = False
                elif char != "^" or not first:
                    first = False
        else:
            out.append(char)
            if char == "[":
                inside, first = True, True
    rewritten = "".join(out)
    if rewritten == pattern:
        return pattern
    try:
        # If this Python version changes its parser contract, leave native
        # fallback in charge. Never approximate a user expression.
        from re import _parser
        if repr(_parser.parse(pattern, re.IGNORECASE)) == repr(_parser.parse(rewritten, re.IGNORECASE)):
            return rewritten
    except (ImportError, AttributeError, ValueError, re.error):
        pass
    return pattern


def compatible_rules(rule_set):
    result = dict(rule_set)
    for key, rule in rule_set.items():
        changed = dict(rule)
        for field in ("include", "exclude"):
            value = rule.get(field)
            if isinstance(value, list):
                changed[field] = [compatible_pattern(item) for item in value]
            elif isinstance(value, str):
                changed[field] = compatible_pattern(value)
        result[key] = changed
    return result


def make_filter(original, enabled):
    @wraps(original)
    def call(groups, torrent_list, rule_set, mediainfo=None, metainfo_options=None):
        native_rules = rule_set
        if (enabled() and (ACTIVE.get() is not None or SUBSCRIPTION_SCOPE.get() is not None)
                and not is_music(getattr(mediainfo, "type", None))):
            rule_set = compatible_rules(rule_set)
        result = original(groups, torrent_list, rule_set, mediainfo, metainfo_options)
        validation = VALIDATION.get()
        if validation is not None and VERIFY_FILTER.get() and result is not None:
            from app.modules.filter import FilterModule
            module = FilterModule()
            module.rule_set = native_rules
            clones = deepcopy(torrent_list)
            index_by_id = {id(item): index for index, item in enumerate(clones)}
            baseline = module._FilterModule__filter_torrents_with_python(groups, clones, mediainfo)
            expected = [(index_by_id[id(item)], item.pri_order) for item in baseline]
            actual = list(map(tuple, result[0]))
            validation.append({"kind": "filter", "candidates": len(torrent_list),
                               "equivalent": expected == actual,
                               "python_results": len(expected), "rust_results": len(actual)})
        return result
    return call
