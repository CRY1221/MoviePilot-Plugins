import copy
import re

from fastsearch.rulecompat import compatible_pattern, compatible_rules, make_filter
from fastsearch.recognition import ACTIVE, SearchReuse


def test_literal_nested_bracket_preserves_complete_expression_and_matches():
    pattern = r"(?<![A-Za-z])[-@&\\[(_.]GTR(?![A-Za-z])"
    rewritten = compatible_pattern(pattern)
    assert rewritten != pattern
    assert repr(re._parser.parse(pattern, re.I)) == repr(re._parser.parse(rewritten, re.I))
    for prefix in map(chr, range(128)):
        for suffix in ("", "A", "."):
            text = prefix + "GTR" + suffix
            assert bool(re.search(pattern, text, re.I)) == bool(re.search(rewritten, text, re.I))


def test_escaped_brackets_flags_comments_negation_ranges_and_bad_patterns():
    for pattern in (r"[-@&\[(_.]GTR", r"\[GTR\]", r"[\]]", "broken[", r"[^[]", r"[][a-z]", r"(?x) [a[] # [comment", r"(?# [comment)[a[]"):
        rewritten = compatible_pattern(pattern)
        if rewritten != pattern:
            assert repr(re._parser.parse(pattern, re.I)) == repr(re._parser.parse(rewritten, re.I))
    assert compatible_pattern(5) == 5


def test_rules_are_not_mutated_and_native_fallback_keeps_original():
    rules = {"a": {"include": [r"[a[]", r"normal"], "exclude": r"[b[]", "size_range": "1-3"}}
    saved = copy.deepcopy(rules)
    normalized = compatible_rules(rules)
    assert normalized != rules and rules == saved
    received = []
    def native(*args):
        received.append(args)
        return None
    wrapper = make_filter(native, lambda: True)
    assert wrapper([], [], rules) is None
    assert received[-1][2] is rules
    token = ACTIVE.set(SearchReuse())
    try:
        assert wrapper([], [], rules) is None
        assert received[-1][2] == normalized
        assert rules == saved
        make_filter(native, lambda: False)([], [], rules)
        assert received[-1][2] is rules
    finally:
        ACTIVE.reset(token)
