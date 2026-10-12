import sys
from types import ModuleType

import pytest

from fastsearch.parsepath import effective_word_count, make_parse_path
from fastsearch.recognition import ACTIVE, SearchReuse
from fastsearch.subscription import SUBSCRIPTION_SCOPE


@pytest.fixture
def scope():
    token = ACTIVE.set(SearchReuse())
    try:
        yield
    finally:
        ACTIVE.reset(token)


def build(monkeypatch, words, capacity=256, enabled=True, configuration=lambda: "k1"):
    counter = {"reads": 0}

    def get_custom_words():
        counter["reads"] += 1
        return words

    module = ModuleType("app.domain.meta.words")
    module.get_custom_words = get_custom_words
    monkeypatch.setitem(sys.modules, "app.domain.meta.words", module)

    def original(title, custom_words=None):
        return title == "tagged"

    wrapper = make_parse_path(original, lambda: enabled, capacity, configuration)
    return wrapper, counter


def test_blank_and_comment_entries_are_not_compiled():
    assert effective_word_count(["a => b", "", "#注释", "  ", "c => d"]) == 2
    assert effective_word_count(None) == 0


def test_moviepilot_decision_is_kept_first(monkeypatch, scope):
    wrapper, _ = build(monkeypatch, ["a"] * 300)
    assert wrapper("tagged", []) is True
    assert wrapper("tagged", ["a"] * 300) is True


def test_disabled_or_out_of_scope_stays_on_rust(monkeypatch, scope):
    words = ["a"] * 300
    wrapper, _ = build(monkeypatch, words, enabled=False)
    assert wrapper("plain", []) is False
    enabled_wrapper, _ = build(monkeypatch, words)
    token = ACTIVE.set(None)
    try:
        assert enabled_wrapper("plain", []) is False
    finally:
        ACTIVE.reset(token)
    token = SUBSCRIPTION_SCOPE.set(object())
    try:
        assert enabled_wrapper("plain", []) is True
    finally:
        SUBSCRIPTION_SCOPE.reset(token)


def test_explicit_list_decides_by_its_own_size(monkeypatch, scope):
    wrapper, _ = build(monkeypatch, ["global"] * 300)
    assert wrapper("plain", ["a"] * 256) is False
    assert wrapper("plain", ["a"] * 257) is True
    assert wrapper("plain", ["a", "#跳过", ""] * 100) is False


def test_global_count_is_taken_once_per_configuration(monkeypatch, scope):
    box = {"key": "k1"}
    wrapper, counter = build(monkeypatch, ["a"] * 300, configuration=lambda: box["key"])
    assert wrapper("plain") is True
    assert wrapper("plain") is True
    assert counter["reads"] == 1
    box["key"] = "k2"
    assert wrapper("plain") is True
    assert counter["reads"] == 2
