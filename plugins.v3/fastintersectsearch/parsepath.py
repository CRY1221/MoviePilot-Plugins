"""Send parsing to MoviePilot's Python reference when Rust's word cache overflows.

MoviePilot-Rust keeps compiled custom-word regexes in a 256-entry LRU
(``src/metainfo/custom_words.rs``). A word list that expands past that capacity
thrashes the cache, so every title recompiles roughly 145 fancy-regex patterns:
82 ms per parse against 4.6 ms for MoviePilot's own Python parser, whose
compiled patterns live in a 1024-slot ``lru_cache``. MoviePilot already sends
titles to Python whenever Rust cannot serve them, so this widens an existing
decision (``_requires_python_metainfo``) instead of adding a new path.
"""
import threading
from functools import wraps

from .recognition import ACTIVE
from .subscription import SUBSCRIPTION_SCOPE

RUST_WORD_CACHE_CAPACITY = 256


def effective_word_count(words):
    """Count the words Rust compiles: no blanks, no comments.

    Rust also skips entries it cannot parse and compiles two patterns for a
    combined replace-and-offset rule, so this is a lower bound. Erring low only
    keeps a list on Rust; it never routes a cheap list to the Python parser.
    """
    return sum(1 for word in words or []
               if (text := str(word).strip()) and not text.startswith("#"))


def make_parse_path(original, enabled, capacity=RUST_WORD_CACHE_CAPACITY,
                    configuration=lambda: None):
    """Keep MoviePilot's decision and add the overflowing word list to it."""
    state = {"key": None, "overflow": False}
    lock = threading.Lock()

    def global_overflow():
        # The settings snapshot is rebuilt on change, so its key decides when
        # the count has to be taken again. Counting on every parse would put
        # the whole word list back on the hot path this wrapper exists to free.
        key = configuration()
        with lock:
            if key != state["key"]:
                from app.domain.meta.words import get_custom_words
                state["key"] = key
                state["overflow"] = effective_word_count(get_custom_words()) > capacity
            return state["overflow"]

    @wraps(original)
    def requires_python(title, custom_words=None):
        if original(title, custom_words):
            return True
        if not enabled() or not (ACTIVE.get() is not None or SUBSCRIPTION_SCOPE.get() is not None):
            return False
        if custom_words:
            return effective_word_count(custom_words) > capacity
        return global_overflow()

    return requires_python
