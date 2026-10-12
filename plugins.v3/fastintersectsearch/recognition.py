"""Search-local calculation reuse; never remove candidates or cache rule decisions."""
import copy
import inspect
import threading
import time
from concurrent.futures import Future
from contextvars import ContextVar
from functools import wraps

ACTIVE = ContextVar("fastsearch_recognition", default=None)


class SearchCache:
    def __init__(self, limit=4096):
        self.limit = limit
        self.lock = threading.RLock()
        self.entries = {}
        self.hits = 0
        self.misses = 0
        self.seconds = 0.0

    def reuse(self, key, operation):
        with self.lock:
            if key in self.entries:
                self.hits += 1
                future, owner = self.entries[key], False
            elif len(self.entries) < self.limit:
                future, owner = Future(), True
                self.entries[key] = future
            else:
                future, owner = Future(), True
        if not owner:
            return copy.deepcopy(future.result())
        try:
            started = time.monotonic()
            value = operation()  # Exceptions are not cached.
            snapshot = copy.deepcopy(value)
            with self.lock:
                self.misses += 1
                self.seconds += time.monotonic() - started
            future.set_result(snapshot)
            return value
        except BaseException as error:
            with self.lock:
                if self.entries.get(key) is future:
                    del self.entries[key]
            future.set_exception(error)
            raise


class SearchReuse:
    def __init__(self):
        self.parse = SearchCache()
        self.identity = SearchCache()
        self.started = time.monotonic()
        self.stages = {}
        self.lock = threading.Lock()

    def report(self):
        from app.sdk.logging import logger
        logger.info(
            f"极速搜索计算缓存：耗时 {time.monotonic() - self.started:.2f}s；"
            f"解析 命中={self.parse.hits} 新算={self.parse.misses} "
            f"计算={self.parse.seconds:.3f}s；"
            f"消歧 命中={self.identity.hits} 新算={self.identity.misses} "
            f"计算={self.identity.seconds:.3f}s；阶段累计={self.stages}")

    def snapshot(self):
        """Counters and summed worker wall time, not exclusive search latency."""
        with self.lock:
            stages = dict(self.stages)
        return {"parse": {"hits": self.parse.hits, "misses": self.parse.misses,
                           "compute_seconds": round(self.parse.seconds, 3)},
                "identity": {"hits": self.identity.hits, "misses": self.identity.misses,
                              "compute_seconds": round(self.identity.seconds, 3)},
                "stage_totals": stages}


def make_stage(original, enabled, name):
    @wraps(original)
    def stage(*args, **kwargs):
        scope = ACTIVE.get()
        if scope is None or not enabled():
            return original(*args, **kwargs)
        started = time.monotonic()
        try:
            return original(*args, **kwargs)
        finally:
            elapsed = time.monotonic() - started
            with scope.lock:
                old = scope.stages.get(name, (0, 0.0))
                scope.stages[name] = (old[0] + 1, round(old[1] + elapsed, 3))
    return stage


def make_meta(original, enabled, configuration):
    @wraps(original)
    def meta(torrent, custom_words, mediainfo):
        cache = ACTIVE.get()
        if cache is None or not enabled() or str(getattr(mediainfo.type, "value", mediainfo.type)).lower() in {"music", "音乐"}:
            return original(torrent, custom_words, mediainfo)
        key = (torrent.title, torrent.description, mediainfo.type,
               None if custom_words is None else tuple(custom_words), configuration())
        return cache.parse.reuse(key, lambda: original(torrent, custom_words, mediainfo))
    return meta


def make_same_work(original, enabled, native_key, configuration=lambda: None):
    @wraps(original)
    def same_work(torrent, torrent_meta, mediainfo, cache):
        scope = ACTIVE.get()
        if scope is None or not enabled():
            return original(torrent, torrent_meta, mediainfo, cache)
        key = native_key(torrent_meta)
        if key not in cache:
            def identify():
                from app.chain.media import MediaChain
                return MediaChain().recognize_by_meta(torrent_meta, obtain_images=False)
            # Keep the native batch key. Across batches, conservatively require
            # all parsed evidence/configuration to agree, not just names/year.
            reuse_key = (key, repr(vars(torrent_meta)), configuration())
            cache[key] = scope.identity.reuse(reuse_key, identify)
        return original(torrent, torrent_meta, mediainfo, cache)
    return same_work


def make_scope(original, enabled, is_music, generator=False):
    signature = inspect.signature(original)

    def allowed(args, kwargs):
        values = signature.bind_partial(*args, **kwargs).arguments
        owner = values.get("manual", values.get("owner"))
        media = getattr(owner, "media", None)
        plan = values.get("plan")
        if plan is not None:
            media = plan.mediainfo
        return enabled() and not is_music(getattr(media, "type", None))

    if not generator:
        @wraps(original)
        def run(*args, **kwargs):
            if ACTIVE.get() is not None or not allowed(args, kwargs):
                return original(*args, **kwargs)
            scope = SearchReuse()
            token = ACTIVE.set(scope)
            try:
                return original(*args, **kwargs)
            finally:
                ACTIVE.reset(token)
                scope.report()
        return run

    @wraps(original)
    async def events(*args, **kwargs):
        inherited = ACTIVE.get()
        scope = inherited or (SearchReuse() if allowed(args, kwargs) else None)
        iterator = original(*args, **kwargs)
        try:
            while True:
                token = ACTIVE.set(scope)
                try:
                    event = await anext(iterator)
                except StopAsyncIteration:
                    break
                finally:
                    ACTIVE.reset(token)
                if scope is not None and isinstance(event, dict) and event.get("type") == "done":
                    event = {**event, "recognition_cache": scope.snapshot()}
                yield event
        finally:
            token = ACTIVE.set(scope)
            try:
                await iterator.aclose()
            finally:
                ACTIVE.reset(token)
                if scope is not None and inherited is None:
                    scope.report()
    return events
