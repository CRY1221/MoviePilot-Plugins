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
        self.operation_seconds = 0.0
        self.copy_seconds = 0.0
        self.wait_seconds = 0.0

    def clone(self, value):
        started = time.monotonic()
        result = copy.deepcopy(value)
        with self.lock:
            self.copy_seconds += time.monotonic() - started
        return result

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
            started = time.monotonic()
            value = future.result()
            with self.lock:
                self.wait_seconds += time.monotonic() - started
            return self.clone(value)
        try:
            started = time.monotonic()
            value = operation()  # Exceptions are not cached.
            operation_seconds = time.monotonic() - started
            snapshot = self.clone(value)
            with self.lock:
                self.misses += 1
                self.seconds += time.monotonic() - started
                self.operation_seconds += operation_seconds
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
        self.probes = {}
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
            probes = copy.deepcopy(self.probes)
        def counters(cache):
            return {"hits": cache.hits, "misses": cache.misses,
                    "compute_seconds": round(cache.seconds, 6),
                    "operation_seconds": round(cache.operation_seconds, 6),
                    "copy_seconds": round(cache.copy_seconds, 6),
                    "wait_seconds": round(cache.wait_seconds, 6)}
        return {"parse": counters(self.parse), "identity": counters(self.identity),
                "stage_totals": stages, "probes": probes}

    def record(self, name, wall, cpu, none=False, error=False):
        with self.lock:
            bucket = self.probes.setdefault(name, {"calls": 0, "wall_seconds": 0.0,
                                                   "thread_cpu_seconds": 0.0,
                                                   "none_results": 0, "errors": 0})
            bucket["calls"] += 1
            bucket["wall_seconds"] += wall
            bucket["thread_cpu_seconds"] += cpu
            bucket["none_results"] += int(none)
            bucket["errors"] += int(error)


def make_probe(original, enabled, name):
    """Observe real native execution and fallback; no return/exception changes."""
    @wraps(original)
    def probe(*args, **kwargs):
        scope = ACTIVE.get()
        if scope is None or not enabled():
            return original(*args, **kwargs)
        wall, cpu = time.monotonic(), time.thread_time()
        none, error = False, True
        try:
            result = original(*args, **kwargs)
            none, error = result is None, False
            return result
        finally:
            scope.record(name, time.monotonic() - wall, time.thread_time() - cpu, none, error)
    return probe


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
        wall, cpu = time.monotonic(), time.thread_time()
        config = configuration()
        cache.record("parse_configuration", time.monotonic() - wall, time.thread_time() - cpu)
        key = (torrent.title, torrent.description, mediainfo.type,
               None if custom_words is None else tuple(custom_words), config)
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
