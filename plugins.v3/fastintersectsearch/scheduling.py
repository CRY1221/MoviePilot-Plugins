"""Search I/O scheduling; no matching, rule selection or download policy."""
import asyncio
import inspect
import threading
import time
from collections import deque
from contextlib import aclosing
from contextvars import ContextVar
from functools import wraps


SEARCH_SCOPE = ContextVar("fastsearch_scope", default=None)
AGGREGATE_PAGES = ContextVar("fastsearch_aggregate_pages", default=False)


def is_music(value):
    value = getattr(value, "value", value)
    return str(value).lower() in {"music", "音乐"}


class SiteRequestGate:
    """Process-wide sync/async FIFO for one site.

    The interval is measured from the end of the previous request on that site,
    matching the old plugin's behaviour: request -> wait -> next request.
    """
    def __init__(self, interval=3.0, clock=time.monotonic, poll_interval=.02):
        self.interval = interval
        self.clock = clock
        self.poll_interval = poll_interval
        self.lock = threading.Lock()
        self.states = {}
        self.samples = deque(maxlen=5000)

    def enqueue(self, site):
        key = str(site.get("id") or site.get("domain") or site.get("url") or site.get("name"))
        ticket = object()
        with self.lock:
            state = self.states.setdefault(key, {"queue": deque(), "active": None, "last": None, "interval": 0})
            state["queue"].append(ticket)
        return state, ticket

    def claim(self, state, ticket, interval):
        with self.lock:
            if (state["active"] is None and state["queue"][0] is ticket
                    and (state["last"] is None or self.clock() - state["last"] >= max(interval, state["interval"]))):
                state["queue"].popleft()
                state["active"] = ticket
                state["interval"] = interval
                return True
        return False

    def release(self, state, ticket):
        with self.lock:
            if state["active"] is ticket:
                state["active"] = None
                # The interval counts from the moment the last request finished.
                state["last"] = self.clock()
            elif ticket in state["queue"]:
                state["queue"].remove(ticket)

    def spacing(self, site):
        try:
            native = float(site.get("limit_seconds") or 0)
        except (ValueError, TypeError):
            native = 0
        return max(self.interval, native)

    def record(self, site, queued, started, failed):
        # Diagnostic data must never change a business result.
        try:
            ended = self.clock()
            with self.lock:
                self.samples.append((ended, str(site.get("name") or site.get("id")),
                                     started - queued, ended - started, failed))
        except Exception:
            pass

    def snapshot(self):
        cutoff = self.clock() - 72 * 3600
        with self.lock:
            samples = [sample for sample in self.samples if sample[0] >= cutoff]
        grouped = {}
        for _, name, wait, elapsed, failed in samples:
            group = grouped.setdefault(name, [])
            group.append((wait, elapsed, failed))
        return [{"site": name, "requests": len(items), "last": round(items[-1][1], 2),
                 "mean": round(sum(item[1] for item in items) / len(items), 2),
                 "wait": round(sum(item[0] for item in items) / len(items), 2),
                 "errors": sum(item[2] for item in items)} for name, items in grouped.items()]


# Survives ordinary disable/re-enable, including old in-flight calls.
SHARED_GATE = SiteRequestGate()


def make_site_sync(original, gate, enabled, minimum_interval=lambda owner: 0):
    @wraps(original)
    def call(owner, site, keyword, mtype=None, page=0):
        if not enabled() or is_music(mtype):
            return original(owner, site, keyword, mtype=mtype, page=page)
        state, ticket = gate.enqueue(site)
        queued, started, failed = gate.clock(), None, True
        try:
            while not gate.claim(state, ticket, max(gate.spacing(site), minimum_interval(owner))):
                time.sleep(gate.poll_interval)
            started = gate.clock()
            result = original(owner, site, keyword, mtype=mtype, page=page)
            failed = False
            return result
        finally:
            gate.release(state, ticket)
            if started is not None:
                gate.record(site, queued, started, failed)
    return call


def make_site_async(original, gate, enabled, minimum_interval=lambda owner: 0):
    @wraps(original)
    async def call(owner, site, keyword, mtype=None, page=0):
        if not enabled() or is_music(mtype):
            return await original(owner, site, keyword, mtype=mtype, page=page)
        state, ticket = gate.enqueue(site)
        queued, started, failed = gate.clock(), None, True
        try:
            while not gate.claim(state, ticket, max(gate.spacing(site), minimum_interval(owner))):
                await asyncio.sleep(gate.poll_interval)
            # Shield the real transport: cancelling an await cannot stop a worker thread.
            started = gate.clock()
            task = asyncio.create_task(original(owner, site, keyword, mtype=mtype, page=page))
            try:
                result = await asyncio.shield(task)
                failed = False
                return result
            except asyncio.CancelledError:
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
                if not task.cancelled():
                    task.exception()
                raise
        finally:
            gate.release(state, ticket)
            if started is not None:
                gate.record(site, queued, started, failed)
    return call


def make_read_pages(original, offload, enabled, max_pages=lambda: 1, per_page=lambda: True):
    @wraps(original)
    async def read_pages(manual, keys, page):
        if not enabled() or is_music(getattr(manual.plan.mediainfo, "type", None)):
            async with aclosing(original(manual, keys, page)) as pages:
                async for item in pages:
                    yield item
            return
        groups = {}
        for key in keys:
            groups.setdefault(manual.sources.queries[key]["site"], deque()).append(key)
        if not groups:
            return

        cap = max(1, int(max_pages()))

        async def read_source(site, _slot):
            key = site["keys"][0]
            requested_page = site.get("page", page)
            facts, torrents = await offload(manual.read, key, requested_page)
            history = site.setdefault("history", [])
            history.append(dict(facts))
            more = (not facts.get("error") and facts.get("can_continue") and torrents
                    and requested_page < page + cap - 1 and manual.sources.pageable(key))
            if more:
                site["page"] = requested_page + 1
            else:
                site["keys"].popleft()
                site["page"], site["history"] = page, []
            facts = {**facts, "pages": list(history)}
            return [(key, facts, torrents)]

        pages = manual.owner._iter_site_page_results(
            indexer_sites=[{"keys": group} for group in groups.values()],
            search_pages=list(range(cap * max(map(len, groups.values())))),
            search_page=read_source,
            should_continue=lambda site, _items: bool(site["keys"]),
            task_owner="chain.search.manual.site_page",
        )
        collected = {}
        async with aclosing(pages):
            async for _site, _page, results, _continued, _elapsed in pages:
                key, facts, torrents = results[0]
                if per_page() and not AGGREGATE_PAGES.get():
                    yield key, facts, torrents
                else:
                    batch = collected.setdefault(key, [])
                    batch.extend(torrents)
                    if not _site["keys"] or _site["keys"][0] != key:
                        yield key, facts, collected.pop(key)
    return read_pages


def make_execution_run(original, enabled, no_plugin_sources):
    @wraps(original)
    def run(owner, plan, media_chain):
        scope = owner if enabled() and not is_music(plan.mediainfo.type) and no_plugin_sources() else None
        token = SEARCH_SCOPE.set(scope)
        try:
            return original(owner, plan, media_chain)
        finally:
            SEARCH_SCOPE.reset(token)
    return run


def make_execution_events(original, enabled, no_plugin_sources):
    @wraps(original)
    async def events(owner, plan, media_chain, streaming=True):
        scope = owner if enabled() and not is_music(plan.mediainfo.type) and no_plugin_sources() else None
        generator = original(owner, plan, media_chain, streaming=streaming)
        try:
            while True:
                token = SEARCH_SCOPE.set(scope)
                try:
                    item = await anext(generator)
                except StopAsyncIteration:
                    break
                finally:
                    SEARCH_SCOPE.reset(token)
                yield item
        finally:
            token = SEARCH_SCOPE.set(scope)
            try:
                await generator.aclose()
            finally:
                SEARCH_SCOPE.reset(token)
    return events


def make_keyword_delay(original, enabled):
    @wraps(original)
    def delay(owner, search_count):
        return 0 if enabled() and SEARCH_SCOPE.get() is owner else original(owner, search_count)
    return delay


def make_title_scope(original, enabled, asynchronous=False, generator=False):
    """Give title search the page budget the id search path already has.

    SEARCH_SCOPE is published by the execution owners, so callers that reach the
    provider directly — the web page, both agent search tools, the workflow
    action and the non-streaming title endpoint — kept the native page count,
    which defaults to a single page. The page list is built at the top of this
    call, so publishing the scope here covers every title variant at one point
    without touching the native policy.
    """
    signature = inspect.signature(original)

    def scope(owner, args, kwargs):
        values = signature.bind(owner, *args, **kwargs).arguments
        mtype = values.get("mtype") or getattr(values.get("mediainfo"), "type", None)
        return owner if enabled() and not is_music(mtype) else None

    if generator:
        @wraps(original)
        async def scoped(owner, *args, **kwargs):
            # The provider body only runs on anext, so the scope has to be held
            # across each pull rather than around the call that builds it.
            value = scope(owner, args, kwargs)
            events = original(owner, *args, **kwargs)
            try:
                while True:
                    token = SEARCH_SCOPE.set(value)
                    try:
                        item = await anext(events)
                    except StopAsyncIteration:
                        break
                    finally:
                        SEARCH_SCOPE.reset(token)
                    yield item
            finally:
                token = SEARCH_SCOPE.set(value)
                try:
                    await events.aclose()
                finally:
                    SEARCH_SCOPE.reset(token)
        return scoped

    if asynchronous:
        @wraps(original)
        async def scoped(owner, *args, **kwargs):
            token = SEARCH_SCOPE.set(scope(owner, args, kwargs))
            try:
                return await original(owner, *args, **kwargs)
            finally:
                SEARCH_SCOPE.reset(token)
        return scoped

    @wraps(original)
    def scoped(owner, *args, **kwargs):
        token = SEARCH_SCOPE.set(scope(owner, args, kwargs))
        try:
            return original(owner, *args, **kwargs)
        finally:
            SEARCH_SCOPE.reset(token)
    return scoped


def make_resource_pages(original, enabled, max_pages):
    """Raise the page budget for影视搜索 only; subtitles keep the native value."""
    @wraps(original)
    def pages():
        if enabled() and SEARCH_SCOPE.get() is not None:
            return max(1, int(max_pages()))
        return original()
    return pages
