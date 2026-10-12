import asyncio
from contextlib import aclosing
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace as NS
import threading
import time

import pytest

from conftest import native_nodes
from fastsearch.hooks import Hooks
from fastsearch.scheduling import (SiteRequestGate, make_site_sync, make_site_async,
    make_read_pages, make_execution_run, make_execution_events, make_keyword_delay, SEARCH_SCOPE)


def test_binding_inheritance_and_third_party_ownership():
    class Base:
        def run(self, submit):
            return self, submit
    class Child(Base):
        pass
    original = Child.run
    hooks = Hooks()
    hooks.install(Child, "run", lambda self, submit: original(self, submit))
    child = Child()
    assert child.run(3) == (child, 3)
    hooks.close()
    assert "run" not in vars(Child)
    hooks.install(Child, "run", lambda self, submit: 1)
    third = lambda self, submit: 2
    Child.run = third
    hooks.close()
    assert Child.run is third


def test_static_descriptor_restored():
    class Owner:
        @staticmethod
        def run(owner, plan):
            return owner, plan
    original = vars(Owner)["run"]
    hooks = Hooks()
    hooks.install(Owner, "run", staticmethod(lambda owner, plan: (owner, plan)))
    assert Owner().run(1, 2) == (1, 2)
    hooks.close()
    assert vars(Owner)["run"] is original


def test_same_site_sync_async_spacing_and_other_site_independence():
    async def scenario():
        gate = SiteRequestGate(interval=.04, poll_interval=.001)
        starts, active, lock = [], set(), threading.Lock()
        def transport(owner, site, keyword, mtype=None, page=0):
            with lock:
                assert site["id"] not in active
                active.add(site["id"])
                starts.append((site["id"], time.monotonic(), keyword, page))
            time.sleep(.012)
            with lock:
                active.remove(site["id"])
            return [keyword, page]
        async def async_transport(*args, **kwargs):
            return await asyncio.to_thread(transport, *args, **kwargs)
        sync = make_site_sync(transport, gate, lambda: True)
        asynchronous = make_site_async(async_transport, gate, lambda: True)
        results = await asyncio.gather(
            asyncio.to_thread(sync, None, {"id": 1}, "a", page=2),
            asynchronous(None, {"id": 1}, "b", page=3),
            asynchronous(None, {"id": 2}, "c", page=4))
        assert results == [["a", 2], ["b", 3], ["c", 4]]
        first_site = sorted(t for site, t, _, _ in starts if site == 1)
        other = next(t for site, t, _, _ in starts if site == 2)
        assert first_site[1] - first_site[0] >= .039
        assert other < first_site[1]
        assert not active
    asyncio.run(scenario())


def test_cancelled_transport_keeps_slot_until_worker_finishes():
    async def scenario():
        gate = SiteRequestGate(interval=0, poll_interval=.001)
        entered, release, calls = asyncio.Event(), asyncio.Event(), []
        async def transport(owner, site, keyword, mtype=None, page=0):
            calls.append(keyword)
            if keyword == "first":
                entered.set()
                await release.wait()
            return keyword
        call = make_site_async(transport, gate, lambda: True)
        first = asyncio.create_task(call(None, {"id": 1}, "first"))
        await entered.wait()
        first.cancel()
        second = asyncio.create_task(call(None, {"id": 1}, "second"))
        await asyncio.sleep(.01)
        assert calls == ["first"]
        assert not first.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert await second == "second"
        assert calls == ["first", "second"]
    asyncio.run(scenario())


def test_waiter_cancellation_does_not_block_fifo():
    async def scenario():
        gate = SiteRequestGate(interval=0, poll_interval=.001)
        state, ticket = gate.enqueue({"id": 1})
        assert gate.claim(state, ticket, 0)
        async def transport(*args, **kwargs):
            return 7
        call = make_site_async(transport, gate, lambda: True)
        waiting = asyncio.create_task(call(None, {"id": 1}, "a"))
        await asyncio.sleep(.005)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        gate.release(state, ticket)
        assert await call(None, {"id": 1}, "b") == 7
    asyncio.run(scenario())


def test_manual_native_scheduler_fast_site_advances_without_round_barrier():
    async def scenario():
        registry = NS(create=lambda coroutine, **kwargs: asyncio.create_task(coroutine))
        scope = native_nodes("app/chain/search/pagination.py", {
            "_run_site_page", "_submit_site_page", "_cancel_pending_pages", "SearchPaginationOwner"},
            {"asyncio": asyncio, "time": time, "_SearchOwnerBase": object,
             "get_task_registry": lambda: registry, "runtime_stop_state": NS(is_system_stopped=False)})
        owner = scope["SearchPaginationOwner"]()
        owner.runtime_config = NS(search_threadpool_size=2)
        reads, slow_release = [], asyncio.Event()
        async def offload(function, key, page):
            if key == "slow1":
                await slow_release.wait()
            reads.append((key, page))
            return {"page": page, "can_continue": True, "error": None}, [key]
        async def original(*args):
            raise AssertionError("enabled movie must use adapted scheduler")
            yield
        manual = NS(plan=NS(mediainfo=NS(type="电影")), owner=owner, read=None,
                    sources=NS(queries={key: {"site": site} for key, site in
                                       [("fast1", "1"), ("slow1", "2"), ("fast2", "1")]}))
        call = make_read_pages(original, offload, lambda: True)
        events = call(manual, list(manual.sources.queries), 3)
        async with aclosing(events):
            assert (await anext(events))[0] == "fast1"
            assert (await asyncio.wait_for(anext(events), .5))[0] == "fast2"
            slow_release.set()
            result = await anext(events)
            assert result == ("slow1", {"page": 3, "can_continue": True, "error": None}, ["slow1"])
        assert reads == [("fast1", 3), ("fast2", 3), ("slow1", 3)]
    asyncio.run(scenario())


@pytest.mark.parametrize("music,plugins,expected", [(False, False, 0), (False, True, 9), (True, False, 9)])
def test_execution_plan_identity_rules_and_wait_scope(music, plugins, expected):
    owner = object()
    plan = NS(mediainfo=NS(type="音乐" if music else "电影"), rule_groups=[],
              no_exists=object(), filter_params=object(), candidate_filter=object())
    delay = make_keyword_delay(lambda *_: 9, lambda: True)
    def original(actual_owner, actual_plan, media_chain):
        assert actual_owner is owner and actual_plan is plan
        assert delay(owner, 2) == expected
        assert delay(object(), 2) == 9
        return actual_plan
    assert make_execution_run(original, lambda: True, lambda: not plugins)(owner, plan, None) is plan
    assert SEARCH_SCOPE.get() is None


def test_event_scope_reset_before_yield_across_tasks():
    async def scenario():
        owner = object()
        async def native(owner, plan, media_chain, streaming=True):
            for i in range(2):
                assert SEARCH_SCOPE.get() is owner
                yield i
        generator = make_execution_events(native, lambda: True, lambda: True)(owner, NS(mediainfo=NS(type="电影")), None)
        assert await asyncio.create_task(anext(generator)) == 0
        assert SEARCH_SCOPE.get() is None
        assert await asyncio.create_task(anext(generator)) == 1
        await asyncio.create_task(generator.aclose())
        assert SEARCH_SCOPE.get() is None
    asyncio.run(scenario())


def test_native_minimum_survives_budget_reservation_delay_and_next_manual_call():
    now = [0.0]
    gate = SiteRequestGate(clock=lambda: now[0])
    state, first = gate.enqueue({"id": 1})
    now[0] = 8  # The native subscription budget was reserved earlier at t=0.
    assert gate.claim(state, first, 10)
    gate.release(state, first)
    _, second = gate.enqueue({"id": 1})
    now[0] = 13
    assert not gate.claim(state, second, 5)
    now[0] = 18
    assert gate.claim(state, second, 5)
    gate.release(state, second)


def test_music_bypasses_gate_and_errors_release_slot():
    gate = SiteRequestGate(interval=0)
    def transport(owner, site, keyword, mtype=None, page=0):
        if keyword == "broken":
            raise ValueError("network failure")
        return keyword
    call = make_site_sync(transport, gate, lambda: True)
    assert call(None, {"id": 1}, "song", mtype="音乐") == "song"
    assert not gate.states
    with pytest.raises(ValueError):
        call(None, {"id": 1}, "broken")
    assert call(None, {"id": 1}, "concert", mtype="电影") == "concert"
    stats = gate.snapshot()
    assert stats[0]["requests"] == 2 and stats[0]["errors"] == 1
