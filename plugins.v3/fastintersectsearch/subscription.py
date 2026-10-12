"""Prefetch current native pages; the native scan remains the only state writer."""
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar, copy_context
from datetime import datetime, timezone
from functools import wraps

SUBSCRIPTION_SCOPE = ContextVar("fastsearch_subscription", default=None)


def make_owned_task(original, enabled):
    @wraps(original)
    def execute(runner, *, subscribe, task_id, lease_token, execution_lease, cancelled):
        scope = None
        if enabled() and runner.task.source == "new":
            scope = (runner.searchchain, task_id, lease_token,
                     execution_lease.subscription_id, execution_lease.owner_token)
        token = SUBSCRIPTION_SCOPE.set(scope)
        try:
            return original(runner, subscribe=subscribe, task_id=task_id, lease_token=lease_token,
                            execution_lease=execution_lease, cancelled=cancelled)
        finally:
            SUBSCRIPTION_SCOPE.reset(token)
    return execute


def matches_scan(scan):
    scope, execution = SUBSCRIPTION_SCOPE.get(), scan.execution
    if scope is None or execution is None:
        return False
    return (scope[0] is scan.owner and scope[1:] == (
        execution.task_id, execution.task_lease,
        execution.lease.subscription_id, execution.lease.owner_token))


def make_advance_sources(original, enabled, workers=8):
    @wraps(original)
    def advance(scan, keys, submit, started):
        if not enabled() or not matches_scan(scan) or scan._stopping():
            return original(scan, keys, submit, started)
        # Ticket recovery may need a different page on this site. Do not spend its
        # native budget speculatively before choose() can recover the candidate.
        recovery_sites = set()
        for candidate_id, candidate in scan.candidates.items():
            context = scan.live.get(candidate_id)
            if context is None or not getattr(context.torrent_info, "enclosure", None):
                recovery_sites.add(scan.queries[candidate["source"]]["site"])
        planned, seen = {}, set()
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        for key in keys:
            site = scan.queries[key]["site"]
            if site in seen or site == "plugin":
                continue
            seen.add(site)
            if site in recovery_sites or (scan.retry.get(key) or "") > now:
                continue
            cursor = scan.collection.sources[key]
            planned[key] = 0 if cursor.head_refresh else cursor.next_page
        if len(planned) < 2:
            return original(scan, keys, submit, started)

        budget = scan._budget()
        fetch, step = scan.sources.fetch, scan.step
        marker = ContextVar("fastsearch_consuming_page", default=None)
        futures = {}
        pool = ThreadPoolExecutor(max_workers=min(workers, len(planned)), thread_name_prefix="fastsearch-page")
        previous_fetch = vars(scan.sources).get("fetch")
        previous_step = vars(scan).get("step")
        had_fetch, had_step = "fetch" in vars(scan.sources), "step" in vars(scan)

        def prefetched(key, page, *, budget=None):
            if (marker.get() == key and planned.get(key) == page and
                    budget is expected_budget and key in futures):
                return futures.pop(key).result()
            return fetch(key, page, budget=budget)

        def consume(key):
            token = marker.set(key)
            try:
                return step(key)
            finally:
                marker.reset(token)

        expected_budget = budget
        try:
            for key, page in planned.items():
                context = copy_context()
                futures[key] = pool.submit(context.run, fetch, key, page, budget=budget)
            scan.sources.fetch, scan.step = prefetched, consume
            return original(scan, keys, submit, started)
        finally:
            if vars(scan.sources).get("fetch") is prefetched:
                if had_fetch:
                    scan.sources.fetch = previous_fetch
                else:
                    del scan.sources.fetch
            if vars(scan).get("step") is consume:
                if had_step:
                    scan.step = previous_step
                else:
                    del scan.step
            # No fetch survives the native task lease. Unused results are not
            # checkpointed, and never cause submit/settle on their own.
            pool.shutdown(wait=True, cancel_futures=True)
    return advance


def make_full_pack_first(original, enabled):
    """Let MoviePilot's own full-pack-first branch run on a subscription's first search.

    MoviePilot already prefers a resource covering every target episode and falls
    back to per-episode downloads when that fails (``policy.py`` 完整包分支),
    but it skips the branch entirely unless the subscription is in per-episode
    best-version mode, because ``build_full_pack_first_no_exists`` returns None
    without ``best_version``. A subscription that has downloaded nothing yet
    wants exactly that behaviour, so ask MoviePilot's own builder for the season
    range and leave everything else — the coverage test, the priority gate, the
    download and its fallback — to MoviePilot.
    """
    from dataclasses import replace

    @wraps(original)
    def build(subscribe, mediakey):
        result = original(subscribe, mediakey)
        if result is not None or not enabled():
            return result
        from app.application.subscription import priority as subscription_priority

        if subscription_priority.is_full_best_version_enabled(subscribe):
            return result
        # 已经下过任何一集就不再介入，完全回到原生行为。
        if subscription_priority.get_downloaded_best_version_episodes(subscribe):
            return result
        return original(replace(subscribe, best_version=1), mediakey)

    return build
