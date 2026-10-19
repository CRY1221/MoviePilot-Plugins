"""外部站点抓取限速：同一站点两次请求至少间隔 3 秒，不同站点可并行。"""

from __future__ import annotations

import threading
import time

MIN_INTERVAL = 3.0

_guard = threading.Lock()
_locks: dict[str, threading.Lock] = {}
_last_request_at: dict[str, float] = {}


def wait(key: str = "default", min_interval: float = MIN_INTERVAL) -> None:
    """
    等待到允许向指定站点发起下一次请求。

    :param key: 站点标识，例如 javbus / javranking
    :param min_interval: 同一站点两次请求之间的最小间隔秒数
    """
    with _guard:
        lock = _locks.setdefault(key, threading.Lock())
    with lock:
        last = _last_request_at.get(key, 0.0)
        delay = max(0.0, float(min_interval or 0)) - (time.time() - last)
        if delay > 0:
            time.sleep(delay)
        _last_request_at[key] = time.time()
