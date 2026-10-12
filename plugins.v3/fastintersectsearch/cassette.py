"""Fixed-input capture and replay so performance work never re-hits the sites.

A recording stores exactly what one real site page returned. Replay feeds those
pages back through the untouched native pipeline, so the resulting numbers
describe MoviePilot's own parsing, filtering, matching, sorting and dedup work
with zero site time. Nothing here changes a business result: the cassette is
only active while an operator explicitly asks for a record or a replay run.

The intercept sits on ``SearchSources.fetch`` because that is the exact site
boundary: everything above it (page cache, page signature, ``can_continue``) is
native logic that replay must still execute, and everything below it is
network.
"""
import json
import threading
import time
from functools import wraps
from pathlib import Path

CORPUS_VERSION = 1

# Site credentials must never reach the corpus file on disk.
CREDENTIAL_FIELDS = frozenset({"site_cookie", "site_ua"})


def mask_torrent(data):
    """Drop credential fields from one serialized torrent."""
    return {key: value for key, value in data.items() if key not in CREDENTIAL_FIELDS}


def mask_context(item):
    """Drop credential fields from one serialized result context."""
    torrent = item.get("torrent_info") if isinstance(item, dict) else None
    if isinstance(torrent, dict):
        return {**item, "torrent_info": mask_torrent(torrent)}
    return item


def mask_contexts(items):
    return [mask_context(item) for item in items or []]


def freeze_page(page):
    """Serialize one native ScanPage for the corpus file."""
    observation = page.observation
    return {
        "torrents": [mask_torrent(torrent.to_dict()) for torrent in page.torrents],
        "observation": {"attempted": observation.attempted, "outcome": observation.outcome,
                        "error": observation.error, "raw_count": observation.raw_count,
                        "has_more": observation.has_more},
        "retry_at": page.retry_at,
        "wait_reason": page.wait_reason,
    }


def build_page(record):
    """Rebuild the native ScanPage a recorded site page produced."""
    from app.application.site.observation import SiteSearchObservation
    from app.chain.search.scan import ScanPage
    from app.domain.context import TorrentInfo

    torrents = []
    for item in record["torrents"]:
        torrent = TorrentInfo()
        torrent.from_dict(item)
        torrents.append(torrent)
    return ScanPage(torrents=torrents, observation=SiteSearchObservation(**record["observation"]),
                    retry_at=record.get("retry_at"), wait_reason=record.get("wait_reason"))


class Cassette:
    """Process-wide record/replay state; only one run is armed at a time."""

    def __init__(self):
        self.lock = threading.Lock()
        self.mode = None
        self.media_id = None
        self.records = {}
        self.order = []
        self.duplicates = 0
        self.replayed = 0
        self.missing = []
        self.started = None

    def armed(self, sources):
        """True when this SearchSources belongs to the armed run."""
        return self.mode is not None and str(getattr(sources.media, "media_id", None)) == str(self.media_id)

    def start(self, mode, media_id):
        with self.lock:
            self.mode, self.media_id = mode, str(media_id)
            self.records, self.order = {}, []
            self.duplicates = self.replayed = 0
            self.missing = []
            self.started = time.monotonic()

    def stop(self):
        with self.lock:
            mode, elapsed = self.mode, None if self.started is None else round(time.monotonic() - self.started, 3)
            self.mode = None
            return {"mode": mode, "elapsed": elapsed, "records": len(self.records),
                    "pages": len(self.order), "duplicates": self.duplicates,
                    "replayed": self.replayed, "missing": list(self.missing)}

    def load(self, records):
        """Install recorded pages for a replay run."""
        with self.lock:
            self.records = {(item["key"], int(item["page"])): item for item in records}
            self.order = [(item["key"], int(item["page"])) for item in records]

    def record(self, sources, key, page, page_result):
        with self.lock:
            identity = (key, int(page))
            if identity in self.records:
                self.duplicates += 1
            else:
                self.order.append(identity)
            self.records[identity] = {"key": key, "page": int(page), **freeze_page(page_result)}

    def lookup(self, key, page):
        with self.lock:
            record = self.records.get((key, int(page)))
            if record is None:
                if len(self.missing) < 20:
                    self.missing.append({"key": key, "page": int(page)})
                return None
            self.replayed += 1
            return record

    def dump(self):
        with self.lock:
            return [self.records[identity] for identity in self.order]


CASSETTE = Cassette()


def make_cassette(original, enabled):
    """Intercept the site boundary; recording and replay never change results."""
    @wraps(original)
    def fetch(self, key, page, *, budget=None):
        if not enabled() or not CASSETTE.armed(self):
            return original(self, key, page, budget=budget)
        if CASSETTE.mode == "record":
            page_result = original(self, key, page, budget=budget)
            CASSETTE.record(self, key, page, page_result)
            return page_result
        record = CASSETTE.lookup(key, page)
        if record is None:
            # Never fall back to the network: a silent live request would make
            # the replay numbers meaningless.
            raise RuntimeError(f"回放缺少站点记录：{key} 第 {page} 页")
        return build_page(record)
    return fetch


def corpus_path(directory, name):
    return Path(directory) / "corpus" / f"{name}.json"


def save_corpus(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, default=str)
    return path


def load_corpus(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)
