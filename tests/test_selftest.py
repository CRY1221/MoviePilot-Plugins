"""Self-test helper: media type parsing and the shape of a measurement run."""
import enum
import sys
import types as pytypes

from fastsearch import selftest


class _MediaType(str, enum.Enum):
    MOVIE = "电影"
    TV = "电视剧"


def patch_types(monkeypatch):
    module = pytypes.ModuleType("app.schemas.types")
    module.MediaType = _MediaType
    monkeypatch.setitem(sys.modules, "app.schemas.types", module)


def test_resolve_mtype_accepts_labels_enum_and_none(monkeypatch):
    patch_types(monkeypatch)
    assert selftest.resolve_mtype("电影") == _MediaType.MOVIE
    assert selftest.resolve_mtype("movie") == _MediaType.MOVIE
    assert selftest.resolve_mtype("TV") == _MediaType.TV
    assert selftest.resolve_mtype(_MediaType.MOVIE) == _MediaType.MOVIE
    assert selftest.resolve_mtype(None) is None
    assert selftest.resolve_mtype("音乐") is None
