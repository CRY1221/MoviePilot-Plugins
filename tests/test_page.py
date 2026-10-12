"""Detail page keeps the v2 board shape and survives an empty statistics set."""
from fastsearch.page import render


SAMPLE = [{"site": "天空", "requests": 12, "last": 1.5, "mean": 2.2, "wait": 0.4, "errors": 0},
          {"site": "红豆饭", "requests": 9, "last": 15.0, "mean": 12.5, "wait": 1.1, "errors": 2}]


def walk(component):
    yield component
    for child in component.get("content") or []:
        yield from walk(child)


def texts(page):
    return [node.get("text") for node in walk({"component": "root", "content": page}) if node.get("text")]


def test_board_shows_cards_icons_and_badges():
    page = render("已启用", SAMPLE, {"天空": "data:image/png;base64,AAA"}, "同站间隔 3 秒")
    flat = list(walk({"component": "root", "content": page}))
    assert any(node.get("component") == "table" for node in flat)
    assert "站点总数" in texts(page) and "平均响应" in texts(page)
    assert "1 / 2" in texts(page)          # 只有 天空 的平均响应小于 3 秒
    assert sum(1 for node in flat if node.get("component") == "VImg") == 1   # 只有有图标的站点画图
    assert "1.50 s" in texts(page) and "15.00 s" in texts(page)
    assert "同站间隔 3 秒" in texts(page)


def test_empty_statistics_show_hint_instead_of_table():
    page = render("未启用", [], {}, "")
    flat = list(walk({"component": "root", "content": page}))
    assert not any(node.get("component") == "table" for node in flat)
    assert any("暂无站点响应耗时统计" in (node.get("text") or "") for node in flat)
    assert "未启用" in texts(page)
