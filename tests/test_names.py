"""Chinese/English name trimming replaces MoviePilot's full alias expansion."""
from types import SimpleNamespace as NS

from fastsearch.names import make_prepare_params, select_search_names


def media(**kwargs):
    defaults = dict(title="", original_title="", en_title="", names=[], hk_title=None,
                    tw_title=None, sg_title=None, type="电影")
    defaults.update(kwargs)
    return NS(**defaults)


def test_chinese_film_keeps_chinese_and_english_only():
    info = media(title="你好，李焕英", original_title="你好，李焕英", en_title="Hi, Mom",
                 names=["Hi, Mom", "你好，李煥英", "Hello, Li Huanying", "Привет, мам",
                        "안녕, 리환잉", "こんにちは、私のお母さん"])
    keywords = ["你好，李焕英", "Hi, Mom", "你好，李煥英", "Hello, Li Huanying",
                "Привет, мам", "안녕, 리환잉"]
    assert select_search_names(info, keywords) == ["你好，李焕英", "Hi, Mom"]


def test_foreign_film_keeps_original_title_and_chinese_name():
    info = media(title="盗梦空间", original_title="Inception", en_title="Inception",
                 names=["盗梦空间", "潛行凶間", "Inception"], hk_title="潛行凶間")
    keywords = ["盗梦空间", "潛行凶間", "Inception"]
    assert select_search_names(info, keywords) == ["Inception", "盗梦空间"]


def test_missing_english_name_falls_back_to_recognised_spelling():
    info = media(title="流浪地球", original_title="流浪地球", names=["The Wandering Earth"],
                 hk_title=None, tw_title=None, sg_title=None)
    assert select_search_names(info, ["流浪地球", "The Wandering Earth"]) == ["流浪地球", "The Wandering Earth"]


def test_prepare_params_trims_only_for_expanded_keywords():
    def original(mediainfo, keyword=None, no_exists=None, include_candidates=False):
        return None, ["a", "b", "c", "d"]
    info = media(title="甲", original_title="甲", en_title="Jia", names=["Jia", "丙", "丁"])
    assert make_prepare_params(original, lambda: True)(info)[1] == ["甲", "Jia"]
    assert make_prepare_params(original, lambda: True)(info, keyword="explicit")[1] == ["a", "b", "c", "d"]
    assert make_prepare_params(original, lambda: False)(info)[1] == ["a", "b", "c", "d"]
    assert make_prepare_params(original, lambda: True)(media(type="音乐"))[1] == ["a", "b", "c", "d"]
