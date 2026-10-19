"""Detail page for the plugin: the obsidian-glass timing board from v2."""

_BORDER = "border-bottom: 1px solid rgba(255, 255, 255, 0.05);"
_CARD = ("background: linear-gradient(160deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0.015) 100%);"
         "border: 1px solid rgba(255,255,255,0.08); border-radius: 14px; padding: 16px 18px;"
         "box-shadow: 0 8px 24px rgba(0,0,0,0.28);")


def _cost_badge(value):
    if value < 2:
        border, background, color = "rgba(52,211,153,0.35)", "rgba(16,185,129,0.12)", "#34d399"
    elif value < 6:
        border, background, color = "rgba(251,191,36,0.35)", "rgba(245,158,11,0.12)", "#fbbf24"
    else:
        border, background, color = "rgba(248,113,113,0.35)", "rgba(239,68,68,0.12)", "#f87171"
    return {
        "component": "span",
        "props": {"style": (f"display: inline-block; padding: 3px 12px; border-radius: 20px; background: {background};"
                            f"border: 1px solid {border}; color: {color}; font-size: 13px; font-weight: 700;"
                            "font-family: monospace; letter-spacing: 0.5px;")},
        "text": f"{value:.2f} s",
    }


def _stat_card(label, value, unit, gradient, accent):
    return {
        "component": "VCol", "props": {"cols": 12, "md": 4},
        "content": [{
            "component": "div",
            "props": {"style": _CARD},
            "content": [
                {"component": "div", "props": {"style": "color: #94a3b8; font-size: 12px; letter-spacing: 0.6px;"}, "text": label},
                {"component": "div", "props": {"style": (f"font-size: 26px; font-weight: 800; font-family: monospace;"
                                                          f"background: {gradient}; -webkit-background-clip: text;"
                                                          "background-clip: text; color: transparent; margin: 6px 0 2px;")},
                 "text": value},
                {"component": "div", "props": {"style": f"color: {accent}; font-size: 11px; opacity: 0.85;"}, "text": unit},
            ],
        }],
    }


def _site_cell(name, icon):
    avatar = [{"component": "VImg", "props": {"src": icon, "width": 20, "height": 20}}] if icon else [
        {"component": "span", "props": {"style": "font-size: 12px; font-weight: 700; color: #38bdf8;"}, "text": name[:1]}]
    return {
        "component": "td",
        "props": {"style": f"width: 30%; text-align: left; vertical-align: middle; padding: 14px 12px 14px 20px; {_BORDER}"},
        "content": [{
            "component": "div",
            "props": {"style": "display: flex; align-items: center; justify-content: flex-start; gap: 10px;"},
            "content": [
                {"component": "VAvatar",
                 "props": {"size": 30, "rounded": "md",
                           "style": ("background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1);"
                                     "box-shadow: 0 2px 6px rgba(0,0,0,0.3); flex-shrink: 0;")},
                 "content": avatar},
                {"component": "span", "props": {"style": "color: #e2e8f0; font-size: 14px; font-weight: 600;"}, "text": name},
            ],
        }],
    }


def _number_cell(value, color="#34d399", suffix=" s"):
    return {
        "component": "td",
        "props": {"style": f"width: 17%; text-align: center; vertical-align: middle; padding: 14px 8px; {_BORDER}"},
        "content": [{"component": "span",
                     "props": {"style": f"color: {color}; font-weight: 700; font-size: 14px; font-family: monospace;"},
                     "text": f"{value:.2f}{suffix}"}],
    }


def render(status, samples, icon_map, summary):
    """Build the plugin detail page: header, summary cards and per-site board."""
    page = [{"component": "VAlert", "props": {"type": "info", "variant": "tonal", "class": "mb-3"}, "text": status}]
    if summary:
        page.append({"component": "VCardText",
                     "props": {"style": "color: #94a3b8; font-size: 12px;"}, "text": summary})

    total = len(samples)
    fast = [item for item in samples if 0 < item["mean"] < 3]
    average = round(sum(item["mean"] for item in samples) / total, 2) if total else 0.0
    page.append({"component": "VRow", "props": {"class": "mb-2"}, "content": [
        _stat_card("站点总数", str(total), "个有请求样本的源", "linear-gradient(135deg,#38bdf8 0%,#0284c7 100%)", "#38bdf8"),
        _stat_card("秒级响应站点 ( < 3s )", f"{len(fast)} / {total}", "高速流水线",
                   "linear-gradient(135deg,#34d399 0%,#059669 100%)", "#34d399"),
        _stat_card("平均响应", f"{average:.2f} s", "含排队等待之外的真实调用",
                   "linear-gradient(135deg,#a78bfa 0%,#7c3aed 100%)", "#a78bfa"),
    ]})

    if not samples:
        page.append({"component": "div",
                     "props": {"style": ("background: rgba(255,255,255,0.02); border: 1px dashed rgba(255,255,255,0.12);"
                                         "border-radius: 12px; padding: 36px; text-align: center; color: #94a3b8;"
                                         "font-size: 14px;")},
                     "text": "📊 暂无站点响应耗时统计，发起一次搜索后将在此自动生成总览大盘。"})
        return page

    header = [{"component": "th", "props": {"style": f"width: {width}; padding: 12px 8px; text-align: center; color: #94a3b8; font-size: 12px; {_BORDER}"}, "text": title}
              for title, width in [("站点名称", "30%"), ("请求数", "17%"), ("上次响应", "17%"),
                                   ("平均响应", "18%"), ("平均排队", "18%")]]
    rows = []
    for item in samples:
        mean_color = "#34d399" if item["mean"] < 3 else ("#fbbf24" if item["mean"] < 7 else "#f87171")
        rows.append({"component": "tr", "props": {"style": "transition: background 0.2s ease;"}, "content": [
            _site_cell(item["site"], icon_map.get(item["site"])),
            _number_cell(item["requests"], color="#94a3b8", suffix=""),
            {"component": "td",
             "props": {"style": f"width: 17%; text-align: center; vertical-align: middle; padding: 14px 8px; {_BORDER}"},
             "content": [_cost_badge(item["last"])]},
            _number_cell(item["mean"], color=mean_color),
            _number_cell(item["wait"], color="#64748b"),
        ]})
    page.append({
        "component": "div",
        "props": {"style": _CARD + " padding: 0; overflow: hidden;"},
        "content": [{
            "component": "table",
            "props": {"style": "width: 100%; border-collapse: collapse; background: rgba(13,18,32,0.55);"},
            "content": [{"component": "thead", "content": [{"component": "tr", "content": header}]},
                        {"component": "tbody", "content": rows}],
        }],
    })
    return page
