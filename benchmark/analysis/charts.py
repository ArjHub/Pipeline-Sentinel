"""Static SVG charts for the README, from metrics.json / the CSV rows (no plotting deps).

Each chart draws its own light surface so it reads the same on GitHub's light and dark
themes. Colors: categorical slots 1-3 of the validated reference palette (Haiku blue,
Sonnet orange, human aqua) and its blue sequential ramp for the confusion matrices. Every
chart has a legend when it has more than one series, and the numbers are in the CSVs.
"""

from __future__ import annotations

import math
import random
from html import escape
from pathlib import Path

SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
SERIES = {
    "haiku": "#2a78d6",
    "sonnet": "#eb6834",
    "human": "#1baf7a",
    # Phase 5c: v1 Haiku keeps its blue; the post-hoc run takes slot 4 (yellow; below 3:1
    # on the surface, so every bar carries a direct k/n label and the CSV is the table).
    "v1": "#2a78d6",
    "posthoc": "#eda100",
    # Phase 5d: round 1 keeps its yellow (the "posthoc" key above); round 2 takes slot 5
    # (magenta; also below 3:1, same relief: direct k/n labels and rounds.csv).
    "round1": "#eda100",
    "round2": "#e87ba4",
}
NAMES = {
    "haiku": "Haiku 5.5",
    "sonnet": "Sonnet 5.5",
    "human": "Manual (you)",
    "v1": "v1 (Phase 5b)",
    "posthoc": "Post-hoc tool fixes",
    "round1": "Post-hoc round 1",
    "round2": "Post-hoc round 2",
}
RAMP = ["#f4f8fd", "#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
FONT = "font-family='-apple-system,Segoe UI,Helvetica,Arial,sans-serif'"


def short(model: str) -> str:
    return model.split("-")[1]


def _svg(w: int, h: int, body: list[str], title: str) -> str:
    return "\n".join(
        [
            f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}' "
            f"viewBox='0 0 {w} {h}' {FONT} font-size='12' role='img'>",
            f"<title>{escape(title)}</title>",
            f"<rect width='{w}' height='{h}' rx='8' fill='{SURFACE}'/>",
            _text(16, 26, title, color=INK, size=15, weight="600"),
            *body,
            "</svg>",
        ]
    )


def _text(x, y, s, anchor="start", color=INK2, size=12, weight="400") -> str:
    return (
        f"<text x='{x:.1f}' y='{y:.1f}' text-anchor='{anchor}' fill='{color}' "
        f"font-size='{size}' font-weight='{weight}'>{escape(str(s))}</text>"
    )


def _legend(x: float, y: float, keys: list[str]) -> list[str]:
    out = []
    for k in keys:
        out.append(
            f"<rect x='{x:.1f}' y='{y - 9:.1f}' width='10' height='10' rx='2' fill='{SERIES[k]}'/>"
        )
        out.append(_text(x + 15, y, NAMES[k], color=INK))
        x += 26 + 7.2 * len(NAMES[k])
    return out


def _bar(x, y, w, h, color) -> str:
    """A vertical bar with a 4px rounded data end, anchored square to the baseline."""
    if h <= 0:
        return ""
    r = min(4, w / 2, h)
    return (
        f"<path d='M{x:.1f},{y + h:.1f} V{y + r:.1f} Q{x:.1f},{y:.1f} {x + r:.1f},{y:.1f} "
        f"H{x + w - r:.1f} Q{x + w:.1f},{y:.1f} {x + w:.1f},{y + r:.1f} V{y + h:.1f} Z' "
        f"fill='{color}'/>"
    )


def accuracy_by_category(metrics: dict) -> str:
    return category_bars(
        {short(m): mm["per_category"] for m, mm in metrics["models"].items()},
        "Diagnosis accuracy by category",
        "Top-1 category accuracy on detected faults, by true "
        "category; labels are correct/diagnoses (all repeats).",
    )


def before_after(before: dict, after: dict) -> str:
    """Phase 5c: Haiku v1 vs. the post-hoc tool-fix re-run, per true category."""
    return category_bars(
        {"v1": before, "posthoc": after},
        "Haiku 5.5: v1 vs. post-hoc tool fixes (not held-out)",
        "Top-1 accuracy by true category, 3 repeats each; labels are correct/diagnoses. "
        "Same 25 faults, fixes chosen after seeing v1.",
        top=80,
    )


def rounds(per_cat: dict[str, dict]) -> str:
    """Phase 5d: Haiku v1 vs. post-hoc rounds 1 and 2, per true category."""
    return category_bars(
        per_cat,
        "Haiku 5.5: v1 vs. post-hoc rounds 1 and 2 (not held-out)",
        "Top-1 accuracy by true category, 3 repeats each; labels are correct/diagnoses. "
        "Same 25 faults; both rounds tuned after seeing them.",
        top=80,
    )


def category_bars(per_cat: dict[str, dict], title: str, caption: str, top: int = 64) -> str:
    keys = list(per_cat)
    cats = sorted(next(iter(per_cat.values())))
    W, H, L, T, B = 760, 360 + top - 64, 48, top, 70
    pw, ph = W - L - 20, H - T - B
    band = pw / len(cats)
    bw = min(26, (band - 18) / len(keys))
    body = _legend(L, 50, keys)
    for p in (0, 25, 50, 75, 100):
        y = T + ph * (1 - p / 100)
        body.append(f"<line x1='{L}' x2='{W - 20}' y1='{y:.1f}' y2='{y:.1f}' stroke='{GRID}'/>")
        body.append(_text(L - 6, y + 4, f"{p}%", "end"))
    for ci, c in enumerate(cats):
        x0 = L + ci * band + (band - bw * len(keys) - 2 * (len(keys) - 1)) / 2
        for ki, k in enumerate(keys):
            a = per_cat[k][c]["top1"]
            v = a["rate"] or 0
            x = x0 + ki * (bw + 2)
            h = ph * v
            body.append(_bar(x, T + ph - h, bw, h, SERIES[k]))
            body.append(_text(x + bw / 2, T + ph - h - 4, f"{a['k']}/{a['n']}", "middle", size=10))
        body.append(
            _text(L + ci * band + band / 2, T + ph + 18, c.replace("_", " "), "middle", INK)
        )
        n = per_cat[keys[0]][c]["faults"]
        body.append(
            _text(L + ci * band + band / 2, T + ph + 33, f"{n} fault(s)", "middle", size=10)
        )
    body.append(_text(L, H - 12, caption, size=11))
    return _svg(W, H, body, title)


def confusion(model: str, conf: dict[str, dict[str, int]]) -> str:
    truths = list(conf)
    preds = [p for p in next(iter(conf.values())) if any(conf[t][p] for t in truths)]
    cell, L, T = 62, 128, 120
    W, H = L + cell * len(preds) + 24, T + cell * len(truths) + 40
    body = []
    for pi, p in enumerate(preds):
        x = L + pi * cell + cell / 2
        body.append(
            f"<text transform='translate({x:.1f},{T - 8}) rotate(-35)' fill='{INK}' "
            f"font-size='11'>{escape(p.replace('_', ' '))}</text>"
        )
    body.append(_text(L, 50, "predicted (top-1) →", color=INK2, size=11))
    for ti, t in enumerate(truths):
        y = T + ti * cell
        body.append(_text(L - 8, y + cell / 2 + 4, t.replace("_", " "), "end", INK))
        total = sum(conf[t].values()) or 1
        for pi, p in enumerate(preds):
            v = conf[t][p]
            share = v / total
            step = 0 if v == 0 else min(len(RAMP) - 1, 1 + int(share * (len(RAMP) - 1)))
            fill = RAMP[step]
            ink = "#ffffff" if step >= 5 else INK
            x = L + pi * cell
            ring = (
                f" stroke='{INK}' stroke-width='1.5'"
                if p == t
                else f" stroke='{SURFACE}' stroke-width='2'"
            )
            body.append(
                f"<rect x='{x}' y='{y}' width='{cell}' height='{cell}' rx='4' fill='{fill}'{ring}/>"
            )
            if v:
                body.append(_text(x + cell / 2, y + cell / 2 + 4, v, "middle", ink, 13, "600"))
    body.append(
        _text(
            16,
            H - 12,
            "Rows: true category. Shade = share of the row. Outlined: the diagonal (exact label).",
            size=11,
        )
    )
    return _svg(W, H, body, f"Confusion matrix, {NAMES[short(model)]} (all repeats)")


def strip(rows_by_model: dict[str, list[float]], title: str, unit: str, fmt) -> str:
    models = list(rows_by_model)
    W, L, T, lane = 760, 110, 70, 64
    H = T + lane * len(models) + 54
    xs = [v for vs in rows_by_model.values() for v in vs]
    hi = max(xs) * 1.08 if xs else 1
    step = 10 ** math.floor(math.log10(hi / 4)) if hi > 0 else 1
    step *= 5 if hi / step > 20 else 2 if hi / step > 8 else 1
    pw = W - L - 30
    sx = lambda v: L + pw * v / hi  # noqa: E731
    body = _legend(L, 50, [short(m) for m in models])
    t = 0.0
    while t <= hi:
        body.append(
            f"<line x1='{sx(t):.1f}' x2='{sx(t):.1f}' y1='{T}' "
            f"y2='{T + lane * len(models)}' stroke='{GRID}'/>"
        )
        body.append(_text(sx(t), T + lane * len(models) + 16, fmt(t), "middle"))
        t += step
    rng = random.Random(7)
    for mi, m in enumerate(models):
        yc = T + mi * lane + lane / 2
        vs = sorted(rows_by_model[m])
        body.append(_text(L - 10, yc + 4, NAMES[short(m)], "end", INK))
        for v in vs:
            body.append(
                f"<circle cx='{sx(v):.1f}' cy='{yc + rng.uniform(-14, 14):.1f}' r='4' "
                f"fill='{SERIES[short(m)]}' fill-opacity='0.75' stroke='{SURFACE}' "
                f"stroke-width='1'/>"
            )
        if vs:
            med = vs[len(vs) // 2] if len(vs) % 2 else (vs[len(vs) // 2 - 1] + vs[len(vs) // 2]) / 2
            body.append(
                f"<line x1='{sx(med):.1f}' x2='{sx(med):.1f}' y1='{yc - 22}' "
                f"y2='{yc + 22}' stroke='{INK}' stroke-width='2'/>"
            )
            body.append(_text(sx(med) + 5, yc - 16, f"median {fmt(med)}", color=INK, size=11))
    body.append(_text(L, H - 12, f"One dot per diagnosis ({unit}). Black tick: median.", size=11))
    return _svg(W, H, body, title)


def manual_vs_agent(rows: list[dict], models: list[str]) -> str:
    keys = ["human", *[short(m) for m in models]]
    field = {
        "human": "human_seconds",
        **{short(m): f"{short(m)}_detect_plus_diagnose_s" for m in models},
    }
    W, L, T, row = 780, 190, 70, 30
    H = T + row * len(rows) + 60
    vals = [r[field[k]] for r in rows for k in keys if r.get(field[k]) is not None]
    hi = max(vals) * 1.05
    pw = W - L - 30
    sx = lambda v: L + pw * v / hi  # noqa: E731
    body = _legend(L, 50, keys)
    step = 60 if hi < 900 else 120
    t = 0
    while t <= hi:
        body.append(
            f"<line x1='{sx(t):.1f}' x2='{sx(t):.1f}' y1='{T - 6}' "
            f"y2='{T + row * len(rows)}' stroke='{GRID}'/>"
        )
        body.append(_text(sx(t), T + row * len(rows) + 16, f"{t // 60}m", "middle"))
        t += step
    for i, r in enumerate(rows):
        y = T + i * row + row / 2
        mark = "✓" if r["human_correct"] else "✗"
        body.append(
            _text(
                L - 10,
                y + 4,
                f"trial {r['trial']} {r['truth'].replace('_', ' ')} {mark}",
                "end",
                INK,
                11,
            )
        )
        pts = [(k, r.get(field[k])) for k in keys if r.get(field[k]) is not None]
        if len(pts) > 1:
            xs = [sx(v) for _, v in pts]
            body.append(
                f"<line x1='{min(xs):.1f}' x2='{max(xs):.1f}' y1='{y:.1f}' "
                f"y2='{y:.1f}' stroke='{GRID}' stroke-width='2'/>"
            )
        for k, v in pts:
            body.append(
                f"<circle cx='{sx(v):.1f}' cy='{y:.1f}' r='5' fill='{SERIES[k]}' "
                f"stroke='{SURFACE}' stroke-width='2'/>"
            )
    body.append(
        _text(
            L,
            H - 26,
            "Manual: alert shown -> diagnosis typed (✓/✗ = your category "
            "right/wrong). Agent: first evidence event ->",
            size=11,
        )
    )
    body.append(
        _text(
            L,
            H - 11,
            "incident (60 s detector poll, replayed) + diagnosis latency (median of repeats).",
            size=11,
        )
    )
    return _svg(W, H, body, "Time to a diagnosis: manual vs. PipelineSentinel")


def write_all(metrics: dict, items: list[dict], manual: list[dict], out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    models = list(metrics["models"])
    files = {"accuracy_by_category.svg": accuracy_by_category(metrics)}
    for m in models:
        files[f"confusion_{short(m)}.svg"] = confusion(m, metrics["models"][m]["confusion"])
    files["latency.svg"] = strip(
        {m: [i["latency_s"] for i in items if i["model"] == m] for m in models},
        "Diagnosis latency per diagnosis",
        "seconds",
        lambda v: f"{v:.0f}s",
    )
    files["cost.svg"] = strip(
        {m: [i["cost_usd"] for i in items if i["model"] == m] for m in models},
        "Cost per diagnosis",
        "USD",
        lambda v: f"${v:.3f}",
    )
    files["manual_vs_agent.svg"] = manual_vs_agent(manual, models)
    paths = []
    for name, svg in files.items():
        p = out_dir / name
        p.write_text(svg)
        paths.append(p)
    return paths
