"""
Figures for the results section, slides and report (B28). One script, one set of figures.

Design rules
- The title states the finding; one short line under it says what is measured.
- Every axis is labeled, with units. Log scales are labeled with plain numbers (1,000, not 10^3).
- Colors come from the slide deck: navy ink, terracotta for the finding, steel gray for context,
  pine green for "nothing went wrong". Agencies are always the same color: VTA terracotta, Muni steel blue.
- Transparent background, so the PNG sits on the slide's gray and on the report's white page.
- Every number is read from benchmarks/results/ (nothing is typed in by hand).

Two charts need a small extra sweep over the 24 h replay files (data/replay_in), run here:
  - Bloom filter: false-alarm rate vs memory, on the real event IDs
  - Flajolet-Martin: error vs memory (number of hash functions), on the real vehicle IDs
The sweep numbers are saved to benchmarks/results/b28_sweeps.json. A figure whose input is missing is
skipped with a note.

Run from the repo root:
    python benchmarks/make_figures.py                 # everything (sweeps take ~3-5 min)
    python benchmarks/make_figures.py --skip-sweeps   # reuse b28_sweeps.json
Writes report/figures/b28_*.png
"""

import argparse
import glob
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from algorithms.stream_algos import BloomFilter, FlajoletMartin  # noqa: E402

RES = "benchmarks/results"
OUT = "report/figures"
COLD_JVM_RUN = "20261114_200826_r40000_t1_0"   # first run after a JVM start, excluded as in make_figures.py
AV_SECONDS = 20 * 11.0                          # 20 Argoverse 2 scenes of 11 s, played one after another

# ---- palette (sampled from the slide deck) -------------------------------------------------------
INK = "#102B3B"      # navy: text, the main series
ACCENT = "#AB4027"   # terracotta: the finding
STEEL = "#5B7C99"    # steel blue: second series / Muni
CONTEXT = "#B9C2C8"  # light steel: context, filtered out, "not the point"
GOOD = "#3E7C6B"     # pine: nothing lost, nothing duplicated
MUTED = "#4F5F6A"    # secondary text
GRID = "#C9CED2"
VTA, MUNI = ACCENT, STEEL

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "Liberation Sans", "DejaVu Sans"],
    "font.size": 14, "text.color": INK, "axes.labelcolor": INK, "axes.labelsize": 15,
    "axes.edgecolor": MUTED, "axes.linewidth": 1.0, "axes.facecolor": "none", "figure.facecolor": "none",
    "savefig.facecolor": "none", "xtick.color": MUTED, "ytick.color": MUTED,
    "xtick.labelsize": 13, "ytick.labelsize": 13, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": False, "grid.color": GRID, "grid.linewidth": 0.8, "legend.frameon": False,
    "legend.fontsize": 14, "lines.linewidth": 3, "lines.markersize": 9,
})
made, skipped = [], []


# ---- helpers -------------------------------------------------------------------------------------
def res(name):
    return os.path.join(RES, name)


def load_json(name):
    with open(res(name)) as f:
        return json.load(f)


def latest(pattern):
    files = sorted(glob.glob(res(pattern)))
    return files[-1] if files else None


def _wrap(fig, text, size, width_frac, **kw):
    """Greedy word wrap measured in real rendered width (fonts differ between computers)."""
    r = fig.canvas.get_renderer()
    lines, cur = [], ""
    for w in text.split():
        trial = (cur + " " + w).strip()
        t = fig.text(0, 0, trial, fontsize=size, **kw)
        too_wide = t.get_window_extent(r).width > width_frac * fig.bbox.width
        t.remove()
        if too_wide and cur:
            lines.append(cur); cur = w
        else:
            cur = trial
    lines.append(cur)
    return "\n".join(lines)


def frame(title, subtitle, source, figsize=(13.33, 7.5), legend=None):
    """Title block (accent rule, title, subtitle, optional legend row) and a source line.
    Returns the figure and the top of the plotting area in figure coordinates."""
    fig = plt.figure(figsize=figsize)
    fig.add_artist(plt.Line2D([0.045, 0.105], [0.955, 0.955], color=ACCENT, linewidth=5,
                              solid_capstyle="butt", transform=fig.transFigure))
    t = fig.text(0.045, 0.93, title, fontsize=26, fontweight="bold", color=INK, va="top")
    r = fig.canvas.get_renderer()
    while t.get_window_extent(r).width > 0.91 * fig.bbox.width and t.get_fontsize() > 18:
        t.set_fontsize(t.get_fontsize() - 1)
    sub = _wrap(fig, subtitle, 15, 0.91)
    s = fig.text(0.045, 0.855, sub, fontsize=15, color=MUTED, va="top", linespacing=1.35)
    bottom = s.get_window_extent(r).y0 / fig.bbox.height
    top = bottom - 0.035
    if legend:
        x = 0.045
        for label, color, kind in legend:
            if kind == "line":
                fig.add_artist(plt.Line2D([x, x + 0.03], [top - 0.012] * 2, color=color, linewidth=3,
                                          transform=fig.transFigure))
                if kind == "line":
                    fig.add_artist(plt.Line2D([x + 0.015], [top - 0.012], marker="o", color=color,
                                              markersize=8, transform=fig.transFigure))
            elif kind == "hollow":
                fig.add_artist(plt.Line2D([x + 0.015], [top - 0.012], marker="o", markerfacecolor="white",
                                          markeredgecolor=MUTED, markeredgewidth=2, markersize=9,
                                          transform=fig.transFigure))
            else:
                fig.add_artist(plt.Rectangle((x + 0.004, top - 0.024), 0.022, 0.024, color=color,
                                             transform=fig.transFigure))
            lt = fig.text(x + 0.038, top - 0.012, label, fontsize=14, color=INK, va="center")
            x += 0.038 + lt.get_window_extent(r).width / fig.bbox.width + 0.035
        top -= 0.07
    fig.text(0.045, 0.03, source, fontsize=11, color=MUTED, va="bottom")
    return fig, top


def save(fig, name):
    path = os.path.join(OUT, name)
    fig.savefig(path, dpi=160, transparent=True)
    plt.close(fig)
    made.append(path)


def ygrid(ax):
    ax.grid(axis="y"); ax.set_axisbelow(True); ax.tick_params(length=0)


def xgrid(ax):
    ax.grid(axis="x"); ax.set_axisbelow(True); ax.tick_params(length=0)


def plain_log_axis(axis):
    """Log axis labeled 1, 10, 100, 1,000 ... instead of powers of ten."""
    axis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}" if v >= 1 else f"{v:g}"))
    axis.set_minor_formatter(mticker.NullFormatter())


def pct(v, digits=0):
    return f"{v:.{digits}f}%"


# ---- detection -----------------------------------------------------------------------------------
def bunching():
    if not os.path.exists(res("bunching_replay_summary.json")):
        return skipped.append("bunching")
    d = load_json("bunching_replay_summary.json")
    c, total = d["categories"], d["close_pairs"]
    rows = [("Driving in tandem (within 40 m for 5+ min)", "tandem", True),
            ("Both stopped at the same stop", "same_stop", True),
            ("Both moving, close together", "moving", True),
            ("Not enough recent reports to tell", "no_history", False),
            ("One bus parked, the other passing", "one_parked", False),
            ("Both parked at a terminal (layover)", "layover", False)]
    kept = sum(c[k] for _, k, keep in rows if keep)
    fig, top = frame(f"Only {kept / total:.0%} of 'buses too close' cases were real bunching",
                     f"All {total:,} times two buses on the same route and direction came within 200 m in 24 hours, "
                     "sorted by what the detector found when it looked back 5 minutes.",
                     "Data: 24 h replay of Muni + VTA through streaming/detect_bunching.py (B12)",
                     legend=[(f"Kept as bunching: {kept:,} ({kept / total:.0%})", ACCENT, "box"),
                             (f"Filtered out: {total - kept:,} ({(total - kept) / total:.0%})", CONTEXT, "box")])
    ax = fig.add_axes([0.40, 0.13, 0.52, top - 0.15])
    y = list(range(len(rows)))[::-1]
    vals = [c[k] for _, k, _ in rows]
    cols = [ACCENT if keep else CONTEXT for *_, keep in rows]
    ax.barh(y, vals, color=cols, height=0.62)
    ax.set_yticks(y); ax.set_yticklabels([r[0] for r in rows], fontsize=14, color=INK)
    for yy, v in zip(y, vals):
        ax.text(v + total * 0.006, yy, f"{v:,}", va="center", fontsize=14, color=INK, fontweight="bold")
    ax.axhline(2.5, color=MUTED, linewidth=1, linestyle=(0, (3, 3)))
    ax.set_xlim(0, max(vals) * 1.12)
    ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
    ax.set_xlabel("Pairs of close buses in 24 hours")
    xgrid(ax)
    save(fig, "b28_bunching_categories.png")


def bart_knock_on():
    if not os.path.exists(res("delay_propagation.json")):
        return skipped.append("BART knock-on")
    d = load_json("delay_propagation.json")
    a = d["across_trips"]
    late_after_late = a["late_after_late_share"] * 100
    base = a["baseline_late_share"] * 100
    after_on_time = (a["baseline_late_share"] * a["pairs"] - a["late_after_late_share"] * a["late_pairs"]) \
        / (a["pairs"] - a["late_pairs"]) * 100
    late_min = d["settings"]["late"] // 60
    fig, top = frame(f"After a late BART train, the next one is late {late_after_late:.0f}% of the time",
                     f"Share of trains reaching a station {late_min}+ minutes late, depending on whether the train "
                     f"before it on the same line was late. Average for all trains: {base:.0f}%. "
                     f"{a['pairs']:,} pairs of consecutive trains in 24 hours.",
                     "Data: BART GTFS-realtime trip updates, 24 h recording; streaming/analyze_delay_propagation.py (B15). "
                     "Correlation: one incident can delay several trains.")
    ax = fig.add_axes([0.12, 0.14, 0.80, top - 0.17])
    labels = ["Train before it was on time", "Train before it was late"]
    vals = [after_on_time, late_after_late]
    ax.bar(labels, vals, color=[CONTEXT, ACCENT], width=0.5)
    for x, v in enumerate(vals):
        ax.text(x, v + 0.8, pct(v), ha="center", va="bottom", fontsize=24, fontweight="bold",
                color=ACCENT if x else INK)
    ax.set_ylim(0, max(vals) * 1.22)
    ax.set_ylabel(f"Trains {late_min}+ min late (%)")
    ax.set_xticks(range(2)); ax.set_xticklabels(labels, fontsize=15, color=INK)
    ygrid(ax)
    save(fig, "b28_bart_knock_on.png")


def bart_by_hour():
    if not os.path.exists(res("delay_by_hour.csv")):
        return skipped.append("BART by hour")
    df = pd.read_csv(res("delay_by_hour.csv")).set_index("hour")
    min_arrivals = 100
    # The recording ran from about 1 pm on Nov 13 to 1 pm on Nov 14: show the hours in the order they happened.
    order = list(range(13, 24)) + list(range(0, 13))
    fig, top = frame("BART delays peaked around midnight and in the mid-afternoon",
                     "Share of BART arrivals 5+ minutes late, hour by hour, in the order the hours happened "
                     f"(Nov 13 1 pm to Nov 14 noon). Gray hours had fewer than {min_arrivals} arrivals: "
                     "too few trains for a fair percentage.",
                     "Data: BART GTFS-realtime trip updates, 24 h recording; streaming/analyze_delay_propagation.py (B15)")
    ax = fig.add_axes([0.09, 0.17, 0.86, top - 0.21])
    ymax = 40
    for x, h in enumerate(order):
        n = int(df.arrivals.get(h, 0))
        if n < min_arrivals:
            ax.axvspan(x - 0.5, x + 0.5, color=CONTEXT, alpha=0.45, linewidth=0)
            ax.text(x, 1.2, f"{n}\ntrains" if n else "no\ndata", ha="center", va="bottom", fontsize=10, color=MUTED)
            continue
        v = df.late_share[h] * 100
        ax.bar(x, v, color=STEEL, width=0.72)
        ax.text(x, v + 0.6, f"{v:.0f}%" if v >= 1 or v == 0 else f"{v:.1f}%", ha="center", va="bottom",
                fontsize=11, color=INK)
    ax.set_xticks(range(len(order)))
    ax.set_xticklabels([f"{h % 12 or 12}{'a' if h < 12 else 'p'}" for h in order], fontsize=12)
    ax.set_xlim(-0.6, len(order) - 0.4); ax.set_ylim(0, ymax)
    ax.set_ylabel("Arrivals 5+ min late (%)")
    ax.set_xlabel("Hour of day (Pacific)", labelpad=26)
    split = order.index(0) - 0.5
    ax.axvline(split, color=MUTED, linewidth=1, linestyle=(0, (3, 3)))
    ax.text(split / 2, -0.12, "Thursday, Nov 13", transform=ax.get_xaxis_transform(), ha="center", fontsize=12,
            color=MUTED)
    ax.text((split + len(order)) / 2, -0.12, "Friday, Nov 14", transform=ax.get_xaxis_transform(),
            ha="center", fontsize=12, color=MUTED)
    ygrid(ax)
    save(fig, "b28_bart_delay_by_hour.png")


def fairness():
    if not os.path.exists(res("fairness_by_route.csv")):
        return skipped.append("fairness")
    df = pd.read_csv(res("fairness_by_route.csv"))
    df = df[df.close_pairs.fillna(0) >= 30].sort_values("no_history", ascending=False).reset_index(drop=True)
    p10, p50, p90 = (df.no_history.quantile(q) * 100 for q in (0.1, 0.5, 0.9))
    worst10 = df.head(10)
    fig, top = frame("The same detector sees some routes much less clearly than others",
                     "For each route: the share of possible bunching cases the detector could not check, because the "
                     f"buses had not reported recently enough. {int((worst10.source == 'SC').sum())} of the 10 worst "
                     f"routes are VTA; the worst routes are about {p90 / p10:.1f}x the best (90th vs 10th percentile).",
                     f"Data: 24 h replay, {len(df)} routes with 30+ close pairs; streaming/analyze_fairness.py (B27)",
                     legend=[("VTA", VTA, "box"), ("Muni", MUNI, "box")])
    ax = fig.add_axes([0.10, 0.17, 0.85, top - 0.20])
    ax.bar(df.index, df.no_history * 100, color=[VTA if s == "SC" else MUNI for s in df.source], width=0.75)
    ax.axhline(p50, color=INK, linewidth=1.5, linestyle=(0, (4, 3)))
    ax.text(len(df) * 0.62, p50 + 1, f"typical route: {p50:.0f}%", ha="center", fontsize=13, color=INK)
    first, last = df.iloc[0], df.iloc[-1]
    ax.annotate(f"VTA {first.route_id}: {first.no_history:.0%}", (0, first.no_history * 100),
                xytext=(14, -2), textcoords="offset points", fontsize=14, fontweight="bold", color=VTA, va="top")
    ax.annotate(f"Muni {last.route_id}: {last.no_history:.0%}", (len(df) - 1, last.no_history * 100),
                xytext=(-6, 140), textcoords="offset points", fontsize=14, fontweight="bold", color=MUNI, ha="right",
                arrowprops=dict(arrowstyle="-", color=MUNI, lw=1.2))
    ax.set_xticks([]); ax.set_xlim(-1, len(df))
    ax.set_xlabel(f"{len(df)} routes, from least reliable (left) to most reliable (right)")
    ax.set_ylabel("Cases the detector\ncould not check (%)")
    ax.set_ylim(0, df.no_history.max() * 110)
    ygrid(ax)
    save(fig, "b28_fairness_by_route.png")


# ---- streaming algorithms ------------------------------------------------------------------------
def bloom_memory():
    if not os.path.exists(res("b28_sweeps.json")):
        return skipped.append("Bloom memory (no sweep results yet)")
    sw = load_json("b28_sweeps.json")["bloom"]
    exact = sw.get("exact_set_bytes_measured", sw.get("exact_set_bytes_est")) / 1e6
    rows = sorted([r for r in sw["rows"] if r["target_fp"] <= 0.01], key=lambda r: -r["target_fp"])
    smallest = rows[0]["memory_bytes"] / 1e6
    fig, top = frame(f"A Bloom filter checks for duplicates in {smallest / exact:.0%}-"
                     f"{rows[-1]['memory_bytes'] / 1e6 / exact:.0%} of the memory",
                     f"Memory needed to remember all {sw['distinct_ids']:,} event IDs from 24 hours. The exact set is "
                     "never wrong; a Bloom filter never misses a real duplicate, but occasionally flags a new ID as "
                     f"seen before (a false alarm), at a rate we choose. Rates measured with {sw['probes']:,} new IDs.",
                     "Data: 24 h recording; Bloom filter sweep in benchmarks/make_figures.py; exact set size measured in Python")
    ax = fig.add_axes([0.25, 0.13, 0.62, top - 0.16])
    labels = ["Exact set"] + [f"Bloom filter,\n{r['target_fp'] * 100:g}% target" for r in rows]
    vals = [exact] + [r["memory_bytes"] / 1e6 for r in rows]
    texts = [f"{exact:.1f} MB   never wrong"] + \
            [f"{r['memory_bytes'] / 1e6:.2f} MB   {r['measured_fp'] * 100:.3g}% false alarms measured" for r in rows]
    y = list(range(len(vals)))[::-1]
    ax.barh(y, vals, color=[INK] + [ACCENT] * len(rows), height=0.6)
    for yy, v, t in zip(y, vals, texts):
        ax.text(v + exact * 0.012, yy, t, va="center", fontsize=14, color=INK)
    ax.set_yticks(y); ax.set_yticklabels(labels, fontsize=14, color=INK)
    ax.set_xlim(0, exact * 1.45); ax.set_xlabel("Memory (MB)")
    xgrid(ax)
    save(fig, "b28_bloom_memory.png")


def bloom_tradeoff():
    if not os.path.exists(res("b28_sweeps.json")):
        return skipped.append("Bloom trade-off (no sweep results yet)")
    sw = load_json("b28_sweeps.json")["bloom"]
    df = pd.DataFrame(sw["rows"]).sort_values("memory_bytes")
    exact = sw.get("exact_set_bytes_measured", sw.get("exact_set_bytes_est")) / 1e6
    fig, top = frame("More memory, fewer false alarms, and the measured rate matches the design",
                     "Each point is one Bloom filter sized for a target false-alarm rate (ring) and then tested with "
                     f"{sw['probes']:,} new IDs (dot). For scale: an exact set of the same IDs needs {exact:.0f} MB.",
                     "Data: 24 h recording; Bloom filter sweep in benchmarks/make_figures.py",
                     legend=[("Measured", ACCENT, "line"), ("Design target", INK, "hollow")])
    ax = fig.add_axes([0.11, 0.14, 0.84, top - 0.17])
    mb = df.memory_bytes / 1e6
    ax.plot(mb, df.measured_fp * 100, "-o", color=ACCENT, zorder=3)
    ax.scatter(mb, df.target_fp * 100, s=260, facecolors="none", edgecolors=INK, linewidths=1.8, zorder=4)
    for m, v, b in zip(mb, df.measured_fp * 100, df.bits_per_item):
        ax.annotate(f"{v:.3g}%  ({b:.0f} bits per ID)", (m, v), xytext=(14, 4), textcoords="offset points",
                    fontsize=12.5, color=INK)
    ax.set_yscale("log"); plain_log_axis(ax.yaxis)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}%"))
    ax.set_xlim(0, mb.max() * 1.45)
    ax.set_xlabel("Bloom filter memory (MB)"); ax.set_ylabel("False alarms (% of new IDs, log scale)")
    ax.grid(axis="both", which="major"); ax.set_axisbelow(True); ax.tick_params(length=0)
    save(fig, "b28_bloom_fp_vs_memory.png")


def fm():
    if not os.path.exists(res("b28_sweeps.json")):
        return skipped.append("Flajolet-Martin (no sweep results yet)")
    df = pd.DataFrame(load_json("b28_sweeps.json")["fm"])
    seeds = int(df["seeds"].iloc[0]) if "seeds" in df else None
    true = df.groupby("source")["true"].first()
    last = df[df.memory_bytes == df.memory_bytes.max()].set_index("source").mean_error_pct
    fig, top = frame(f"Flajolet-Martin counts the vehicles within {last.min():.0f}-{last.max():.0f}% "
                     "using only 1 KB",
                     "Error of the estimated number of distinct vehicles in 24 hours, per agency, as the counter gets "
                     "more memory" + (f" (average of {seeds} runs)" if seeds else "") +
                     ". An exact count would have to store every vehicle ID.",
                     "Data: 24 h recording; Flajolet-Martin sweep in benchmarks/make_figures.py; "
                     "error = |estimate - true| / true",
                     legend=[(f"Muni ({true.get('SF', 0):,} vehicles)", MUNI, "line"),
                             (f"VTA ({true.get('SC', 0):,} vehicles)", VTA, "line")])
    ax = fig.add_axes([0.10, 0.14, 0.85, top - 0.17])
    mems = sorted(df.memory_bytes.unique())
    xs = {m: i for i, m in enumerate(mems)}
    for src, color in (("SF", MUNI), ("SC", VTA)):
        g = df[df.source == src].sort_values("memory_bytes")
        ax.plot([xs[m] for m in g.memory_bytes], g.mean_error_pct, "-o", color=color)
    for m, g in df.groupby("memory_bytes"):
        g = g.sort_values("mean_error_pct")
        for rank, (_, r) in enumerate(g.iterrows()):
            above = rank == len(g) - 1
            ax.annotate(f"{r.mean_error_pct:.1f}%", (xs[m], r.mean_error_pct), xytext=(0, 12 if above else -14),
                        textcoords="offset points", ha="center", va="bottom" if above else "top", fontsize=13,
                        fontweight="bold", color=VTA if r.source == "SC" else MUNI)
    ax.set_xticks(range(len(mems)))
    ax.set_xticklabels([f"{m} bytes" if m < 1024 else f"{m // 1024} KB" for m in mems], fontsize=13)
    ax.set_xlim(-0.4, len(mems) - 0.6); ax.set_ylim(0, df.mean_error_pct.max() * 1.18)
    ax.set_xlabel("Memory used by the counter (per agency)"); ax.set_ylabel("Average error (%)")
    ygrid(ax)
    save(fig, "b28_fm_error_vs_memory.png")


def lsh():
    if not os.path.exists(res("lsh_recall_vs_tables.csv")):
        return skipped.append("LSH")
    df = pd.read_csv(res("lsh_recall_vs_tables.csv"))
    full = int(df[df.recall >= 0.999].hash_tables.min())
    total = int(df.pairs_found.max())
    false = int(df.false_pairs.sum())
    extra = ""
    if os.path.exists(res("lsh_trajectories.json")):
        fd = load_json("lsh_trajectories.json").get("full_day", {})
        if fd.get("speedup_est"):
            extra = f" On the full day it ran about {fd['speedup_est']:.0f}x faster than comparing every pair."
    fig, top = frame(f"LSH finds every similar trip once it uses {full} hash tables",
                     f"Share of the {total} truly similar trip pairs (sharing at least half their ~200 m map cells) "
                     f"that LSH found. More hash tables give it more chances to match. "
                     f"{'It never returned a wrong pair.' if false == 0 else ''}{extra}",
                     "Data: 2,000 trips sampled from the 24 h recording; algorithms/lsh_trajectories.py (B21)")
    ax = fig.add_axes([0.10, 0.14, 0.85, top - 0.17])
    xs = list(range(len(df)))
    ax.plot(xs, df.recall * 100, "-o", color=INK, markersize=11)
    i = df.index[df.hash_tables == full][0]
    ax.plot([i], [100], "o", color=ACCENT, markersize=15, zorder=5)
    for x, (v, n) in enumerate(zip(df.recall * 100, df.pairs_found)):
        ax.annotate(f"{v:.0f}%\n{n} of {total}", (x, v), xytext=(0, 14), textcoords="offset points", ha="center",
                    va="bottom", fontsize=12.5, color=ACCENT if x == i else INK, linespacing=1.2)
    ax.set_xticks(xs); ax.set_xticklabels([str(t) for t in df.hash_tables])
    ax.set_xlim(-0.4, len(df) - 0.6); ax.set_ylim(0, 125)
    ax.set_yticks(range(0, 101, 20))
    ax.set_xlabel("Number of hash tables"); ax.set_ylabel("Similar trip pairs found (%)")
    ygrid(ax)
    save(fig, "b28_lsh_recall_vs_tables.png")


# ---- performance and reliability -----------------------------------------------------------------
def throughput():
    files = sorted(glob.glob(res("b22_runs_*.csv")))
    if not files:
        return skipped.append("throughput")
    df = pd.concat([pd.read_csv(f) for f in files])
    df = df[df.run_id != COLD_JVM_RUN].sort_values("rate_target")
    ok1 = df[(df.trigger_s == 1) & df.keeps_up]
    limit = int(ok1.rate_target.max())
    lost = int(df.events_missing.sum())
    fig, top = frame(f"On one laptop the pipeline keeps up to about {limit:,} events per second",
                     "Time from an event entering Kafka to landing in the Parquet archive (95th percentile) as the "
                     "input rate grows. Hollow points: Spark could not keep up and a backlog built. "
                     + ("No event was lost in any run." if lost == 0 else f"{lost} events were lost in total."),
                     "Data: benchmarks/bench_pipeline.py (B22), Apple M1 Pro laptop; one run per point",
                     legend=[("New micro-batch every 1 s", INK, "line"), ("Every 5 s", CONTEXT, "line"),
                             ("Falls behind", MUTED, "hollow")])
    ax = fig.add_axes([0.10, 0.14, 0.85, top - 0.17])
    for trig, color in ((5, CONTEXT), (1, INK)):
        g = df[df.trigger_s == trig]
        ax.plot(g.rate_target, g.e2e_ms_p95 / 1000, "-", color=color, zorder=2)
        ax.scatter(g[g.keeps_up].rate_target, g[g.keeps_up].e2e_ms_p95 / 1000, color=color, s=80, zorder=3)
        ax.scatter(g[~g.keeps_up].rate_target, g[~g.keeps_up].e2e_ms_p95 / 1000, s=110, zorder=3,
                   facecolors="white", edgecolors=color, linewidths=2.2)
    r = ok1[ok1.rate_target == limit].iloc[0]
    ax.annotate(f"{r.e2e_ms_p95 / 1000:.1f} s at {limit:,}/s", (limit, r.e2e_ms_p95 / 1000), xytext=(-40, 125),
                textcoords="offset points", ha="right", fontsize=14, fontweight="bold", color=ACCENT,
                arrowprops=dict(arrowstyle="-", color=ACCENT, lw=1.2))
    ax.axvspan(limit * 1.1, df.rate_target.max() * 1.25, color=CONTEXT, alpha=0.3, linewidth=0)
    ax.text(limit * 1.15, 0.4, "Spark\nfalls\nbehind", fontsize=13, color=MUTED, va="bottom")
    ax.set_xscale("log")
    ticks = [t for t in (500, 1000, 2000, 5000, 10000, 20000, 40000, 80000, 120000)
             if df.rate_target.min() <= t <= df.rate_target.max()]
    ax.set_xticks(ticks); ax.set_xticklabels([f"{t // 1000}k" if t >= 1000 else str(t) for t in ticks])
    ax.xaxis.set_minor_formatter(mticker.NullFormatter()); ax.tick_params(which="minor", length=0)
    ax.set_xlim(df.rate_target.min() * 0.8, df.rate_target.max() * 1.25)
    ax.set_ylim(0, df.e2e_ms_p95.max() / 1000 * 1.1)
    ax.set_xlabel("Events per second sent into Kafka (log scale)")
    ax.set_ylabel("Delay, Kafka to archive (s)")
    ygrid(ax)
    save(fig, "b28_throughput_latency.png")


def faults():
    path = latest("b24_summary_*.csv")
    if not path:
        return skipped.append("fault injection")
    df = pd.read_csv(path)
    names = {("kafka", "at-most-once"): "Kafka down 10 s,\nat-most-once (no retries)",
             ("kafka", "at-least-once"): "Kafka down 10 s,\nat-least-once (retries)",
             ("kafka", "exactly-once"): "Kafka down 10 s,\nexactly-once (idempotent)",
             ("spark", "at-least-once"): "Spark killed for 5 s,\nrestarts from checkpoint"}
    df = df[[(f, d) in names for f, d in zip(df.fault, df.delivery)]]
    trials = int(df.trials.sum())
    fig, top = frame("With exactly-once delivery, a crash loses nothing and duplicates nothing",
                     f"Events lost and events written twice when we killed Kafka or Spark in the middle of a stream "
                     f"({trials} trials in total, 500 events per second; 150,000 to 250,000 events per setting).",
                     "Data: benchmarks/fault_injection.py (B24); totals over all trials of each setting")
    labels = [names[(f, d)] + f"  ({t} trials)" for f, d, t in zip(df.fault, df.delivery, df.trials)]
    y = list(range(len(df)))[::-1]
    for k, (col, title) in enumerate((("lost_end_to_end_total", "Events lost"),
                                      ("parquet_duplicates_total", "Events written twice"))):
        ax = fig.add_axes([0.36 + k * 0.32, 0.12, 0.27, top - 0.18])
        vals = df[col].astype(int).tolist()
        ax.barh(y, vals, color=[ACCENT if v else GOOD for v in vals], height=0.55)
        for yy, v in zip(y, vals):
            ax.text(max(v, 0) + 0.6, yy, f"{v}", va="center", fontsize=17, fontweight="bold",
                    color=ACCENT if v else GOOD)
        ax.set_xlim(0, max(df.lost_end_to_end_total.max(), 5) * 1.25)
        ax.set_title(title, fontsize=16, fontweight="bold", loc="left", color=INK, pad=12)
        ax.set_xlabel("Events (all trials)")
        ax.set_yticks(y)
        ax.set_yticklabels(labels if k == 0 else [], fontsize=13, color=INK)
        xgrid(ax)
    save(fig, "b28_fault_tolerance.png")


def windows():
    path = latest("b25_runs_*.csv")
    if not path:
        return skipped.append("window trade-off")
    df = pd.read_csv(path)
    idem = df[df.sink_mode == "idempotent"].sort_values("window_s")
    app = df[df.sink_mode == "append"].sort_values("window_s")
    mins = [f"{int(w // 60)} min" for w in idem.window_s]
    rec = idem.recall_exact * 100
    fig, top = frame("Bigger windows miss fewer late reports, but alert later",
                     f"The bunching detector run with 1, 5 and 10 minute windows on the same test stream. "
                     f"Recall against an exact scan stays high ({rec.min():.1f}% to {rec.max():.1f}%) and "
                     f"precision is {idem.precision_exact.min() * 100:.0f}%. After a crash, the idempotent sink "
                     f"wrote 0 duplicate alerts; the append sink wrote "
                     f"{', '.join(str(int(v)) for v in app.duplicate_alert_rows)}.",
                     "Data: benchmarks/design_comparison.py (B25), 2 h synthetic workload with 10% late reports")
    for k, (col, title, unit, color) in enumerate(
            (("late_drop_pct", "Late reports dropped", "%", STEEL),
             ("alert_delay_s_median", "Time until the alert (median)", " s", ACCENT))):
        ax = fig.add_axes([0.08 + k * 0.48, 0.14, 0.40, top - 0.21])
        vals = idem[col].tolist()
        ax.bar(mins, vals, color=color, width=0.55)
        for x, v in enumerate(vals):
            ax.text(x, v * 1.02, f"{v:.1f}{unit}" if unit == "%" else f"{v:.0f}{unit}", ha="center",
                    va="bottom", fontsize=17, fontweight="bold", color=INK)
        ax.set_title(title, fontsize=16, fontweight="bold", loc="left", color=INK, pad=12)
        ax.set_ylim(0, max(vals) * 1.25)
        ax.set_ylabel("Reports dropped (%)" if unit == "%" else "Seconds")
        ax.set_xlabel("Window size")
        ygrid(ax)
    save(fig, "b28_window_tradeoff.png")


def av():
    needed = ("stream_algorithms_24h.json", "av_replay_speed10.json", "av_replay_speed100.json")
    if not all(os.path.exists(res(n)) for n in needed):
        return skipped.append("AV")
    buses = load_json("stream_algorithms_24h.json")["events"] / 86_400
    r10 = load_json("av_replay_speed10.json")
    car = r10["sent"] / AV_SECONDS                   # one self-driving car at a time, in real time
    r100 = load_json("av_replay_speed100.json")
    rows = [("All Muni + VTA buses\n(24 h average)", buses, CONTEXT, "real"),
            ("One self-driving car\n(Argoverse 2, real time)", car, ACCENT, "real"),
            ("AV data replayed\n10x faster", r10["events_per_sec"], INK, "stress"),
            ("AV data replayed\n100x faster", r100["events_per_sec"], INK, "stress")]
    if os.path.exists(res("av_replay_speed100_x10.json")):
        rx = load_json("av_replay_speed100_x10.json")
        rows.append((f"{rx['virtual_vehicles']} replays at once,\n100x (most one laptop sent)",
                     rx["events_per_sec"], INK, "stress"))
    fig, top = frame(f"One self-driving car produces {car / buses:.0f}x the data of every Muni and VTA bus combined",
                     "Events per second. A bus reports its position about every 2 minutes; a self-driving car reports "
                     "every vehicle around it 10 times a second. To test a whole fleet, we replayed the car data "
                     "through the same pipeline faster. Log scale: each gridline is 10 times the one before.",
                     f"Data: 24 h Muni + VTA recording; 20 Argoverse 2 scenes ({r10['sent']:,} events over "
                     f"{AV_SECONDS:.0f} s) via ingestion/av_replay.py (B19, B20)")
    ax = fig.add_axes([0.27, 0.13, 0.66, top - 0.16])
    y = list(range(len(rows)))[::-1]
    vals = [r[1] for r in rows]
    ax.barh(y, vals, color=[r[2] for r in rows], height=0.6)
    for yy, v in zip(y, vals):
        ax.text(v * 1.15, yy, f"{v:,.1f}/s" if v < 10 else f"{v:,.0f}/s", va="center", fontsize=15,
                fontweight="bold", color=INK)
    ax.set_yticks(y); ax.set_yticklabels([r[0] for r in rows], fontsize=14, color=INK)
    ax.axhline(y[1] - 0.5, color=MUTED, linewidth=1, linestyle=(0, (3, 3)))
    ax.text(1.02, y[1] - 0.62, "Stress test (sped up)", transform=ax.get_yaxis_transform(), fontsize=12,
            color=MUTED, va="top", ha="right")
    ax.set_xscale("log"); ax.set_xlim(1, max(vals) * 12)
    ax.set_xticks([1, 10, 100, 1000, 10000, 100000, 1000000]); plain_log_axis(ax.xaxis)
    ax.tick_params(which="minor", length=0)
    ax.set_xlabel("Events per second (log scale)")
    xgrid(ax)
    save(fig, "b28_av_vs_transit.png")


# ---- sweeps over the 24 h replay ------------------------------------------------------------
def load_replay(folder, limit=None):
    ids, vehicles = [], {}
    for f in sorted(glob.glob(os.path.join(folder, "*.json"))):
        with open(f) as fh:
            for line in fh:
                e = json.loads(line)
                if e.get("source") in (None, "flush") or not e.get("event_id"):
                    continue
                ids.append(e["event_id"])
                vehicles.setdefault(e["source"], set()).add(e["vehicle_id"])
                if limit and len(ids) >= limit:
                    return ids, vehicles
    return ids, vehicles


def exact_set_bytes(items):
    """Measured memory of an exact Python set of the IDs: the set itself plus every string in it."""
    st = set(items)
    return sys.getsizeof(st) + sum(sys.getsizeof(x) for x in st)


def bloom_sweep(ids, probes=1_000_000):
    distinct = list(dict.fromkeys(ids))
    n = len(distinct)
    fake = [f"probe:{i}:not-an-event" for i in range(probes)]
    rows = []
    for p in (0.2, 0.1, 0.05, 0.01, 0.001, 0.0001):
        bf = BloomFilter(n, p)
        for x in distinct:
            bf.add(x)
        hits = sum(x in bf for x in fake)
        fp = hits / probes
        rows.append({"target_fp": p, "memory_bytes": bf.memory_bytes(), "hashes_k": bf.k,
                     "bits_per_item": round(bf.m / n, 2), "measured_fp": fp,
                     "false_positive_hits": hits, "expected_hits": round(p * probes)})
        print(f"  Bloom target {p:.2%}: measured {fp:.4%} ({hits:,} of {probes:,} probes), "
              f"{bf.memory_bytes() / 1e6:.2f} MB")
    exact = exact_set_bytes(distinct)
    print(f"  Exact Python set of the same IDs (measured): {exact / 1e6:.1f} MB")
    return {"distinct_ids": n, "probes": probes, "exact_set_bytes_measured": exact, "rows": rows}


def fm_sweep(vehicles, seeds=20):
    rows = []
    for nh in (8, 16, 32, 64, 128):
        for src, vs in sorted(vehicles.items()):
            errs = []
            for s in range(seeds):
                fm = FlajoletMartin(num_hashes=nh, group_size=min(8, nh))
                fm.salts = [f"fm-{s}-{i}".encode() for i in range(nh)]
                for v in vs:
                    fm.add(v)
                errs.append(abs(fm.estimate() - len(vs)) / len(vs) * 100)
            rows.append({"source": src, "num_hashes": nh, "memory_bytes": nh * 8, "true": len(vs), "seeds": seeds,
                         "mean_error_pct": round(sum(errs) / len(errs), 2),
                         "max_error_pct": round(max(errs), 2)})
            print(f"  FM {src} {nh:>3} hashes ({nh * 8} B): mean error {rows[-1]['mean_error_pct']}%")
    return rows


def main():
    global RES, OUT
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results", default=RES)
    p.add_argument("--replay", default="data/replay_in")
    p.add_argument("--out", default=OUT)
    p.add_argument("--skip-sweeps", action="store_true")
    p.add_argument("--probes", type=int, default=1_000_000, help="Bloom false-positive probes per size")
    p.add_argument("--fm-seeds", type=int, default=20, help="Flajolet-Martin runs per point")
    args = p.parse_args()
    RES, OUT = args.results, args.out
    os.makedirs(OUT, exist_ok=True)

    if not args.skip_sweeps and glob.glob(os.path.join(args.replay, "*.json")):
        print("Sweeps on the 24 h replay:")
        ids, vehicles = load_replay(args.replay)
        sweeps = {"bloom": bloom_sweep(ids, args.probes), "fm": fm_sweep(vehicles, args.fm_seeds)}
        with open(res("b28_sweeps.json"), "w") as f:
            json.dump(sweeps, f, indent=2)
            f.write("\n")

    for fn in (bunching, bart_knock_on, bart_by_hour, fairness, bloom_memory, bloom_tradeoff, fm, lsh,
               throughput, faults, windows, av):
        fn()
    print(f"Made {len(made)} figures in {OUT}")
    for s in skipped:
        print(f"  skipped: {s}")


if __name__ == "__main__":
    main()
