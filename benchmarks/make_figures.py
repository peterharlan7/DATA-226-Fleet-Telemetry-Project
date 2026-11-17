"""
Figures for the results section, slides and report (B28: results analysis + trade-off curves).

Reads the result files in benchmarks/results/ (from every card) and writes PNGs to
report/figures/. Two trade-off curves need a small extra sweep over the 24 h
replay files (data/replay_in), run here:
  - Bloom filter: false-positive rate vs memory, on the real event IDs
  - Flajolet-Martin: error vs memory (number of hash functions), on the real vehicle IDs
Every other figure only reads saved results. A figure whose input is missing is
skipped with a note, so this can be rerun as more results land.

Run from the repo root:
    python benchmarks/make_figures.py                 # everything (sweeps take ~3-5 min)
    python benchmarks/make_figures.py --skip-sweeps   # only the saved results
Writes report/figures/*.png and benchmarks/results/b28_sweeps.json (sweep numbers).
"""

import argparse
import glob
import json
import os
import sys
import textwrap

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from algorithms.stream_algos import BloomFilter, FlajoletMartin  # noqa: E402

RES = "benchmarks/results"
COLD_JVM_RUN = "20261114_200826_r40000_t1_0"
AV_PARQUET = "data/av2/av2_20_scenarios.parquet"
MIN_ARRIVALS = 100          # BART chart: hours with fewer arrivals are too small for a percentage
OUT = "report/figures"

# Validated reference palette (light mode); categorical slots in fixed order.
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#a3a29c", "#e4e3df", "#fcfcfb"
AGENCY = {"SF": BLUE, "SC": ORANGE}
AGENCY_NAME = {"SF": "Muni (SF)", "SC": "VTA (SC)"}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.size": 12, "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.labelsize": 12, "xtick.labelsize": 11.5, "ytick.labelsize": 11.5,
    "axes.edgecolor": MUTED, "axes.labelcolor": INK2, "text.color": INK,
    "xtick.color": INK2, "ytick.color": INK2, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "lines.linewidth": 2.5, "lines.markersize": 8, "legend.frameon": False,
})
made, skipped = [], []
FIGSIZE = (10, 5.8)          # 16:9-ish: fits a slide and a report column


def load_json(path):
    with open(path) as f:
        return json.load(f)


def latest(pattern):
    files = sorted(glob.glob(os.path.join(RES, pattern)))
    return files[-1] if files else None


def need(*paths):
    missing = [p for p in paths if not p or not os.path.exists(p)]
    return not missing


def square(color):
    return plt.Rectangle((0, 0), 1, 1, color=color)


def header(fig, title, subtitle, handles=None, labels=None):
    """Takeaway title, a plain-language subtitle saying what is measured, and the legend
    in one row under them; the plots are laid out below all three (nothing overlaps)."""
    sub = textwrap.wrap(subtitle, 118)
    top = 0.955
    fig.text(0.012, top, title, fontsize=17, fontweight="bold", color=INK, va="top")
    fig.text(0.012, top - 0.075, "\n".join(sub), fontsize=12, color=INK2, va="top", linespacing=1.35)
    y = top - 0.075 - 0.047 * len(sub) - 0.012
    if handles:
        fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.006, y), ncol=len(handles),
                   fontsize=11.5, handlelength=1.4, columnspacing=1.6, borderaxespad=0)
        y -= 0.065
    return y


def save(fig, name, source, plot_top):
    fig.tight_layout(rect=(0, 0.035, 1, plot_top))
    fig.text(0.012, 0.012, textwrap.fill(f"Source: {source}", 165), fontsize=9, color=INK2, va="bottom")
    path = os.path.join(OUT, name)
    fig.savefig(path, dpi=200, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    made.append(path)


def bar_labels(ax, bars, fmt, horizontal=False, inside_min=None, color=INK2, size=11.5):
    for b in bars:
        v = b.get_width() if horizontal else b.get_height()
        if horizontal:
            ax.annotate(fmt(v), (v, b.get_y() + b.get_height() / 2), xytext=(6, 0), textcoords="offset points",
                        va="center", fontsize=size, color=color)
        else:
            ax.annotate(fmt(v), (b.get_x() + b.get_width() / 2, v), xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=size, color=color)


# ---------------------------------------------------------------- streaming algorithms (B17, B23, B21)

def fig_bloom(sw):
    """Slide version: memory to remember every event ID, exact set vs Bloom filter."""
    df = pd.DataFrame(sw["rows"]).sort_values("target_fp")
    if "exact_set_bytes_measured" in sw:
        exact, how = sw["exact_set_bytes_measured"] / 1e6, "measured"
    else:
        exact, how = sw["exact_set_bytes_est"] / 1e6, "estimated"
    keep = df[df.target_fp.isin([0.0001, 0.001, 0.01])].sort_values("target_fp")
    rows = [("Exact set (Python)\nnever wrong", exact, ORANGE, "0% false alarms")]
    for r in keep.itertuples():
        rows.append((f"Bloom filter\n{r.target_fp:.2%} target".replace(".00%", "%").replace("0.10%", "0.1%"),
                     r.memory_bytes / 1e6, BLUE, f"measured {r.measured_fp:.3%} false alarms".replace("0.0", "0.0")))
    rows = rows[::-1]
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, f"A Bloom filter checks for duplicates in about 1% of the memory",
               f"Memory needed to remember all {sw['distinct_ids']:,} event IDs from the 24 h recording. The exact set "
               "never makes a mistake; a Bloom filter allows a small false-alarm rate that we choose "
               "(measured with 1,000,000 test IDs). It never misses a real duplicate.")
    bars = ax.barh([r[0] for r in rows], [r[1] for r in rows], color=[r[2] for r in rows], height=0.6,
                   edgecolor=SURFACE, linewidth=2)
    for b, r in zip(bars, rows):
        ax.annotate(f"{r[1]:.2f} MB  ·  {r[3]}" if r[1] < 10 else f"{r[1]:.1f} MB ({how})  ·  {r[3]}",
                    (b.get_width(), b.get_y() + b.get_height() / 2), xytext=(8, 0), textcoords="offset points",
                    va="center", fontsize=11.5, color=INK2)
    ax.set_xlim(0, exact * 1.75)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Memory (MB)")
    save(fig, "b28_bloom_memory.png", "Bloom filter sweep on the 24 h replay (benchmarks/make_figures.py, "
         "algorithms/stream_algos.py); exact set measured with sys.getsizeof", y)

    # Report version: the full trade-off curve.
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, "Bloom filter: each extra bit per ID cuts false alarms about in half",
               "False-alarm rate measured with 1,000,000 test IDs at six filter sizes, against the target rate the "
               f"filter was sized for. The orange line is the memory of an exact set ({exact:.0f} MB, {how}).",
               [plt.Line2D([], [], color=BLUE, marker="o"), plt.Line2D([], [], color=MUTED, ls="--")],
               ["measured", "target"])
    ax.plot(df.memory_bytes / 1e6, df.measured_fp * 100, "-o", color=BLUE)
    ax.plot(df.memory_bytes / 1e6, df.target_fp * 100, "--", color=MUTED, linewidth=1.8)
    ax.set_xscale("log"); ax.set_yscale("log")
    for r in df.itertuples():
        ax.annotate(f"{r.bits_per_item:.0f} bits per ID", (r.memory_bytes / 1e6, r.measured_fp * 100),
                    textcoords="offset points", xytext=(9, 2), fontsize=10.5, color=INK2)
    ax.axvline(exact, color=ORANGE, linewidth=2)
    ax.annotate(f"exact set\n{exact:.0f} MB", (exact, df.measured_fp.max() * 100), xytext=(-8, 0),
                textcoords="offset points", ha="right", va="top", fontsize=11, color=INK2)
    ax.set_xlabel("Memory (MB, log scale)"); ax.set_ylabel("False alarms (% of new IDs, log scale)")
    save(fig, "b28_bloom_fp_vs_memory.png", "24 h recording, Nov 13-14; algorithms/stream_algos.py", y)


def fig_fm(rows):
    df = pd.DataFrame(rows)
    seeds = int(df["seeds"].iloc[0]) if "seeds" in df else 5
    total = int(df.groupby("source")["true"].first().sum())
    fig, ax = plt.subplots(figsize=FIGSIZE)
    handles, labels = [], []
    for src, g in df.groupby("source"):
        c = AGENCY.get(src, AQUA)
        ax.plot(g.memory_bytes, g.mean_error_pct, "-o", color=c)
        handles.append(plt.Line2D([], [], color=c, marker="o"))
        labels.append(f"{AGENCY_NAME.get(src, src)}: {g['true'].iloc[0]} vehicles")
    last = df[df.memory_bytes == df.memory_bytes.max()]
    lo, hi = last.mean_error_pct.min(), last.mean_error_pct.max()
    y = header(fig, f"Flajolet-Martin counts about {total:,} vehicles within {lo:.0f}-{hi:.0f}% using 1 KB",
               "Error of the distinct-vehicle estimate per agency for 24 h, against how much memory the counter uses "
               f"(8 bytes per hash function). Each point is the average of {seeds} runs. An exact count would "
               "have to store every vehicle ID.", handles, labels)
    ax.set_xscale("log", base=2)
    ticks = sorted(df.memory_bytes.unique())
    ax.set_xticks(ticks); ax.set_xticklabels([f"{b} B" if b < 1024 else f"{b // 1024} KB" for b in ticks])
    ax.set_ylim(0, df.mean_error_pct.max() * 1.15)
    ax.set_xlabel("Memory per agency"); ax.set_ylabel("Average error (%)")
    save(fig, "b28_fm_error_vs_memory.png", "24 h recording; error = |estimate - true| / true", y)


def fig_lsh():
    path = os.path.join(RES, "lsh_recall_vs_tables.csv")
    if not need(path):
        return skipped.append("LSH (lsh_recall_vs_tables.csv)")
    df = pd.read_csv(path)
    full = df[df.recall >= 0.999].hash_tables.min()
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, f"LSH finds every similar trip with {int(full)} hash tables",
               "Of all trips that follow mostly the same streets, the share LSH found. "
               "More hash tables give it more chances to match. It never returned a wrong match.")
    xs = list(range(len(df)))                       # evenly spaced: 1, 2, 4, 8, 16 tables
    ax.plot(xs, df.recall * 100, "-o", color=BLUE, linewidth=3, markersize=10)
    for x, v in zip(xs, df.recall * 100):
        ax.annotate(f"{v:.0f}%", (x, v), xytext=(0, 12), textcoords="offset points", ha="center",
                    fontsize=13, color=INK2)
    ax.set_xticks(xs); ax.set_xticklabels([str(t) for t in df.hash_tables])
    ax.set_xlim(-0.4, len(df) - 0.6)
    ax.set_ylim(0, 112); ax.grid(axis="x", visible=False)
    ax.set_xlabel("Number of hash tables"); ax.set_ylabel("Similar trips found (%)")
    save(fig, "b28_lsh_recall_vs_tables.png", "algorithms/lsh_trajectories.py, 2,000 trips sampled from the "
         "24 h recording; similar = at least half of their ~200 m map cells shared", y)


# ---------------------------------------------------------------- detection (B12, B15, B27)

def fig_bunching():
    path = os.path.join(RES, "bunching_replay_summary.json")
    if not need(path):
        return skipped.append("bunching (bunching_replay_summary.json)")
    cats = load_json(path)["categories"]
    order = [("tandem", "Driving in tandem (< 40 m for 5 min)", True),
             ("same_stop", "Both at the same stop", True),
             ("moving", "Both moving, close together", True),
             ("no_history", "Not enough recent data to tell", False),
             ("one_parked", "One bus parked, the other passing", False),
             ("layover", "Both parked at a terminal", False)]
    total, kept = sum(cats.values()), sum(cats.get(k, 0) for k, _, keep in order if keep)
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, f"Only {kept / total:.0%} of 'buses too close' are real bunching",
               f"All {total:,} times two buses on the same route and direction were within 200 m in 24 h. "
               "The detector looks back 5 minutes to see whether both were actually moving.",
               [square(BLUE), square(MUTED)], [f"counted as bunching ({kept:,})", f"filtered out ({total - kept:,})"])
    labels = [lab for _, lab, _ in order][::-1]
    vals = [cats.get(k, 0) for k, _, _ in order][::-1]
    cols = [BLUE if keep else MUTED for _, _, keep in order][::-1]
    bars = ax.barh(labels, vals, color=cols, height=0.62, edgecolor=SURFACE, linewidth=2)
    bar_labels(ax, bars, lambda v: f"{v:,.0f}", horizontal=True)
    ax.set_xlim(0, max(vals) * 1.15)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Number of close pairs in 24 h")
    save(fig, "b28_bunching_categories.png", "streaming/detect_bunching.py on the 24 h replay (Muni + VTA)", y)


def fig_knock_on():
    prop = os.path.join(RES, "delay_propagation.json")
    if not need(prop):
        return skipped.append("BART knock-on (delay_propagation.json)")
    a = load_json(prop)["across_trips"]
    late_any = a["baseline_late_share"] * a["pairs"]
    late_after_late = a["late_after_late_share"] * a["late_pairs"]
    after_on_time = (late_any - late_after_late) / (a["pairs"] - a["late_pairs"])
    rows = [("after an on-time train", after_on_time, MUTED), ("any train (average)", a["baseline_late_share"], MUTED),
            ("after a late train", a["late_after_late_share"], ORANGE)]
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, f"After a late BART train, the next one is late {a['late_after_late_share']:.0%} of the time",
               f"Share of trains reaching a station 5+ minutes late, when the train before it on the same line was late "
               f"or on time (next train within 30 min; {a['pairs']:,} train pairs, 24 h). That is "
               f"{a['late_after_late_share'] / a['baseline_late_share']:.1f}x the average: delays spread from train to "
               "train. (Correlation; one incident can delay several trains.)")
    bars = ax.bar([r[0] for r in rows], [r[1] * 100 for r in rows], color=[r[2] for r in rows], width=0.55,
                  edgecolor=SURFACE, linewidth=2)
    bar_labels(ax, bars, lambda v: f"{v:.0f}%", size=13)
    ax.set_ylim(0, max(r[1] for r in rows) * 100 * 1.2)
    ax.grid(axis="x", visible=False)
    ax.set_ylabel("Next train 5+ min late (%)")
    save(fig, "b28_bart_knock_on.png", "streaming/analyze_delay_propagation.py (B15), BART GTFS-rt trip updates", y)


def fig_delay():
    path = os.path.join(RES, "delay_by_hour.csv")
    if not need(path):
        return skipped.append("BART delay by hour (delay_by_hour.csv)")
    df = pd.read_csv(path)
    small = df.arrivals < MIN_ARRIVALS
    shown = df[~small]
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, "BART lateness peaked late at night and on the afternoon of Nov 13",
               "Share of BART arrivals 5+ minutes late, by hour (one day). Shaded hours had no trains or fewer "
               f"than {MIN_ARRIVALS}, too few for a percentage.")
    ax.bar(shown.hour, shown.late_share * 100, color=BLUE, width=0.7, edgecolor=SURFACE, linewidth=2)
    ax.set_xticks(range(0, 24, 3)); ax.set_xticklabels(["12 am", "3 am", "6 am", "9 am", "12 pm", "3 pm", "6 pm", "9 pm"])
    ax.set_xlim(-0.7, 23.7)
    top = max(shown.late_share * 100) * 1.25
    ax.set_ylim(0, top)
    gap = sorted(set(range(24)) - set(shown.hour))
    runs, start = [], None
    for h in gap + [None]:
        if start is None:
            start = prev = h
        elif h is not None and h == prev + 1:
            prev = h
        else:
            runs.append((start, prev)); start = prev = h
    for s, e in [r for r in runs if r[0] is not None]:
        ax.axvspan(s - 0.45, e + 0.45, color=GRID, alpha=0.6, linewidth=0, zorder=0)
        ax.text((s + e) / 2, top * 0.5, "too few\ntrains", ha="center", va="center", fontsize=11, color=INK2)
    for r in shown.itertuples():
        if r.late_share * 100 < 0.5:
            ax.text(r.hour, top * 0.015, f"{r.late_share * 100:.1f}%".replace(".0%", "%"), ha="center",
                    va="bottom", fontsize=10, color=INK2)
    ax.grid(axis="x", visible=False)
    ax.set_xlabel("Hour of day (Pacific)"); ax.set_ylabel("Arrivals 5+ min late (%)")
    left_out = ", ".join(f"{int(r.hour):02d}:00 ({int(r.arrivals)})" for r in df[small].itertuples())
    missing = sorted(set(range(24)) - set(df.hour))
    note = f" Left out (fewer than {MIN_ARRIVALS} arrivals): {left_out}." if left_out else ""
    if missing:
        note += " No trip updates at " + ", ".join(f"{h:02d}:00" for h in missing) + "."
    save(fig, "b28_bart_delay_by_hour.png", f"streaming/analyze_delay_propagation.py (B15).{note}", y)


def fig_fairness():
    path = os.path.join(RES, "fairness_by_route.csv")
    if not need(path):
        return skipped.append("fairness (fairness_by_route.csv)")
    df = pd.read_csv(path)
    df = df[df.close_pairs.fillna(0) >= 30].sort_values("no_history", ascending=False).reset_index(drop=True)
    p10, p90 = df.no_history.quantile([0.1, 0.9])
    worst10 = df.head(10)
    vta10 = int((worst10.source == "SC").sum())
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, "The same detector sees some routes much less clearly than others",
               "For each route: the share of possible bunching cases the detector could not check, because the buses "
               f"had not reported often enough. Typical route {df.no_history.median():.0%}; the worst routes about "
               f"{p90 / p10:.1f}x the best (90th vs 10th percentile). {vta10} of the 10 worst are VTA.",
               [square(c) for c in AGENCY.values()], list(AGENCY_NAME.values()))
    bars = ax.bar(df.index, df.no_history * 100, color=[AGENCY.get(s, AQUA) for s in df.source], width=0.8,
                  edgecolor=SURFACE, linewidth=1)
    name = lambda r: f"{AGENCY_NAME.get(r.source, r.source).split()[0]} {r.route_id}"  # noqa: E731
    first, last = df.iloc[0], df.iloc[-1]
    med = df.no_history.median() * 100
    ax.axhline(med, color=INK2, linewidth=1.2, linestyle=":")
    ax.text(len(df) * 0.72, med + 1.2, f"typical route: {med:.0f}%", ha="center", va="bottom", fontsize=11,
            color=INK2)
    ax.set_xticks([0, len(df) - 1])                    # name the two ends on the axis itself
    ax.set_xticklabels([f"{name(first)}\n{first.no_history:.0%}", f"{name(last)}\n{last.no_history:.0%}"])
    ax.grid(axis="x", visible=False)
    ax.set_ylim(0, df.no_history.max() * 100 * 1.12)
    ax.set_xlabel(f"{len(df)} routes, from least reliable (left) to most reliable (right)")
    ax.set_ylabel("Cases the detector\ncould not check (%)")
    save(fig, "b28_fairness_by_route.png", "streaming/analyze_fairness.py (B27), 24 h replay; routes with 30+ close pairs", y)


# ---------------------------------------------------------------- performance and reliability (B22, B24, B25, B19/B20)

def fig_throughput():
    files = sorted(glob.glob(os.path.join(RES, "b22_runs_*.csv")))
    if not files:
        return skipped.append("B22 throughput (b22_runs_*.csv)")
    df = pd.concat([pd.read_csv(f) for f in files])
    df = df[df.run_id != COLD_JVM_RUN]          # cold-JVM first run, repeated later (benchmarks/README.md)
    g1 = df[df.trigger_s == 1].sort_values("rate_target")
    ok = g1[g1.keeps_up]
    limit = ok.rate_target.max() if len(ok) else None
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, f"On one laptop the pipeline keeps up to about {limit / 1000:.0f},000 events per second"
               if limit else "Pipeline latency vs load",
               "Time from an event entering Kafka to landing in Parquet (95th percentile), as the input rate grows. "
               "Hollow points: Spark could not keep up and a backlog built. No event was lost in any run.",
               [plt.Line2D([], [], color=BLUE, marker="o"), plt.Line2D([], [], color=MUTED, marker="o"),
                plt.Line2D([], [], color=INK2, marker="o", mfc=SURFACE, ls="")],
               ["1 s micro-batches", "5 s micro-batches", "falls behind"])
    for trig, color in ((5, MUTED), (1, BLUE)):
        g = df[df.trigger_s == trig].sort_values("rate_target")
        ax.plot(g.rate_target, g.e2e_ms_p95 / 1000, "-", color=color, zorder=2)
        good, bad = g[g.keeps_up], g[~g.keeps_up]
        ax.scatter(good.rate_target, good.e2e_ms_p95 / 1000, color=color, zorder=3, s=55)
        ax.scatter(bad.rate_target, bad.e2e_ms_p95 / 1000, facecolor=SURFACE, edgecolor=color, linewidth=2.2,
                   zorder=3, s=65)
    if limit:
        ax.axvspan(limit * 1.1, df.rate_target.max() * 1.3, color=GRID, alpha=0.5, linewidth=0, zorder=0)
        ax.text(limit * 1.16, ax.get_ylim()[0] + 0.3, "Spark\nfalls\nbehind", fontsize=10.5, color=INK2, va="bottom")
    ax.set_xscale("log")
    ticks = [t for t in (1000, 5000, 10000, 20000, 40000, 80000, 120000) if df.rate_target.min() <= t <= df.rate_target.max()]
    ax.set_xticks(ticks); ax.set_xticklabels([f"{t // 1000}k" for t in ticks]); ax.minorticks_off()
    ax.set_xlabel("Events per second into Kafka (log scale)"); ax.set_ylabel("Latency, 95th percentile (s)")
    save(fig, "b28_throughput_latency.png", "benchmarks/bench_pipeline.py (B22), Apple M1 Pro laptop", y)


def fig_faults():
    path = latest("b24_summary_*.csv")
    if not need(path):
        return skipped.append("B24 faults (b24_summary_*.csv)")
    df = pd.read_csv(path)
    plain = {"at-most-once": "at-most-once (no retries)", "at-least-once": "at-least-once (retries)",
             "exactly-once": "exactly-once (idempotent)"}
    names = [f"{'Kafka' if r.fault == 'kafka' else 'Spark'} killed · {plain.get(r.delivery, r.delivery)}"
             for r in df.itertuples()][::-1]
    lost, dup = list(df.lost_end_to_end_total)[::-1], list(df.parquet_duplicates_total)[::-1]
    trials = int(df.trials.sum())
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, "With exactly-once delivery, a crash loses nothing and duplicates nothing",
               f"Events lost and events written twice when we killed Kafka (10 s) or Spark (5 s) mid-stream, for each "
               f"delivery guarantee ({trials} trials in total, 500 events/s). Restarted Spark resumes from its checkpoint.",
               [square(ORANGE), square(BLUE)], ["events lost", "events duplicated"])
    yy = range(len(df))
    h = 0.36
    b1 = ax.barh([i + h / 2 for i in yy], lost, h, color=ORANGE, edgecolor=SURFACE, linewidth=1.5)
    b2 = ax.barh([i - h / 2 for i in yy], dup, h, color=BLUE, edgecolor=SURFACE, linewidth=1.5)
    bar_labels(ax, list(b1) + list(b2), lambda v: f"{v:.0f}", horizontal=True)
    ax.set_yticks(list(yy)); ax.set_yticklabels(names)
    ax.set_xlim(0, max(max(lost), max(dup), 1) * 1.2)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Events (all trials)")
    save(fig, "b28_fault_tolerance.png", "benchmarks/fault_injection.py (B24)", y)


def fig_windows():
    path = latest("b25_runs_*.csv")
    if not need(path):
        return skipped.append("B25 window trade-off (b25_runs_*.csv)")
    df = pd.read_csv(path)
    idem = df[df.sink_mode == "idempotent"].sort_values("window_s")
    app = df[df.sink_mode == "append"].sort_values("window_s")
    mins = [f"{int(w // 60)} min" for w in idem.window_s]
    rec = idem.recall_exact * 100
    fig, axes = plt.subplots(1, 2, figsize=FIGSIZE)
    dups = "/".join(str(int(d)) for d in app.duplicate_alert_rows)
    y = header(fig, "Bigger windows miss fewer late reports, but alert later",
               f"Bunching detector with 1, 5 and 10 minute windows. Recall against an exact scan rises only slightly "
               f"({rec.iloc[0]:.1f}% to {rec.iloc[-1]:.1f}%; precision 100%). After a crash the idempotent sink wrote 0 "
               f"duplicate alerts, the append sink {dups}.")
    panels = [(axes[0], idem.late_drop_pct, "Late reports dropped (%)", lambda v: f"{v:.1f}%"),
              (axes[1], idem.alert_delay_s_median, "Median time to alert (s)", lambda v: f"{v:.0f} s")]
    for ax, vals, title, fmt in panels:
        bars = ax.bar(mins, vals, color=BLUE, width=0.55, edgecolor=SURFACE, linewidth=2)
        bar_labels(ax, bars, fmt)
        ax.set_ylim(0, max(vals) * 1.2)
        ax.set_title(title, fontsize=13, color=INK, pad=10); ax.grid(axis="x", visible=False)
        ax.set_xlabel("Window size")
    save(fig, "b28_window_tradeoff.png", "benchmarks/design_comparison.py (B25)", y)


def fig_av():
    reports = {s: os.path.join(RES, f"av_replay_{s}.json") for s in ("speed10", "speed100", "speed100_x10")}
    algo = os.path.join(RES, "stream_algorithms_24h.json")
    if not need(*reports.values(), algo):
        return skipped.append("AV vs transit (av_replay_*.json)")
    transit = load_json(algo)["events"] / 86_400
    if os.path.exists(AV_PARQUET):
        av = pd.read_parquet(AV_PARQUET, columns=["scenario_id", "track_id", "t_sec"])
        n_av = av.groupby(["scenario_id", "track_id"]).ngroups
        realtime = len(av) / (av.t_sec.max() - av.t_sec.min())
        rt_label = f"{n_av:,} vehicles seen by\nself-driving cars (real time)"
    else:
        realtime = load_json(reports["speed10"])["events_per_sec"] / 10
        rt_label = "Vehicles seen by self-driving\ncars (real time, 10x run / 10)"
    rates = [("All Muni + VTA buses\n(24 h average)", transit, MUTED),
             (rt_label, realtime, ORANGE),
             ("AV replay at 10x", load_json(reports["speed10"])["events_per_sec"], BLUE),
             ("AV replay at 100x", load_json(reports["speed100"])["events_per_sec"], BLUE),
             ("Most one laptop\nproducer could send", load_json(reports["speed100_x10"])["events_per_sec"], BLUE)]
    fig, ax = plt.subplots(figsize=FIGSIZE)
    y = header(fig, f"Self-driving cars produce {realtime / transit:.0f}x the data of every Muni and VTA bus combined",
               "Events per second. A bus reports about every 2 minutes; a self-driving car tracks every nearby vehicle "
               "10 times a second. We replayed real Argoverse 2 data through the same pipeline at 10x and 100x speed. "
               "Log scale: each gridline is 10 times the last.")
    labels = [r[0] for r in rates][::-1]
    vals = [r[1] for r in rates][::-1]
    bars = ax.barh(labels, vals, color=[r[2] for r in rates][::-1], height=0.62, edgecolor=SURFACE, linewidth=2)
    bar_labels(ax, bars, lambda v: f"{v:,.0f} /s" if v >= 10 else f"{v:.1f} /s", horizontal=True)
    ax.set_xscale("log"); ax.set_xlim(1, max(vals) * 10)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Events per second (log scale)")
    save(fig, "b28_av_vs_transit.png", "Argoverse 2 (20 scenarios) via ingestion/av_replay.py; 24 h 511 recording", y)


# ---------------------------------------------------------------- sweeps on the real day

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

    sweeps_path = os.path.join(RES, "b28_sweeps.json")
    sweeps = load_json(sweeps_path) if os.path.exists(sweeps_path) else None
    if not args.skip_sweeps and glob.glob(os.path.join(args.replay, "*.json")):
        print("Sweeps on the 24 h replay:")
        ids, vehicles = load_replay(args.replay)
        sweeps = {"bloom": bloom_sweep(ids, args.probes), "fm": fm_sweep(vehicles, args.fm_seeds)}
        with open(sweeps_path, "w") as f:
            json.dump(sweeps, f, indent=2)
            f.write("\n")
    if sweeps:
        fig_bloom(sweeps["bloom"])
        fig_fm(sweeps["fm"])
    else:
        skipped.append(f"Bloom / FM sweeps (no {args.replay} and no {sweeps_path})")

    for fn in (fig_lsh, fig_bunching, fig_knock_on, fig_delay, fig_fairness, fig_throughput, fig_faults, fig_windows,
               fig_av):
        fn()
    print(f"\nMade {len(made)} figures in {OUT}:")
    for m in made:
        print(f"  {m}")
    for s in skipped:
        print(f"  skipped: {s}")


if __name__ == "__main__":
    main()
