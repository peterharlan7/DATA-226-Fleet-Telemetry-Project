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
    python benchmarks/make_figures.py                 # everything (sweeps take ~2 min)
    python benchmarks/make_figures.py --skip-sweeps   # only the saved results
Writes report/figures/*.png and benchmarks/results/b28_sweeps.json (sweep numbers).
"""

import argparse
import glob
import json
import os
import random
import sys
import textwrap

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from algorithms.stream_algos import BloomFilter, FlajoletMartin  # noqa: E402

RES = "benchmarks/results"
OUT = "report/figures"

# Validated reference palette (light mode); categorical slots in fixed order.
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#a3a29c", "#e4e3df", "#fcfcfb"
AGENCY = {"SF": BLUE, "SC": ORANGE}
AGENCY_NAME = {"SF": "Muni (SF)", "SC": "VTA (SC)"}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.edgecolor": MUTED, "axes.labelcolor": INK2, "text.color": INK,
    "xtick.color": INK2, "ytick.color": INK2, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "lines.linewidth": 2, "lines.markersize": 7, "legend.frameon": False,
})
made, skipped = [], []


def finish(ax, title, handles=None, labels=None, ncol=None):
    """Wrapped title, and the legend in one row ABOVE the plot (never on top of the data)."""
    if handles is None:
        handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(handles, labels, loc="lower left", bbox_to_anchor=(0, 1.02), borderaxespad=0,
                  ncol=ncol or len(handles), fontsize=9.5, handlelength=1.6, columnspacing=1.2)
    ax.set_title(textwrap.fill(title, 70), pad=30 if handles else 8)


def save(fig, name, source, bottom=0.0):
    fig.tight_layout()
    fig.text(0.01, -0.01, f"Source: {source}", fontsize=8, color=INK2, va="top")
    path = os.path.join(OUT, name)
    fig.savefig(path, dpi=200, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    made.append(path)


def latest(pattern):
    files = sorted(glob.glob(os.path.join(RES, pattern)))
    return files[-1] if files else None


def need(*paths):
    missing = [p for p in paths if not p or not os.path.exists(p)]
    return not missing


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


def bloom_sweep(ids, probes=50_000):
    distinct = list(dict.fromkeys(ids))
    n = len(distinct)
    fake = [f"probe:{i}:not-an-event" for i in range(probes)]
    rows = []
    for p in (0.2, 0.1, 0.05, 0.01, 0.001, 0.0001):
        bf = BloomFilter(n, p)
        for x in distinct:
            bf.add(x)
        fp = sum(x in bf for x in fake) / probes
        rows.append({"target_fp": p, "memory_bytes": bf.memory_bytes(), "hashes_k": bf.k,
                     "bits_per_item": round(bf.m / n, 2), "measured_fp": fp})
        print(f"  Bloom target {p:.2%}: measured {fp:.3%}, {bf.memory_bytes() / 1e6:.2f} MB")
    return {"distinct_ids": n, "exact_set_bytes_est": n * 120, "rows": rows}


def fm_sweep(vehicles, seeds=5):
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
            rows.append({"source": src, "num_hashes": nh, "memory_bytes": nh * 8, "true": len(vs),
                         "mean_error_pct": round(sum(errs) / len(errs), 2),
                         "max_error_pct": round(max(errs), 2)})
            print(f"  FM {src} {nh:>3} hashes ({nh * 8} B): mean error {rows[-1]['mean_error_pct']}%")
    return rows


def fig_bloom(sw):
    df = pd.DataFrame(sw["rows"])
    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.plot(df.memory_bytes / 1e6, df.measured_fp * 100, "-o", color=BLUE, label="measured")
    ax.plot(df.memory_bytes / 1e6, df.target_fp * 100, "--", color=MUTED, linewidth=1.5, label="target")
    ax.set_xscale("log"); ax.set_yscale("log")
    for _, r in df.iterrows():
        ax.annotate(f"{r.bits_per_item:.0f} bits/ID", (r.memory_bytes / 1e6, r.measured_fp * 100),
                    textcoords="offset points", xytext=(6, 4), fontsize=8, color=INK2)
    exact = sw["exact_set_bytes_est"] / 1e6
    ax.axvline(exact, color=ORANGE, linewidth=1.5)
    ax.text(exact, ax.get_ylim()[1], f" exact set ≈ {exact:.0f} MB\n (0% false positives)", va="top",
            fontsize=9, color=INK2)
    ax.set_xlabel("Memory (MB, log scale)"); ax.set_ylabel("False-positive rate (%, log scale)")
    finish(ax, f"Bloom filter: accuracy vs memory ({sw['distinct_ids']:,} real event IDs, 24 h)")
    save(fig, "b28_bloom_fp_vs_memory.png", "24 h recording, Nov 13-14; algorithms/stream_algos.py")


def fig_fm(rows):
    df = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(8, 4.8))
    for src, g in df.groupby("source"):
        ax.plot(g.memory_bytes, g.mean_error_pct, "-o", color=AGENCY.get(src, AQUA),
                label=f"{AGENCY_NAME.get(src, src)} ({g['true'].iloc[0]} vehicles)")
    ax.set_xscale("log", base=2)
    ax.set_xticks(df.memory_bytes.unique()); ax.set_xticklabels([f"{b} B" for b in df.memory_bytes.unique()])
    ax.set_xlabel("Memory per agency (bytes; 8 per hash function)"); ax.set_ylabel("Mean error (%)")
    finish(ax, "Flajolet-Martin: distinct-vehicle error vs memory (5 seeds)")
    save(fig, "b28_fm_error_vs_memory.png", "24 h recording; error = |estimate - true| / true")


# ---------------------------------------------------------------- figures from saved results

def fig_lsh():
    path = os.path.join(RES, "lsh_recall_vs_tables.csv")
    if not need(path):
        return skipped.append("LSH (lsh_recall_vs_tables.csv)")
    df = pd.read_csv(path)
    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.plot(df.hash_tables, df.recall * 100, "-o", color=BLUE, label="measured recall")
    t = range(1, int(df.hash_tables.max()) + 1)
    ax.plot(list(t), [100 * (1 - 0.5 ** k) for k in t], "--", color=MUTED, linewidth=1.5,
            label="theory at Jaccard = 0.5: 1 - 0.5^tables")
    for _, r in df.iterrows():
        ax.annotate(f"{r.recall:.0%}", (r.hash_tables, r.recall * 100), textcoords="offset points",
                    xytext=(0, -16), ha="center", fontsize=9, color=INK2)
    ax.set_xscale("log", base=2); ax.set_xticks(df.hash_tables); ax.set_xticklabels(df.hash_tables)
    ax.set_ylim(0, 105)
    ax.set_xlabel("Hash tables"); ax.set_ylabel("Recall of similar trip pairs (%)")
    finish(ax, "MinHash LSH: recall vs hash tables (0 false pairs)")
    save(fig, "b28_lsh_recall_vs_tables.png", "algorithms/lsh_trajectories.py on the 24 h recording")


def fig_bunching():
    path = os.path.join(RES, "bunching_replay_summary.json")
    if not need(path):
        return skipped.append("bunching (bunching_replay_summary.json)")
    cats = json.load(open(path))["categories"]
    order = [("layover", "both parked (layover)", False), ("one_parked", "one bus parked", False),
             ("no_history", "too little history", False), ("moving", "both moving", True),
             ("same_stop", "same stop", True), ("tandem", "tandem (<40 m for 5 min)", True)]
    labels = [lab for _, lab, _ in order][::-1]
    vals = [cats.get(k, 0) for k, _, _ in order][::-1]
    cols = [BLUE if keep else MUTED for _, _, keep in order][::-1]
    total, kept = sum(cats.values()), sum(cats.get(k, 0) for k, _, keep in order if keep)
    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.barh(labels, vals, color=cols, height=0.6, edgecolor=SURFACE, linewidth=2)
    for i, v in enumerate(vals):
        ax.text(v + total * 0.01, i, f"{v:,}", va="center", fontsize=9, color=INK2)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Close pairs (< 200 m) in 24 h")
    finish(ax, f"Bunching filter: {kept:,} of {total:,} close pairs kept ({kept / total:.0%})",
           [plt.Rectangle((0, 0), 1, 1, color=BLUE), plt.Rectangle((0, 0), 1, 1, color=MUTED)],
           ["counted as bunching", "filtered out"])
    ax.set_xlim(right=max(vals) * 1.12)
    save(fig, "b28_bunching_categories.png", "streaming/detect_bunching.py, 24 h replay")


def fig_throughput():
    files = sorted(glob.glob(os.path.join(RES, "b22_runs_*.csv")))
    if not files:
        return skipped.append("B22 throughput (b22_runs_*.csv)")
    df = pd.concat([pd.read_csv(f).assign(file=os.path.basename(f)) for f in files])
    # The 20:08 sweep had a cold JVM on its first run (benchmarks/README.md); its 40k / 1 s run was repeated at 20:13.
    df = df[~((df.file.str.contains("2008")) & (df.rate_target == 40000) & (df.trigger_s == 1))]
    fig, ax = plt.subplots(figsize=(8, 4.8))
    for trig, color in ((1, BLUE), (5, ORANGE)):
        g = df[df.trigger_s == trig].sort_values("rate_target")
        ax.plot(g.rate_target, g.e2e_ms_p95 / 1000, "-", color=color, label=f"{trig} s trigger")
        ok, bad = g[g.keeps_up], g[~g.keeps_up]
        ax.scatter(ok.rate_target, ok.e2e_ms_p95 / 1000, color=color, zorder=3, s=40)
        ax.scatter(bad.rate_target, bad.e2e_ms_p95 / 1000, facecolor=SURFACE, edgecolor=color,
                   linewidth=2, zorder=3, s=50)
    ax.scatter([], [], facecolor=SURFACE, edgecolor=INK2, linewidth=2, label="falls behind (backlog)")
    ax.set_xscale("log")
    ax.set_xlabel("Input rate (events/s, log scale)"); ax.set_ylabel("End-to-end latency p95 (s)")
    finish(ax, "Kafka → Spark → Parquet: latency vs load")
    save(fig, "b28_throughput_latency.png", "benchmarks/bench_pipeline.py (B22), M1 Pro, 0 events missing")


def fig_faults():
    path = latest("b24_summary_*.csv")
    if not need(path):
        return skipped.append("B24 faults (b24_summary_*.csv)")
    df = pd.read_csv(path)
    names = [f"{'Kafka' if r.fault == 'kafka' else 'Spark'} down\n{r.delivery}" for r in df.itertuples()]
    lost, dup = df.lost_end_to_end_total, df.parquet_duplicates_total
    x = range(len(df))
    fig, ax = plt.subplots(figsize=(8, 4.8))
    w = 0.38
    ax.bar([i - w / 2 for i in x], lost, w, color=ORANGE, label="events lost", edgecolor=SURFACE, linewidth=2)
    ax.bar([i + w / 2 for i in x], dup, w, color=BLUE, label="duplicates", edgecolor=SURFACE, linewidth=2)
    for i, (l, d) in enumerate(zip(lost, dup)):
        ax.text(i - w / 2, l + 0.5, str(l), ha="center", fontsize=9, color=INK2)
        ax.text(i + w / 2, d + 0.5, str(d), ha="center", fontsize=9, color=INK2)
    ax.set_xticks(list(x)); ax.set_xticklabels(names, fontsize=9)
    ax.grid(axis="x", visible=False)
    ax.set_ylabel("Events (all trials)")
    trials = int(df.trials.sum())
    ax.set_ylim(top=max(max(lost), max(dup), 1) * 1.15)
    finish(ax, f"Fault injection: events lost and duplicated ({trials} trials)")
    save(fig, "b28_fault_tolerance.png", "benchmarks/fault_injection.py (B24): 10 s Kafka / 5 s Spark outage, 500 ev/s")


def fig_windows():
    path = latest("b25_runs_*.csv")
    if not need(path):
        return skipped.append("B25 window trade-off (b25_runs_*.csv, after #25 is merged)")
    df = pd.read_csv(path)
    idem = df[df.sink_mode == "idempotent"].sort_values("window_s")
    app = df[df.sink_mode == "append"].sort_values("window_s")
    mins = (idem.window_s // 60).astype(str) + " min"
    fig, axes = plt.subplots(1, 3, figsize=(11, 4.2))
    panels = [(axes[0], idem.late_drop_pct, "Late reports dropped (%)"),
              (axes[1], idem.alert_delay_s_median, "Median alert delay (s)"),
              (axes[2], idem.recall_exact * 100, "Recall vs exact scan (%)")]
    for ax, vals, title in panels:
        ax.bar(mins, vals, color=BLUE, width=0.55, edgecolor=SURFACE, linewidth=2)
        for i, v in enumerate(vals):
            ax.text(i, v, f"{v:.1f}" if v < 100 else f"{v:.0f}", ha="center", va="bottom", fontsize=9, color=INK2)
        ax.set_title(title, fontsize=11); ax.grid(axis="x", visible=False)
    axes[2].set_ylim(90, 100)
    dup = ", ".join(f"{int(w // 60)} min: {d}" for w, d in zip(app.window_s, app.duplicate_alert_rows))
    fig.suptitle("Window size trade-off (bunching detector, crash mid-run)", x=0.01, ha="left",
                 fontweight="bold", fontsize=13)
    save(fig, "b28_window_tradeoff.png", "benchmarks B25 design comparison (Jason). Duplicate alerts after the "
         f"crash: append sink {dup}; idempotent sink 0 for every window.")
    return None


def fig_av():
    reports = {s: os.path.join(RES, f"av_replay_{s}.json") for s in ("speed10", "speed100", "speed100_x10")}
    algo = os.path.join(RES, "stream_algorithms_24h.json")
    if not need(*reports.values(), algo):
        return skipped.append("AV vs transit (av_replay_*.json)")
    transit = json.load(open(algo))["events"] / 86_400
    rates = [("Muni + VTA buses,\nlive average", transit, MUTED),
             ("958 AVs, real time\n(10 Hz)", json.load(open(reports["speed10"]))["events_per_sec"] / 10, BLUE),
             ("AV replay 10x", json.load(open(reports["speed10"]))["events_per_sec"], BLUE),
             ("AV replay 100x", json.load(open(reports["speed100"]))["events_per_sec"], BLUE),
             ("AV 100x, 10 copies\n(producer ceiling)", json.load(open(reports["speed100_x10"]))["events_per_sec"], BLUE)]
    fig, ax = plt.subplots(figsize=(8, 4.8))
    labels = [r[0] for r in rates][::-1]
    vals = [r[1] for r in rates][::-1]
    ax.barh(labels, vals, color=[r[2] for r in rates][::-1], height=0.6, edgecolor=SURFACE, linewidth=2)
    ax.set_xscale("log")
    for i, v in enumerate(vals):
        ax.text(v * 1.15, i, f"{v:,.0f}/s" if v >= 10 else f"{v:.1f}/s", va="center", fontsize=9, color=INK2)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Events per second (log scale)")
    finish(ax, f"AV fleets are a different load: {vals[-2] / vals[-1]:.0f}x the bus rate at real time")
    ax.set_xlim(right=max(vals) * 8)
    save(fig, "b28_av_vs_transit.png", "Argoverse 2 (20 scenarios) via ingestion/av_replay.py; 24 h 511 recording")


def fig_delay():
    path = os.path.join(RES, "delay_by_hour.csv")
    if not need(path):
        return skipped.append("BART delay by hour (delay_by_hour.csv)")
    df = pd.read_csv(path)
    fig, ax = plt.subplots(figsize=(8, 4.6))
    small = df.arrivals < 100
    ax.bar(df.hour[~small], df.late_share[~small] * 100, color=BLUE, width=0.7, edgecolor=SURFACE, linewidth=2,
           label="late share")
    ax.bar(df.hour[small], df.late_share[small] * 100, color=MUTED, width=0.7, edgecolor=SURFACE, linewidth=2,
           label="fewer than 100 arrivals (unreliable)")
    ax.set_xticks(range(0, 24, 2)); ax.set_xticklabels([f"{h:02d}" for h in range(0, 24, 2)])
    ax.grid(axis="x", visible=False)
    ax.set_xlabel("Hour of day (Pacific)"); ax.set_ylabel("Arrivals 5+ min late (%)")
    finish(ax, "BART: share of late arrivals by hour (one day, Nov 13-14)")
    prop = os.path.join(RES, "delay_propagation.json")
    if os.path.exists(prop):
        a = json.load(open(prop))["across_trips"]
        ax.text(0.99, 0.97, f"After a late train, the next is late {a['late_after_late_share']:.0%}\n"
                f"vs {a['baseline_late_share']:.0%} overall ({a['lift']}x lift)", transform=ax.transAxes,
                ha="right", va="top", fontsize=9, color=INK2, zorder=5,
                bbox=dict(facecolor=SURFACE, edgecolor="none", pad=3))
    save(fig, "b28_bart_delay_by_hour.png", "streaming/analyze_delay_propagation.py (B15), BART GTFS-rt")


def fig_fairness():
    path = os.path.join(RES, "fairness_by_route.csv")
    if not need(path):
        return skipped.append("fairness (fairness_by_route.csv)")
    df = pd.read_csv(path)
    df = df[df.close_pairs.fillna(0) >= 30].sort_values("no_history", ascending=False).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    ax.bar(df.index, df.no_history * 100, color=[AGENCY.get(s, AQUA) for s in df.source], width=0.8,
           edgecolor=SURFACE, linewidth=1)
    worst = ", ".join(f"{AGENCY_NAME[r.source].split()[0]} {r.route_id} {r.no_history:.0%}" for r in df.head(3).itertuples())
    best = ", ".join(f"{AGENCY_NAME[r.source].split()[0]} {r.route_id} {r.no_history:.0%}" for r in df.tail(2).itertuples())
    ax.text(0.99, 0.97, f"Least reliable: {worst}\nMost reliable: {best}", transform=ax.transAxes,
            ha="right", va="top", fontsize=8.5, color=INK2, zorder=5,
            bbox=dict(facecolor=SURFACE, edgecolor="none", pad=3))
    ax.set_xticks([]); ax.grid(axis="x", visible=False)
    ax.set_xlabel(f"{len(df)} routes with 30+ close pairs, sorted")
    ax.set_ylabel("Close pairs the detector\ncould not judge (%)")
    med = df.no_history.median() * 100
    ax.axhline(med, color=INK2, linewidth=1, linestyle=":")
    ax.text(len(df) - 1, med + 1, f"median {med:.0f}%", ha="right", fontsize=9, color=INK2)
    finish(ax, "Fairness: detection reliability varies 3.5x across routes",
           [plt.Rectangle((0, 0), 1, 1, color=c) for c in AGENCY.values()], list(AGENCY_NAME.values()))
    save(fig, "b28_fairness_by_route.png", "streaming/analyze_fairness.py (B27), 24 h replay")


def main():
    global RES, OUT
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results", default=RES)
    p.add_argument("--replay", default="data/replay_in")
    p.add_argument("--out", default=OUT)
    p.add_argument("--skip-sweeps", action="store_true")
    args = p.parse_args()
    RES, OUT = args.results, args.out
    os.makedirs(OUT, exist_ok=True)

    sweeps_path = os.path.join(RES, "b28_sweeps.json")
    sweeps = json.load(open(sweeps_path)) if os.path.exists(sweeps_path) else None
    if not args.skip_sweeps and glob.glob(os.path.join(args.replay, "*.json")):
        print("Sweeps on the 24 h replay:")
        ids, vehicles = load_replay(args.replay)
        random.seed(7)
        sweeps = {"bloom": bloom_sweep(ids), "fm": fm_sweep(vehicles)}
        with open(sweeps_path, "w") as f:
            json.dump(sweeps, f, indent=2)
    if sweeps:
        fig_bloom(sweeps["bloom"])
        fig_fm(sweeps["fm"])
    else:
        skipped.append(f"Bloom / FM sweeps (no {args.replay} and no {sweeps_path})")

    for fn in (fig_lsh, fig_bunching, fig_throughput, fig_faults, fig_windows, fig_av, fig_delay, fig_fairness):
        fn()
    print(f"\nMade {len(made)} figures in {OUT}:")
    for m in made:
        print(f"  {m}")
    for s in skipped:
        print(f"  skipped: {s}")


if __name__ == "__main__":
    main()
