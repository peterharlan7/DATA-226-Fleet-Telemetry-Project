"""
B25: the comparison table (Markdown) and the plot for the report, from a design_comparison.py sweep.

    python benchmarks/plot_design_comparison.py                       # newest b25_runs_*.csv
    python benchmarks/plot_design_comparison.py --runs benchmarks/results/b25_runs_20261114_2305.csv

Writes benchmarks/results/b25_design_comparison_<stamp>.png and prints the table.
Needs pandas and matplotlib (requirements.txt).
"""

import argparse
import glob
import os

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "benchmarks", "results")

# Default chart palette, categorical slots 1-2, plus recessive chrome (light surface).
COLORS = {"at-least-once": "#2a78d6", "exactly-once": "#eb6834"}
MARKERS = {"at-least-once": "o", "exactly-once": "s"}
INK, INK_2, GRID, SURFACE = "#1f1f1e", "#5f5e58", "#e4e3de", "#fcfcfb"


def table(df):
    """One Markdown row per (window, delivery)."""
    head = ("| Window | Delivery | Recall vs exact | Precision vs exact | Planted caught | "
            "Median / max alert delay (s) | Late window-rows dropped | Duplicate alerts after 1 crash | "
            "State rows (max) | Batch ms (mean) |")
    lines = [head, "|" + "|".join([" --- "] * (head.count("|") - 1)) + "|"]
    for _, r in df.sort_values(["window_s", "delivery"]).iterrows():
        lines.append(
            f"| {r.window_s // 60} min | {r.delivery} | {r.recall_exact:.1%} ({r.true_alerts}/{r.exact_alerts}) | "
            f"{r.precision_exact:.1%} | {r.planted_caught}/{r.planted_judged} | "
            f"{r.alert_delay_s_median:.0f} / {r.alert_delay_s_max:.0f} | {r.late_drop_pct:.1f}% | "
            f"**{r.duplicate_alert_rows}** of {r.alert_rows} rows | {r.state_rows_max} | {r.batch_ms_mean} |")
    return "\n".join(lines)


def plot(df, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK_2,
                         "xtick.color": INK_2, "ytick.color": INK_2, "text.color": INK,
                         "axes.titleweight": "bold", "axes.titlesize": 10.5})
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8), facecolor=SURFACE)
    windows = sorted(df.window_s.unique())
    xs = range(len(windows))
    labels = [f"{w // 60} min" for w in windows]
    deliveries = [d for d in COLORS if d in set(df.delivery)]

    def style(ax, title, ylabel):
        ax.set_facecolor(SURFACE)
        ax.set_title(title, loc="left", color=INK)
        ax.set_ylabel(ylabel)
        ax.set_xticks(list(xs), labels)
        ax.set_xlabel("Window size")
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)

    # (a) recall against the exact answer: the cost of closing windows with a 30 s watermark
    ax = axes[0]
    for i, d in enumerate(deliveries):
        # the lines coincide, so the first is drawn wide and hollow under the second
        sub = df[df.delivery == d].set_index("window_s").reindex(windows)
        ax.plot(xs, sub.recall_exact * 100, color=COLORS[d], marker=MARKERS[d],
                markersize=13 if i == 0 else 6, linewidth=6 if i == 0 else 2, alpha=0.45 if i == 0 else 1,
                label=d, markeredgecolor=SURFACE, markeredgewidth=1.5, zorder=2 + i)
    lo = df.recall_exact.min() * 100
    ax.set_ylim(max(0, lo - 3), 100.5)
    style(ax, "Recall vs exact answer", "% of exact bunching alerts")
    last = df[df.window_s == windows[-1]].recall_exact.mean() * 100
    ax.annotate("both deliveries\noverlap", (xs[-1], last), xytext=(-8, -30), textcoords="offset points",
                ha="right", color=INK_2, fontsize=9)

    # (b) how long after bunching starts the alert can be emitted (window end + watermark)
    ax = axes[1]
    sub = df[df.delivery == deliveries[0]].set_index("window_s").reindex(windows)
    ax.bar(xs, sub.alert_delay_s_median, width=0.5, color=INK_2, alpha=0.85, label="median")
    ax.scatter(xs, sub.alert_delay_s_max, marker="_", s=400, color=INK, linewidths=2, label="max", zorder=3)
    for x, m in zip(xs, sub.alert_delay_s_median):
        ax.annotate(f"{m:.0f} s", (x, m / 2), ha="center", va="center", color=SURFACE, fontsize=9,
                    fontweight="bold")
    style(ax, "Alert delay (planted events)", "seconds after bunching starts")
    ax.legend(frameon=False, loc="upper left")

    # (c) duplicate alert rows left by one crash between sink write and batch commit
    ax = axes[2]
    width = 0.36
    for i, d in enumerate(deliveries):
        sub = df[df.delivery == d].set_index("window_s").reindex(windows)
        pos = [x + (i - (len(deliveries) - 1) / 2) * (width + 0.04) for x in xs]
        ax.bar(pos, sub.duplicate_alert_rows, width=width, color=COLORS[d], label=d,
               edgecolor=SURFACE, linewidth=2)
        for x, v in zip(pos, sub.duplicate_alert_rows):
            ax.annotate(f"{v:.0f}", (x, v), xytext=(0, 3), textcoords="offset points", ha="center",
                        color=INK, fontsize=9)
    style(ax, "Duplicate alerts after one crash", "alert rows written twice")
    ax.legend(frameon=False, loc="upper left", title="delivery", title_fontsize=9)
    ax.set_ylim(0, max(1, df.duplicate_alert_rows.max()) * 1.6)

    handles, names = axes[0].get_legend_handles_labels()
    axes[0].legend(handles, names, frameon=False, loc="lower left", title="delivery", title_fontsize=9)
    fig.tight_layout()
    fig.savefig(path, dpi=200, facecolor=SURFACE)
    print(f"Plot saved to {path}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--runs", default=None, help="b25_runs_<stamp>.csv (default: the newest)")
    args = p.parse_args()
    runs = args.runs or max(glob.glob(os.path.join(RESULTS, "b25_runs_*.csv")))
    df = pd.read_csv(runs)
    stamp = os.path.basename(runs)[len("b25_runs_"):-len(".csv")]
    print(table(df))
    plot(df, os.path.join(os.path.dirname(runs), f"b25_design_comparison_{stamp}.png"))


if __name__ == "__main__":
    main()
