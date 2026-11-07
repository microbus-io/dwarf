"""Charts for docs/benchmark.md (the local four-dialect comparison). The cloud page's charts are drawn by
bench/charts_cloud.py.

Run from the repo root with matplotlib available:
    python3 -m venv tmp/viz && tmp/viz/bin/pip install matplotlib && tmp/viz/bin/python bench/charts.py
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Reference palette (dataviz skill), fixed slot order 1-4; text/surface tokens.
S1, S2, S3, S4 = "#2a78d6", "#1baf7a", "#eda100", "#008300"
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#8a8984"
SURFACE, GRID = "#fcfcfb", "#e8e7e3"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "text.color": INK, "axes.edgecolor": MUTED,
    "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold",
    "axes.titlelocation": "left",
})
OUT = "docs"

def finish(fig, ax, path):
    fig.tight_layout()
    fig.savefig(path, dpi=200, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)

# ---- 6. Local four-dialect comparison (docs/benchmark.md) ----
# Medians of three runs from fixtures/benchmark_test.go; those come from `go test`
# output rather than a JSON artifact, so they are transcribed here.
DIALECTS = ["SQLite", "PostgreSQL", "MariaDB", "SQL Server"]
STEPS_BY_SHARD = {              # shards -> steps/s per dialect, median of 3 runs
    1: [1381, 1152, 848, 441],
    2: [2016, 1226, 719, 489],
    3: [1992, 1109, 789, 473],
}
fig, ax = plt.subplots(figsize=(8, 4.4))
xs = range(len(DIALECTS))
w = 0.21
for i, (shards, color) in enumerate([(1, S1), (2, S2), (3, S3)]):
    # 2px surface gap between adjacent bars comes from the width/offset pair below.
    off = (i - 1) * (w + 0.015)
    bars = ax.bar([x + off for x in xs], STEPS_BY_SHARD[shards], width=w,
                  color=color, zorder=2, label=f"{shards} shard" + ("s" if shards > 1 else ""))
    if shards == 1:  # direct-label one series only; a number on every bar is noise
        for b, v in zip(bars, STEPS_BY_SHARD[shards]):
            ax.annotate(f"{v}", (b.get_x() + b.get_width() / 2, v), xytext=(0, 4),
                        textcoords="offset points", ha="center", color=INK, fontsize=9.5)
ax.set_title("Steps throughput by dialect and shard count (laptop, co-located)")
ax.set_ylabel("steps / second")
ax.set_xticks(list(xs))
ax.set_xticklabels(DIALECTS)
ax.set_ylim(0, 2300)
ax.legend(loc="upper right", frameon=False, fontsize=9.5)
ax.grid(axis="x", visible=False)
finish(fig, ax, f"{OUT}/benchmark-dialects.png")

# ---- 7. Local state-payload byte rate (docs/benchmark.md) ----
SIZES = [4, 64, 256, 1024]       # KB
PAYLOAD_MBPS = {                 # dialect -> payloadMB/s, median of 3 runs
    "SQLite":     [5.1, 44.0, 74.1, 83.0],
    "PostgreSQL": [3.9, 59.0, 128.0, 211.7],
    "MariaDB":    [2.5, 31.7, 71.5, 92.1],
    "SQL Server": [1.8, 22.5, 63.7, 100.5],
}
fig, ax = plt.subplots(figsize=(8, 4.4))
# SQL Server and SQLite cross between 256 KB and 1 MB, so they take the best-separated
# pair of hues (ΔE 22.9) rather than the two greens the natural ordering would give them.
for (name, color) in [("PostgreSQL", S1), ("SQL Server", S2), ("SQLite", S3), ("MariaDB", S4)]:
    ys = PAYLOAD_MBPS[name]
    ax.plot(SIZES, ys, color=color, linewidth=2, marker="o", markersize=6,
            markeredgecolor=SURFACE, markeredgewidth=1.5, label=name)
    ax.annotate(name, (SIZES[-1], ys[-1]), xytext=(8, 0), textcoords="offset points",
                color=INK, fontsize=9.5, va="center")
ax.set_title("Caller state bytes moved per second, by payload size")
ax.set_xlabel("payload carried through every step")
ax.set_ylabel("payload MB / second")
ax.set_xscale("log", base=2)
ax.set_xticks(SIZES)
ax.set_xticklabels(["4 KB", "64 KB", "256 KB", "1 MB"])
ax.set_xlim(3, 2400)
ax.set_ylim(0, 235)
ax.legend(loc="upper left", frameon=False, fontsize=9.5)
finish(fig, ax, f"{OUT}/benchmark-dialects-payload.png")
