"""Benchmark charts for docs/benchmark-cloud.md, regenerated from bench/results artifacts.

Run from the repo root with matplotlib available:
    python3 -m venv /tmp/viz && /tmp/viz/bin/pip install matplotlib && /tmp/viz/bin/python bench/charts.py
"""
import json, glob, os
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

def load(prefix):
    runs = {}
    for f in glob.glob(f"bench/results/{prefix}*.json"):
        a = json.load(open(f))
        runs[a["label"]] = a
    return runs

def finish(fig, ax, path):
    fig.tight_layout()
    fig.savefig(path, dpi=200, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)

# ---- 1. Tier ladder: steps/s vs connections, per DB tier (post-fix engine) ----
runs = load("runlr-")
ms = [8, 16, 32, 48, 64, 96]
tiers = [("cpu8", "8 vCPU", S1), ("cpu4", "4 vCPU", S2), ("cpu2", "2 vCPU", S3), ("cpu1", "1 vCPU", S4)]
fig, ax = plt.subplots(figsize=(8, 4.6))
for key, label, color in tiers:
    ys = [runs[f"lr-{key}-M{m}-r1"]["results"][0]["stepsPerSec"] for m in ms]
    ax.plot(ms, ys, color=color, linewidth=2, marker="o", markersize=5.5, label=label)
    ax.annotate(label, (ms[-1], ys[-1]), xytext=(8, 0), textcoords="offset points",
                color=INK, fontsize=10, va="center")
ax.annotate("over-connection collapse", (48, 427), xytext=(46, 1450),
            color=INK2, fontsize=9.5, ha="center",
            arrowprops=dict(arrowstyle="-", color=MUTED, lw=1))
ax.set_title("Steps throughput by database tier and connection pool size")
ax.set_xlabel("connection pool size (M)")
ax.set_ylabel("steps / second")
ax.set_xticks(ms)
ax.set_ylim(0, 5000)
ax.legend(loc="upper left", frameon=False, fontsize=9.5)
finish(fig, ax, f"{OUT}/benchmark-cloud-tiers.png")

# ---- 2. Latency: conn-held time vs RTT (the k measurement) ----
rtt = [0.28, 0.82, 1.34, 2.35, 5.34]
held = [8/1021.2*1000, 8/529.3*1000, 8/372.0*1000, 8/243.9*1000, 8/115.7*1000]
fig, ax = plt.subplots(figsize=(7, 4.2))
xs = [0, 5.6]
ax.plot(xs, [12.1*x + 4.4 for x in xs], color=MUTED, linewidth=1.5, linestyle="--", zorder=1)
ax.plot(rtt, held, color=S1, linewidth=0, marker="o", markersize=8, zorder=2,
        markeredgecolor=SURFACE, markeredgewidth=1.5, label="measured (M=8, connection-bound)")
ax.annotate("db = 12.1·RTT + 4.4 ms   (slope = k, the round-trips per step)",
            (2.6, 12.1*2.6 + 4.4), xytext=(0, -26), textcoords="offset points",
            color=INK2, fontsize=9.5, rotation=0)
ax.set_title("Per-step database time is linear in round-trip latency")
ax.set_xlabel("measured RTT to the database (ms)")
ax.set_ylabel("connection-held time per step (ms)")
ax.set_xlim(0, 5.7)
ax.set_ylim(0, 75)
finish(fig, ax, f"{OUT}/benchmark-cloud-latency.png")

# ---- 3. Workers x task delay (pre-fix engine, M=30) ----
w0 = [(32, 443.5), (48, 558.9), (64, 641.8), (96, 696.6), (128, 707.2), (512, 744.1)]
w100 = [(32, 182.9), (128, 591.1), (192, 677.7), (256, 672.3), (384, 638.9), (512, 712.2)]
fig, ax = plt.subplots(figsize=(8, 4.4))
for pts, label, color in [(w0, "no task delay", S1), (w100, "100 ms task delay", S2)]:
    xs_, ys_ = zip(*pts)
    ax.plot(xs_, ys_, color=color, linewidth=2, marker="o", markersize=5.5, label=label)
ax.annotate("worker-bound: throughput = N / T", (40, 260), color=INK2, fontsize=9.5)
ax.set_title("Task time moves the worker knee right (N ≈ M × T/db)")
ax.set_xlabel("workers (N)")
ax.set_ylabel("steps / second")
ax.set_xscale("log", base=2)
ax.set_xticks([32, 64, 128, 256, 512])
ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
ax.set_ylim(0, 850)
ax.legend(loc="lower right", frameon=False, fontsize=9.5)
finish(fig, ax, f"{OUT}/benchmark-cloud-workers.png")

# ---- 4. Scale-out: measured x-factor at 2 shards vs linear ----
labels = ["steps/s\n(linear workload)", "MB/s\n(1 MB payloads)", "MB/s\n(8 MB payloads)"]
factors = [6719.2/3720, 81.83/46.33, 109.44/60.0]
fig, ax = plt.subplots(figsize=(6.4, 4.2))
bars = ax.bar(labels, factors, width=0.52, color=S1, zorder=2)
for b, f in zip(bars, factors):
    ax.annotate(f"×{f:.2f}", (b.get_x() + b.get_width()/2, f), xytext=(0, 5),
                textcoords="offset points", ha="center", color=INK, fontsize=11, fontweight="bold")
ax.axhline(2.0, color=MUTED, linewidth=1.5, linestyle="--", zorder=1)
ax.annotate("linear scaling (×2)", (0.99, 2.0/2.3), xycoords="axes fraction", xytext=(0, 4),
            textcoords="offset points", color=INK2, fontsize=9.5, ha="right", va="bottom")
ax.axhline(1.0, color=GRID, linewidth=1, zorder=1)
ax.set_title("Two shards on two database instances vs one")
ax.set_ylabel("throughput vs a single shard")
ax.set_ylim(0, 2.3)
ax.grid(axis="x", visible=False)
finish(fig, ax, f"{OUT}/benchmark-cloud-scaleout.png")

# ---- 5. Fan-out: the engine-bound step ceiling on a 4 vCPU host ----
# Two stacked panels sharing an x-axis (never a dual y-axis): throughput on top,
# the engine CPU that bought it underneath, against the host's 4-core wall.
HOST_CORES = 4.0

def med(vals):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2

def fanout_series(width):
    """median steps/s and engine cores per concurrency across the interleaved rounds"""
    byconc = {}
    for r in (1, 2, 3):
        try:
            a = json.load(open(f"bench/results/r-fanout-w{width}-r{r}.json"))
        except FileNotFoundError:
            continue
        for res in a["results"]:
            byconc.setdefault(res["concurrency"], []).append(
                (res["stepsPerSec"], res["host"]["cpuCores"]))
    out = []
    for c in sorted(byconc):
        out.append((c, med([s for s, _ in byconc[c]]), med([k for _, k in byconc[c]])))
    return out

widths = [(4, S1), (16, S2), (64, S3)]
series = [(w, color, fanout_series(w)) for w, color in widths]
if any(pts for _, _, pts in series):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 6.4), sharex=True,
                                   gridspec_kw={"height_ratios": [1.45, 1]})
    peak = 0
    for w, color, pts in series:
        if not pts:
            continue
        xs = [p[0] for p in pts]
        ax1.plot(xs, [p[1] for p in pts], color=color, linewidth=2, marker="o",
                 markersize=6, markeredgecolor=SURFACE, markeredgewidth=1.5,
                 label=f"width {w}")
        ax2.plot(xs, [p[2] for p in pts], color=color, linewidth=2, marker="o",
                 markersize=6, markeredgecolor=SURFACE, markeredgewidth=1.5,
                 label=f"width {w}")
        peak = max(peak, max(p[1] for p in pts))
    ax1.set_title("Fan-out throughput ceiling on a 4 vCPU engine (3 × 8-vCPU shards)")
    ax1.set_ylabel("steps / second")
    ax1.legend(loc="upper left", frameon=False, fontsize=9.5)
    ax1.set_ylim(0, peak * 1.18)
    ax2.axhline(HOST_CORES, color=MUTED, linewidth=1.5, linestyle="--", zorder=1)
    ax2.annotate("all 4 vCPUs", (0.99, HOST_CORES), xycoords=("axes fraction", "data"),
                 xytext=(0, 5), textcoords="offset points", color=INK2, fontsize=9.5,
                 ha="right", va="bottom")
    ax2.set_ylabel("engine CPU (cores)")
    ax2.set_xlabel("closed-loop concurrency (flows in flight)")
    ax2.set_ylim(0, HOST_CORES * 1.15)
    ax2.set_xscale("log", base=2)
    allx = sorted({p[0] for _, _, pts in series for p in pts})
    ax2.set_xticks(allx)
    ax2.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    fig.tight_layout()
    fig.savefig(f"{OUT}/benchmark-cloud-fanout.png", dpi=200, facecolor=SURFACE,
                bbox_inches="tight")
    plt.close(fig)
    print("wrote", f"{OUT}/benchmark-cloud-fanout.png")

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

# ---- 8. Shard ladder: steps/s vs shard count, and the refiller headroom behind the taper ----
# Supersedes an n=1 campaign that reported "the second shard doubles, the third does nothing" and
# attributed it to cross-shard straggler waits. That campaign ran on a 4-vCPU engine host which was
# ITSELF the bottleneck: per-shard database CPU fell from ~82% at one shard to ~51% at three while
# the engine saturated, and the knee moved when the engine was resized - so the plateau was a
# property of the load generator, not of sharding. Re-measured on a 16-vCPU host, n=3 interleaved,
# a dedicated Cloud SQL instance and a fresh database per run, 256 fairness keys and a 5ms task
# delay (the single-key zero-delay corner exaggerates the refiller's cost).
LADDER = {}
for f in glob.glob("bench/results/r-shardladder6-*shard-r*.json"):
    a = json.load(open(f))
    n = int(os.path.basename(f).split("-")[2].replace("shard", ""))
    r = a["results"][0]
    c = r["engineCounters"]
    sel = c.get("dwarf_refill_candidates_selected", 0)
    LADDER.setdefault(n, []).append((
        r["stepsPerSec"],
        sel / r["windowSec"] / r["stepsPerSec"] if r["stepsPerSec"] else 0,
    ))

SHARDS = sorted(LADDER)
steps = [med([v[0] for v in LADDER[n]]) for n in SHARDS]
lo = [min(v[0] for v in LADDER[n]) for n in SHARDS]
hi = [max(v[0] for v in LADDER[n]) for n in SHARDS]
head = [med([v[1] for v in LADDER[n]]) for n in SHARDS]
ideal = [steps[0] * n for n in SHARDS]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(7.2, 6.4), sharex=True,
                               gridspec_kw={"height_ratios": [1.45, 1]})
x = [str(n) for n in SHARDS]
# Ideal linear is a reference, not a measurement - dashed and recessive so it never reads as data.
ax1.plot(x, ideal, color=MUTED, linewidth=2, linestyle=(0, (5, 4)), zorder=1)
ax1.annotate("ideal linear", (x[-1], ideal[-1]), xytext=(-6, -14),
             textcoords="offset points", ha="right", color=MUTED, fontsize=9.5)
ax1.errorbar(x, steps, yerr=[[s - l for s, l in zip(steps, lo)],
                             [h - s for s, h in zip(steps, hi)]],
             color=S1, linewidth=2, marker="o", markersize=8, capsize=4,
             markeredgecolor=SURFACE, markeredgewidth=1.5, zorder=3)
for xi, s_, n in zip(x, steps, SHARDS):
    if n == len(SHARDS):
        ax1.annotate(f"x{s_/steps[0]:.1f} vs 1 shard", (xi, s_), xytext=(-8, 12),
                     textcoords="offset points", ha="right",
                     color=INK, fontsize=11, fontweight="bold")
# Title states only what n=3 supports. The refiller headroom below explains the taper toward six
# shards; it does NOT explain the flat step at three (headroom is at its HIGHEST there), and that
# arm's wide replicate spread is left visible rather than smoothed away.
ax1.set_title("Shards keep paying past three; the refiller binds at the top")
ax1.set_ylabel("steps / second")
ax1.set_ylim(0, max(ideal) * 1.06)
ax1.grid(axis="x", visible=False)

# The mechanism panel. Headroom is how far the refiller's candidate supply runs ahead of what the
# workers consume; at 1.0 it is handing out exactly what is taken and IS the ceiling.
ax2.plot(x, head, color=S3, linewidth=2, marker="o", markersize=8,
         markeredgecolor=SURFACE, markeredgewidth=1.5, zorder=3)
ax2.axhline(1.0, color=MUTED, linewidth=1, linestyle=(0, (5, 4)))
ax2.annotate("refiller is the ceiling", (x[-1], 1.0), xytext=(-6, 6),
             textcoords="offset points", ha="right", color=INK2, fontsize=9.5)
ax2.set_ylabel("refiller headroom (supply / used)")
ax2.set_xlabel("shards (one 8-vCPU database instance each)")
ax2.set_ylim(0.9, max(head) * 1.12)
ax2.grid(axis="x", visible=False)
fig.tight_layout()
fig.savefig(f"{OUT}/benchmark-cloud-shardladder.png", dpi=200, facecolor=SURFACE, bbox_inches="tight")
plt.close(fig)
print("wrote", f"{OUT}/benchmark-cloud-shardladder.png")
