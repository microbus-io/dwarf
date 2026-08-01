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
    """Artifacts by label. Searches nested campaign directories as well as the top level, because
    campaigns are filed under bench/results/<NN-name>/ once they are done; a flat-only glob silently
    returns nothing and every chart built from it dies on a KeyError far from the cause."""
    runs = {}
    for pat in (f"bench/results/{prefix}*.json", f"bench/results/*/{prefix}*.json"):
        for f in glob.glob(pat):
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

# ---- 2. Latency: conn-held time vs RTT, at TWO pool sizes (the k and s measurements) ----
# Two sweeps, and the pair is the point: the SLOPE (k, round trips per step) barely moves between them,
# while the INTERCEPT (s, time actually inside the database) nearly doubles - s absorbs queueing, so it
# grows with the pool. Carrying M=8 constants to an M=96 rig over-predicts throughput by 21-44%.
rtt8 = [0.28, 0.82, 1.34, 2.35, 5.34]
held8 = [8/1021.2*1000, 8/529.3*1000, 8/372.0*1000, 8/243.9*1000, 8/115.7*1000]
rtt96 = [0.829, 1.313, 1.806, 2.800, 4.814]                       # campaign 27, linear, M=96
held96 = [16.77, 22.19, 25.06, 35.63, 53.21]                      # every arm verified 96/96 in use
fig, ax = plt.subplots(figsize=(7, 4.2))
xs = [0, 5.6]
ax.plot(xs, [12.1*x + 4.4 for x in xs], color=MUTED, linewidth=1.5, linestyle="--", zorder=1)
ax.plot(xs, [9.11*x + 9.49 for x in xs], color=MUTED, linewidth=1.5, linestyle="--", zorder=1)
ax.plot(rtt8, held8, color=S1, linewidth=0, marker="o", markersize=8, zorder=3,
        markeredgecolor=SURFACE, markeredgewidth=1.5, label="M=8   ·  db = 12.1·RTT + 4.4")
ax.plot(rtt96, held96, color=S2, linewidth=0, marker="s", markersize=8, zorder=3,
        markeredgecolor=SURFACE, markeredgewidth=1.5, label="M=96 ·  db = 9.11·RTT + 9.49")
ax.annotate("slope k ≈ 9–12: round trips per step, a property of the code path",
            (3.9, 12.1*3.9 + 4.4), xytext=(-8, 10), textcoords="offset points",
            color=INK2, fontsize=9, ha="right")
ax.annotate("intercept s doubles with the pool:\nit absorbs queueing inside the database",
            (0.06, 9.49), xytext=(1.55, 6.5), textcoords="data", color=INK2, fontsize=9,
            arrowprops=dict(arrowstyle="->", color=MUTED, lw=1))
ax.set_title("Per-step database time is linear in round-trip latency")
ax.set_xlabel("measured RTT to the database (ms)")
ax.set_ylabel("connection-held time per step (ms)")
ax.set_xlim(0, 5.7)
ax.set_ylim(0, 75)
ax.legend(loc="upper left", frameon=False, fontsize=9.5)
finish(fig, ax, f"{OUT}/benchmark-cloud-latency.png")

# ---- 2b. The operator-facing form: what RTT does to the CEILING at the derived pool ----
# Same data as 2, expressed as throughput rather than occupancy, because that is the number an operator
# is sizing against. The GENUINE-PLACEMENT points are the validation: if netem were not a faithful
# stand-in for distance they would sit off the curve. They land within 2-6%.
fig, ax = plt.subplots(figsize=(7, 4.2))
xs = [x/100 for x in range(20, 520)]
ax.plot(xs, [96/(9.11*x + 9.49)*1000 for x in xs], color=MUTED, linewidth=1.5, linestyle="--", zorder=1,
        label="96 / (9.11·RTT + 9.49)")
ax.plot(rtt96, [96/h*1000 for h in held96], color=S1, linewidth=0, marker="o", markersize=8, zorder=3,
        markeredgecolor=SURFACE, markeredgewidth=1.5, label="injected latency (tc netem)")
ax.plot([0.339, 0.378, 0.818], [7866, 7887, 5568], color=S3, linewidth=0, marker="D", markersize=8,
        zorder=4, markeredgecolor=SURFACE, markeredgewidth=1.5,
        label="genuine placements, two different instances")
ax.annotate("two same-zone, same-tier instances —\n29% apart on placement alone",
            (0.60, 6600), xytext=(1.35, 4400), textcoords="data", color=INK2, fontsize=9,
            arrowprops=dict(arrowstyle="->", color=MUTED, lw=1))
ax.set_title("Round-trip time sets the ceiling (16 vCPU, derived pool M=96)")
ax.set_xlabel("measured RTT to the database (ms)")
ax.set_ylabel("steps / second")
ax.set_xlim(0, 5.2)
ax.set_ylim(0, 9000)
ax.legend(loc="lower left", frameon=False, fontsize=9.5)
finish(fig, ax, f"{OUT}/benchmark-cloud-rtt-ceiling.png")

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

# ---- 8. Shard ladder: steps/s vs shard count ----
# Six 8-vCPU shards, one Cloud SQL instance each, 1 TB PD-SSD (~30K IOPS), 16-vCPU engine host, n=3
# interleaved, fresh database per run, 256 fairness keys, 5 ms task delay.
#
# The disk size is not incidental: on the 100 GB default (~3,000 IOPS) the same ladder tapered at the
# top and the taper was read as a supply-side ceiling. With IOPS headroom the 5->6 step gains +29%.
# The whiskers are min/max of the three replicates and are LEFT VISIBLE - the 1-shard arm's spread is
# 22% of its mean, which is wider than several adjacent-arm differences, so the endpoint is the claim
# and the individual steps are not.
LADDER_DIR = "bench/results/11-shardladder-1tb-20260723"
LADDER = {}
# A chart whose artifacts are absent SKIPS with a message rather than dying - every campaign directory
# is gitignored, so on any machine but the one that ran the campaign its charts have no inputs, and one
# missing set must not stop the rest of the file rendering. The directory is pinned rather than globbed
# because later campaigns reuse these filenames on different hardware.
for f in glob.glob(f"{LADDER_DIR}/r-l*shard-r*.json"):
    a = json.load(open(f))
    n = int(os.path.basename(f).split("-")[1].replace("shard", "").lstrip("l"))
    LADDER.setdefault(n, []).append(a["results"][0]["stepsPerSec"])

SHARDS = sorted(LADDER)
if not SHARDS:
    print(f"skip docs/benchmark-cloud-shardladder.png (no artifacts in {LADDER_DIR})")
else:
    # Means, not medians, so the plotted points are the same statistic the doc's table reports.
    steps = [sum(LADDER[n]) / len(LADDER[n]) for n in SHARDS]
    lo = [min(LADDER[n]) for n in SHARDS]
    hi = [max(LADDER[n]) for n in SHARDS]
    ideal = [steps[0] * n for n in SHARDS]
    x = [str(n) for n in SHARDS]

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    # Ideal linear is a reference, not a measurement - dashed and recessive so it never reads as data.
    ax.plot(x, ideal, color=MUTED, linewidth=2, linestyle=(0, (5, 4)), zorder=1)
    ax.annotate("ideal linear", (x[-1], ideal[-1]), xytext=(-6, -14),
                textcoords="offset points", ha="right", color=MUTED, fontsize=9.5)
    ax.errorbar(x, steps, yerr=[[s - l for s, l in zip(steps, lo)],
                                [h - s for s, h in zip(steps, hi)]],
                color=S1, linewidth=2, marker="o", markersize=8, capsize=4,
                markeredgecolor=SURFACE, markeredgewidth=1.5, zorder=3)
    ax.annotate(f"x{steps[-1]/steps[0]:.2f} vs 1 shard", (x[-1], steps[-1]), xytext=(-8, 14),
                textcoords="offset points", ha="right", color=INK, fontsize=11, fontweight="bold")
    ax.set_title("Sharding keeps paying to six shards")
    ax.set_ylabel("steps / second")
    ax.set_xlabel("shards (one 8-vCPU database instance each)")
    ax.set_ylim(0, max(ideal) * 1.06)
    ax.grid(axis="x", visible=False)
    finish(fig, ax, f"{OUT}/benchmark-cloud-shardladder.png")

# ---- Campaign 14: vertical scaling 1-64 vCPU (2026-07-27) ----
# One session, one engine host, 1TB SSD and ~4GB RAM per vCPU on every tier, open-loop `linear`.
# Each tier runs the connection pool the engine derives for it, which is why the ratio changes at 32.
VERT_DIR = "bench/results/14-vertical-20260727"
VERT = [(1, "t1_", 6, S4), (2, "t2_", 6, "#6b8f3a"), (4, "t4_", 6, S3),
        (8, "s8_", 6, "#d4682a"), (16, "s16b_", 6, S1), (32, "s32_", 12, S2),
        (64, "s64_", 12, "#8e5bb5")]


def vert_series():
    """vCPUs -> (pool ratio, colour, sorted [(offered, achieved, p99ms, dbCpuPct)])."""
    import re as _re
    out = {}
    for v, pre, ratio, colour in VERT:
        pts = []
        for f in glob.glob(f"{VERT_DIR}/{pre}*.json"):
            d = json.load(open(f))
            r = (d.get("results") or d.get("steps"))[0]
            m = _re.search(r"_r(\d+)", d["label"])
            pts.append((int(m.group(1)) * 10, r["stepsPerSec"], r["p99Ms"],
                        (d.get("dbCpu") or {}).get("peakPct")))
        if pts:
            out[v] = (ratio, colour, sorted(pts))
    return out


# A rung is SERVED only if it both keeps up and stays responsive. Throughput alone puts the mark too
# late - the 16-vCPU instance still made its rate at 800 flows/s while p99 had gone 151ms -> 3,851ms,
# "keeping up" by running a 3,315-flow backlog. It is only locatable where rungs are fine: the upper
# tiers step 40%+ per rung, so a rung either clears this comfortably or misses badly, and the mark
# lands wherever the gap happens to fall. Hence Chart B plots the PEAK, which every ladder resolves.
def vert_knee(pts):
    best = None
    for offered, achieved, p99, _ in pts:
        if achieved >= 0.95 * offered and p99 < 1000:
            best = (offered, achieved)
    return best


def chart_vertical_curves():
    series = vert_series()
    fig, ax = plt.subplots(figsize=(10.2, 5.6))
    hi = max(o for _, _, pts in series.values() for o, _, _, _ in pts)
    ax.plot([0, hi], [0, hi], color=MUTED, lw=1, ls=(0, (4, 3)), zorder=1)
    ax.annotate("offered = achieved", (hi * 0.42, hi * 0.42), rotation=34, color=MUTED,
                fontsize=9, ha="center", va="bottom")
    for v in sorted(series):
        ratio, colour, pts = series[v]
        ax.plot([p[0] for p in pts], [p[1] for p in pts], color=colour, lw=2,
                marker="o", ms=4.5, label=f"{v} vCPU", zorder=3)
        # Label at each curve's LAST point, not its peak: the curves end at different offered loads
        # so the labels spread themselves, whereas the peaks cluster near the diagonal and collide.
        end = pts[-1]
        pk = max(pts, key=lambda p: p[1])
        ax.annotate(f"{v} vCPU · peak {pk[1]:,.0f}", (end[0], end[1]), xytext=(10, -3),
                    textcoords="offset points", color=colour, fontsize=9.5, fontweight="bold",
                    va="center")
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_title("Achieved throughput vs offered load, by database size")
    ax.set_xlim(right=hi * 3.4)
    ax.set_xlabel("offered load (steps / second, log scale)")
    ax.set_ylabel("achieved throughput (steps / second, log scale)")
    ax.legend(loc="upper left", frameon=False, fontsize=9, ncol=2)
    finish(fig, ax, f"{OUT}/benchmark-cloud-vertical.png")


def chart_vertical_scaling():
    series = vert_series()
    vs = sorted(series)
    peaks = [max(series[v][2], key=lambda p: p[1])[1] for v in vs]
    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.plot(vs, [peaks[0] * v / vs[0] for v in vs], color=MUTED, lw=1, ls=(0, (4, 3)),
            zorder=1, label="linear scaling")
    ax.plot(vs, peaks, color=S1, lw=2.5, marker="o", ms=7, zorder=3, label="measured peak")
    for v, p in zip(vs, peaks):
        ratio = series[v][0]
        ax.annotate(f"{p:,.0f}", (v, p), xytext=(0, 11), textcoords="offset points",
                    color=INK, fontsize=9.5, fontweight="bold", ha="center")
        ax.annotate(f"{ratio}x", (v, p), xytext=(0, -16), textcoords="offset points",
                    color=MUTED, fontsize=8, ha="center")
    ax.set_xscale("log", base=2); ax.set_yscale("log")
    ax.set_xticks(vs); ax.set_xticklabels([str(v) for v in vs])
    ax.set_title("Vertical scaling: peak throughput by database vCPU count")
    ax.set_xlabel("database vCPUs (log scale)")
    ax.set_ylabel("peak steps / second (log scale)")
    ax.annotate("grey = connections per vCPU the engine derives", (vs[0], peaks[-1]),
                color=MUTED, fontsize=8.5, va="top")
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    finish(fig, ax, f"{OUT}/benchmark-cloud-vertical-scaling.png")


chart_vertical_curves()
chart_vertical_scaling()
