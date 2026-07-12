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
