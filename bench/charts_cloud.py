"""Charts for docs/benchmark-cloud.md.

Run from the repo root with matplotlib available:

    python3 -m venv /tmp/viz && /tmp/viz/bin/pip install matplotlib && /tmp/viz/bin/python bench/charts_cloud.py [out-dir]

PNGs go to out-dir (default docs/). Three charts draw from tables in this file, because their numbers come from
many runs and from CPU profiles rather than from one artifact: database size (tiers), shards, and engine host
size. Three read run artifacts under bench/results/, which are kept on the machine that ran them: replicas and
minute-by-minute (redo-one-20261009, redo-rs-20261009) and failover (redo-fo-20261011). A chart whose
artifacts are missing is skipped with a note.
"""
import collections
import glob
import json
import os
import re
import sys
from datetime import datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.transforms as mtransforms  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

# Reference palette, the same slots and tokens as bench/charts.py.
S1, S2, S3, S4, S5 = "#2a78d6", "#1baf7a", "#eda100", "#008300", "#8e5bb5"
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#8a8984"
SURFACE, GRID = "#fcfcfb", "#e8e7e3"
OUTAGE = "#d64545"
plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "text.color": INK, "axes.edgecolor": MUTED,
    "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold",
    "axes.titlelocation": "left",
})
OUT = sys.argv[1] if len(sys.argv) > 1 else "docs"
RESULTS = "bench/results"
kilo = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:.0f}k")


def finish(fig, name):
    fig.tight_layout()
    path = os.path.join(OUT, name)
    fig.savefig(path, dpi=200, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)


def skip(name, why):
    print(f"skip {name}: {why}")


# ---- Database size with its engine pairing: throughput and $ per million steps ----
# (database vCPU, steps/s, engines, monthly $ for the database, its 1 TB SSD and both engine hosts). One shard,
# saturated, 2 x engines at half the database's vCPU; 32 vCPU is the per-shard rate of 4 shards x 8 replicas.
TIERS = [(8, 4041, "2 x 2 vCPU", 702), (16, 7675, "2 x 4 vCPU", 1237), (32, 18720, "2 x 8 vCPU", 2306)]


def chart_tiers():
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.4))
    xs = range(len(TIERS))
    cols = [S5, S4, S2]
    axes[0].bar(xs, [t[1] for t in TIERS], width=0.6, color=cols)
    for x, (v, steps, eng, usd) in zip(xs, TIERS):
        axes[0].text(x, steps + 400, f"{steps:,}", ha="center", va="bottom", fontsize=9.5, color=INK)
    axes[0].set_ylim(0, 21000)
    axes[0].set_yticks(range(0, 20001, 5000))
    axes[0].yaxis.set_major_formatter(kilo)
    axes[0].set_ylabel("steps/s (16-min window mean)")
    axes[0].set_title("Throughput")
    per = [t[3] / (t[1] * 730 * 3600 / 1e6) for t in TIERS]  # at the measured rate, 730 h a month
    axes[1].bar(xs, per, width=0.6, color=cols)
    for x, v in zip(xs, per):
        axes[1].text(x, v + 0.0012, f"${v:.4f}", ha="center", va="bottom", fontsize=9.5, color=INK)
    axes[1].set_ylim(0, 0.08)
    axes[1].yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"${v:.2f}"))
    axes[1].set_ylabel("$ per million steps (lower is better)", labelpad=8)
    axes[1].set_title("Cost per million steps")
    for ax in axes:
        ax.set_xticks(list(xs))
        ax.set_xticklabels([f"{t[0]} vCPU DB\n{t[2]}" for t in TIERS], fontsize=9.5)
    finish(fig, "benchmark-cloud-tiers.png")


# ---- Shards: grouped bars by fleet shape, a dashed linear mark over each group ----
ONE_SHARD = 17938
SHARD_GROUPS = [  # (shards, [(steps/s, tick label, colour)])
    (1, [(17938, "1 replica\n16 vCPU", S1)]),
    (2, [(34115, "2 replicas\n16 vCPU", S1), (34049, "1 replica\n48 vCPU", S3)]),
    (4, [(69382, "4 replicas\n16 vCPU", S1), (58719, "1 replica\n48 vCPU", S3), (74879, "8 replicas\n8 vCPU", S2)]),
]


def chart_shards():
    fig, ax = plt.subplots(figsize=(9, 5.2))
    w = 0.27
    ticks, ticklabels = [], []
    for g, (n, bars) in enumerate(SHARD_GROUPS):
        xs = [g + (i - (len(bars) - 1) / 2) * w for i in range(len(bars))]
        lin = n * ONE_SHARD
        for x, (y, label, colour) in zip(xs, bars):
            ax.bar(x, y, width=w * 0.9, color=colour)
            ax.text(x, (max(y, lin) if abs(y - lin) < 3000 else y) + 1300, f"{y / 1000:.1f}k", ha="center",
                    va="bottom", fontsize=9.5, color=INK)
            ticks.append(x)
            ticklabels.append(label)
        ax.plot([xs[0] - w / 2, xs[-1] + w / 2], [lin, lin], color=INK2, lw=1.4, ls=(0, (4, 3)))
        ax.text(g, -0.15, f"{n} shard" + ("s" if n > 1 else ""), ha="center", va="top", fontsize=11.5, color=INK,
                transform=mtransforms.blended_transform_factory(ax.transData, ax.transAxes))
    ax.set_xticks(ticks)
    ax.set_xticklabels(ticklabels, fontsize=8.5)
    ax.tick_params(axis="x", length=0)
    ax.set_xlim(-0.6, len(SHARD_GROUPS) - 0.4)
    ax.set_ylim(0, 88000)
    ax.yaxis.set_major_formatter(kilo)
    ax.set_ylabel("steps/s (16-min window mean)")
    ax.set_title("Throughput by shard count and fleet shape")
    ax.legend(handles=[Patch(color=S1, label="one 16 vCPU replica per shard"),
                       Patch(color=S2, label="two 8 vCPU replicas per shard"),
                       Patch(color=S3, label="one 48 vCPU replica for all shards"),
                       Line2D([0], [0], color=INK2, lw=1.4, ls=(0, (4, 3)), label="linear: shards x the 1-shard run")],
              frameon=False, loc="upper left", fontsize=9.5)
    finish(fig, "benchmark-cloud-shards.png")


# ---- Engine host size: GC share of engine CPU, and steps per engine core ----
# One row per engine replica: (engine host vCPU, steps/s, engine cores used, GC % of engine CPU). GC share is
# the CPU profile's cumulative runtime.gcBgMarkWorker + gcAssistAlloc + bgsweep; steps and cores are the
# artifact's stepsPerSec and host.cpuCores.
ENGINE_HOSTS = [
    (16, 17938, 5.43, 11.3), (16, 8404, 2.66, 10.6), (16, 17133, 5.32, 12.1), (16, 16767, 5.27, 11.9),
    (16, 17502, 5.32, 11.3), (16, 32564, 10.3, 11.7),
    (8, 8455, 2.47, 14.6), (8, 9684, 2.97, 10.8), (8, 9770, 2.83, 11.4), (8, 9762, 2.86, 11.0),
    (8, 9491, 2.86, 11.0), (8, 9545, 2.87, 10.8), (8, 9837, 2.75, 11.4), (8, 8385, 2.33, 11.2),
    (48, 34049, 14.79, 20.2), (48, 58696, 20.05, 22.3),
    (2, 2010, 0.71, 10.7), (4, 3816, 1.27, 11.1), (2, 2049, 0.7, 9.7), (4, 3892, 1.32, 10.1),
]


def chart_engine_hosts():
    pos = {2: 0, 4: 1, 8: 2, 16: 3, 48: 4}
    colour = {2: S5, 4: S4, 8: S2, 16: S1, 48: S3}
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.4))
    for vcpu, steps, cores, gc in ENGINE_HOSTS:
        axes[0].scatter(pos[vcpu], gc, s=46, color=colour[vcpu], zorder=3)
        axes[1].scatter(pos[vcpu], steps / cores, s=46, color=colour[vcpu], zorder=3)
    for vcpu, steps, cores, gc in ENGINE_HOSTS:
        if vcpu == 48:
            axes[0].annotate(f"{steps / 1000:.0f}k steps/s", (pos[vcpu], gc), xytext=(8, -3),
                             textcoords="offset points", fontsize=9, color=INK2)
    for ax in axes:
        ax.set_xticks([0, 1, 2, 3, 4])
        ax.set_xticklabels(["2", "4", "8", "16", "48"])
        ax.set_xlabel("engine host vCPU")
        ax.set_xlim(-0.5, 4.9)
    axes[0].set_ylim(0, 25)
    axes[0].set_ylabel("% of engine CPU")
    axes[0].set_title("Garbage collection")
    axes[1].set_ylim(0, 4000)
    axes[1].yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    axes[1].set_ylabel("steps/s per engine core used")
    axes[1].set_title("Engine efficiency")
    finish(fig, "benchmark-cloud-engine-hosts.png")


# ---- Replicas and minute by minute: the R and S arms of 2026-10-09 ----
ONE = os.path.join(RESULTS, "redo-one-20261009/out")
RS = os.path.join(RESULTS, "redo-rs-20261009")
ARMS = {"r1": "1 shard, 1 replica", "r2": "1 shard, 2 replicas", "r4": "1 shard, 4 replicas",
        "s2": "2 shards, 2 replicas", "s4": "4 shards, 4 replicas"}
ARM_SHARDS = {"r1": 1, "r2": 1, "r4": 1, "s2": 2, "s4": 4}


def arm_files(tag, ext):
    if tag == "r1":
        return [os.path.join(ONE, f"new-k1.{ext}")]
    return sorted(glob.glob(os.path.join(RS, "*", "out", f"{tag}-e*.{ext}")))


def per_vm_steps(tag):
    return [json.load(open(p))["results"][0]["stepsPerSec"] for p in arm_files(tag, "json")]


def per_minute(tag):
    """Window steps/s per minute, summed over the arm's engine VMs (each VM's 10 s samples averaged first)."""
    total = [0.0] * 16
    for path in arm_files(tag, "log"):
        sums, ns = [0.0] * 16, [0] * 16
        for line in open(path):
            f = line.split()
            if len(f) < 2 or not f[0].isdigit() or not f[1].isdigit() or int(f[0]) >= 960:
                continue
            sums[int(f[0]) // 60] += int(f[1])
            ns[int(f[0]) // 60] += 1
        for m in range(16):
            total[m] += sums[m] / ns[m] if ns[m] else 0
    return total


def chart_replicas():
    if not all(arm_files(t, "json") and all(os.path.exists(p) for p in arm_files(t, "json")) for t in ("r1", "r2", "r4")):
        return skip("benchmark-cloud-replicas.png", f"artifacts not in {ONE} / {RS}")
    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    for i, t in enumerate(["r1", "r2", "r4"]):
        per_vm = per_vm_steps(t)
        working = [v for v in per_vm if v > 0]
        bottom = 0
        for j, v in enumerate(working):
            ax.bar(i, v, bottom=bottom, width=0.6, color=[S1, S2][j % 2], edgecolor=SURFACE, lw=1)
            bottom += v
        idle = len(per_vm) - len(working)
        ax.text(i, bottom + 400, f"{bottom:,.0f}" + (f"\n+{idle} standby" if idle else ""),
                ha="center", va="bottom", fontsize=10, color=INK)
    ax.set_xticks(range(3))
    ax.set_xticklabels(["1 replica", "2 replicas", "4 replicas"])
    ax.set_ylim(0, 24000)
    ax.set_ylabel("steps/s (16-min window mean)")
    ax.yaxis.set_major_formatter(kilo)
    ax.set_title("Replicas on one shard: each colour is one replica's share")
    finish(fig, "benchmark-cloud-replicas.png")


def chart_minutes():
    if not all(arm_files(t, "log") and all(os.path.exists(p) for p in arm_files(t, "log")) for t in ARMS):
        return skip("benchmark-cloud-minutes.png", f"logs not in {ONE} / {RS}")
    fig, ax = plt.subplots(figsize=(8.5, 4.6))
    for (t, name), colour in zip(ARMS.items(), [S1, S2, S3, S4, S5]):
        pm = [v / ARM_SHARDS[t] for v in per_minute(t)]
        ax.plot(range(1, 17), pm, color=colour, lw=2, marker="o", ms=3.5, label=name)
    ax.set_xlabel("minute of the measurement window")
    ax.set_ylabel("steps/s per shard")
    ax.set_ylim(14000, 21000)
    ax.set_xticks(range(1, 17))
    ax.yaxis.set_major_formatter(kilo)
    ax.set_title("Minute by minute, per shard")
    ax.legend(frameon=False, ncol=2, fontsize=9.5, loc="lower left")
    finish(fig, "benchmark-cloud-minutes.png")


# ---- Failover: 2 shards x 8 replicas, 1, 2, 3 then 4 dispatchers killed at once ----
FO = os.path.join(RESULTS, "redo-fo-20261011")
FO_WINDOW_START = datetime(2026, 10, 11, 6, 47, 10).timestamp()  # the replicas' window start (local time)
FO_LABELS = {1: "1 of 4 dispatchers", 2: "2: all of shard 1", 3: "3: shard 1 +\nhalf of shard 2",
             4: "4: every\ndispatcher"}


def fleet_series():
    """Fleet steps/s per 10 s from every replica process's stats lines. A process's stats t(s) counts from its
    window start: launch + 120 s for the originals (2 min warmup), launch for the restarts (no warmup)."""
    fleet = collections.defaultdict(float)
    for log in glob.glob(os.path.join(FO, "dwarf-fo-[0-9]*", "out", "fo-*.log")):
        tag = os.path.basename(log)[:-4]
        launched = float(open(log[:-4] + ".launched").read().split()[0])
        start = launched + (0 if "-r" in tag else 120)
        for line in open(log):
            m = re.match(r"^(\d+)\s+(\d+)\s", line)
            if m:
                fleet[int((start + int(m.group(1)) - FO_WINDOW_START) // 10) * 10] += int(m.group(2))
    return sorted(fleet.items())


def chart_failover():
    if not os.path.exists(os.path.join(FO, "kills.txt")):
        return skip("benchmark-cloud-failover.png", f"artifacts not in {FO}")
    kills = []
    for line in open(os.path.join(FO, "kills.txt")):
        m = re.match(r"\[(\d+):(\d+):(\d+)\]\s+KILLED (\d+)", line)
        if m:
            h, mi, s, k = map(int, m.groups())
            kills.append((datetime(2026, 10, 11, h, mi, s).timestamp() - FO_WINDOW_START, k))
    shard = {1: [], 2: []}
    for line in open(os.path.join(FO, "dwarf-fo-m", "out", "mon-fo.log")):
        p = line.split()
        try:
            shard[int(p[1])].append((float(p[0]) - FO_WINDOW_START, int(p[2])))
        except (ValueError, IndexError, KeyError):
            pass
    end = 20.5
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 6.6), sharex=True, gridspec_kw={"height_ratios": [3, 2]})
    for ax in (a1, a2):
        for t, _ in kills:
            ax.axvspan(t / 60, (t + 40) / 60, color=OUTAGE, alpha=0.13, lw=0)
            ax.axvline(t / 60, color=OUTAGE, lw=1.1)
        ax.axvline(18, color=INK2, lw=1, ls=":")
    fleet = [(t, v) for t, v in fleet_series() if t / 60 <= end]
    a1.plot([t / 60 for t, _ in fleet], [v for _, v in fleet], color=S1, lw=1.8, label="steps executed, all replicas")
    a1.axhline(28000, color=MUTED, lw=1.2, ls=(0, (4, 3)), label="offered: 28,000 steps/s")
    for t, k in kills:
        a1.text(t / 60 + 0.12, 36500, FO_LABELS[k], color=OUTAGE, fontsize=9, va="top")
    a1.text(18.1, 36500, "admission\nstops", color=INK2, fontsize=9, va="top")
    a1.set_ylim(0, 37000)
    a1.yaxis.set_major_formatter(kilo)
    a1.set_ylabel("steps/s (10 s)")
    a1.set_title("Failover: dispatchers killed 1, 2, 3, then 4 at a time, offered load held constant")
    a1.legend(frameon=False, loc="lower left", fontsize=9.5, bbox_to_anchor=(0.0, 0.0))
    for s, col in ((1, S2), (2, S3)):
        r = sorted(shard[s])
        pts = [(t1 / 60, (c1 - c0) / (t1 - t0)) for (t0, c0), (t1, c1) in zip(r, r[1:]) if 0 <= t1 / 60 <= end]
        a2.plot([p[0] for p in pts], [p[1] for p in pts], color=col, lw=1.4, label=f"shard {s}")
    a2.set_ylim(0, 60000)
    a2.yaxis.set_major_formatter(kilo)
    a2.set_ylabel("database commits/s (5 s)")
    a2.set_xlabel("minute of the measurement window   (shaded: the 40 s until a dead engine is detected)")
    a2.set_xlim(0, end)
    a2.legend(frameon=False, loc="lower left", fontsize=9.5, ncol=2)
    finish(fig, "benchmark-cloud-failover.png")


chart_tiers()
chart_shards()
chart_engine_hosts()
chart_replicas()
chart_minutes()
chart_failover()
