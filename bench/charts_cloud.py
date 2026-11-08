"""Charts for docs/benchmark-cloud.md and docs/benchmark-day.md.

Run from the repo root with matplotlib available:

    python3 -m venv tmp/viz && tmp/viz/bin/pip install matplotlib && tmp/viz/bin/python bench/charts_cloud.py [out-dir]

PNGs go to out-dir (default docs/). Three charts draw from tables in this file, because their numbers come from
many runs, from Cloud Monitoring and from CPU profiles rather than from one artifact: database size (tiers), shards,
and engine host size. The rest read run artifacts under bench/results/, which are kept on the machine that ran
them: replicas (redo-one-20261009, redo-rs-20261009), minute-by-minute and payload size (redo-ha-20261106), failover
(redo-fo-20261011), headroom and the growing archive (redo-va-20261107, with its monitors), and the day of varied load (redo-real-20261107, with its monitors and Cloud Monitoring's
database metrics in cloudmon.json). A chart whose artifacts are missing is skipped with a note.
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


# ---- Database size: throughput (a candle where a tier has several instances), $ per million steps, database CPU ----
# One shard per tier, saturated 0 B linear, 2 + 6 min, 20 GB max_wal_size + lz4, the archive on, 250 connections
# unless stated, engine 284b0f4 (T-A, 2026-11-06, redo-ta{4,8,16,64}-20261106: two engine hosts at half the database's
# vCPU, a 3 min warm-up) and, for 32 vCPU, four instances: redo-ha-20261106 (ha-1b) and redo-hb-20261106 (hb-0-250),
# one c4a-16 engine each, redo-sa-20261106 (s1-r2: two c4a-8, engine e5dbf59) and redo-va-20261107 (hr-sat: one
# c4a-16, engine bbc057a). Each run is (steps/s, WAL flush ms under load, database CPU busy %, database core-ms per
# step); CPU is Cloud Monitoring over the measurement window alone (dbcpu.sh -w, PAD_SECONDS=0), never the artifact
# span, whose warmup and drain edges dilute it.
# Monthly $ = on-demand us-east4, priced with the pairing rule (two engine hosts totalling half the database's vCPU):
# Cloud SQL ~$0.069/vCPU-h, SSD ~$0.23/h per TB, c4a ~$0.045/vCPU-h, 730 h.
TIERS = [  # (database vCPU, engines, monthly $, [runs], label of the run quoted for 64)
    (4, "2 x 1 vCPU", 435, [(1850, 9.32, 99, 2.14)]),
    (8, "2 x 2 vCPU", 702, [(4593, 3.68, 92, 1.60)]),
    (16, "2 x 4 vCPU", 1237, [(8219, 2.36, 83, 1.61)]),
    (32, "2 x 8 vCPU", 2305, [(18852, 1.03, 74, 1.26), (21938, 0.92, 77, 1.13), (24108, 0.88, 77, 1.03),
                              (24262, 0.82, 77, 1.03)]),
    (64, "2 x 16 vCPU", 4443, [(29340, 1.31, 60, 1.32)]),  # MaxConnections 500; at 250 the same instance ran:
]
TIER64_250 = (24593, 1.04, 42, 1.10)


def chart_tiers():
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.8))
    xs = list(range(len(TIERS)))
    cols = [S5, S4, S3, S2, S1]
    labels = [f"{t[0]} vCPU\n{t[1].replace(' vCPU', '').replace(' x ', ' × ')}" for t in TIERS]
    a0 = axes[0]
    for x, t, c in zip(xs, TIERS, cols):
        ys = [r[0] for r in t[3]]
        if len(ys) > 1:  # a candle: the instances measured, low to high
            a0.vlines(x, min(ys), max(ys), color=c, lw=6, alpha=0.35)
        left = len(ys) > 1  # a candle's labels go left, clear of the next tier's
        for r in t[3]:
            a0.plot(x, r[0], "o", color=c, ms=8, mec=SURFACE, mew=1.5, zorder=3)
            if r[0] not in (min(ys), max(ys)):
                continue  # a candle labels its ends; the instances between are its dots
            a0.annotate(f"{r[0]:,}\n{r[1]:.2f} ms flush", (x, r[0]), xytext=(-9 if left else 9, 0),
                        textcoords="offset points", ha="right" if left else "left", va="center", fontsize=8,
                        color=INK2)
    x64 = xs[-1]
    a0.plot(x64, TIER64_250[0], "o", mfc=SURFACE, mec=cols[-1], mew=1.8, ms=8, zorder=3)
    a0.annotate(f"{TIER64_250[0]:,} at 250\n{TIER64_250[1]:.2f} ms flush", (x64, TIER64_250[0]), xytext=(9, -4),
                textcoords="offset points", va="top", fontsize=8, color=INK2)
    a0.set_ylim(0, 34000)
    a0.yaxis.set_major_formatter(kilo)
    a0.set_ylabel("steps/s (6-min window mean)")
    a0.set_title("Throughput")
    a0.legend(handles=[Line2D([0], [0], marker="o", ls="", color=MUTED, ms=7, label="an instance"),
                       Line2D([0], [0], color=MUTED, lw=6, alpha=0.35, label="range across instances"),
                       Line2D([0], [0], marker="o", ls="", mfc=SURFACE, mec=MUTED, ms=7, label="64 vCPU at 250 connections")],
              frameon=False, fontsize=8.5, loc="upper left")
    a1 = axes[1]
    for x, t, c in zip(xs, TIERS, cols):
        per = [t[2] / (r[0] * 730 * 3600 / 1e6) for r in t[3]]  # at the measured rate, 730 h a month
        a1.bar(x, max(per), width=0.6, color=c, alpha=0.35 if len(per) > 1 else 1.0)
        if len(per) > 1:
            a1.bar(x, min(per), width=0.6, color=c)
        txt = f"${min(per):.3f}" if len(per) == 1 else f"${min(per):.3f}-{max(per):.3f}"
        a1.text(x, max(per) + 0.0015, txt, ha="center", va="bottom", fontsize=9, color=INK)
    a1.set_ylim(0, 0.105)
    a1.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"${v:.2f}"))
    a1.set_ylabel("$ per million steps (lower is better)", labelpad=8)
    a1.set_title("Cost per million steps")
    a2 = axes[2]
    a2.axvspan(-0.5, 2.5, color=OUTAGE, alpha=0.07, lw=0)
    for x, t, c in zip(xs, TIERS, cols):
        cpu = [r[2] for r in t[3]]
        a2.bar(x, max(cpu), width=0.6, color=c)
        txt = f"{max(cpu)}%" if len(cpu) == 1 else f"{min(cpu)}-{max(cpu)}%"
        a2.text(x, max(cpu) + 2, txt, ha="center", va="bottom", fontsize=9, color=INK)
    a2.set_ylim(0, 118)
    a2.set_ylabel("database CPU busy (%)")
    a2.text(1, 110, "CPU-bound", ha="center", fontsize=9.5, color=INK2)
    a2.text(3.5, 110, "log-flush-bound", ha="center", fontsize=9.5, color=INK2)
    a2.set_title("What limits each size")
    for ax in axes:
        ax.set_axisbelow(True)
        ax.set_xticks(xs)
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_xlim(-0.6, len(TIERS) - 0.4)
    axes[0].set_xlim(-0.6, len(TIERS) + 0.3)  # room for the 64 vCPU labels
    fig.text(0.5, -0.01, "Each label: database size, then its two engine hosts' vCPU.", ha="center", fontsize=9,
             color=INK2)
    finish(fig, "benchmark-cloud-tiers.png")


# ---- Shards: grouped bars by fleet shape, a dashed linear mark over each group ----
# S-A: one session on one set of four 32 vCPU instances, engine e5dbf59, archive on, 20 GB + lz4, 2 + 6 min,
# 2026-11-06 (redo-sa-20261106): the 4-shard arms first, then the two slowest instances removed for 2 shards, then
# the next for 1. Each bar is the sum of its replicas' artifacts.
ONE_SHARD = 24108
SHARD_GROUPS = [  # (shards, [(steps/s, tick label, colour)])
    (1, [(24108, "2 replicas\n8 vCPU", S2)]),
    (2, [(47437, "4 replicas\n8 vCPU", S2)]),
    (4, [(91800, "8 replicas\n8 vCPU", S2), (84648, "1 replica\n48 vCPU", S3)]),
]


def chart_shards():
    fig, ax = plt.subplots(figsize=(9, 5.2))
    ax.set_axisbelow(True)
    w = 0.27
    ticks, ticklabels = [], []
    for g, (n, bars) in enumerate(SHARD_GROUPS):
        xs = [g + (i - (len(bars) - 1) / 2) * w for i in range(len(bars))]
        lin = n * ONE_SHARD
        for x, (y, label, colour) in zip(xs, bars):
            ax.bar(x, y, width=w * 0.9, color=colour)
            ax.text(x, (max(y, lin) if abs(y - lin) < 5000 else y) + 1300, f"{y / 1000:.1f}k", ha="center",
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
    ax.set_ylim(0, 106000)
    ax.yaxis.set_major_formatter(kilo)
    ax.set_ylabel("steps/s (6-min window mean)")
    ax.set_title("Throughput by shard count and fleet shape")
    ax.legend(handles=[Patch(color=S2, label="two 8 vCPU replicas per shard"),
                       Patch(color=S3, label="one 48 vCPU replica for all shards"),
                       Line2D([0], [0], color=INK2, lw=1.4, ls=(0, (4, 3)), label="linear: shards x the 1-shard run")],
              frameon=False, loc="upper left", fontsize=9.5)
    finish(fig, "benchmark-cloud-shards.png")


# ---- Engine host size: GC share of engine CPU, and steps per engine core ----
# One row per engine replica: (engine host vCPU, steps/s, engine cores used, GC % of engine CPU), from the artifact's
# stepsPerSec, host.cpuCores and host.gcCPUPct (the Go runtime's own GC CPU accounting), all 2026-11-06 on the
# archive build: 1, 2 and 4 vCPU from T-A (pairs on 4, 8, 16 vCPU databases), 16 vCPU from T-A's 64 vCPU arms and the
# HA rig's 0 B runs, 8 vCPU from S-A's 8-, 4- and 2-replica arms, 48 vCPU from S-A's one replica on four shards.
ENGINE_HOSTS = [
    (1, 925, 0.24, 7.8), (1, 925, 0.24, 7.7), (2, 2295, 0.66, 9.7), (2, 2298, 0.60, 10.3),
    (4, 4106, 1.24, 10.2), (4, 4114, 1.18, 9.9),
    (8, 11101, 2.77, 9.5), (8, 10915, 2.75, 9.3), (8, 11092, 2.73, 9.9), (8, 11927, 2.85, 9.6),
    (8, 12036, 2.93, 9.4), (8, 11868, 2.93, 9.5), (8, 11939, 3.13, 10.0), (8, 10922, 2.68, 9.3),
    (8, 11835, 2.89, 9.4), (8, 11867, 2.98, 9.6), (8, 11822, 2.82, 9.4), (8, 11913, 2.88, 10.1),
    (8, 12023, 2.94, 9.8), (8, 12085, 2.99, 9.3),
    (16, 14681, 3.77, 8.2), (16, 12300, 3.13, 8.3), (16, 14659, 3.86, 8.1), (16, 12293, 3.22, 8.0),
    (16, 23938, 5.70, 8.1), (16, 24262, 5.79, 8.0),
    (48, 84648, 22.78, 8.6),
]


def chart_engine_hosts():
    sizes = [1, 2, 4, 8, 16, 48]
    pos = {v: i for i, v in enumerate(sizes)}
    colour = {1: INK2, 2: S5, 4: S4, 8: S2, 16: S1, 48: S3}
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.4))
    for vcpu, steps, cores, gc in ENGINE_HOSTS:
        axes[0].scatter(pos[vcpu], gc, s=46, color=colour[vcpu], zorder=3)
        axes[1].scatter(pos[vcpu], steps / cores, s=46, color=colour[vcpu], zorder=3)
    for vcpu, steps, cores, gc in ENGINE_HOSTS:
        if vcpu == 48:
            axes[0].annotate(f"{steps / 1000:.0f}k steps/s", (pos[vcpu], gc), xytext=(8, -3),
                             textcoords="offset points", fontsize=9, color=INK2)
    for ax in axes:
        ax.set_axisbelow(True)
        ax.set_xticks(range(len(sizes)))
        ax.set_xticklabels([str(v) for v in sizes])
        ax.set_xlabel("engine host vCPU")
        ax.set_xlim(-0.5, len(sizes) - 0.1)
    axes[0].set_ylim(0, 15)
    axes[0].set_ylabel("% of engine CPU")
    axes[0].set_title("Garbage collection")
    axes[1].set_ylim(0, 5000)
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


# ---- Minute by minute: three saturated 0 B runs on one 32 vCPU instance, archive on (redo-ha-20261106) ----
HA = os.path.join(RESULTS, "redo-ha-20261106/out")


def series(path):
    """An artifact's 10 s window series as (t seconds, steps/s, flows outstanding)."""
    ser = json.load(open(path))["results"][0]["gauges"]["series"]
    return [(p["t"], p["stepsPerSec"], p.get("outstanding", 0)) for p in ser]


def chart_minutes():
    runs = [(os.path.join(HA, f"{tag}.json"), label) for tag, label in
            (("ha-1", "16-minute run (first on a new server)"), ("ha-1b", "6-minute run"), ("ha-1c", "6-minute run"))]
    if not all(os.path.exists(p) for p, _ in runs):
        return skip("benchmark-cloud-minutes.png", f"artifacts not in {HA}")
    fig, ax = plt.subplots(figsize=(8.5, 4.6))
    for (path, label), colour in zip(runs, [S1, S2, S3]):
        mins = collections.defaultdict(list)
        for t, steps, _ in series(path):
            mins[int((t - 1) // 60)].append(steps)  # a sample at t belongs to the minute it ends in
        pm = [sum(mins[m]) / len(mins[m]) for m in range(16) if len(mins[m]) == 6]
        ax.plot(range(1, len(pm) + 1), pm, color=colour, lw=2, marker="o", ms=3.5, label=label)
    ax.set_xlabel("minute of the measurement window")
    ax.set_ylabel("steps/s")
    ax.set_ylim(14000, 26000)
    ax.set_xticks(range(1, 16))
    ax.yaxis.set_major_formatter(kilo)
    ax.set_title("Minute by minute: three saturated runs on one 32 vCPU database")
    ax.legend(frameon=False, ncol=1, fontsize=9.5, loc="lower right")
    finish(fig, "benchmark-cloud-minutes.png")


# ---- Payload size: steps/s, payload MB/s and the log written per step against a compressible JSON payload ----
# (redo-ha-20261106). Payloads sit on a doubling scale, so every size from 1 KB to 64 KB has its slot; a size not yet
# measured (None) has no marker, and the line crosses its slot.
PAYLOAD = [  # (label, KB, linear artifact, fan-out artifact)
    ("none", 0, "ha-1b", "wa-f0"), ("1 KB", 1, "pl-1k", None), ("2 KB", 2, "pl-2k", None), ("4 KB", 4, "pl-4k", "wa-f4k"),
    ("8 KB", 8, "ha-2", None), ("16 KB", 16, "pl-16k", None), ("32 KB", 32, None, None), ("64 KB", 64, "pl-64k", "wa-f64k")]


def chart_payload():
    need = [t for _, _, a, b in PAYLOAD for t in (a, b) if t]
    if not all(os.path.exists(os.path.join(HA, f"{t}.json")) for t in need):
        return skip("benchmark-cloud-payload.png", f"artifacts not in {HA}")

    def read(tag, kb):
        r = json.load(open(os.path.join(HA, f"{tag}.json")))["results"][0]
        win = len(r["gauges"]["series"]) * 10
        steps = r["stepsPerSec"]  # MB/s as the tasks wrote it: a document runs a little under its nominal size
        return steps, r["mbPerSec"], r["db"]["walBytes"] / (steps * win) / 1024
    xs = list(range(len(PAYLOAD)))
    lin = [(x, read(a, kb)) for x, (_, kb, a, _) in zip(xs, PAYLOAD) if a]
    fan = [(x, read(b, kb)) for x, (_, kb, _, b) in zip(xs, PAYLOAD) if b]
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.4))
    panels = [(0, "steps/s", "Throughput", kilo, 27000, lambda v: f"{v:,.0f}"),
              (1, "payload MB/s (steps/s x payload)", "Payload moved", None, 200, lambda v: f"{v:.0f}"),  # noqa
              (2, "write-ahead log per step (KB)", "Log written per step", None, 120, lambda v: f"{v:.1f}")]
    for k, ylabel, title, fmt, top, text in panels:
        ax = axes[k]
        ax.set_axisbelow(True)
        pts = [(x, v[k]) for x, v in lin if not (k == 1 and PAYLOAD[x][1] == 0)]
        # One line through every measured size; it crosses an unmeasured slot without a marker there.
        ax.plot([q[0] for q in pts], [q[1] for q in pts], color=S1, lw=2.2, marker="o", ms=6, label="linear")
        if k < 2:
            fp = [(x, v[k]) for x, v in fan if not (k == 1 and PAYLOAD[x][1] == 0)]
            ax.plot([q[0] for q in fp], [q[1] for q in fp], color=S2, lw=0, marker="D", ms=6, label="fan-out, 8 branches")
        for x, y in pts:
            first = x == 0
            ax.annotate(text(y), (x, y), xytext=(6 if first else -6, 7), textcoords="offset points",
                        ha="left" if first else "right", fontsize=8.5, color=INK2)
        ax.set_ylim(0, top)
        if fmt:
            ax.yaxis.set_major_formatter(fmt)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.set_xticks(xs)
        ax.set_xticklabels([p[0] for p in PAYLOAD])
        ax.set_xlim(-0.5, len(PAYLOAD) - 0.5)
        ax.set_xlabel("JSON payload per step (compresses ~3.4x)")
    axes[0].legend(frameon=False, fontsize=9, loc="upper right")
    finish(fig, "benchmark-cloud-payload.png")


# ---- Headroom: offered 100% and 95% of the instance's saturated rate (va, 2026-11-08, archive on) ----
# hr-sat measured 21,921 steps/s, so the arms offered 2,192 and 2,082 flows/s of 10-step linear 0 B, 16 min each.
VA = os.path.join(RESULTS, "redo-va-20261107")


def chart_headroom():
    runs = [("hr-100", "100%: 21,920 steps/s offered", OUTAGE), ("hr-95", "95%: 20,820 steps/s offered", S2)]
    out = os.path.join(VA, "out")
    if not all(os.path.exists(os.path.join(out, f"{t}.json")) for t, _, _ in runs):
        return skip("benchmark-cloud-headroom.png", f"artifacts not in {out}")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4.4))
    for tag, label, colour in runs:
        ser = series(os.path.join(out, f"{tag}.json"))
        a1.plot([t / 60 for t, _, _ in ser], [s for _, s, _ in ser], color=colour, lw=1.6, label=label)
        a2.plot([t / 60 for t, _, _ in ser], [max(o, 1) for _, _, o in ser], color=colour, lw=1.8, label=label)
    a1.set_ylim(16000, 25000)
    a1.yaxis.set_major_formatter(kilo)
    a1.set_ylabel("steps/s delivered (10 s)")
    a1.set_title("Delivered")
    a2.set_yscale("log")
    a2.set_ylim(50, 40000)
    a2.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    a2.set_ylabel("flows waiting (log scale)")
    a2.set_title("Backlog")
    for ax in (a1, a2):
        ax.set_xlabel("minute of the measurement window")
        ax.set_xlim(0, 16)
        ax.legend(frameon=False, fontsize=9.5, loc="lower right" if ax is a1 else "upper left")
    finish(fig, "benchmark-cloud-headroom.png")


# ---- Archive volume: one 32 vCPU database saturated with linear 0 B until its archive held 100M step rows, all in
# one archive day (va-big, 2026-11-08). The fill paused at 0, 25M, 50M and 100M step rows for a 6-min saturated probe
# and a timing of the archive's reads: History and Step of the fill's first flow, ListClosed's newest 100.


def va_big():
    path = os.path.join(VA, "out", "va-big.json")
    return json.load(open(path))["volume"]["checkpoints"] if os.path.exists(path) else None


def chart_volume():
    cps = va_big()
    if not cps:
        return skip("benchmark-cloud-volume.png", f"va-big.json not in {VA}/out")
    xs = list(range(len(cps)))
    labels = ["empty" if c["stepRows"] == 0 else
              f"{c['stepRows'] / 1e6:.0f}M rows\n{(c['archiveStepsDiskMB'] + c['archiveFlowsDiskMB']) / 1024:.0f} GB"
              for c in cps]
    sat = [c["saturated"]["stepsPerSec"] for c in cps]
    fig, (a1, a2, a3) = plt.subplots(1, 3, figsize=(15, 4.6))
    a1.plot(xs, sat, color=S1, lw=2, marker="o", ms=8, markeredgecolor=SURFACE, markeredgewidth=2)
    a1.axhline(sat[0], color=INK2, lw=1.2, ls=(0, (4, 3)))
    a1.text(xs[-1], sat[0] - 900, f"the empty database: {sat[0]:,.0f}", ha="right", va="top", fontsize=9.5,
            color=INK2)
    a1.set_ylim(0, 26000)
    a1.yaxis.set_major_formatter(kilo)
    a1.set_ylabel("steps/s, saturated (6-min probe)")
    a1.set_title("Throughput")
    for key, colour, name in (("p99Ms", S1, "p99"), ("p50Ms", S2, "p50")):
        vals = [c["saturated"][key] / 1000 for c in cps]
        a2.plot(xs, vals, color=colour, lw=2, marker="o", ms=8, markeredgecolor=SURFACE, markeredgewidth=2)
        a2.text(xs[-1] + 0.15, vals[-1], name, va="center", fontsize=9.5, color=INK)
    a2.set_ylim(0, 2)
    a2.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(0.5))
    a2.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.1f} s"))
    a2.set_ylabel("flow latency, saturated")
    a2.set_title("Latency")
    rx = xs[1:]  # the empty database has nothing archived to read
    ends = {}
    for key, colour, name in (("archivedHistoryMs", S1, "History"), ("archivedStepMs", S2, "Step"),
                              ("listClosedMs", S3, "ListClosed")):
        vals = [c[key] for c in cps[1:]]
        a3.plot(rx, vals, color=colour, lw=2, marker="o", ms=8, markeredgecolor=SURFACE, markeredgewidth=2)
        ends[name] = vals[-1]
    upper, lower = sorted(("History", "Step"), key=ends.get, reverse=True)
    if ends[upper] - ends[lower] < 1.2:  # two labels this close would print over each other
        mid = (ends[upper] + ends[lower]) / 2
        ends[upper], ends[lower] = mid + 0.6, mid - 0.6
    for name, y in ends.items():
        a3.text(rx[-1] + 0.15, y, name, va="center", fontsize=9.5, color=INK)
    a3.set_ylim(0, 25)
    a3.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f} ms"))
    a3.set_ylabel("one call, under saturated load")
    a3.set_title("Reading the archive")
    for ax in (a1, a2, a3):
        ax.set_xticks(xs)
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_xlim(-0.4, xs[-1] + (0.4 if ax is a1 else 0.9))
        ax.set_xlabel("step rows archived, and the archive's size")
    fig.suptitle("One 32 vCPU database as its archive grows", x=0.01, ha="left", fontsize=13, fontweight="bold")
    finish(fig, "benchmark-cloud-volume.png")


# ---- The same 85-minute run, minute by minute: steps/s and the database's size against the step rows stored. The
# run's only steps/s record between probes is the database's commit counter (s-mon), so a minute's steps/s is its
# commits over the run's commits per step, calibrated on the row counts the checkpoints took; sizes are v-mon's.


def va_big_minutes():
    """(minute end epoch, steps/s, step rows stored) per minute of va-big, from the commit counter."""
    cps = va_big()
    art = json.load(open(os.path.join(VA, "out", "va-big.json")))
    t0 = datetime.fromisoformat(art["startedAt"][:19] + "+00:00").timestamp()
    ticks = []
    for line in open(os.path.join(VA, "out", "mon-va-big.log")):
        f = line.split()
        if len(f) >= 4 and f[2].isdigit() and float(f[0]) >= t0:
            ticks.append((float(f[0]), int(f[2])))
    last = cps[-1]  # checkpoint tSec counts from the run's start
    at_last = min(ticks, key=lambda tc: abs(tc[0] - (t0 + last["tSec"])))
    per_step = (at_last[1] - ticks[0][1]) / last["stepRows"]
    by_min = collections.defaultdict(list)
    for t, c in ticks:
        by_min[int((t - t0) // 60)].append((t, c))
    mins, prev = [], ticks[0]
    for m in sorted(by_min):
        t, c = by_min[m][-1]
        if t - prev[0] > 30:
            mins.append((t, (c - prev[1]) / (t - prev[0]) / per_step, (c - ticks[0][1]) / per_step))
        prev = (t, c)
    full = sorted(p[1] for p in mins)[len(mins) // 2]
    return [p for p in mins if p[1] > full / 2]  # drop the drain's partial minute after the last probe


def chart_volume_rows():
    if not va_big() or not os.path.exists(os.path.join(VA, "out", "vmon-va-big.log")):
        return skip("benchmark-cloud-volume-rows.png", f"va-big artifacts not in {VA}/out")
    mins = va_big_minutes()
    first = sum(s for _, s, _ in mins[:10]) / 10
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 4.4))
    a1.plot([r / 1e6 for _, _, r in mins], [s for _, s, _ in mins], color=S1, lw=1.6)
    a1.axhline(first, color=INK2, lw=1.2, ls=(0, (4, 3)))
    a1.text(mins[-1][2] / 1e6, first + 700, f"the first 10 minutes: {first:,.0f}", ha="right", va="bottom",
            fontsize=9.5, color=INK2)
    a1.set_ylim(0, 26000)
    a1.yaxis.set_major_formatter(kilo)
    a1.set_ylabel("steps/s (1-min mean)")
    a1.set_title("Throughput")
    # v-mon, once a minute: database size, dwarf_steps (table + indexes), dwarf_flows; each placed at the rows
    # stored when it was read.
    size = []
    for line in open(os.path.join(VA, "out", "vmon-va-big.log")):
        f = line.split()
        if len(f) >= 5 and f[1].isdigit():
            size.append((float(f[0]), int(f[1]) / 2**30, (int(f[2]) + int(f[4])) / 2**30))
    ts, rows = [t for t, _, _ in mins], [r for _, _, r in mins]

    def rows_at(t):
        if t <= ts[0]:
            return rows[0] * max(t - (ts[0] - 60), 0) / 60
        for i in range(1, len(ts)):
            if t <= ts[i]:
                return rows[i - 1] + (rows[i] - rows[i - 1]) * (t - ts[i - 1]) / (ts[i] - ts[i - 1])
        return None

    pts = [(rows_at(t), whole, live) for t, whole, live in size if rows_at(t) is not None]
    a2.plot([r / 1e6 for r, _, _ in pts], [w for _, w, _ in pts], color=S1, lw=2, label="the whole database")
    a2.plot([r / 1e6 for r, _, _ in pts], [l for _, _, l in pts], color=S2, lw=2, label="the live tables")
    a2.set_ylabel("GB")
    a2.set_ylim(0, None)
    a2.set_title("Storage")
    a2.legend(frameon=False, fontsize=9.5, loc="upper left")
    for ax in (a1, a2):
        ax.set_xlim(0, mins[-1][2] / 1e6 * 1.02)
        ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f}M"))
        ax.set_xlabel("step rows stored, nothing deleted")
    fig.suptitle("One 32 vCPU database, saturated for 85 minutes", x=0.01, ha="left", fontsize=13,
                 fontweight="bold")
    finish(fig, "benchmark-cloud-volume-rows.png")


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


# ---- A day: REAL, three 90-minute day/night cycles over five priority bands (redo-real-20261107) ----
# Two engine replicas each generate half the commanded rate. A replica's series t counts from the day's shared epoch,
# so the restarted process (day2-*) lines up with the first (day1-*). Pending and oldest-pending come from the backlog
# leader's gauges, refreshed every 30 s, so a minute's reading is the max over the processes. The admin host's
# monitors (s-mon: WAL every 5 s; v-mon: table sizes every 60 s) and Cloud Monitoring (cloudmon.json, database CPU)
# are on wall-clock time and are placed on the day's minutes by the epoch in day-args.txt.
REAL = os.path.join(RESULTS, "redo-real-20261107")
BANDS = [f"band:{i}" for i in range(1, 6)]
BAND_COLOURS = [S1, S2, S3, S4, S5]
DAY_END = 270
DB_RESTART_UTC = "2026-11-08T04:45:53Z"  # Cloud SQL's own fast shutdown and restart, ~30 s down


def real_epoch():
    m = re.search(r"-arrival-epoch (\d+)", open(os.path.join(REAL, "day-args.txt")).read())
    return int(m.group(1))


def real_minutes():
    """Per run minute: the fleet's commanded and ended flows/s, ended flows/s by band and engine CPU cores, and pending
    and oldest pending by band (max over the processes). A replica's samples are pooled by 10 s slot first: in a restart
    minute its stopping and its restarted process both report, so their completions add and their commanded rate (the
    replica's share of one schedule) is taken once."""
    slots = collections.defaultdict(lambda: {"commanded": 0.0, "ended": 0.0, "cpu": 0.0, "endedBy": collections.Counter()})
    gauges = collections.defaultdict(lambda: {"pending": {}, "oldest": {}})
    for path in glob.glob(os.path.join(REAL, "dwarf-real-e*", "out", "day*.series.jsonl")):
        replica = os.path.basename(path).split(".")[0].split("-", 1)[1]
        for line in open(path):
            p = json.loads(line)
            d = slots[(replica, int(p["t"] // 10))]
            d["commanded"] = max(d["commanded"], p["commandedFlowsPerSec"])
            d["ended"] += p["endedFlowsPerSec"]
            d["cpu"] += p.get("cpuCores") or 0
            for b in BANDS:
                d["endedBy"][b] += p.get("endedBy", {}).get(b, 0)
            g = gauges[int(p["t"] // 60)]
            for key, gauge in (("pending", "pending"), ("oldest", "oldestPendingSec")):
                for b, v in (p.get(gauge) or {}).items():
                    g[key][b] = max(g[key].get(b, 0), v)
    per = collections.defaultdict(lambda: collections.defaultdict(list))
    for (replica, slot), d in slots.items():
        per[slot // 6][replica].append(d)
    rows = []
    for m in sorted(per):
        row = {"min": m, "commanded": 0.0, "ended": 0.0, "cpu": 0.0, "endedBy": collections.Counter(), **gauges[m]}
        for ds in per[m].values():
            n = len(ds)
            for k in ("commanded", "ended", "cpu"):
                row[k] += sum(d[k] for d in ds) / n
            for b in BANDS:
                row["endedBy"][b] += sum(d["endedBy"][b] for d in ds) / n
        rows.append(row)
    return [r for r in rows if r["min"] < DAY_END]


def real_utc_min(iso):
    return (datetime.strptime(iso[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=__import__("datetime").timezone.utc)
            .timestamp() - real_epoch()) / 60


def shade_day(ax):
    """The schedule behind each panel: day level shaded, bursts darker, the restarts as dotted lines and the
    database's own restart in red."""
    for c in range(0, DAY_END, 90):
        ax.axvspan(c + 25, c + 90, color=GRID, alpha=0.55, lw=0)  # ramp up, day, ramp down
        for at, length in ((10, 2), (40, 2), (58, 5)):
            ax.axvspan(c + at, c + at + length, color=MUTED, alpha=0.18, lw=0)
    for at in (137, 141):
        ax.axvline(at, color=INK2, lw=1, ls=":")
    ax.axvline(real_utc_min(DB_RESTART_UTC), color=OUTAGE, lw=1.2, ls="--")
    ax.set_xlim(0, DAY_END)
    ax.set_xticks(range(0, DAY_END + 1, 30))


def chart_day():
    if not glob.glob(os.path.join(REAL, "dwarf-real-e*", "out", "day*.series.jsonl")):
        return skip("benchmark-day.png", f"artifacts not in {REAL}")
    rows = real_minutes()
    xs = [r["min"] for r in rows]
    fig, ax = plt.subplots(figsize=(11, 4.8))
    shade_day(ax)
    ax.stackplot(xs, [[r["endedBy"][b] for r in rows] for b in BANDS], colors=BAND_COLOURS,
                 labels=[f"priority {b[5:]}" for b in BANDS], edgecolor=SURFACE, lw=0.4)
    ax.plot(xs, [r["commanded"] for r in rows], color=INK, lw=1.4, ls=(0, (4, 2)), label="offered")
    top = 1.15 * max(max(r["commanded"], r["ended"]) for r in rows)
    ax.set_ylim(0, top)
    ax.text(137.8, top * 0.98, "replica restarts", color=INK2, fontsize=9, va="top")
    ax.text(real_utc_min(DB_RESTART_UTC) - 0.8, top * 0.98, "database\nrestart", color=OUTAGE, fontsize=9, va="top",
            ha="right")
    ax.set_xlabel("minute of the day   (shaded: day level; darker: bursts)")
    ax.set_ylabel("flows completed/s, by priority")
    ax.set_title("A day on one database: offered load and completed flows by priority")
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    finish(fig, "benchmark-day.png")


def chart_day_backlog():
    if not glob.glob(os.path.join(REAL, "dwarf-real-e*", "out", "day*.series.jsonl")):
        return skip("benchmark-day-backlog.png", f"artifacts not in {REAL}")
    rows = real_minutes()
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 6.6), sharex=True)
    for ax, key, label in ((a1, "pending", "steps waiting (log scale)"),
                           (a2, "oldest", "oldest waiting step, s (log scale)")):
        shade_day(ax)
        for b, colour in zip(BANDS, BAND_COLOURS):
            pts = [(r["min"], r[key].get(b)) for r in rows]
            ax.plot([m for m, _ in pts], [max(v, 1) if v is not None else float("nan") for _, v in pts], color=colour,
                    lw=1.8, label=f"priority {b[5:]}")
        ax.set_yscale("log")
        ax.set_ylabel(label)
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    a1.set_title("A day on one database: the backlog by priority")
    a1.legend(frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    a2.set_xlabel("minute of the day")
    finish(fig, "benchmark-day-backlog.png")


def real_wal_minutes():
    """Per run minute from s-mon: WAL MB/s (the cumulative wal_bytes differenced) and the mean count of backends
    waiting on LWLock/WALWrite."""
    path = os.path.join(REAL, "dwarf-real-m", "out", "mon-day.log")
    epoch, prev, acc = real_epoch(), None, collections.defaultdict(lambda: [0.0, 0.0, 0])
    for line in open(path):
        f = line.split()
        if len(f) < 4 or not f[3].isdigit():
            continue
        t, wal = float(f[0]), int(f[3])
        waiters = sum(int(x.rsplit(":", 1)[1]) for x in f[4:] if "LWLock/WALWrite" in x)
        if prev:
            m = int((t - epoch) // 60)
            a = acc[m]
            a[0] += wal - prev[1]
            a[1] += t - prev[0]
            a[2] += 0
            a.append(waiters)
        prev = (t, wal)
    return {m: (a[0] / a[1] / 1e6, sum(a[3:]) / max(1, len(a) - 3)) for m, a in acc.items() if a[1] > 0}


def chart_day_database():
    mon, cm = os.path.join(REAL, "dwarf-real-m", "out", "mon-day.log"), os.path.join(REAL, "cloudmon.json")
    if not (os.path.exists(mon) and os.path.exists(cm)):
        return skip("benchmark-day-database.png", f"monitor files not in {REAL}")
    rows = real_minutes()
    cpu = [(real_utc_min(t) - 1, v * 32) for t, v in json.load(open(cm))["database/cpu/utilization"]]
    wal = real_wal_minutes()
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 6.2), sharex=True)
    shade_day(a1)
    a1.plot([m for m, _ in cpu if 0 <= m < DAY_END], [v for m, v in cpu if 0 <= m < DAY_END], color=S1, lw=1.8,
            label="database (32 vCPU)")
    a1.plot([r["min"] for r in rows], [r["cpu"] for r in rows], color=S2, lw=1.8, label="both engine hosts (2 x 8 vCPU)")
    a1.set_ylim(0, 32)
    a1.set_ylabel("CPU cores busy")
    a1.set_title("A day on one database: what it cost the database and the engines")
    a1.legend(frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    shade_day(a2)
    ms = sorted(m for m in wal if 0 <= m < DAY_END)
    ys = [wal[m][0] for m in ms]
    a2.plot(ms, ys, color=S3, lw=0.8, alpha=0.45, label="per minute")
    a2.plot(ms, [sum(ys[max(0, i - 2):i + 3]) / len(ys[max(0, i - 2):i + 3]) for i in range(len(ys))], color=S3, lw=2,
            label="5-minute mean")
    a2.legend(frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    a2.set_ylim(0, None)
    a2.set_ylabel("write-ahead log, MB/s")
    a2.set_xlabel("minute of the day")
    finish(fig, "benchmark-day-database.png")


def chart_day_storage():
    path = os.path.join(REAL, "dwarf-real-m", "out", "vmon-day.log")
    if not os.path.exists(path):
        return skip("benchmark-day-storage.png", f"v-mon log not in {REAL}")
    epoch, pts = real_epoch(), []
    for line in open(path):
        f = line.split()
        if len(f) >= 7 and f[1].isdigit():
            pts.append(((int(f[0]) - epoch) / 60, int(f[1]) / 1e9, int(f[2]) / 1e9, int(f[4]) / 1e9, int(f[5]),
                        int(f[6])))
    pts = [p for p in pts if 0 <= p[0] <= DAY_END]
    xs = [p[0] for p in pts]
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 6.2), sharex=True)
    shade_day(a1)
    a1.plot(xs, [p[2] for p in pts], color=S1, lw=2, label="live steps table")
    a1.plot(xs, [p[3] for p in pts], color=S2, lw=2, label="live flows table")
    a1.set_ylim(0, None)
    a1.set_ylabel("GB, with indexes and TOAST")
    a1.set_title("A day on one database: the live tables level off while the archive grows")
    a1.legend(frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    shade_day(a2)
    a2.plot(xs, [p[1] for p in pts], color=S5, lw=2, label="whole database (the archive is most of it)")
    a2.set_ylim(0, None)
    a2.set_ylabel("GB")
    a2.set_xlabel("minute of the day")
    a2.legend(frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    finish(fig, "benchmark-day-storage.png")


def real_slots():
    """Per replica and 10 s slot of the day: steps/s and offered, admitted and ended flows/s, a stopping and a
    restarted process of one replica summed."""
    keys = ("stepsPerSec", "offeredFlowsPerSec", "admittedFlowsPerSec", "endedFlowsPerSec")
    per = collections.defaultdict(list)  # (replica, slot, process) -> samples: a stopping process writes a last one
    for path in glob.glob(os.path.join(REAL, "dwarf-real-e*", "out", "day*.series.jsonl")):
        proc = os.path.basename(path).split(".")[0]
        for line in open(path):
            p = json.loads(line)
            per[(proc.split("-", 1)[1], int(p["t"] // 10), proc)].append(p)
    slots = collections.defaultdict(lambda: collections.defaultdict(float))
    for (replica, slot, _), samples in per.items():
        for k in keys:
            slots[(replica, slot)][k] += sum(p.get(k) or 0 for p in samples) / len(samples)
    return slots


def chart_day_restarts():
    rs = os.path.join(REAL, "restarts.txt")
    if not os.path.exists(rs):
        return skip("benchmark-day-restarts.png", f"restarts.txt not in {REAL}")
    slots, replicas = real_slots(), ["dwarf-real-ea", "dwarf-real-eb"]
    events = [(real_utc_min(t), vm, what) for t, vm, what in (l.split() for l in open(rs))]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4.6))
    # Left: the rolling restart, each replica's steps/s. A slot a replica has no sample for is a gap.
    lo, hi = 133, 147
    for vm, colour, name in zip(replicas, (S1, S2), ("replica A", "replica B")):
        xs = [s / 6 for s in range(lo * 6, hi * 6)]
        a1.plot(xs, [slots[(vm, s)]["stepsPerSec"] if (vm, s) in slots else float("nan") for s in range(lo * 6, hi * 6)],
                color=colour, lw=1.8, label=name)
    for at, vm, what in events:
        colour = S1 if vm.endswith("ea") else S2
        a1.axvline(at, color=colour, lw=1, ls=":" if what == "SIGTERM" else "-")
    a1.set_xlim(lo, hi)
    a1.set_ylim(0, None)
    a1.yaxis.set_major_formatter(kilo)
    a1.set_xlabel("minute of the day   (dotted: told to stop; solid: started again)")
    a1.set_ylabel("steps/s executed")
    a1.set_title("A rolling restart")
    a1.legend(frameon=False, fontsize=9, loc="lower left")
    # Right: the database restart, the fleet's flows/s offered, admitted and ended.
    lo, hi = 248, 256
    xs = [s / 6 for s in range(lo * 6, hi * 6)]
    for key, colour, ls, name in (("offeredFlowsPerSec", INK, (0, (4, 2)), "offered"),
                                  ("admittedFlowsPerSec", S3, "-", "admitted"),
                                  ("endedFlowsPerSec", S1, "-", "completed")):
        a2.plot(xs, [sum(slots[(vm, s)][key] for vm in replicas if (vm, s) in slots) for s in range(lo * 6, hi * 6)],
                color=colour, lw=1.8, ls=ls, label=name)
    down = real_utc_min(DB_RESTART_UTC)
    a2.axvspan(down, down + 0.5, color=OUTAGE, alpha=0.15, lw=0)
    a2.text(down + 0.05, 0.97, "database\ndown", color=OUTAGE, fontsize=9, va="top", transform=a2.get_xaxis_transform())
    a2.set_xlim(lo, hi)
    a2.set_ylim(0, None)
    a2.set_xlabel("minute of the day")
    a2.set_ylabel("flows/s, both replicas")
    a2.set_title("The database restarts")
    a2.legend(frameon=False, fontsize=9, loc="lower left")
    finish(fig, "benchmark-day-restarts.png")


chart_tiers()
chart_shards()
chart_engine_hosts()
chart_replicas()
chart_minutes()
chart_payload()
chart_headroom()
chart_volume()
chart_volume_rows()
chart_failover()
chart_day()
chart_day_backlog()
chart_day_database()
chart_day_storage()
chart_day_restarts()
