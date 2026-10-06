"""Charts and tables for the cloud benchmark rerun, from bench/gcp/cell.sh cells and -volume artifacts.

Each cell directory holds cell.json (written by cell.sh: the cell's coordinates, the derived and optimum
pools, and one row per run) beside the run artifacts. Run dbcpu.sh with WRITE=1 over the artifacts first,
so each carries its database CPU. Then, from the repo root, with matplotlib available:

    python3 -m venv /tmp/viz && /tmp/viz/bin/pip install matplotlib
    /tmp/viz/bin/python bench/charts_redo.py <results-root> [out-dir]

PNGs go to out-dir (default docs/); the tables print as markdown. A chart whose cells are not there yet
is skipped with a note, so a partial campaign charts what it has.
"""
import glob
import json
import os
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Reference palette, the same slots and tokens as bench/charts.py.
S1, S2, S3, S4, S5 = "#2a78d6", "#1baf7a", "#eda100", "#008300", "#8e5bb5"
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#8a8984"
SURFACE, GRID = "#fcfcfb", "#e8e7e3"
RTT_COLOURS = {0.25: S1, 0.5: S2, 1.0: S3, 1.5: S4, 2.0: S5}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "text.color": INK, "axes.edgecolor": MUTED,
    "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold",
    "axes.titlelocation": "left",
})

# The cells every R3 series runs at: R2's 16 vCPU row at the nearest RTT the rig can draw.
R3_RTT, R3_VCPUS = 0.25, 16


def finish(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=200, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)


def load_cells(root):
    """Every cell under root, its runs joined to their artifacts. Runs cell.sh marked bad are dropped."""
    cells = []
    for path in glob.glob(os.path.join(root, "**", "cell.json"), recursive=True):
        doc = json.load(open(path))
        if "cell" not in doc:
            continue
        d = os.path.dirname(path)
        runs = []
        for r in doc["runs"]:
            if r.get("bad"):
                continue
            art = os.path.join(d, r["tag"] + ".json")
            r["artifact"] = json.load(open(art)) if os.path.exists(art) else {}
            runs.append(r)
        cells.append(dict(doc["cell"], derived=doc["derivedPool"], optimum=doc.get("optimumPool"), runs=runs))
    return cells


def peaks(cell):
    """The cell's peak runs at the derived pool, and the confirming peak at the optimum if one ran."""
    ps = [r for r in cell["runs"] if r["tag"].startswith("peak-")]
    confirm = next((r for r in cell["runs"] if r["tag"].startswith("confirm-")), None)
    return ps, confirm


def published_peak(cell):
    """The number to publish: the confirming peak when the sweep moved the pool, else the mean peak."""
    ps, confirm = peaks(cell)
    if confirm:
        return confirm["steps"]
    return statistics.mean(r["steps"] for r in ps) if ps else None


def operating_point(cell):
    """The 2/3 run at the pool that ships for this cell: the optimum, else the derived one."""
    pool = cell["optimum"] or cell["derived"]
    return next((r for r in cell["runs"] if r["tag"].startswith("sweep-") and r["pool"] == pool), None)


def task_bytes(run):
    """What the tasks declared writing over the window - the denominator of every amplification."""
    r = run["artifact"].get("results", [{}])[0]
    return r.get("mbPerSec", 0) * (1 << 20) * r.get("windowSec", 0)


# ---- R2: the tier x RTT grid (linear, no payload) ----

def r2_cells(cells):
    return [c for c in cells if c["workload"] == "linear" and c["payload"] == 0]


def chart_r2_peak(cells, out):
    cs = r2_cells(cells)
    if not cs:
        print("R2 peak chart: no linear cells yet")
        return
    fig, ax = plt.subplots(figsize=(8.4, 5))
    for rtt in sorted({c["targetRtt"] for c in cs}):
        row = sorted((c for c in cs if c["targetRtt"] == rtt), key=lambda c: c["vcpus"])
        xs = [c["vcpus"] for c in row]
        ys = [published_peak(c) for c in row]
        colour = RTT_COLOURS.get(rtt, MUTED)
        ax.plot(xs, ys, color=colour, lw=2, marker="o", ms=5.5, label=f"{rtt:g} ms")
        if rtt == min(RTT_COLOURS):
            # Replicate range at the nearest RTT, where the n=3 peaks are.
            lo = [min(r["steps"] for r in peaks(c)[0]) for c in row]
            hi = [max(r["steps"] for r in peaks(c)[0]) for c in row]
            ax.vlines(xs, lo, hi, color=colour, lw=4, alpha=0.35)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    vs = sorted({c["vcpus"] for c in cs})
    ax.set_xticks(vs)
    ax.set_xticklabels([str(v) for v in vs])
    ax.set_title("Sustained peak throughput by database size and round-trip time")
    ax.set_xlabel("database vCPUs (log scale)")
    ax.set_ylabel("steps / second, 10-minute mean (log scale)")
    ax.annotate("plan at <= 2/3 of the peak", xy=(0.02, 0.97), xycoords="axes fraction",
                color=INK2, fontsize=9.5, va="top")
    ax.legend(title="RTT", loc="lower right", frameon=False, fontsize=9)
    finish(fig, os.path.join(out, "benchmark-cloud-peak.png"))


def chart_r2_pool(cells, out):
    cs = r2_cells(cells)
    if not cs:
        print("R2 pool chart: no linear cells yet")
        return
    fig, ax = plt.subplots(figsize=(8.4, 4.8))
    for rtt in sorted({c["targetRtt"] for c in cs}):
        row = sorted((c for c in cs if c["targetRtt"] == rtt), key=lambda c: c["vcpus"])
        xs = [c["vcpus"] for c in row]
        ys = [(c["optimum"] or c["derived"]) / c["vcpus"] for c in row]
        ax.plot(xs, ys, color=RTT_COLOURS.get(rtt, MUTED), lw=2, marker="o", ms=5.5, label=f"{rtt:g} ms")
    ax.set_xscale("log", base=2)
    vs = sorted({c["vcpus"] for c in cs})
    ax.set_xticks(vs)
    ax.set_xticklabels([str(v) for v in vs])
    ax.set_title("Connections per vCPU chosen at 2/3 load")
    ax.set_xlabel("database vCPUs (log scale)")
    ax.set_ylabel("pool / vCPU")
    ax.legend(title="RTT", loc="upper right", frameon=False, fontsize=9)
    finish(fig, os.path.join(out, "benchmark-cloud-pool.png"))


def table_r2_latency(cells):
    cs = r2_cells(cells)
    if not cs:
        return
    rtts = sorted({c["targetRtt"] for c in cs})
    print("\n### Latency at 2/3 load (p50 / p99 ms)\n")
    print("| vCPU | " + " | ".join(f"{r:g} ms" for r in rtts) + " |")
    print("|---|" + "---|" * len(rtts))
    for v in sorted({c["vcpus"] for c in cs}):
        cols = []
        for rtt in rtts:
            c = next((c for c in cs if c["vcpus"] == v and c["targetRtt"] == rtt), None)
            op = operating_point(c) if c else None
            cols.append(f"{op['p50']:.0f} / {op['p99']:.0f}" if op else "-")
        print(f"| {v} | " + " | ".join(cols) + " |")


def table_r2_dbcpu(cells):
    cs = [c for c in r2_cells(cells) if c["targetRtt"] == min(RTT_COLOURS)]
    if not cs:
        return
    print(f"\n### Database CPU and top wait events at peak ({min(RTT_COLOURS):g} ms)\n")
    print("| vCPU | DB CPU mean | top wait events (share of active backends) |")
    print("|---|---|---|")
    for c in sorted(cs, key=lambda c: c["vcpus"]):
        ps = peaks(c)[0]
        cpu = [r["artifact"].get("dbCpu", {}).get("meanPct") for r in ps]
        cpu = [x for x in cpu if x is not None]
        waits = {}
        for r in ps:
            for w in r["artifact"].get("results", [{}])[0].get("waitProfile") or []:
                if w.get("meanBackends"):  # the synthetic total row
                    continue
                waits.setdefault(w["event"], []).append(w["pct"])
        top = sorted(((statistics.mean(v), e) for e, v in waits.items()), reverse=True)[:3]
        cpu_s = f"{statistics.mean(cpu):.0f}%" if cpu else "(run dbcpu.sh WRITE=1)"
        print(f"| {c['vcpus']} | {cpu_s} | " + ", ".join(f"{e} {p:.0f}%" for p, e in top) + " |")


# ---- R3: payload and shape ----

def r3_series(cells):
    """(name, [(payload, cell)]) per workload shape, at the R3 coordinates."""
    out = {}
    for c in cells:
        if c["targetRtt"] != R3_RTT or c["vcpus"] != R3_VCPUS or c["workload"] == "linear" and c["payload"] == 0:
            continue
        if c["workload"] == "linear":
            name = "linear"
        elif c["workload"] == "fanout":
            name = f"fanout W={c['width']}"
        else:
            name = f"scatter W={c['width']} D={c['depth']}"
        out.setdefault(name, []).append((c["payload"], c))
    return {k: sorted(v, key=lambda x: x[0]) for k, v in out.items()}


def chart_r3(cells, out):
    series = r3_series(cells)
    if not series:
        print("R3 charts: no payload cells yet")
        return
    colours = [S1, S2, S3, S4, S5]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8))
    for (name, pts), colour in zip(sorted(series.items()), colours):
        xs = [max(p, 1) for p, _ in pts]
        axes[0].plot(xs, [published_peak(c) for _, c in pts], color=colour, lw=2, marker="o", label=name)
        mb = []
        for _, c in pts:
            ps = peaks(c)[0]
            mb.append(statistics.mean(r["artifact"]["results"][0]["mbPerSec"] for r in ps) if ps else None)
        axes[1].plot(xs, mb, color=colour, lw=2, marker="o", label=name)
    for ax, title, ylabel in ((axes[0], "Peak throughput by payload size", "steps / second"),
                              (axes[1], "Task bytes written at peak", "MB / second")):
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_title(title)
        ax.set_xlabel("payload P per task write (bytes, log scale)")
        ax.set_ylabel(ylabel)
    axes[0].legend(frameon=False, fontsize=9)
    finish(fig, os.path.join(out, "benchmark-cloud-payload.png"))

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8))
    for (name, pts), colour in zip(sorted(series.items()), colours):
        xs, stored, wal = [], [], []
        for p, c in pts:
            ps = [r for r in peaks(c)[0] if r.get("db") and task_bytes(r) > 0]
            if not ps:
                continue
            xs.append(p)
            stored.append(statistics.mean((r["db"]["stepsBytes"] + r["db"]["flowsBytes"]) / task_bytes(r) for r in ps))
            wal.append(statistics.mean(r["db"]["walBytes"] / task_bytes(r) for r in ps))
        axes[0].plot(xs, stored, color=colour, lw=2, marker="o", label=name)
        axes[1].plot(xs, wal, color=colour, lw=2, marker="o", label=name)
    for ax, title in ((axes[0], "Stored bytes per task byte"), (axes[1], "WAL bytes per task byte")):
        ax.set_xscale("log")
        ax.set_title(title)
        ax.set_xlabel("payload P per task write (bytes, log scale)")
        ax.set_ylabel("bytes / task byte")
        ax.axhline(1, color=MUTED, lw=1, ls=(0, (4, 3)))
    axes[0].legend(frameon=False, fontsize=9)
    finish(fig, os.path.join(out, "benchmark-cloud-amplification.png"))


def table_r3_itemref(cells):
    """The item-ref cache check: state-ref bytes read per flow against the array each flow fans out over.
    ~1x per replica means each replica read the array once per cohort; ~W x means the anchor was evicted
    between siblings."""
    rows = []
    for c in cells:
        if c["workload"] != "scatter":
            continue
        for r in peaks(c)[0]:
            res = r["artifact"].get("results", [{}])[0]
            flows = res.get("flows") or 0
            counters = res.get("engineCounters") or {}
            ref = sum(v for k, v in counters.items() if k.startswith("dwarf_state_read_bytes|column=state_ref"))
            array = c["width"] * c["payload"]
            if flows and array:
                rows.append((c, r["tag"], ref / flows / array))
    if not rows:
        return
    print("\n### Item-ref cache check (scatter): state-ref bytes read per flow / array bytes per flow\n")
    print("| shape | P | run | reads per array |")
    print("|---|---|---|---|")
    for c, tag, ratio in sorted(rows, key=lambda x: (x[0]["depth"], x[0]["payload"], x[1])):
        print(f"| W={c['width']} D={c['depth']} | {c['payload']} | {tag} | {ratio:.2f}x |")


# ---- R7: database size ----

def chart_r7(root, out):
    vols = []
    for path in glob.glob(os.path.join(root, "**", "*.json"), recursive=True):
        try:
            doc = json.load(open(path))
        except (ValueError, OSError):
            continue
        if isinstance(doc, dict) and doc.get("volume") and doc["volume"].get("checkpoints"):
            vols.append(doc)
    if not vols:
        print("R7 chart: no -volume artifacts yet")
        return
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8))
    for doc, colour in zip(sorted(vols, key=lambda d: d.get("label", "")), [S1, S2, S3, S4, S5]):
        cps = [cp for cp in doc["volume"]["checkpoints"] if cp.get("saturated")]
        payload = doc.get("config", {}).get("payloadBytes", 0)
        # The bytes axis is the database size; the rows axis is the step-row count.
        bytes_axis = doc.get("config", {}).get("workload") == "state" or payload > 0
        xs = [(cp["stepsDiskMB"] + cp["flowsDiskMB"]) / 1024 if bytes_axis else cp["stepRows"] for cp in cps]
        label = doc.get("label") or ("bytes" if bytes_axis else "rows")
        axes[0].plot(xs, [cp["saturated"]["stepsPerSec"] for cp in cps], color=colour, lw=2, marker="o", label=label)
        axes[1].plot(xs, [cp["twoThirds"]["p99Ms"] if cp.get("twoThirds") else None for cp in cps],
                     color=colour, lw=2, marker="o", label=label)
    axes[0].set_title("Saturated throughput as the database grows")
    axes[0].set_ylabel("steps / second")
    axes[1].set_title("p99 at 2/3 load as the database grows")
    axes[1].set_ylabel("p99 ms")
    for ax in axes:
        ax.set_xlabel("step rows (rows axis) / GB on disk (bytes axis)")
        ax.set_ylim(bottom=0)
    axes[0].legend(frameon=False, fontsize=9)
    finish(fig, os.path.join(out, "benchmark-cloud-volume.png"))


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    root = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else "docs"
    os.makedirs(out, exist_ok=True)
    cells = load_cells(root)
    print(f"{len(cells)} cell(s) under {root}")
    chart_r2_peak(cells, out)
    chart_r2_pool(cells, out)
    chart_r3(cells, out)
    chart_r7(root, out)
    table_r2_latency(cells)
    table_r2_dbcpu(cells)
    table_r3_itemref(cells)


if __name__ == "__main__":
    main()
