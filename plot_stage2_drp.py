#!/usr/bin/env python3
import argparse
import json
import os
import statistics
from pathlib import Path
from glob import glob

import matplotlib.pyplot as plt


def find_latest_run(results_root="pre6g/results"):
    runs = sorted(glob(os.path.join(results_root, "*", "*", "run_*")), reverse=True)
    if not runs:
        raise FileNotFoundError(f"No runs found under {results_root}/<exp>/<date>/run_*")
    return Path(runs[0])


def load_jsonl(path: Path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                # skip bad lines
                continue
    return rows


def load_drp_events(run_dir: Path):
    p = run_dir / "10_raw" / "events" / "bn_drp.jsonl"
    ev = load_jsonl(p)
    # normalize to list of (t, rate_mbit)
    out = []
    for r in ev:
        t = r.get("t")
        rate = r.get("rate_mbit")
        if t is None or rate is None:
            continue
        out.append((float(t), float(rate)))
    out.sort(key=lambda x: x[0])
    return p, out


def load_iperf_series(run_dir: Path):
    iperf_dir = run_dir / "10_raw" / "iperf"
    series = {}  # tag -> list[(t, mbps)]
    for fp in sorted(iperf_dir.glob("iperf_*.json")):
        tag = fp.stem.replace("iperf_", "")
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue

        intervals = data.get("intervals", [])
        pts = []
        for itv in intervals:
            # iperf3 JSON usually has:
            # itv["sum_received"]["start"], ["end"], ["bits_per_second"]
            # fallback to itv["sum"]
            blk = itv.get("sum_received") or itv.get("sum") or {}
            t = blk.get("start")
            bps = blk.get("bits_per_second")
            if t is None or bps is None:
                continue
            pts.append((float(t), float(bps) / 1e6))
        if pts:
            series[tag] = pts
    return series


def drp_key_times():
    # 你 IETF pattern 的 period 邊界
    return [0, 40, 60, 80, 100, 120, 140]

def draw_key_vlines(ax):
    for t in drp_key_times():
        ax.axvline(t, linewidth=1, alpha=0.6)

def _pick_rtt_ms(flow_obj: dict):
    # 你的 monitor 結構：rtt_ms_median / rtt_ms_max
    if flow_obj.get("n_conns", 0) == 0:
        return None

    # 優先用 median
    if flow_obj.get("rtt_ms_median") is not None:
        return float(flow_obj["rtt_ms_median"])

    # fallback 用 max
    if flow_obj.get("rtt_ms_max") is not None:
        return float(flow_obj["rtt_ms_max"])

    return None


def load_rtt_series_from_monitors(run_dir: Path):
    mon_dir = run_dir / "10_raw" / "mon"
    series = {}  # tag -> list[(t, rtt_ms)]

    # Only ue*c / ue*l files have flow RTT; ue_shell is links-only
    for fp in sorted(mon_dir.glob("samples_ue*c.jsonl")) + sorted(mon_dir.glob("samples_ue*l.jsonl")):
        rows = load_jsonl(fp)
        for r in rows:
            t = r.get("t")
            flows = r.get("flows") or {}
            if t is None or not isinstance(flows, dict):
                continue
            t = float(t)
            for tag, fo in flows.items():
                if not isinstance(fo, dict):
                    continue
                rtt = _pick_rtt_ms(fo)
                if rtt is None:
                    continue
                series.setdefault(tag, []).append((t, rtt))

    # sort points
    for tag in list(series.keys()):
        series[tag].sort(key=lambda x: x[0])
        # optional: drop crazy RTTs if needed (keep raw by default)
    return series


def draw_vlines(ax, drp_events):
    # draw vertical lines at DRP event times (including ramp steps)
    times = [t for (t, _) in drp_events]
    for t in times:
        ax.axvline(t, linewidth=1, alpha=0.5)


def plot_rtt(run_dir: Path, rtt_series):
    outdir = run_dir / "40_figs"
    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / "rtt_timeseries.png"

    plt.figure()
    ax = plt.gca()

    # 1) per-flow 淡線
    for tag, pts in sorted(rtt_series.items()):
        xs = [t for (t, _) in pts]
        ys = [v for (_, v) in pts]
        ax.plot(xs, ys, linewidth=1, alpha=0.25)

    # 2) group median：classic vs l4s
    # 用每個時間點 bin(1s) 算 median
    def group_median(kind_kw):
        bins = {}
        for tag, pts in rtt_series.items():
            if kind_kw not in tag:
                continue
            for (t, rtt) in pts:
                k = int(t)  # 1s bin
                bins.setdefault(k, []).append(rtt)
        xs, ys = [], []
        for k in sorted(bins.keys()):
            xs.append(k)
            ys.append(statistics.median(bins[k]))
        return xs, ys

    xc, yc = group_median("classic")
    xl, yl = group_median("l4s")

    if xc:
        ax.plot(xc, yc, linewidth=2.5, label="classic median")
    if xl:
        ax.plot(xl, yl, linewidth=2.5, label="l4s median")

    draw_key_vlines(ax)
    ax.set_xlabel("time (s)")
    ax.set_ylabel("RTT (ms)")
    ax.set_title("RTT vs time (per-flow faint + group median)")
    ax.legend()

    plt.tight_layout()
    plt.savefig(out, dpi=160)
    plt.close()
    return out

def plot_rtt_delta(run_dir: Path, rtt_series):
    outdir = run_dir / "40_figs"
    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / "rtt_delta_timeseries.png"

    # baseline per-flow: median RTT in [0,40)
    baseline = {}
    for tag, pts in rtt_series.items():
        base_pts = [r for (t, r) in pts if 0 <= t < 40]
        if base_pts:
            baseline[tag] = statistics.median(base_pts)

    plt.figure()
    ax = plt.gca()

    # per-flow faint ΔRTT
    for tag, pts in sorted(rtt_series.items()):
        if tag not in baseline:
            continue
        b = baseline[tag]
        xs = [t for (t, _) in pts]
        ys = [r - b for (_, r) in pts]
        ax.plot(xs, ys, linewidth=1, alpha=0.25)

    # group median ΔRTT (classic vs l4s)
    def group_delta_median(kind_kw):
        bins = {}
        for tag, pts in rtt_series.items():
            if kind_kw not in tag or tag not in baseline:
                continue
            b = baseline[tag]
            for (t, rtt) in pts:
                k = int(t)
                bins.setdefault(k, []).append(rtt - b)
        xs, ys = [], []
        for k in sorted(bins.keys()):
            xs.append(k)
            ys.append(statistics.median(bins[k]))
        return xs, ys

    xc, yc = group_delta_median("classic")
    xl, yl = group_delta_median("l4s")

    if xc: ax.plot(xc, yc, linewidth=2.5, label="classic median ΔRTT")
    if xl: ax.plot(xl, yl, linewidth=2.5, label="l4s median ΔRTT")

    draw_key_vlines(ax)
    ax.set_xlabel("time (s)")
    ax.set_ylabel("ΔRTT (ms) vs baseline[0,40)")
    ax.set_title("ΔRTT vs time (baseline removed)")
    ax.legend()

    plt.tight_layout()
    plt.savefig(out, dpi=160)
    plt.close()
    return out

def plot_throughput(run_dir: Path, iperf_series):
    outdir = run_dir / "40_figs"
    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / "throughput_timeseries.png"

    plt.figure()
    ax = plt.gca()

    # build per-second totals
    totals = {}
    totals_c = {}
    totals_l = {}

    for tag, pts in iperf_series.items():
        for (t, mbps) in pts:
            k = int(t)  # 1s bin
            totals[k] = totals.get(k, 0.0) + mbps
            if "classic" in tag:
                totals_c[k] = totals_c.get(k, 0.0) + mbps
            if "l4s" in tag:
                totals_l[k] = totals_l.get(k, 0.0) + mbps

    xs = sorted(totals.keys())
    ys = [totals[x] for x in xs]
    ax.plot(xs, ys, linewidth=2.5, label="total received")

    if totals_c:
        xc = sorted(totals_c.keys()); yc = [totals_c[x] for x in xc]
        ax.plot(xc, yc, linewidth=2.0, label="classic total")
    if totals_l:
        xl = sorted(totals_l.keys()); yl = [totals_l[x] for x in xl]
        ax.plot(xl, yl, linewidth=2.0, label="l4s total")

    # 畫 capacity pattern（你的 high/low/vlow）
    # 0-40:60, 40-60:100, 60-80:30, 80-100:60, 100-120 ramp 60->100, 120-140 ramp 100->60, 140+:60
    cap = {}
    for t in range(0, 161):
        if t < 40: cap[t] = 60
        elif t < 60: cap[t] = 100
        elif t < 80: cap[t] = 30
        elif t < 100: cap[t] = 60
        elif t < 120: cap[t] = 60 + (100-60) * ((t-100)/20)
        elif t < 140: cap[t] = 100 + (60-100) * ((t-120)/20)
        else: cap[t] = 60
    ax.plot(list(cap.keys()), list(cap.values()), linewidth=2.5, label="capacity (Mbps)")

    draw_key_vlines(ax)
    ax.set_xlabel("time (s)")
    ax.set_ylabel("rate (Mbps)")
    ax.set_title("Total received rate vs DRP capacity")
    ax.legend()

    plt.tight_layout()
    plt.savefig(out, dpi=160)
    plt.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", type=str, default="", help="run directory path (default: latest under pre6g/results)")
    ap.add_argument("--results-root", type=str, default="pre6g/results")
    args = ap.parse_args()

    run_dir = Path(args.run) if args.run else find_latest_run(args.results_root)
    if not run_dir.exists():
        raise FileNotFoundError(run_dir)

    drp_path, drp_events = load_drp_events(run_dir)
    iperf_series = load_iperf_series(run_dir)
    rtt_series = load_rtt_series_from_monitors(run_dir)

    print("[RUN]", run_dir)
    print("[DRP]", drp_path if drp_path.exists() else "(missing)")
    print("[DRP] events:", len(drp_events))
    print("[IPERF] flows:", len(iperf_series))
    print("[RTT] flows:", len(rtt_series))

    fig1 = plot_throughput(run_dir, iperf_series)
    fig2 = plot_rtt(run_dir, rtt_series)
    fig3 = plot_rtt_delta(run_dir, rtt_series)

    print("[WROTE]", fig1)
    print("[WROTE]", fig2)
    print("[WROTE]", fig3)


if __name__ == "__main__":
    main()