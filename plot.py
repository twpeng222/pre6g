# pre6g/plot.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple

import matplotlib.pyplot as plt


def _find_latest_run(results_dir: Path) -> Optional[Path]:
    """
    Find latest run_* folder under results/*/*/run_*
    """
    if not results_dir.exists():
        return None
    runs = sorted(results_dir.glob("*/*/run_*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return runs[0] if runs else None


def _read_tsv_2col(path: Path, x_idx: int, y_idx: int) -> Tuple[list, list]:
    xs, ys = [], []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) <= max(x_idx, y_idx):
                continue
            try:
                xs.append(float(parts[x_idx]))
                ys.append(float(parts[y_idx]))
            except ValueError:
                continue
    return xs, ys


def plot_latency_timeseries(aligned_dir: Path, out_dir: Optional[Path] = None) -> Path:
    """
    Input: latency_qdisc.tsv
      cols: t, delay_c_ms, delay_l_ms, backlog_bytes, ecn_mark
    Output: fig_latency_timeseries.png
    """
    tsv = aligned_dir / "latency_qdisc.tsv"
    if not tsv.exists():
        raise FileNotFoundError(f"missing: {tsv}")

    out_dir = out_dir or aligned_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_png = out_dir / "fig_latency_timeseries.png"

    t, dc = _read_tsv_2col(tsv, 0, 1)
    _, dl = _read_tsv_2col(tsv, 0, 2)
    _, ecn_rate = _read_tsv_2col(tsv, 0, 6)

    fig, ax1 = plt.subplots()
    ax1.plot(t, dc, label="delay_c_ms")
    ax1.plot(t, dl, label="delay_l_ms")
    ax1.set_xlabel("time (s)")
    ax1.set_ylabel("queue delay (ms)")
    ax1.grid(True)

    ax2 = ax1.twinx()
    ax2.plot(t, ecn_rate, label="ecn_mark_rate", linestyle="--")
    ax2.set_ylabel("ECN mark rate (marks/s)")

    ax1.set_title("DualPI2 delay + ECN marking rate (Classic vs L4S)")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)

    return out_png


def plot_queue_vs_latency(aligned_dir: Path, out_dir: Optional[Path] = None) -> Path:
    """
    Input: queue_vs_latency.tsv
      cols: backlog_bytes, delay_c_ms, delay_l_ms
    Output: fig_queue_vs_latency.png
    """
    tsv = aligned_dir / "queue_vs_latency.tsv"
    if not tsv.exists():
        raise FileNotFoundError(f"missing: {tsv}")

    out_dir = out_dir or aligned_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_png = out_dir / "fig_queue_vs_latency.png"

    backlog, dc = _read_tsv_2col(tsv, 0, 1)
    _, dl = _read_tsv_2col(tsv, 0, 2)

    plt.figure()
    plt.scatter(backlog, dc, s=6, label="delay_c_ms")
    plt.scatter(backlog, dl, s=6, label="delay_l_ms")
    plt.xlabel("backlog (bytes)")
    plt.ylabel("queue delay (ms)")
    plt.title("Backlog vs queue delay (Classic vs L4S)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()

    return out_png


def plot_minimal_for_run(run_dir: Path) -> dict:
    """
    Generate the 2 minimal figures under:
      <run_dir>/30_analysis/aligned/
    """
    aligned_dir = run_dir / "30_analysis" / "aligned"
    if not aligned_dir.exists():
        raise FileNotFoundError(f"missing aligned dir: {aligned_dir}")

    out1 = plot_latency_timeseries(aligned_dir)
    out2 = plot_queue_vs_latency(aligned_dir)
    return {
        "fig_latency_timeseries": str(out1),
        "fig_queue_vs_latency": str(out2),
    }


def main():
    ap = argparse.ArgumentParser(description="pre6g plotting utilities (minimal figures)")
    ap.add_argument("--run", type=str, default="", help="run dir path (e.g., pre6g/results/.../run_*)")
    ap.add_argument("--latest", action="store_true", help="use latest run under pre6g/results/")
    ap.add_argument("--results-dir", type=str, default="pre6g/results", help="results root")
    args = ap.parse_args()

    if args.latest:
        run_dir = _find_latest_run(Path(args.results_dir))
        if run_dir is None:
            raise SystemExit(f"No runs found under: {args.results_dir}")
    elif args.run:
        run_dir = Path(args.run)
    else:
        raise SystemExit("Provide --run <path> or --latest")

    run_dir = run_dir.resolve()
    outs = plot_minimal_for_run(run_dir)

    print("[PLOT] wrote:", outs["fig_latency_timeseries"])
    print("[PLOT] wrote:", outs["fig_queue_vs_latency"])


if __name__ == "__main__":
    main()