# pre6g/export_plot_data.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any

def _get(obj: Dict[str, Any], path: str, default=0):
    """Safe nested get: path like 'qdisc.dualpi2_delay_c_us'."""
    cur = obj
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur if cur is not None else default

def export_plot_tsv_from_aligned(aligned_jsonl: Path, out_dir: Path) -> dict:
    """
    Input: 30_analysis/aligned/aligned_timeseries.jsonl
    Output (in same folder by default):
      - latency_qdisc.tsv: t, delay_c_ms, delay_l_ms, backlog_bytes, ecn_mark
      - queue_vs_latency.tsv: backlog_bytes, delay_c_ms, delay_l_ms
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    latency_tsv = out_dir / "latency_qdisc.tsv"
    qvl_tsv = out_dir / "queue_vs_latency.tsv"

    with aligned_jsonl.open("r", encoding="utf-8") as f_in, \
        latency_tsv.open("w", encoding="utf-8") as f_lat, \
        qvl_tsv.open("w", encoding="utf-8") as f_qvl:

        prev_t = None
        prev_ecn = None
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            t = obj.get("t", 0.0)

            dc_us = _get(obj, "qdisc.dualpi2_delay_c_us", 0)
            dl_us = _get(obj, "qdisc.dualpi2_delay_l_us", 0)
            backlog = _get(obj, "qdisc.dualpi2_backlog_bytes", 0)
            ecn_mark = _get(obj, "qdisc.dualpi2_ecn_mark", 0)

            dc_ms = float(dc_us) / 1000.0
            dl_ms = float(dl_us) / 1000.0

            # ECN delta / rate
            ecn_delta = 0
            ecn_rate = 0.0
            if prev_t is not None and prev_ecn is not None:
                dt = float(t) - float(prev_t)
                ecn_delta = int(ecn_mark) - int(prev_ecn)
                if dt > 0:
                    ecn_rate = ecn_delta / dt
            prev_t = t
            prev_ecn = ecn_mark

            f_lat.write(f"{t}\t{dc_ms}\t{dl_ms}\t{backlog}\t{ecn_mark}\t{ecn_delta}\t{ecn_rate}\n")
            f_qvl.write(f"{backlog}\t{dc_ms}\t{dl_ms}\n")

    return {
        "latency_qdisc_tsv": str(latency_tsv),
        "queue_vs_latency_tsv": str(qvl_tsv),
    }

def export_plot_tsv_if_aligned_exists(run_dir: Path) -> dict:
    """
    Convenience: look for aligned file under:
      <run_dir>/30_analysis/aligned/aligned_timeseries.jsonl
    If exists -> export TSV into same folder.
    """
    aligned = run_dir / "30_analysis" / "aligned" / "aligned_timeseries.jsonl"
    if not aligned.exists():
        return {"skipped": True, "reason": "aligned_timeseries.jsonl not found", "aligned": str(aligned)}
    out_dir = aligned.parent
    out = export_plot_tsv_from_aligned(aligned, out_dir)
    out.update({"skipped": False, "aligned": str(aligned)})
    return out