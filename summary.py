from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

def _count_lines(p: Path) -> int:
    n = 0
    with open(p, "r", encoding="utf-8") as f:
        for _ in f:
            n += 1
    return n


def _scan_timeline_from_jsonl(p: Path) -> dict:
    """
    Scan jsonl with {"t": <float>, ...} per line.
    Returns: {t_min, t_max, duration_s_est, interval_est_s, has_gaps}
    """
    t_min = None
    t_max = None
    prev_t = None
    dts = []

    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            t = obj.get("t", None)
            if t is None:
                continue

            if t_min is None or t < t_min:
                t_min = t
            if t_max is None or t > t_max:
                t_max = t

            if prev_t is not None:
                dt = t - prev_t
                if dt > 0:
                    dts.append(dt)
            prev_t = t

    if t_min is None or t_max is None:
        return {
            "t_min": None,
            "t_max": None,
            "duration_s_est": None,
            "interval_est_s": None,
            "has_gaps": None,
        }

    duration = t_max - t_min

    # interval estimate: median dt
    interval = None
    if dts:
        dts_sorted = sorted(dts)
        mid = len(dts_sorted) // 2
        interval = dts_sorted[mid] if len(dts_sorted) % 2 == 1 else (dts_sorted[mid - 1] + dts_sorted[mid]) / 2

    # gap rule (v0): any dt > 3x median interval => gap
    has_gaps = False
    if interval and dts:
        thr = 3.0 * interval
        has_gaps = any(dt > thr for dt in dts)

    return {
        "t_min": t_min,
        "t_max": t_max,
        "duration_s_est": duration,
        "interval_est_s": interval,
        "has_gaps": has_gaps,
    }


def _compute_access_usage_from_merged(p: Path) -> dict:
    """
    Sum tx_delta per access (A/B/C) from merged jsonl.
    """
    totals = {"A": 0, "B": 0, "C": 0}

    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            links = obj.get("links", {})
            for k in ("A", "B", "C"):
                if k in links:
                    totals[k] += links[k].get("tx_delta", 0)

    total_all = sum(totals.values())

    stats = {}
    for k, v in totals.items():
        share = v / total_all if total_all > 0 else 0.0
        stats[k] = {
            "tx_bytes_total": v,
            "share": share,
        }

    return stats


def _percentiles(values: list[float], ps=(0.5, 0.95)) -> dict:
    if not values:
        return {}
    vs = sorted(values)
    n = len(vs)
    out = {}
    for p in ps:
        idx = int(round(p * (n - 1)))
        out[p] = vs[idx]
    return out


def _qdisc_delay_stats_from_series(p: Path) -> dict:
    c_ms = []
    l_ms = []
    pkts_c = []
    pkts_l = []
    drops = []
    ecn_marks = []
    pkts_total = []

    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            c = obj.get("dualpi2_delay_c_us", None)
            l = obj.get("dualpi2_delay_l_us", None)
            if c is not None:
                c_ms.append((c or 0) / 1000.0)
            if l is not None:
                l_ms.append((l or 0) / 1000.0)
            
            d = obj.get("tbf_dropped", None)
            if d is not None:
                drops.append(d)

            em = obj.get("dualpi2_ecn_mark", None)
            if em is not None:
                ecn_marks.append(em)

            pc = obj.get("dualpi2_pkts_in_c", None)
            pl = obj.get("dualpi2_pkts_in_l", None)
            if pc is not None:
                pkts_c.append(pc)
            if pl is not None:
                pkts_l.append(pl)
            if pc is not None and pl is not None:
                pkts_total.append(pc + pl)

            

    def pct(vs):
        pctd = _percentiles(vs, ps=(0.5, 0.95))
        if not pctd:
            return None
        nonzero = sum(1 for x in vs if x > 0)
        return {"p50": pctd[0.5], "p95": pctd[0.95], "n": len(vs), "n_nonzero": nonzero}
    
    # counters are cumulative -> use delta(end - start)
    def delta_of(lst):
        if not lst:
            return None
        return lst[-1] - lst[0]

    # collect cumulative counters too
    drop_total = delta_of(drops)
    ecn_mark_delta = delta_of(ecn_marks)
    pkts_in_delta = delta_of(pkts_total)

    ce_ratio_est = None
    if ecn_mark_delta is not None and pkts_in_delta and pkts_in_delta > 0:
        ce_ratio_est = ecn_mark_delta / pkts_in_delta

    out = {
        "delay_c_ms": pct(c_ms),
        "delay_l_ms": pct(l_ms),
        "pkts_in_c_max": max(pkts_c) if pkts_c else None,
        "pkts_in_l_max": max(pkts_l) if pkts_l else None,
        "drop_total": drop_total,
        "ce_ratio_est": ce_ratio_est,
        "delay_src": "dualpi2_delay_{c,l}_us",
    }
    return out


def _rtt_stats_from_mon_samples(p: Path) -> dict | None:
    rtts = []
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            flows = obj.get("flows") or {}
            # v0: aggregate across flows in this host snapshot
            for _fid, st in flows.items():
                v = st.get("rtt_ms_median")
                if v is not None:
                    rtts.append(float(v))

    if not rtts:
        return None

    pct = _percentiles(rtts, ps=(0.5, 0.95))
    return {
        "rtt_ms_p50": pct[0.5],
        "rtt_ms_p95": pct[0.95],
        "n": len(rtts),
    }


def _throughput_stats_from_iperf_dir(iperf_dir: Path) -> dict | None:
    bps = []
    if not iperf_dir.exists():
        return None

    for p in sorted(iperf_dir.glob("iperf_*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f)

            v = (
                obj.get("end", {})
                   .get("sum_sent", {})
                   .get("bits_per_second", None)
            )
            if v is not None:
                bps.append(float(v))
        except Exception:
            continue

    if not bps:
        return None

    pct = _percentiles(bps, ps=(0.5, 0.95))
    mean_bps = sum(bps) / len(bps)

    return {
        "n_flows": len(bps),
        "mean_mbps": mean_bps / 1e6,
        "p50_mbps": pct[0.5] / 1e6,
        "p95_mbps": pct[0.95] / 1e6,
        "src": "iperf3.end.sum_sent.bits_per_second",
    }




def generate_summary_v0(
    run_dir: Path,
    merged_rel: str | None = None,
    qdisc_rel: str | None = None,
    policy_rel: str | None = None,
) -> str:
    """
    Generate minimal summary v0 (manifest only).
    Returns relative path to summary.json.
    """

    analysis_dir = run_dir / "30_analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    summary_path = analysis_dir / "summary.json"

    # build artifacts dict only if values exist
    artifacts = {}
    if merged_rel:
        artifacts["merged_samples_jsonl"] = merged_rel
    if qdisc_rel:
        artifacts["qdisc_series_jsonl"] = qdisc_rel
    if policy_rel:
        artifacts["policy_jsonl"] = policy_rel

    summary = {
        "schema": {
            "name": "pre6g.summary",
            "version": "0.0",
        },
        "generated_at": datetime.now(
            ZoneInfo("Asia/Taipei")
        ).isoformat(),
        "run_ref": "../00_meta/run.json",
        "inputs": {
            "artifacts": artifacts
        },
        "qc": {},
        "stats": {},
        "notes": [
            "v0 manifest only (no QC/stats yet)"
        ],
    }

    # ---- QC v0 (merged only) ----
    qc = {
        "exists": {"merged": False, "qdisc_series": False},
        "readable": {"merged": False, "qdisc_series": False},
        "counts": {},
        "timeline": {},
    }

    if merged_rel:
        merged_path = (run_dir / merged_rel)
        qc["exists"]["merged"] = merged_path.exists()
        if qc["exists"]["merged"]:
            try:
                # readable + counts
                qc["counts"]["merged_lines"] = _count_lines(merged_path)
                qc["readable"]["merged"] = True

                # timeline
                qc["timeline"] = _scan_timeline_from_jsonl(merged_path)
            except Exception:
                qc["readable"]["merged"] = False
    
    if qdisc_rel:
        qdisc_path = (run_dir / qdisc_rel)
        qc["exists"]["qdisc_series"] = qdisc_path.exists()
        if qc["exists"]["qdisc_series"]:
            try:
                qc["counts"]["qdisc_series_lines"] = _count_lines(qdisc_path)
                qc["readable"]["qdisc_series"] = True
            except Exception:
                qc["readable"]["qdisc_series"] = False


    summary["qc"] = qc

    # ---- Access stats v0 ----
    stats = {}

    if merged_rel and qc["exists"]["merged"] and qc["readable"]["merged"]:
        try:
            access_stats = _compute_access_usage_from_merged(merged_path)
            stats["access"] = access_stats
        except Exception:
            pass
    
    # ---- Qdisc stats v0 ----
    if qdisc_rel and qc["exists"]["qdisc_series"] and qc["readable"]["qdisc_series"]:
        try:
            qdisc_stats = _qdisc_delay_stats_from_series(qdisc_path)
            if qdisc_stats:
                stats["qdisc"] = qdisc_stats
        except Exception:
            pass

    summary["stats"] = stats

    # ---- RTT host-level (v0) ----
    rtt_hosts = {}
    mon_dir = run_dir / "10_raw" / "mon"
    if mon_dir.exists():
        for p in sorted(mon_dir.glob("samples_ue*[cl].jsonl")):
            host = p.name.replace("samples_", "").replace(".jsonl", "")
            st = _rtt_stats_from_mon_samples(p)
            if st is not None:
                rtt_hosts[host] = st

    summary["stats"]["rtt_hosts"] = rtt_hosts

    # ---- throughput stats (v0) ----
    iperf_dir = run_dir / "10_raw" / "iperf"
    tp = _throughput_stats_from_iperf_dir(iperf_dir)
    summary["stats"]["throughput"] = tp or {}





    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # return path relative to run_dir
    return str(summary_path.relative_to(run_dir))


def write_report_summary_v0(run_dir: Path) -> str:
    """
    Write 50_report/summary.json from 30_analysis/summary.json (v0 report view).
    Returns relative path to report summary.
    """
    analysis_summary = run_dir / "30_analysis" / "summary.json"
    report_dir = run_dir / "50_report"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "summary.json"

    # load analysis summary (internal)
    with open(analysis_summary, "r", encoding="utf-8") as f:
        a = json.load(f)

    # --- minimal run health (v0) ---
    # count error files / lines in 21_errors
    err_dir = run_dir / "21_errors"
    err_files = []
    err_lines_total = 0
    if err_dir.exists():
        for p in err_dir.rglob("*"):
            if not p.is_file():
                continue
            try:
                if p.stat().st_size == 0:
                    continue  # ✅ v0: ignore empty stderr files
            except Exception:
                pass

            err_files.append(str(p.relative_to(run_dir)))
            try:
                err_lines_total += _count_lines(p)
            except Exception:
                pass


    qc = a.get("qc", {})
    stats = a.get("stats", {})

    report = {
        "schema": {"name": "pre6g.report_summary", "version": "0.0"},
        "generated_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
        "run_ref": "../00_meta/run.json",

        # v0 KPIs (only what we already have, stable)
        "kpi": {
            "access_share": stats.get("access", {}),
            "bottleneck_qdisc": stats.get("qdisc", {}),
            "rtt_hosts": stats.get("rtt_hosts", {}),
            "throughput": stats.get("throughput", {}),
        },

        # v0 health
        "health": {
            "qc": {
                "exists": qc.get("exists", {}),
                "readable": qc.get("readable", {}),
                "counts": qc.get("counts", {}),
                "timeline": qc.get("timeline", {}),
            },
            "errors": {
                "error_files_count": len(err_files),
                "error_lines_total": err_lines_total,
                "error_files": err_files[:20],  # v0: cap to avoid huge file
            },
        },

        "notes": [
            "v0 report summary (KPI + health only).",
            "Derived from 30_analysis/summary.json.",
        ],
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    return str(report_path.relative_to(run_dir))
