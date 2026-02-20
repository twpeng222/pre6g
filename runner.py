#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import subprocess
import secrets
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

from mininet.net import Mininet
from mininet.node import Controller
from mininet.link import TCLink
from mininet.log import setLogLevel

from pre6g.topo import build_multiaccess_ue_topo, load_topo_profile, DEFAULT_ACCESS_LIST, DEFAULT_ACCESS_PARAMS
from pre6g.flows import load_flow_file, normalize_run_cfg, build_flow_access_map
from pre6g.net import (
    configure_min_ip_only,
    configure_phase1_forwarding_all_ues,
    configure_phase2_mark_routing_all_ues,
)
from pre6g.experiment import run_experiment


PROJECT_ROOT = Path(__file__).resolve().parent
TODAY = datetime.now().strftime("%Y%m%d")


def _make_run_id(args) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rnd = secrets.token_hex(2)  # 4 hex chars
    flow = Path(args.flow_file).stem
    topo = Path(args.topo_file).stem if getattr(args, "topo_file", None) else "notopo"
    return f"run_{ts}_{flow}_{topo}_{args.aqm}_bn{args.bn_rate_mbit}_{rnd}"

def _require_root() -> None:
    if os.geteuid() != 0:
        print("請使用 sudo 執行此腳本")
        sys.exit(1)


def _mn_cleanup(enabled: bool) -> None:
    if not enabled:
        return
    subprocess.run(["mn", "-c"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _kill_iperf3(net: Mininet) -> None:
    try:
        for h in net.hosts:
            h.cmd("pkill -9 iperf3 2>/dev/null || true")
    except Exception:
        pass


def main(args) -> Dict[str, Any]:
    """
    Run one experiment and return metadata about produced raw artifacts.

    Returns:
        dict with keys like:
          - outdir
          - run_cfg
          - flow_access_map
          - raw_paths (best-effort)
    """
    _require_root()
    setLogLevel("warning")
    

    # ===== Path Creation =====
    DEFAULT_RESULT_DIR = (
        PROJECT_ROOT
        / "results"
        / args.exp_type
        / TODAY
    )

    run_id = getattr(args, "run_id", None) or _make_run_id(args)
    args.run_id = run_id

    out_root = Path(args.outdir or DEFAULT_RESULT_DIR)
    args.outdir = str(out_root / run_id)

    print(f"[RUN] run_id={args.run_id}")
    print(f"[RUN] outdir={args.outdir}")

    # ---- logging v2 dirs ----
    run_dir = Path(args.outdir)

    dirs = {
        "meta_dir": run_dir / "00_meta",
        "raw_dir": run_dir / "10_raw",
        "log_dir": run_dir / "20_logs",
        "err_dir": run_dir / "21_errors",
        "analysis_dir": run_dir / "30_analysis",
        "fig_dir": run_dir / "40_figs",
        "report_dir": run_dir / "50_report",
    }
    for p in dirs.values():
        p.mkdir(parents=True, exist_ok=True)

    # expose to args
    for k, p in dirs.items():
        setattr(args, k, str(p))

    # optional: bg subfolders (for Step2)
    (Path(args.log_dir) / "bg").mkdir(parents=True, exist_ok=True)
    (Path(args.err_dir) / "bg").mkdir(parents=True, exist_ok=True)
    (Path(args.raw_dir) / "mon").mkdir(parents=True, exist_ok=True)
    (Path(args.err_dir) / "mon").mkdir(parents=True, exist_ok=True)
    (Path(args.raw_dir) / "qdisc").mkdir(parents=True, exist_ok=True)
    (Path(args.raw_dir) / "policy").mkdir(parents=True, exist_ok=True)
    (Path(args.meta_dir) / "mon").mkdir(parents=True, exist_ok=True)
    (Path(args.analysis_dir) / "merged").mkdir(parents=True, exist_ok=True)
    (Path(args.analysis_dir) / "aligned").mkdir(parents=True, exist_ok=True)  # 你已有 ts alignment 就先預留


    # ===== 1) Load configuration =====

    flow_cfg = load_flow_file(args.flow_file)
    run_cfg = normalize_run_cfg(args, flow_cfg)
    flow_access_map = build_flow_access_map(run_cfg["flows"])
    server_ip = run_cfg["server_ip"]

    if getattr(args, "topo_file", None):
        access_list, access_params = load_topo_profile(args.topo_file)
    else:
        access_list, access_params = DEFAULT_ACCESS_LIST, DEFAULT_ACCESS_PARAMS

    if not getattr(args, "no_clean", False):
        _mn_cleanup(True)

    # Bottleneck profile (目前你是空 dict；之後要做 topo.json/aqm sweep 可以放進來)
    bottleneck_params: Dict[str, Any] = {}

    # ===== 2) Build network =====
    net = Mininet(controller=Controller, link=TCLink)
    net.addController("c0")

    topo = build_multiaccess_ue_topo(
        net,
        access_list=access_list,
        access_params=access_params,
        bottleneck_params=bottleneck_params,
        n_ues=args.n_ues,
        ue_internal_params={},
    )

    # ===== 3) Configure control plane =====
    try:
        net.start()

        configure_min_ip_only(topo)
        configure_phase1_forwarding_all_ues(topo, return_access="A")
        configure_phase2_mark_routing_all_ues(
            topo,
            flow_access_map=flow_access_map,
            fallback_access="A",
            server_ip=server_ip,
            nft_table_name="ma_mark",
        )

        # (Optional) debug dump; 建議之後改成 args.debug_routes 才印
        if getattr(args, "debug_routes", False):
            for ue in range(1, topo["n_ues"] + 1):
                ue_sh = net.get(f"ue{ue}_shell")
                print(ue_sh.cmd("ip rule show"))
                print(ue_sh.cmd("ip route get 10.0.40.2 mark 1"))
                print(ue_sh.cmd("ip route get 10.0.40.2 mark 2"))

        # ===== 4) Run experiment (produce raw jsonl) =====
        exp_ret = run_experiment(net, topo, args)
        return exp_ret

    finally:
        _kill_iperf3(net)
        net.stop()


if __name__ == "__main__":
    raise SystemExit("Please run via: sudo python3 -m pre6g.cli ...")
