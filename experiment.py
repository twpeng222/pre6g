# pre6g/experiment.py
from __future__ import annotations
from typing import Any, Dict, List
import json
import time
import os
import subprocess
from pathlib import Path
from datetime import datetime
from pre6g.flows import load_flow_file, normalize_run_cfg, tag_for_flow, assign_cports, flow_kind_from_cc, port_for_flow
from pre6g.net import setup_aqm_on_bottleneck
from pre6g.topo import ue_access_ip, ue_internal_ip
from pre6g.validate import verify_flow_access_mapping, verify_per_flow_access_with_tcpdump, auto_acceptance, print_flow_traffic_summary
from pre6g.parse import extract_qdisc_series
from pre6g.summary import generate_summary_v0, write_report_summary_v0
from pre6g.export_plot_data import export_plot_tsv_if_aligned_exists
from pre6g.plot import plot_minimal_for_run

# ===== public API =====
def run_experiment(net, topo, args):
    """
    讀 flow-file -> 啟動 servers/clients/monitors -> merge jsonl
    """
    flow_cfg = load_flow_file(args.flow_file)
    run = normalize_run_cfg(args, flow_cfg)

    outdir = args.outdir
    ensure_dir(outdir)

    server_ip = run["server_ip"]
    dur = int(run["duration"])
    interval = float(run["interval"])
    flows_in = run["flows"]

    # 先補齊 tag，否則 assign_cports 會用不到
    for f in flows_in:
        f["tag"] = tag_for_flow(f)

    # ✅ 這行你漏掉：分配每個 flow 的固定 cport
    flows_in = assign_cports(flows_in)

    server = net.get("server")
    server.cmd("sysctl -w net.ipv4.tcp_ecn=3")
    server.cmd("sysctl -w net.ipv4.tcp_ecn_fallback=0")

    # ---- clean (all hosts) ----
    for h in net.hosts:
        h.cmd("pkill -9 iperf3 2>/dev/null || true")
    time.sleep(0.2)

    # ---- apply AQM on shared bottleneck (r <-> server) ----
    r = topo["r"]
    bn_dev = topo["linkmap"]["r_to_server"]["r_intf"]

    rate_mbit = None if int(args.bn_rate_mbit) <= 0 else int(args.bn_rate_mbit)
    
    aqm_applied = setup_aqm_on_bottleneck(
        r,
        bn_dev,
        aqm_type=args.aqm,
        rate_mbit=rate_mbit,
        delay_ms=int(args.bn_delay_ms),
        burst_kb=int(args.bn_burst_kb),
        latency_ms=int(args.bn_latency_ms),
        dualpi2_target_ms=int(getattr(args, "dualpi2_target_ms", 15)),
        dualpi2_tupdate_ms=int(getattr(args, "dualpi2_tupdate_ms", 16)),
    )


    print(f"[AQM] applied={aqm_applied} dev={bn_dev}")
    print(r.cmd(f"tc -s qdisc show dev {bn_dev}"))
    print(r.cmd(f"tc -d -s qdisc show dev {bn_dev}"))

    # ---- start qdisc monitor (background) ----
    qdisc_mon = start_qdisc_monitor(
        r,
        dev=bn_dev,
        outdir=outdir,
        duration=float(dur) + 1.0,
        interval=float(getattr(args, "qdisc_interval", 0.25)),
    )
    print("[QDISC_MON] pid:", qdisc_mon["pid"])
    print("[QDISC_MON] out:", qdisc_mon["out"])
    print("[QDISC_MON] chk:\n", qdisc_mon["chk"])


    # ---- build runtime flows: add tag/local_ip/port/cport ----
    runtime = []
    ports = []
    for f in flows_in:
        ue = int(f["ue"])
        access = f["access"]
        cc = f["cc"]
        tag = f["tag"]
        kind = f.get("kind", flow_kind_from_cc(cc))
        parallel = int(f.get("parallel", 1))
        cport = int(f["cport"])

        local_ip = ue_access_ip(ue, access, topo)
        port = int(f.get("port", port_for_flow({**f, "kind": kind})))
        if kind == "classic":
            bind_ip = ue_internal_ip(ue, "classic")
        else:
            bind_ip = ue_internal_ip(ue, "l4s")

        runtime.append({
            "ue": ue, "access": access, "cc": cc, "kind": kind, "tag": tag,
            "local_ip": local_ip, "port": port, "parallel": parallel,
            "cport": cport,
            "bind_ip": bind_ip,
        })
        ports.append(port)

    # ---- start iperf servers ----
    server_pids = start_iperf_servers(
        server,
        ports,
        outdir,
        log_dir=args.log_dir,
        err_dir=args.err_dir,
    )

    # ---- start monitors: per-UE (uec / uel for cwnd) + ueshell for link bytes ----
    mon_outs = []
    for ue in sorted(set(x["ue"] for x in runtime)):

        # ---------- (A) link monitor on ue_shell ----------
        ue_shell = net.get(f"ue{ue}_shell")
        links_cfg = {
            acc: {"intf": topo["linkmap"]["ue_shell_to_r"][ue][acc]["ue_intf"]}
            for acc in topo["access_list"]
        }
        shell_script = str(Path(outdir) / "00_meta" / "mon" / f"mon_ue{ue}_shell.py")
        shell_out = str(Path(args.raw_dir) / "mon" / f"samples_ue{ue}_shell.jsonl")
        shell_err = str(Path(args.err_dir) / "mon" / f"samples_ue{ue}_shell.err")


        install_multi_flow_monitor(
            ue_shell,
            script_path=shell_script,
            outpath=shell_out,
            duration=float(dur) + 1.0,
            interval=interval,
            warmup=0.20,
            peer_ip=server_ip,
            flows_cfg={},          # <<<<<< links-only
            links_cfg=links_cfg
        )
        ue_shell.cmd(f"rm -f {shell_out} {shell_err} 2>/dev/null || true")
        ue_shell.cmd(f"bash -lc 'python3 -u {shell_script} > {shell_out} 2> {shell_err} & echo $!'").strip()
        mon_outs.append(shell_out)

        # ---------- (B) cwnd monitor on ue{ue}c (classic sender) ----------
        ue_c = net.get(f"ue{ue}c")
        classic_flows_cfg = {}
        for x in runtime:
            if x["ue"] == ue and x["kind"] == "classic":
                classic_flows_cfg[x["tag"]] = {
                    "peer_port": int(x["port"]),
                    "local_ip": ue_internal_ip(ue, "classic"),
                }

        if classic_flows_cfg:
            c_script = str(Path(outdir) / "00_meta" / "mon" / f"mon_ue{ue}c.py")
            c_out = str(Path(args.raw_dir) / "mon" / f"samples_ue{ue}c.jsonl")
            c_err = str(Path(args.err_dir) / "mon" / f"samples_ue{ue}c.err")

            install_multi_flow_monitor(
                ue_c,
                script_path=c_script,
                outpath=c_out,
                duration=float(dur) + 1.0,
                interval=interval,
                warmup=0.20,
                peer_ip=server_ip,
                flows_cfg=classic_flows_cfg,
                links_cfg={}          # <<<<<< flows-only
            )
            ue_c.cmd(f"rm -f {c_out} {c_err} 2>/dev/null || true")
            ue_c.cmd(f"bash -lc 'python3 -u {c_script} > {c_out} 2> {c_err} & echo $!'").strip()
            mon_outs.append(c_out)

        # ---------- (C) cwnd monitor on ue{ue}l (l4s sender) ----------
        ue_l = net.get(f"ue{ue}l")
        l4s_flows_cfg = {}
        for x in runtime:
            if x["ue"] == ue and x["kind"] == "l4s":
                l4s_flows_cfg[x["tag"]] = {
                    "peer_port": int(x["port"]),
                    "local_ip": ue_internal_ip(ue, "l4s"),
                }

        if l4s_flows_cfg:
            l_script = str(Path(outdir) / "00_meta" / "mon" / f"mon_ue{ue}l.py")
            l_out = str(Path(args.raw_dir) / "mon" / f"samples_ue{ue}l.jsonl")
            l_err = str(Path(args.err_dir) / "mon" / f"samples_ue{ue}l.err")

            install_multi_flow_monitor(
                ue_l,
                script_path=l_script,
                outpath=l_out,
                duration=float(dur) + 1.0,
                interval=interval,
                warmup=0.20,
                peer_ip=server_ip,
                flows_cfg=l4s_flows_cfg,
                links_cfg={}
            )
            ue_l.cmd(f"rm -f {l_out} {l_err} 2>/dev/null || true")
            ue_l.cmd(f"bash -lc 'python3 -u {l_script} > {l_out} 2> {l_err} & echo $!'").strip()
            mon_outs.append(l_out)


    # ---- CC availability detect (once) ----
    prague_ok = load_cc_modules()
    avail = set(get_available_cc())

    classic_cc = "cubic" if "cubic" in avail else "reno"

    # L4S sender CC preference: prague > dctcp > fallback
    if prague_ok and "prague" in avail:
        l4s_cc = "prague"
    elif "dctcp" in avail:
        l4s_cc = "dctcp"
    else:
        # 沒有 prague/dctcp 就只能先用 classic 當 baseline
        l4s_cc = classic_cc

    print("[CC] available:", " ".join(sorted(avail)))
    print("[CC] classic_cc:", classic_cc, "l4s_cc:", l4s_cc)

    # ---- apply per-sender host sysctl ----
    for i in range(1, topo["n_ues"] + 1):
        ue_c = net.get(f"ue{i}c")
        ue_l = net.get(f"ue{i}l")

        configure_host(ue_c, classic_cc, ecn=True)

        # 只有真的有 L4S CC 時才開 L4S 相關 sysctl
        if l4s_cc == "prague":
            configure_host(ue_l, "prague", ecn=True)   # 會設 tcp_ecn=3
        elif l4s_cc == "dctcp":
            configure_host(ue_l, "dctcp", ecn=True)    # 會設 ecn=1 + fallback=0
        else:
            configure_host(ue_l, classic_cc, ecn=True) # fallback：至少確保能跑


    # ---- prepare iperf raw outputs (JSON) ----
    raw_iperf_dir = Path(args.raw_dir) / "iperf"
    raw_iperf_dir.mkdir(parents=True, exist_ok=True)

    (Path(args.err_dir) / "bg").mkdir(parents=True, exist_ok=True)

    # ---- start DRP scheduler (IETF pattern default OFF) ----
    use_drp = (getattr(args, "bn_drp", "") == "ietf")
    t0_mono = None
    drp_log = None
    wait_exec = None

    if use_drp:
        wait_exec = ensure_wait_exec(outdir)

        # 留 2 秒讓背景監控 ready
        t0_mono = time.monotonic() + 2.0
        print("[T0] monotonic start at:", t0_mono)
        print("[T0] wait_exec:", wait_exec)

        # 啟動 DRP（用同一個 t0_mono）
        high, low, vlow = 100, 60, 30
        drp_log = start_bn_drp_scheduler(
            r, bn_dev, outdir,
            burst_kb=int(args.bn_burst_kb),
            latency_ms=int(args.bn_latency_ms),
            high=high, low=low, vlow=vlow,
            step_s=float(getattr(args, "bn_drp_step_s", 1.0)),
            t0_mono=t0_mono,
        )
        print("[DRP] enabled ietf pattern, log:", drp_log)


    # ---- start clients (STEP 1: send from ue{ue}c / ue{ue}l, not ue_shell) ----
    for x in runtime:
        ue = int(x["ue"])
        kind = str(x.get("kind", "")).lower()

        # client host
        if kind == "classic":
            client = net.get(f"ue{ue}c")
        else:
            client = net.get(f"ue{ue}l")
        
        bind_ip = x["bind_ip"]


        cport = int(x["cport"])
        server_port = int(x["port"])

        cmd = (
            f"iperf3 -4 -c {server_ip} -p {server_port} "
            f"--cport {cport} "
            f"-B {bind_ip} "
            f"-t {dur} -i 1 -P {x['parallel']} -J "
            f"-C {x['cc']}"
        )

        # --- iperf3 client (JSON -> 10_raw/iperf/, stderr -> 21_errors/bg/) ---
        iperf_json = str(raw_iperf_dir / f"iperf_{x['tag']}.json")
        iperf_err  = str(Path(args.err_dir) / "bg" / f"iperf_{x['tag']}.err")

        client.cmd(f"rm -f {iperf_json} {iperf_err} 2>/dev/null || true")

        if not use_drp:
            pid = client.cmd(f"bash -lc '{cmd} >{iperf_json} 2>{iperf_err} & echo $!'").strip()
        else:
            # cmd 目前是 "iperf3 ... -C xxx"
            # 我們用 bash -lc 去執行：python3 wait_exec.py --t0 T0 -- <iperf3 ...>
            wrapped = f"python3 -u {wait_exec} --t0 {t0_mono} -- {cmd}"
            pid = client.cmd(f"bash -lc '{wrapped} >{iperf_json} 2>{iperf_err} & echo $!'").strip()

        alive = client.cmd(f"bash -lc 'ps -p {pid} -o pid=,cmd= 2>/dev/null || echo DEAD'").strip()
        print(f"[BG] {client.name} tag=iperf_{x['tag']} pid={pid} alive_check={alive}")

    
    # ✅ 立刻開始抽樣（在 flow 還活著的時候）
    policy_mon = start_ueshell_policy_monitor(
        net, topo, outdir=outdir,
        duration=float(dur) + 1.0,
        interval=0.5
    )
    policy_log = policy_mon["out"]
    print("[UE_SHELL_MON_BG] pid:", policy_mon["pid"])
    print("[UE_SHELL_MON_BG] out:", policy_mon["out"])
    print("[UE_SHELL_MON_BG] chk:\n", policy_mon["chk"])
    
    # ✅ 立刻做「逐條 flow 實際走哪條 access」驗收（最重要）
    ok_flow, flow_detail = verify_per_flow_access_with_tcpdump(
        net, topo, runtime, server_ip=server_ip,
        warmup=2,          # 等 iperf 起跑
        sniff_sec=0.8,       # 每條 flow 抓一下就夠
        min_hits=5,        # 太嚴可調 1~3
        verbose=True
    )
    if not ok_flow:
        print("[FATAL] per-flow access check failed (tcpdump).")
        # 你要直接 raise 也可以：
        # raise RuntimeError("per-flow access mapping failed")


    # 等所有 client 結束
    time.sleep(float(dur) + 2)

    print("\n=== BASIC ROUTING CHECK ===")
    auto_acceptance(net, topo, server_ip, marks=(1,2))

    print("\n=== FLOW SEMANTIC CHECK (MOST IMPORTANT) ===")
    verify_flow_access_mapping(policy_log, runtime)
    print_flow_traffic_summary(runtime, outdir)

    # ---- merge jsonl to one file (on root) ----
    merged = str(Path(outdir) / "30_analysis" / "merged" / "multiaccess_samples.jsonl")
    merge_jsonl_on_root(mon_outs, merged)

    # ---- extract qdisc series (dualpi2 fields) ----
    qdisc_series = str(Path(args.raw_dir) / "qdisc" / f"qdisc_{bn_dev}_series.jsonl")
    extract_qdisc_series(qdisc_mon["out"], qdisc_series)


    # ---- build categorized monitor file lists (optional but useful) ----
    shell_samples = [p for p in mon_outs if "_shell.jsonl" in os.path.basename(p)]
    cwnd_c_samples = [p for p in mon_outs if os.path.basename(p).startswith("samples_ue") and os.path.basename(p).endswith("c.jsonl")]
    cwnd_l_samples = [p for p in mon_outs if os.path.basename(p).startswith("samples_ue") and os.path.basename(p).endswith("l.jsonl")]

    # ---- write a standard meta (safe even if you already have write_run_meta) ----
    meta_path = str(Path(outdir) / "00_meta" / "run.json")

    run_dir = Path(outdir).resolve()

    def rel(p: str) -> str:
        try:
            return str(Path(p).resolve().relative_to(run_dir))
        except Exception:
            return p

    meta_all = {
        "run_id": getattr(args, "run_id", None),
        "ts": datetime.now().isoformat(timespec="seconds"),
        "flow_file": args.flow_file,
        "topo_file": getattr(args, "topo_file", None),
        "n_ues": topo["n_ues"],
        "server_ip": server_ip,
        "duration_s": float(dur),
        "interval_s": float(interval),
        "aqm": {
            "type": args.aqm,
            "applied": bool(aqm_applied),
            "rate_mbit": rate_mbit,
            "delay_ms": int(args.bn_delay_ms),
            "burst_kb": int(args.bn_burst_kb),
            "latency_ms": int(args.bn_latency_ms),
            "bottleneck_dev": bn_dev,
            "dualpi2_target_ms": int(getattr(args, "dualpi2_target_ms", 15)),
            "dualpi2_tupdate_ms": int(getattr(args, "dualpi2_tupdate_ms", 16)),
        },
        "ports": sorted(set(ports)),

        # 新增：paths（全部相對）
        "paths": {
            "meta_json": rel(meta_path),
            "qdisc_raw_jsonl": rel(qdisc_mon["out"]),
            "qdisc_series_jsonl": rel(qdisc_series),
            "policy_jsonl": rel(policy_log),
            "merged_samples_jsonl": rel(merged),
            "monitor_outputs": [rel(p) for p in mon_outs],
            "shell_samples": [rel(p) for p in shell_samples],
            "cwnd_samples_classic": [rel(p) for p in cwnd_c_samples],
            "cwnd_samples_l4s": [rel(p) for p in cwnd_l_samples],
            "iperf_json": [rel(str(Path(args.raw_dir)/"iperf"/f"iperf_{x['tag']}.json")) for x in runtime],
            "drp_events_jsonl": rel(drp_log) if drp_log else None,
        },

        # 新增：validation
        "validation": {
            "per_flow_tcpdump_ok": bool(ok_flow),
            "per_flow_tcpdump_detail": flow_detail,
        },
    }

        # ---- generate summary v0 (manifest only) ----
    summary_rel = generate_summary_v0(
        run_dir=run_dir,
        merged_rel=rel(merged),
        qdisc_rel=rel(qdisc_series),
        policy_rel=rel(policy_log),
    )
    meta_all["paths"]["summary_json"] = summary_rel
    report_rel = write_report_summary_v0(run_dir)
    meta_all["paths"]["report_summary_json"] = report_rel

    # ---- export plot TSV (if aligned exists) ----
    try:
        exp = export_plot_tsv_if_aligned_exists(run_dir)
        meta_all["paths"]["plot_tsv"] = exp
        if not exp.get("skipped", False):
            print("[PLOT] wrote:", Path(exp["latency_qdisc_tsv"]).relative_to(run_dir))
            print("[PLOT] wrote:", Path(exp["queue_vs_latency_tsv"]).relative_to(run_dir))
        else:
            print("[PLOT] skipped:", exp.get("reason", ""))
    except Exception as e:
        meta_all["paths"]["plot_tsv"] = {"skipped": True, "reason": f"exception: {e!r}"}
        print("[PLOT] export failed:", repr(e))
    
    # ---- minimal plots ----
    try:
        plots = plot_minimal_for_run(run_dir)
        meta_all["paths"]["plots"] = plots
        print("[PLOT] wrote:", Path(plots["fig_latency_timeseries"]).relative_to(run_dir))
        print("[PLOT] wrote:", Path(plots["fig_queue_vs_latency"]).relative_to(run_dir))
    except Exception as e:
        meta_all["paths"]["plots"] = {"skipped": True, "reason": f"{e!r}"}
        print("[PLOT] skipped:", repr(e))

    Path(meta_path).write_text(json.dumps(meta_all, indent=2), encoding="utf-8")

    print("\n=== DONE ===")
    print("run_id:", getattr(args, "run_id", None))
    print("outdir:", outdir)
    print("merged jsonl:", merged)
    print("qdisc raw:", qdisc_mon["out"])
    print("qdisc series:", qdisc_series)
    print("policy log:", policy_log)

    return meta_all


# ===== iperf related =====
def start_iperf_servers(server, ports: List[int], outdir: str,
                        log_dir: str | None = None,
                        err_dir: str | None = None):
    server.cmd("pkill -9 iperf3 2>/dev/null || true")
    server.cmd(f"mkdir -p {outdir}")

    pids = []

    outdir_p = Path(outdir)
    logp = Path(log_dir) if log_dir else (outdir_p / "20_logs")
    errp = Path(err_dir) if err_dir else (outdir_p / "21_errors")

    (logp / "bg").mkdir(parents=True, exist_ok=True)
    (errp / "bg").mkdir(parents=True, exist_ok=True)

    for p in ports:
        out = str(logp / "bg" / f"iperf_server_{p}.out")
        err = str(errp / "bg" / f"iperf_server_{p}.err")

        server.cmd(f"rm -f {out} {err} 2>/dev/null || true")
        pid = server.cmd(
            f"bash -lc 'iperf3 -s -p {p} >{out} 2>{err} & echo $!'"
        ).strip()

        print(f"[SERVER] port={p} pid={pid}")
        pids.append(pid)

    return pids

# ===== monitor related =====
def start_qdisc_monitor(router, dev: str, outdir: str, duration: float, interval: float = 0.25):
    """
    跟 install_multi_flow_monitor 同寫法：
    1) cat > mon_qdisc_<dev>.py << 'EOF'  落地腳本
    2) test -s + ls -l 驗證腳本存在且非空
    3) 背景啟動：python3 -u script > out 2> err & echo $!
    產出：
      outdir/mon_qdisc_<dev>.py
      outdir/qdisc_<dev>.jsonl
      outdir/qdisc_<dev>.err
    """
    router.cmd(f"mkdir -p {outdir}")

    # scripts snapshot -> 00_meta/mon/
    script_path = str(Path(outdir) / "00_meta" / "mon" / f"mon_qdisc_{dev}.py")
    # raw qdisc -> 10_raw/qdisc/
    outpath = str(Path(outdir) / "10_raw" / "qdisc" / f"qdisc_{dev}.jsonl")
    # stderr -> 21_errors/mon/
    errpath = str(Path(outdir) / "21_errors" / "mon" / f"qdisc_{dev}.err")


    script = """\
import time, subprocess, json

DEV = "__DEV__"
DURATION = float("__DURATION__")
INTERVAL = float("__INTERVAL__")

def read_link_bytes(intf: str):
    rr = subprocess.run(["ip","-s","link","show","dev",intf], capture_output=True, text=True)
    if rr.returncode != 0:
        return None
    lines = [ln.strip() for ln in rr.stdout.splitlines() if ln.strip()]
    rx = tx = None
    for i, ln in enumerate(lines):
        if ln.startswith("RX:") and i+1 < len(lines):
            rx = int(lines[i+1].split()[0])
        if ln.startswith("TX:") and i+1 < len(lines):
            tx = int(lines[i+1].split()[0])
    if rx is None or tx is None:
        return None
    return rx, tx

start = time.time()
while True:
    t = time.time() - start
    if t > DURATION:
        break
    
    r = subprocess.run(["tc","-s","qdisc","show","dev",DEV], capture_output=True, text=True)
    rd = subprocess.run(["tc","-d","-s","qdisc","show","dev",DEV], capture_output=True, text=True)

    bt = read_link_bytes(DEV)
    rx_bytes = bt[0] if bt else None
    tx_bytes = bt[1] if bt else None

    now = time.time()
    print(json.dumps({
        "t": round(t, 6),
        "ts": round(now, 6),
        "dev": DEV,
        "rc": r.returncode,
        "stdout": r.stdout,
        "stderr": r.stderr,
        "stdout_d": rd.stdout,
        "stderr_d": rd.stderr,
        "rx_bytes": rx_bytes,
        "tx_bytes": tx_bytes,
    }), flush=True)

    time.sleep(INTERVAL)
"""
    script = (script
          .replace("__DEV__", dev)
          .replace("__DURATION__", str(duration))
          .replace("__INTERVAL__", str(interval)))


    # 1) 落地腳本（完全同 install_multi_flow_monitor 的 'EOF'）
    router.cmd(f"cat > {script_path} << 'EOF'\n{script}\nEOF")

    # 2) 清舊檔
    router.cmd(f"rm -f {outpath} {errpath} 2>/dev/null || true")

    # 3) 驗證腳本存在且非空（關鍵！）
    chk = router.cmd(
        f"bash -lc '"
        f"test -s {script_path} && echo OK_SCRIPT || echo BAD_SCRIPT; "
        f"ls -l {script_path}"
        f"'"
    ).strip()

    # 4) 背景啟動
    pid = router.cmd(
        f"bash -lc 'python3 -u {script_path} > {outpath} 2> {errpath} & echo $!'"
    ).strip()

    return {"pid": pid, "out": outpath, "err": errpath, "script": script_path, "chk": chk}




def start_ueshell_policy_monitor(net, topo, outdir: str,
                                   duration: float,
                                   interval: float = 0.5,
                                   table: str = "ma_mark",
                                   chain: str = "MA_MARK"):
    """
    Background policy monitor (root process) using mnexec to enter each ue_shell netns.

    Output JSONL format is compatible with your verify_flow_access_mapping():
      {"t":..., "ues": {"ue1_shell": {"tx": {...}, "nft": "..."}, ...}}

    Returns dict like start_qdisc_monitor:
      {"pid","out","err","script","chk"}
    """
    Path(outdir).mkdir(parents=True, exist_ok=True)

    script_path = str(Path(outdir) / "00_meta" / "mon" / "mon_ueshell_policy.py")
    outpath     = str(Path(outdir) / "10_raw" / "policy" / "ueshell_policy.jsonl")
    errpath     = str(Path(outdir) / "21_errors" / "mon" / "ueshell_policy.err")

    access_list = list(topo["access_list"])
    n_ues = int(topo["n_ues"])

    # Build map: ue_shell name -> pid + access intf names
    uemap = {}
    for ue in range(1, n_ues + 1):
        h = net.get(f"ue{ue}_shell")
        # Mininet Host object normally has .pid
        pid = getattr(h, "pid", None)
        if pid is None:
            # fallback: get shell pid inside netns (rarely needed)
            pid_txt = h.cmd("bash -lc 'echo $$'").strip()
            pid = int(pid_txt) if pid_txt.isdigit() else None

        acc_intfs = {}
        for acc in access_list:
            intf = topo["linkmap"]["ue_shell_to_r"][ue][acc]["ue_intf"]
            acc_intfs[acc] = intf

        uemap[f"ue{ue}_shell"] = {"pid": int(pid), "intfs": acc_intfs}

    uemap_json = json.dumps(uemap, ensure_ascii=False)

    # The script uses mnexec -a <pid> to run commands inside each ue_shell netns
    script = f"""\
import time, json, subprocess, re, sys

UEMAP = {uemap_json}
DURATION = float({float(duration)})
INTERVAL = float({float(interval)})
TABLE = {json.dumps(str(table))}
CHAIN = {json.dumps(str(chain))}

# filter nft output lines (like your egrep)
KEEP_PAT = re.compile(r"(counter|tcp dport|ct direction reply|return|jump)")

def mnexec_run(pid: int, cmd_list, timeout=1.0):
    # cmd_list is like ["cat", "/sys/..."]
    p = subprocess.run(["mnexec","-a",str(pid)] + cmd_list,
                       capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr

def read_tx_bytes(pid: int, intf: str):
    rc, out, err = mnexec_run(pid, ["cat", f"/sys/class/net/{{intf}}/statistics/tx_bytes"], timeout=0.5)
    s = (out or "").strip()
    return int(s) if s.isdigit() else None

def read_nft_snippet(pid: int):
    rc, out, err = mnexec_run(pid, ["nft","-a","list","chain","ip",TABLE,CHAIN], timeout=1.0)
    if rc != 0:
        return ""
    lines = []
    for ln in (out or "").splitlines():
        if KEEP_PAT.search(ln):
            lines.append(ln.rstrip())
    return "\\n".join(lines).strip()

t0 = time.time()
with open({json.dumps(outpath)}, "w", encoding="utf-8") as f:
    while True:
        t = time.time() - t0
        if t > DURATION:
            break
        
        
        now = time.time()
        row = {{"t": round(t, 6), "ts": round(now, 6), "ues": {{}}}}

        for ue_name, cfg in UEMAP.items():
            pid = int(cfg["pid"])
            intfs = cfg["intfs"]

            tx = {{}}
            for acc, intf in intfs.items():
                tx[acc] = {{"intf": intf, "tx_bytes": read_tx_bytes(pid, intf)}}

            nft_txt = read_nft_snippet(pid)

            row["ues"][ue_name] = {{"tx": tx, "nft": nft_txt}}

        f.write(json.dumps(row, ensure_ascii=False) + "\\n")
        f.flush()
        time.sleep(INTERVAL)
"""

    # 1) 落地腳本
    Path(script_path).write_text(script, encoding="utf-8")

    # 2) 清舊檔
    subprocess.run(["bash","-lc", f"rm -f {outpath} {errpath} 2>/dev/null || true"])

    # 3) 驗證腳本存在且非空（關鍵！）
    chk = subprocess.run(
        ["bash","-lc", f"test -s {script_path} && echo OK_SCRIPT || echo BAD_SCRIPT; ls -l {script_path}"],
        capture_output=True, text=True
    ).stdout.strip()

    # 4) 背景啟動（同 start_qdisc_monitor 風格）
    pid = subprocess.run(
        ["bash","-lc", f"python3 -u {script_path} > /dev/null 2> {errpath} & echo $!"],
        capture_output=True, text=True
    ).stdout.strip()

    return {"pid": pid, "out": outpath, "err": errpath, "script": script_path, "chk": chk}


def install_multi_flow_monitor(
    host,
    *,
    script_path: str,
    outpath: str,
    duration: float,
    interval: float,
    warmup: float,
    peer_ip: str,
    flows_cfg: dict,
    links_cfg: dict,
):
    """
    Monitor script (per-host) that can:
      - track cwnd per flow (by local_ip + peer_ip + peer_port)
      - track per-interface link bytes/deltas (optional)

    flows_cfg: dict[tag] = {
        "peer_port": <server port>,
        "local_ip": "<local IP on THIS sender host>"   # strongly recommended
    }

    links_cfg: dict[acc] = {"intf": "<intf name>"} or {}.
    """
    flows_json = json.dumps(flows_cfg, ensure_ascii=False)
    links_json = json.dumps(links_cfg, ensure_ascii=False)

    script = r"""\
import time, subprocess, json, re, statistics

ADDR_RE = re.compile(r"(?P<ip>\d+\.\d+\.\d+\.\d+):(?P<port>\d+)")
FIRSTLINE_RE = re.compile(r"^(?P<state>\S+)\s+(?P<recvq>\d+)\s+(?P<sendq>\d+)\s+(?P<local>\S+)\s+(?P<peer>\S+)\s*$")
CWND_RE = re.compile(r"\bcwnd:(\d+)\b")
RTT_RE = re.compile(r"\brtt:([0-9.]+)(?:/([0-9.]+))?\b")


PEER_IP = "__PEER_IP__"
FLOWS = __FLOWS_JSON__
LINKS = __LINKS_JSON__
DURATION = float("__DURATION__")
INTERVAL = float("__INTERVAL__")
WARMUP = float("__WARMUP__")
HOSTNAME = "__HOSTNAME__"

def split_blocks(ss_out: str):
    lines = [ln.rstrip("\n") for ln in ss_out.splitlines()]
    blocks, cur = [], []
    for ln in lines:
        if ln.startswith("State ") or not ln.strip():
            continue
        if FIRSTLINE_RE.match(ln.strip()):
            if cur:
                blocks.append("\n".join(cur))
            cur = [ln]
        else:
            if cur:
                cur.append(ln)
    if cur:
        blocks.append("\n".join(cur))
    return blocks

def read_link_bytes(intf: str):
    r = subprocess.run(["ip","-s","link","show","dev",intf], capture_output=True, text=True)
    if r.returncode != 0:
        return None
    lines = [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]
    rx = tx = None
    for i, ln in enumerate(lines):
        if ln.startswith("RX:") and i+1 < len(lines):
            rx = int(lines[i+1].split()[0])
        if ln.startswith("TX:") and i+1 < len(lines):
            tx = int(lines[i+1].split()[0])
    if rx is None or tx is None:
        return None
    return rx, tx

start = time.time()
prev = {k: {"rx": None, "tx": None} for k in LINKS.keys()}

while True:
    t = time.time() - start
    if t > DURATION:
        break

    ss = subprocess.run(["ss","-tin"], capture_output=True, text=True, timeout=1).stdout

    flows_out = {}

    # Match by: local_ip + peer_ip + peer_port
    for tag, cfg in FLOWS.items():
        want_local_ip = cfg.get("local_ip")
        want_peer_port = int(cfg["peer_port"])

        cwnds = []
        rtts = []
        nmatch = 0

        for b in split_blocks(ss):
            m = FIRSTLINE_RE.match(b.splitlines()[0].strip())
            if not m:
                continue
            mloc = ADDR_RE.match(m.group("local"))
            mpeer = ADDR_RE.match(m.group("peer"))
            if not (mloc and mpeer):
                continue

            lip = mloc.group("ip")
            pip = mpeer.group("ip")
            pport = int(mpeer.group("port"))

            if want_local_ip is not None and lip != want_local_ip:
                continue
            if pip != PEER_IP or pport != want_peer_port:
                continue

            nmatch += 1
            cw = CWND_RE.search(b)
            if cw:
                cwnds.append(int(cw.group(1)))

            rm = RTT_RE.search(b)
            if rm:
                try:
                    rtts.append(float(rm.group(1)))
                except:
                    pass

        cwnd_max = max(cwnds) if cwnds else None
        flows_out[tag] = {
            "cwnd_pkts_max": cwnd_max,
            "cwnd_pkts_sum": (sum(cwnds) if cwnds else None),
            "rtt_ms_median": (statistics.median(rtts) if rtts else None),
            "rtt_ms_max": (max(rtts) if rtts else None),
            "n_conns": nmatch,
            "cwnd_pkts": max(cwnds) if cwnds else None,
            "peer_port": want_peer_port,
            "local_ip": want_local_ip,
        }

    links_out = {}
    for acc, cfg in LINKS.items():
        intf = cfg["intf"]
        bt = read_link_bytes(intf)
        if bt is None:
            links_out[acc] = {"intf": intf}
            continue
        rx, tx = bt
        prx = prev[acc]["rx"]
        ptx = prev[acc]["tx"]
        links_out[acc] = {
            "intf": intf,
            "rx_bytes": rx,
            "tx_bytes": tx,
            "rx_delta": (rx - prx) if prx is not None else 0,
            "tx_delta": (tx - ptx) if ptx is not None else 0,
        }
        prev[acc]["rx"] = rx
        prev[acc]["tx"] = tx

    if t >= WARMUP:
        now = time.time()
        print(json.dumps({
            "t": round(t, 6),
            "ts": round(now, 6),
            "host": HOSTNAME,
            "flows": flows_out,
            "links": links_out
        }), flush=True)

    time.sleep(INTERVAL)
"""

    script = (script
              .replace("__PEER_IP__", str(peer_ip))
              .replace("__FLOWS_JSON__", flows_json)
              .replace("__LINKS_JSON__", links_json)
              .replace("__DURATION__", str(duration))
              .replace("__INTERVAL__", str(interval))
              .replace("__WARMUP__", str(warmup))
              .replace("__HOSTNAME__", str(host.name)))

    host.cmd(f"cat > {script_path} << 'EOF'\n{script}\nEOF")
    host.cmd(f"rm -f {outpath} 2>/dev/null || true")
    return script_path

# ===== utils =====
def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def start_bg(host, cmd: str, tag: str, outdir: str, *, log_dir: str | None = None, err_dir: str | None = None):
    outdir_p = Path(outdir)
    logp = Path(log_dir) if log_dir else (outdir_p / "20_logs")
    errp = Path(err_dir) if err_dir else (outdir_p / "21_errors")

    (logp / "bg").mkdir(parents=True, exist_ok=True)
    (errp / "bg").mkdir(parents=True, exist_ok=True)

    out = str(logp / "bg" / f"{tag}.out")
    err = str(errp / "bg" / f"{tag}.err")

    host.cmd(f"rm -f {out} {err} 2>/dev/null || true")
    pid = host.cmd(f"bash -lc '{cmd} >{out} 2>{err} & echo $!'").strip()

    alive = host.cmd(f"bash -lc 'ps -p {pid} -o pid=,cmd= 2>/dev/null || echo DEAD'").strip()
    print(f"[BG] {host.name} tag={tag} pid={pid} alive_check={alive}")
    return pid, out, err



def load_cc_modules():
    """載入 TCP CC 模組"""
    modules = ['tcp_cubic', 'tcp_bbr', 'tcp_dctcp', 'tcp_reno']
    for mod in modules:
        subprocess.run(['modprobe', mod], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # 嘗試載入 Prague (可能不可用)
    result = subprocess.run(['modprobe', 'tcp_prague'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    prague_available = result.returncode == 0
    
    return prague_available


def get_available_cc():
    """獲取可用的拥塞控制算法"""
    result = subprocess.run(
        ['sysctl', '-n', 'net.ipv4.tcp_available_congestion_control'],
        capture_output=True, text=True
    )
    return result.stdout.strip().split()

def merge_jsonl_on_root(out_paths: list[str], merged_path: str):
    # 在 root host 合併（方便你之後一個檔案讀）
    with open(merged_path, "w", encoding="utf-8") as w:
        for p in out_paths:
            if not os.path.exists(p):
                continue
            with open(p, "r", encoding="utf-8") as r:
                for line in r:
                    if line.strip():
                        w.write(line)
    # 不排序（保持每個 UE 的時間序可能交錯），你要排序再用後處理做


def configure_host(host, cc, ecn=True):
    """配置主機的 TCP 參數"""
    host.cmd(f'sysctl -w net.ipv4.tcp_congestion_control={cc}')
    host.cmd(f'sysctl -w net.ipv4.tcp_ecn={1 if ecn else 0}')
    
    # 對於 DCTCP，需要額外設置
    if cc == 'dctcp':
        host.cmd('sysctl -w net.ipv4.tcp_ecn=1')
        host.cmd('sysctl -w net.ipv4.tcp_ecn_fallback=0')
    
    # 對於 Prague，需要 L4S 設置
    if cc == 'prague':
        host.cmd('sysctl -w net.ipv4.tcp_ecn=3')  # ECN for L4S
        host.cmd('sysctl -w net.ipv4.tcp_ecn_fallback=1')


# DRP helpers
import threading

def _tbf_change_rate(r, dev, rate_mbit, burst_kb, latency_ms):
    cmd = (
        f"tc qdisc change dev {dev} parent 1: handle 2: "
        f"tbf rate {rate_mbit}mbit burst {burst_kb}kb latency {latency_ms}ms"
    )
    r.cmd(cmd)

def _build_ietf_drp(high, low, vlow, step_s):
    events = []
    # t1..t4 jumps: (t_rel, rate)
    events += [(0, low), (40, high), (60, vlow), (80, low)]

    # t5 ramp low -> high (100~120)
    T = 20.0
    n = max(1, int(round(T / step_s)))
    for i in range(1, n + 1):
        t = 100.0 + i * step_s
        rate = low + (high - low) * (i / n)
        events.append((t, rate))

    # t6 ramp high -> low (120~140)
    for i in range(1, n + 1):
        t = 120.0 + i * step_s
        rate = high + (low - high) * (i / n)
        events.append((t, rate))

    # t7 hold low (140~160)
    events.append((140.0, low))
    return events

def start_bn_drp_scheduler(r, dev, outdir, burst_kb, latency_ms, high, low, vlow, step_s, t0_mono):
    events_dir = Path(outdir) / "10_raw" / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    log_path = events_dir / "bn_drp.jsonl"

    events = _build_ietf_drp(high, low, vlow, step_s)

    def worker():
        # 等到共同起跑點
        while True:
            now = time.monotonic()
            dt = t0_mono - now
            if dt <= 0:
                break
            time.sleep(min(0.05, dt))

        for (t_rel, rate) in events:
            target = t0_mono + float(t_rel)
            now = time.monotonic()
            dt = target - now
            if dt > 0:
                time.sleep(dt)

            r_mbit = int(round(rate))
            _tbf_change_rate(r, dev, r_mbit, burst_kb, latency_ms)

            rec = {
                "t": float(t_rel),
                "rate_mbit": int(r_mbit),
                "pattern": "ietf",
                "t0_mono": float(t0_mono),
            }
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec) + "\n")

    th = threading.Thread(target=worker, daemon=True)
    th.start()
    return str(log_path)


def ensure_wait_exec(outdir: str) -> str:
    p = Path(outdir) / "00_meta" / "bin"
    p.mkdir(parents=True, exist_ok=True)
    script = p / "wait_exec.py"
    script.write_text(
        """#!/usr/bin/env python3
import argparse, os, shlex, time, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--t0", type=float, required=True, help="CLOCK_MONOTONIC absolute time")
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    a = ap.parse_args()

    if not a.cmd or a.cmd[0] != "--":
        print("usage: wait_exec.py --t0 <T0> -- <cmd...>", file=sys.stderr)
        return 2
    cmd = a.cmd[1:]
    if not cmd:
        print("empty cmd", file=sys.stderr)
        return 2

    # wait until T0 (monotonic)
    while True:
        dt = a.t0 - time.monotonic()
        if dt <= 0:
            break
        time.sleep(0.001 if dt < 0.05 else 0.01)

    os.execvp(cmd[0], cmd)

if __name__ == "__main__":
    raise SystemExit(main())
""",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return str(script)