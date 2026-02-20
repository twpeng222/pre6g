# pre6g/validate.py

from __future__ import annotations
from typing import Any, Dict
import time, re
from pre6g.sysnet import _sh


def _has(out: str, pat: str) -> bool:
    return re.search(pat, out or "", re.M) is not None

def _nft_counter_hits(nft_list_out: str):
    """
    Parse nft -a list chain output lines like:
      ... counter packets 5 bytes 300 ... # handle 7
    Return total packets sum, and (packets, bytes) list for visibility (minimal).
    """
    pkts = 0
    hits = []
    for m in re.finditer(r"counter packets\s+(\d+)\s+bytes\s+(\d+)", nft_list_out or ""):
        p = int(m.group(1)); b = int(m.group(2))
        pkts += p
        hits.append((p, b))
    return pkts, hits


def _nft_ma_mark_counters(ue_sh, table="ma_mark", chain="MA_MARK"):
    # 只取 counter + dport + reply/return 的行，輸出精簡
    cmd = (
        f"bash -lc \"nft -a list chain ip {table} {chain} 2>/dev/null | "
        "egrep 'counter|tcp dport|ct direction reply|return|jump' || true\""
    )
    return ue_sh.cmd(cmd).strip()


# def dump_ue2_shell_ma_mark_chain(net, nft_table="ma_mark", nft_chain="MA_MARK"):
#     ue2_sh = net.get("ue2_shell")
#     cmd = (
#         f"bash -lc '"
#         f"echo \"=== ue2_shell nft -a list chain ip {nft_table} {nft_chain} ===\"; "
#         f"nft -a list chain ip {nft_table} {nft_chain} 2>&1; "
#         f"echo; "
#         f"echo \"=== (filtered: counter|dport|mark|return|jump) ===\"; "
#         f"nft -a list chain ip {nft_table} {nft_chain} 2>&1 | "
#         f"egrep \"counter|dport|meta mark|ct mark|return|jump\" || true"
#         f"'"
#     )
#     out = ue2_sh.cmd(cmd)
#     print(out)
#     return out

def verify_flow_access_mapping(policy_log_path, flows, access_list=("A", "B", "C"),
                               last_n_samples=30,
                               min_total_bytes=50_000,
                               min_share=0.05,
                               use_parallel_weight=True,
                               strict_fail_on_ratio=False,
                               ratio_tolerance=0.20,
                               verbose=False):
    """
    Verify flow -> access mapping using ue_shell TX bytes monitor (policy_log).

    1) Per-flow PASS/FAIL (routing correctness):
       - For each flow f (ue, access), expected access must carry "significant" bytes:
         exp_bytes >= max(min_total_bytes, total_bytes * min_share)

       This is parallel-safe: we don't require exclusivity.

    2) Ratio comparison (informational):
       - For each UE, compute expected share across accesses based on flows (+parallel weights).
       - Compare with actual share from bytes deltas.
       - By default does NOT fail. You can enable strict_fail_on_ratio if you want.

    Inputs:
      - policy_log_path: outdir/ueshell_policy.jsonl
      - flows: list of dicts, each has at least: {"ue": int, "access": "A|B|C", "tag": str}
               optional: "parallel" (int)
      - access_list: tuple/list of access names

    Returns: dict summary
    """
    import json
    from collections import defaultdict

    # --------------------------
    # Load samples
    # --------------------------
    samples = []
    with open(policy_log_path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                samples.append(json.loads(ln))
            except Exception:
                continue

    if len(samples) < 5:
        print("[VERIFY] Not enough samples in policy_log.")
        return {"ok": False, "reason": "not_enough_samples"}

    samples = samples[-last_n_samples:] if len(samples) > last_n_samples else samples

    # --------------------------
    # Aggregate bytes deltas per UE per access
    # --------------------------
    per_ue_access_bytes = defaultdict(lambda: defaultdict(int))

    prev = None
    for s in samples:
        if prev is None:
            prev = s
            continue

        ues = s.get("ues", {})
        prev_ues = prev.get("ues", {})

        for ue_name, ud in ues.items():
            tx = (ud.get("tx") or {})
            ptx = (prev_ues.get(ue_name, {}).get("tx") or {})

            for a in access_list:
                v1 = (tx.get(a, {}) or {}).get("tx_bytes")
                v0 = (ptx.get(a, {}) or {}).get("tx_bytes")
                if isinstance(v1, int) and isinstance(v0, int):
                    d = v1 - v0
                    if d > 0:
                        per_ue_access_bytes[ue_name][a] += d

        prev = s

    # --------------------------
    # Build per-UE expected weights across accesses (from flows)
    # --------------------------
    per_ue_expect_w = defaultdict(lambda: defaultdict(float))
    per_ue_flowlist = defaultdict(list)

    for f in flows:
        ue = int(f.get("ue"))
        acc = str(f.get("access"))
        tag = str(f.get("tag", f"ue{ue}_{acc}"))
        par = int(f.get("parallel", 1))
        w = float(par if use_parallel_weight else 1.0)

        ue_name = f"ue{ue}_shell"
        if acc not in access_list:
            continue

        per_ue_expect_w[ue_name][acc] += w
        per_ue_flowlist[ue_name].append((tag, acc, par, w))

    # --------------------------
    # Per-flow correctness check
    # --------------------------
    print("\n=== FLOW ACCESS VERIFICATION (parallel-aware) ===")
    ok_all = True
    flow_results = []

    for f in flows:
        ue = int(f["ue"])
        expect = str(f["access"])
        tag = str(f.get("tag", f"ue{ue}_{expect}"))
        ue_name = f"ue{ue}_shell"

        bytes_map = per_ue_access_bytes.get(ue_name, {})
        total = sum(bytes_map.get(a, 0) for a in access_list)
        exp_bytes = bytes_map.get(expect, 0)

        if total <= 0:
            ok = False
            reason = "NO_TRAFFIC_SEEN"
            thr = None
            share = 0.0
        else:
            share = exp_bytes / total
            thr = max(int(min_total_bytes), int(total * float(min_share)))
            ok = exp_bytes >= thr
            reason = "OK" if ok else f"LOW_BYTES(thr={thr})"

        ok_all &= ok
        flow_results.append({
            "ue_shell": ue_name,
            "tag": tag,
            "expect": expect,
            "total_bytes": total,
            "expect_bytes": exp_bytes,
            "expect_share": share,
            "threshold": thr,
            "ok": ok,
            "reason": reason
        })

        dominant = None
        if total > 0:
            dominant = max(access_list, key=lambda a: bytes_map.get(a, 0))

        print(f"{ue_name} flow {tag}: expect={expect} "
              f"exp_bytes={exp_bytes} share={share:.2f} "
              f"dominant={dominant}  {'OK' if ok else 'WRONG'}"
              f"{'' if ok else '  ('+reason+')'}")

        if verbose:
            print("  bytes_by_access:", {a: bytes_map.get(a, 0) for a in access_list})

    # --------------------------
    # Ratio check per UE (informational by default)
    # --------------------------
    print("\n=== UE-LEVEL RATIO CHECK (informational) ===")
    ratio_ok_all = True
    ratio_results = []

    for ue_name, wmap in per_ue_expect_w.items():
        wtot = sum(wmap.get(a, 0.0) for a in access_list)
        bytes_map = per_ue_access_bytes.get(ue_name, {})
        btot = sum(bytes_map.get(a, 0) for a in access_list)

        if wtot <= 0:
            continue  # no expected info
        if btot <= 0:
            print(f"{ue_name}: actual traffic=0 (skip ratio)")
            ratio_results.append({"ue_shell": ue_name, "skip": True, "reason": "no_actual_bytes"})
            continue

        exp_share = {a: (wmap.get(a, 0.0) / wtot) for a in access_list}
        act_share = {a: (bytes_map.get(a, 0) / btot) for a in access_list}
        diff = {a: (act_share[a] - exp_share[a]) for a in access_list}

        # optional strict check
        ratio_ok = True
        if strict_fail_on_ratio:
            for a in access_list:
                if abs(diff[a]) > ratio_tolerance:
                    ratio_ok = False
                    break
        ratio_ok_all &= ratio_ok

        print(f"{ue_name}: "
              f"expected={{{', '.join([f'{a}:{exp_share[a]:.2f}' for a in access_list])}}}  "
              f"actual={{{', '.join([f'{a}:{act_share[a]:.2f}' for a in access_list])}}}  "
              f"{'OK' if ratio_ok else 'OFF'}")

        ratio_results.append({
            "ue_shell": ue_name,
            "expected_share": exp_share,
            "actual_share": act_share,
            "diff": diff,
            "ok": ratio_ok
        })

    print("\nRESULT:", "PASS" if ok_all else "FAIL",
          "(ratio_check:", "OK" if ratio_ok_all else "OFF", ")")
    return {
        "ok": ok_all,
        "ratio_ok": ratio_ok_all,
        "flow_results": flow_results,
        "ratio_results": ratio_results,
        "per_ue_access_bytes": {k: dict(v) for k, v in per_ue_access_bytes.items()},
    }



def auto_acceptance(net, topo, server_ip: str, *,
                    ue_list=None,
                    access_list=None,
                    nft_table="ma_mark",
                    nft_chain="MA_MARK",
                    marks=(1, 2),
                    step2_require_nft=True,
                    step3_require_iprule=True,
                    verbose=False):
    """
    自動驗收 Step2/3（Step1 已刪除）
    - Step2: nft counter 會動（可選：要求 table/chain 存在）
    - Step3: (A) ip rule 真的有 fwmark -> lookup table
             (B) ip route get <server_ip> mark <m> 的 dev 對應正確 access intf
             (C) 可選：檢查 main table default 只有一條（避免 ECMP 假分流）
    """

    if ue_list is None:
        ue_list = list(range(1, topo["n_ues"] + 1))
    if access_list is None:
        access_list = topo["access_list"]

    def ue_shell_name(ue): return f"ue{ue}_shell"
    def ue_access_intf(ue, acc):
        return topo["linkmap"]["ue_shell_to_r"][ue][acc]["ue_intf"]

    # --------------------------
    # STEP 2: nft classification counter moves
    # --------------------------
    step2_ok = True
    step2_detail = []

    for ue in ue_list:
        ue_sh = net.get(ue_shell_name(ue))

        nft_rc = _sh(ue_sh, "bash -lc 'command -v nft >/dev/null 2>&1; echo RC:$?'").strip()
        nft_exist = ("RC:0" in nft_rc)

        if not nft_exist:
            if step2_require_nft:
                step2_ok = False
            step2_detail.append({"ue": ue, "nft_exist": False, "reason": "NO_NFT"})
            continue

        tbl_out = _sh(ue_sh, f"bash -lc 'nft list table ip {nft_table} 2>/dev/null || true'")
        chain_out = _sh(ue_sh, f"bash -lc 'nft -a list chain ip {nft_table} {nft_chain} 2>/dev/null || true'")

        tbl_exist = (tbl_out.strip() != "")
        chain_exist = (chain_out.strip() != "")

        pkts_sum, hits = _nft_counter_hits(chain_out)

        ok = True
        if step2_require_nft:
            ok = ok and tbl_exist and chain_exist
        ok = ok and (pkts_sum > 0)

        step2_ok = step2_ok and ok
        step2_detail.append({
            "ue": ue,
            "nft_exist": True,
            "table": tbl_exist,
            "chain": chain_exist,
            "counter_pkts_sum": pkts_sum,
            "counter_samples": hits[:6],
        })
        if verbose:
            step2_detail[-1]["chain_raw"] = chain_out[-1200:]

    # --------------------------
    # STEP 3: policy routing mark -> correct dev
    # --------------------------
    step3_ok = True
    step3_detail = []

    default_mark_to_acc = {1: "A", 2: "B", 3: "C"}  # 你目前就是這樣設計

    def _has_fwmark_lookup_rules(ip_rule_out: str, marks_tuple):
        """
        用更可靠的方式判斷：有沒有出現 fwmark X lookup <table>
        同時支援 `fwmark 0x1` / `fwmark 1` 兩種顯示。
        """
        s = ip_rule_out or ""
        for m in marks_tuple:
            # ip rule 常見輸出：fwmark 0x1 lookup 100
            # 或：fwmark 1 lookup 100
            pat1 = rf"\bfwmark\s+0x{m:x}\b.*\blookup\b"
            pat2 = rf"\bfwmark\s+{m}\b.*\blookup\b"
            if re.search(pat1, s) or re.search(pat2, s):
                return True
        return False

    def _main_default_count(host):
        """
        看 main table default 有幾條，避免 ECMP 假分流：
        若 >1，代表你可能沒清乾淨 main default。
        """
        out = _sh(host, "bash -lc \"ip route show table main | awk '$1==\\\"default\\\"{c++} END{print c+0}'\"")
        out = (out or "").strip()
        return int(out) if out.isdigit() else None

    for ue in ue_list:
        ue_sh = net.get(ue_shell_name(ue))

        iprule_out = _sh(ue_sh, "bash -lc 'ip -o rule show 2>/dev/null || ip rule show || true'")
        has_fwmark_rules = _has_fwmark_lookup_rules(iprule_out, marks)

        main_def_cnt = _main_default_count(ue_sh)

        # 如果你要求 Step3 必須有 iprule，但又沒看到 fwmark -> FAIL
        if step3_require_iprule and (not has_fwmark_rules):
            step3_ok = False

        per_mark = {}
        for m in marks:
            route_out = _sh(ue_sh, f"ip route get {server_ip} mark {m} 2>/dev/null || true").strip()
            dev = None
            mm = re.search(r"\bdev\s+(\S+)", route_out)
            if mm:
                dev = mm.group(1)

            exp_acc = default_mark_to_acc.get(m)
            exp_dev = ue_access_intf(ue, exp_acc) if exp_acc in access_list else None

            ok = (dev is not None) and (exp_dev is not None) and (dev == exp_dev)
            per_mark[m] = {
                "dev": dev,
                "expected_dev": exp_dev,
                "ok": ok
            }
            if verbose:
                per_mark[m]["route_get"] = route_out

            step3_ok = step3_ok and ok

        step3_detail.append({
            "ue": ue,
            "has_fwmark_rules": has_fwmark_rules,
            "main_default_count": main_def_cnt,
            "marks": per_mark,
        })

    # --------------------------
    # Print minimal report
    # --------------------------
    def _pf(b): return "PASS" if b else "FAIL"

    print("\n=== AUTO ACCEPTANCE REPORT ===")

    print(f"[STEP 2] nft classification counter moves: {_pf(step2_ok)}")
    for d in step2_detail:
        ue = d["ue"]
        if not d.get("nft_exist", False):
            print(f"  ue{ue}_shell: NO_NFT")
            continue
        print(f"  ue{ue}_shell: table={d['table']} chain={d['chain']} counter_pkts_sum={d['counter_pkts_sum']} samples={d['counter_samples']}")

    print(f"\n[STEP 3] mark -> table policy routing works: {_pf(step3_ok)}")
    for d in step3_detail:
        ue = d["ue"]
        print(f"  ue{ue}_shell: has_fwmark_rules={d['has_fwmark_rules']} main_default_count={d['main_default_count']}")
        for m, md in d["marks"].items():
            print(f"    mark {m}: dev={md['dev']} expected={md['expected_dev']} ok={md['ok']}")

    all_ok = step2_ok and step3_ok
    print(f"\n[OVERALL] {_pf(all_ok)}")

    return {
        "step2_ok": step2_ok, "step2_detail": step2_detail,
        "step3_ok": step3_ok, "step3_detail": step3_detail,
        "overall_ok": all_ok
    }


# def _tcpdump_count(host, intf: str, bpf: str, seconds: float = 0.8) -> int:
#     """
#     在 host 的某個介面上跑 tcpdump 秒數，回傳命中的封包行數。
#     """
#     sec = max(0.2, float(seconds))
#     cmd = (
#         "bash -lc "
#         + repr(
#             f"timeout {sec:.2f} tcpdump -ni {intf} {bpf} 2>/dev/null | wc -l"
#         )
#     )
#     out = _sh(host, cmd).strip()
#     try:
#         return int(out)
#     except:
#         return 0

def _tcpdump_count(host, intf: str, bpf: str, seconds: float) -> int:
    """
    更穩：直接拿 tcpdump stdout 的封包行數（不要用 | wc -l，避免 1/1/1 假象）
    """
    sec = max(0.3, float(seconds))
    cmd = (
        "bash -lc "
        + repr(
            # -tt 讓每行都從 timestamp 開頭，便於我們可靠計數
            f"timeout {sec:.2f} tcpdump -tt -n -i {intf} {bpf} 2>/dev/null || true"
        )
    )
    out = host.cmd(cmd) or ""
    # 只數「看起來像封包行」：以數字(時間戳)開頭
    n = 0
    for ln in out.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        if ln[0].isdigit():
            n += 1
    return n


def verify_per_flow_access_with_tcpdump(net, topo, runtime, server_ip: str,
                                        warmup: float = 0.8,
                                        sniff_sec: float = 1.2,
                                        min_hits: int = 10,
                                        verbose: bool = True):
    """
    逐條 flow 驗證（parallel-safe）：
    用 (src host = bind_ip) + (dst host=server_ip) + (dst port=server_port)
    來抓該 flow 的所有 parallel connections。
    """

    estimated_check_time = len(runtime) * 3 * sniff_sec
    print(f"[DEBUG] per-flow tcpdump estimated_check_time = {estimated_check_time:.2f}s "
          f"(flows={len(runtime)}, sniff_sec={sniff_sec})")

    access_list = topo["access_list"]
    linkmap = topo["linkmap"]

    def ue_shell(ue): return net.get(f"ue{ue}_shell")
    def acc_intf(ue, acc): return linkmap["ue_shell_to_r"][ue][acc]["ue_intf"]

    def _tcpdump_count(host, intf: str, bpf: str, seconds: float) -> int:
        sec = max(0.3, float(seconds))
        cmd = (
            "bash -lc "
            + repr(f"timeout {sec:.2f} tcpdump -ni {intf} {bpf} 2>/dev/null | wc -l")
        )
        out = host.cmd(cmd).strip()
        try:
            return int(out)
        except:
            return 0
    
    def sender_host(ue, kind: str):
        # runtime 由 experiment.py 建的：classic -> ue{ue}c, l4s -> ue{ue}l
        if str(kind).lower() == "classic":
            return net.get(f"ue{ue}c")
        else:
            return net.get(f"ue{ue}l")

    time.sleep(max(0.0, float(warmup)))

    print("\n=== PER-FLOW ACCESS CHECK (tcpdump on ue_shell, parallel-safe) ===")

    ok_all = True
    results = []

    for x in runtime:
        ue = int(x["ue"])
        expect_acc = x["access"]
        tag = x.get("tag", f"ue{ue}_{expect_acc}")

        bind_ip = x.get("bind_ip") or x.get("local_ip")  # 你 runtime 裡最好放 bind_ip
        sport = int(x["port"])  # server port

        if not bind_ip:
            print(f"{tag}: NO bind_ip in runtime (need bind_ip/local_ip).")
            ok_all = False
            continue

        sh = ue_shell(ue)

        # ✅ 抓整條 flow：src host=bind_ip + dst host=server_ip + dst port=sport
        bpf = f"\"tcp and dst host {server_ip} and dst port {sport}\""

        counts = {}
        for acc in access_list:
            intf = acc_intf(ue, acc)
            counts[acc] = _tcpdump_count(sh, intf, bpf, seconds=sniff_sec)

        # ---- DEBUG: if looks like NO_DATA (e.g., 1/1/1) or expect has too few hits, dump TCP state ----
        max_hit = max(counts.values()) if counts else 0
        exp_hit = counts.get(expect_acc, 0)
        looks_no_data = (max_hit <= 2)  # 典型 1/1/1

        if looks_no_data or (exp_hit < min_hits):
            try:
                snd = sender_host(ue, x.get("kind", "classic"))
                srv = net.get("server")

                print(f"[DBG][{tag}] tcpdump looks NO_DATA={looks_no_data} exp_hit={exp_hit} max_hit={max_hit}")
                print(f"[DBG][{tag}] sender={snd.name} bind_ip={bind_ip} -> server={server_ip}:{sport}")

                # sender 端：看是否有 ESTAB / SYN-SENT / bytes_acked / cwnd / rtt
                print(f"[DBG][{tag}] SENDER ss -tin (filter by dst {server_ip} and dport {sport})")
                print(snd.cmd(
                    "bash -lc "
                    + repr(f"ss -tin dst {server_ip} | egrep 'ESTAB|SYN-SENT|dport:{sport}|:{sport}' -n || true")
                ).strip() or "(empty)")

                # server 端：看 iperf3 是否在 listen（以及是否有該 port）
                print(f"[DBG][{tag}] SERVER ss -ltnp | grep iperf3")
                print(srv.cmd("bash -lc " + repr("ss -ltnp | grep iperf3 || true")).strip() or "(empty)")

                print(f"[DBG][{tag}] SENDER pgrep iperf3")
                print(snd.cmd("bash -lc 'pgrep -af iperf3 || true'").strip() or "(no iperf3 proc)")


            except Exception as e:
                print(f"[DBG][{tag}] debug dump failed: {e}")


        best_acc = max(counts, key=counts.get) if counts else None
        best_val = counts.get(best_acc, 0) if best_acc else 0
        exp_val = counts.get(expect_acc, 0)

        # ✅ NEW: 判斷是否「根本沒抓到資料」或「三條一樣(平手)」
        max_hit = best_val
        tie = (sum(1 for v in counts.values() if v == max_hit) > 1) if counts else False
        looks_no_data = (max_hit < min_hits)

        if looks_no_data or tie:
            # 不要亂猜 actual=A，直接標記 NO_DATA
            best_acc = "NO_DATA"
            ok = False
        else:
            ok = (exp_val >= min_hits) and (exp_val == best_val)

        ok_all &= ok

        results.append((tag, ue, expect_acc, best_acc, counts, ok))

        if verbose:
            status = "OK" if ok else ("NO_DATA" if best_acc == "NO_DATA" else "WRONG")
            print(f"{tag}: expect={expect_acc} actual={best_acc} hits={counts} => {status}")


    print("\nRESULT:", "PASS" if ok_all else "FAIL")
    return ok_all, results


# ---- iperf3 stdout summary (per-flow bytes) ----

def _parse_iperf_transfer_to_bytes(xfer: str, unit: str) -> int:
    try:
        v = float(xfer)
    except Exception:
        return 0
    u = (unit or "").lower()
    if u.startswith("k"):
        return int(v * 1024)
    if u.startswith("m"):
        return int(v * 1024**2)
    if u.startswith("g"):
        return int(v * 1024**3)
    if u.startswith("t"):
        return int(v * 1024**4)
    # fallback: bytes
    return int(v)

_IPERF_SUM_RE = re.compile(
    r"\]\s+\d+\.\d+-\d+\.\d+\s+sec\s+([0-9.]+)\s+([KMGTP]?Bytes)\s+.*\b(receiver|sender)\b",
    re.I
)

def iperf_out_total_bytes(path: str) -> dict:
    """
    Parse iperf3 text output and return:
      {"bytes_receiver": int, "bytes_sender": int, "raw_line": str}
    Prefer receiver summary if present.
    """
    try:
        txt = open(path, "r", encoding="utf-8", errors="ignore").read()
    except Exception:
        return {"bytes_receiver": 0, "bytes_sender": 0, "raw_line": ""}

    last_sender = None
    last_receiver = None
    for ln in txt.splitlines():
        m = _IPERF_SUM_RE.search(ln)
        if not m:
            continue
        b = _parse_iperf_transfer_to_bytes(m.group(1), m.group(2))
        who = m.group(3).lower()
        if who == "sender":
            last_sender = (b, ln.strip())
        else:
            last_receiver = (b, ln.strip())

    out = {"bytes_receiver": 0, "bytes_sender": 0, "raw_line": ""}
    if last_receiver:
        out["bytes_receiver"] = last_receiver[0]
        out["raw_line"] = last_receiver[1]
    if last_sender:
        out["bytes_sender"] = last_sender[0]
        if not out["raw_line"]:
            out["raw_line"] = last_sender[1]
    return out


def print_flow_traffic_summary(runtime: list[dict], outdir: str):
    """
    Print per-flow total bytes by (UE, kind, access, tag).
    Reads: <outdir>/20_logs/bg/iperf_<tag>.out  (created by start_bg)
    """
    from pathlib import Path
    bg = Path(outdir) / "20_logs" / "bg"
    print("\n=== PER-FLOW TRAFFIC SUMMARY (from iperf stdout) ===")
    any_zero = False

    for x in runtime:
        ue = int(x.get("ue"))
        kind = str(x.get("kind", ""))
        acc = str(x.get("access", ""))
        tag = str(x.get("tag", f"ue{ue}_{acc}_{kind}"))
        port = int(x.get("port", 0))

        p = bg / f"iperf_{tag}.out"
        s = iperf_out_total_bytes(str(p))
        b = s["bytes_receiver"] or s["bytes_sender"]
        if b <= 0:
            any_zero = True

        mb = b / (1024**2) if b else 0.0
        print(f"{tag:18s} ue={ue} kind={kind:7s} access={acc} port={port}  bytes={b} ({mb:.2f} MiB)  file={'OK' if p.exists() else 'MISSING'}")

    if any_zero:
        print("[WARN] Some flows have 0 bytes (client may not have started, ended early, or output missing).")
