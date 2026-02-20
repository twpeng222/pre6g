# pre6g/parse.py

from __future__ import annotations
from typing import Any, Dict
import json, re

# 建議：讓 .*? 可以跨行（更穩）
DUALPI2_RE = {
    "delay":   re.compile(r"delay_c\s+(?P<dc>\d+)us\s+delay_l\s+(?P<dl>\d+)us"),
    "ecn":     re.compile(r"ecn_mark\s+(?P<ecn>\d+)\s+step_mark\s+(?P<step>\d+)"),
    "backlog": re.compile(r"backlog\s+(?P<b_bytes>\d+)b\s+(?P<b_pkts>\d+)p"),
    "pkts_in": re.compile(r"pkts_in_c\s+(?P<pic>\d+)\s+pkts_in_l\s+(?P<pil>\d+)"),
    "prob":    re.compile(r"prob\s+(?P<prob>[0-9.]+)"),
}

TBF_RE = {
    "sent":   re.compile(r"qdisc tbf .*?Sent \d+ bytes \d+ pkt \(dropped (?P<drop>\d+), overlimits (?P<ov>\d+)", re.S),
    "backlog":re.compile(r"qdisc tbf .*?backlog (?P<b_bytes>\d+)b (?P<b_pkts>\d+)p", re.S),
}

FQ_CODEL_RE = {
    "backlog":  re.compile(r"qdisc fq_codel .*?backlog (?P<b_bytes>\d+)b (?P<b_pkts>\d+)p", re.S),
    "drop_over":re.compile(r"qdisc fq_codel .*?Sent \d+ bytes \d+ pkt \(dropped (?P<drop>\d+), overlimits (?P<ov>\d+)", re.S),
    "ecn":      re.compile(r"\becn_mark\s+(?P<ecn>\d+)"),
}

PIE_RE = {
    "backlog":  re.compile(r"qdisc pie .*?backlog (?P<b_bytes>\d+)b (?P<b_pkts>\d+)p", re.S),
    "drop_over":re.compile(r"qdisc pie .*?Sent \d+ bytes \d+ pkt \(dropped (?P<drop>\d+), overlimits (?P<ov>\d+)", re.S),
    "prob":     re.compile(r"\bprob\s+(?P<prob>[0-9.]+)"),
    "delay":    re.compile(r"\bqdelay\s+(?P<qd>[0-9.]+)ms"),
    "ecn":      re.compile(r"\becn_mark\s+(?P<ecn>\d+)"),
}

PFIFO_RE = {
    "backlog":  re.compile(r"qdisc pfifo .*?backlog (?P<b_bytes>\d+)b (?P<b_pkts>\d+)p", re.S),
    "drop_over":re.compile(r"qdisc pfifo .*?Sent \d+ bytes \d+ pkt \(dropped (?P<drop>\d+), overlimits (?P<ov>\d+)", re.S),
}


def split_qdisc_blocks(stdout: str) -> dict:
    """
    把 tc -s qdisc show 的輸出切成每個 qdisc 的 block。
    回傳: {"tbf": "...", "dualpi2": "...", "fq_codel": "...", "pie": "...", "pfifo": "...", ...}
    """
    lines = stdout.splitlines()
    blocks = {}
    cur_name = None
    cur = []

    def flush():
        nonlocal cur_name, cur
        if cur_name and cur:
            blocks[cur_name] = "\n".join(cur) + "\n"
        cur_name, cur = None, []

    for ln in lines:
        if ln.startswith("qdisc "):
            flush()
            parts = ln.split()
            cur_name = parts[1] if len(parts) > 1 else "unknown"
            cur = [ln]
        else:
            if cur_name is not None:
                cur.append(ln)

    flush()
    return blocks


def parse_dualpi2_stdout(stdout: str) -> dict:
    """
    作法2：先把 stdout 切成各個 qdisc 的 block，再分別 parse。
    這樣 fq_codel/pie 的 ecn_mark 不會誤吃 dualpi2 的 ecn_mark。
    """
    out = {}
    blocks = split_qdisc_blocks(stdout)

    # ========== tbf (root shaper) ==========
    # 注意：你的 stdout 可能有 "qdisc tbf ..." 的 root block
    if "tbf" in blocks:
        s = blocks["tbf"]

        mt = TBF_RE["sent"].search(s)
        if mt:
            out["tbf_dropped"] = int(mt.group("drop"))
            out["tbf_overlimits"] = int(mt.group("ov"))

        mt = TBF_RE["backlog"].search(s)
        if mt:
            out["tbf_backlog_bytes"] = int(mt.group("b_bytes"))
            out["tbf_backlog_pkts"] = int(mt.group("b_pkts"))

    # ========== dualpi2 ==========
    if "dualpi2" in blocks:
        s = blocks["dualpi2"]

        m = DUALPI2_RE["delay"].search(s)
        if m:
            out["dualpi2_delay_c_us"] = int(m.group("dc"))
            out["dualpi2_delay_l_us"] = int(m.group("dl"))

        m = DUALPI2_RE["ecn"].search(s)
        if m:
            out["dualpi2_ecn_mark"] = int(m.group("ecn"))
            out["dualpi2_step_mark"] = int(m.group("step"))

        m = DUALPI2_RE["backlog"].search(s)
        if m:
            out["dualpi2_backlog_bytes"] = int(m.group("b_bytes"))
            out["dualpi2_backlog_pkts"] = int(m.group("b_pkts"))

        m = DUALPI2_RE["pkts_in"].search(s)
        if m:
            out["dualpi2_pkts_in_c"] = int(m.group("pic"))
            out["dualpi2_pkts_in_l"] = int(m.group("pil"))

        m = DUALPI2_RE["prob"].search(s)
        if m:
            out["dualpi2_prob"] = float(m.group("prob"))

    # ========== fq_codel ==========
    if "fq_codel" in blocks:
        s = blocks["fq_codel"]

        m = FQ_CODEL_RE["backlog"].search(s)
        if m:
            out["fq_codel_backlog_bytes"] = int(m.group("b_bytes"))
            out["fq_codel_backlog_pkts"] = int(m.group("b_pkts"))

        m = FQ_CODEL_RE["drop_over"].search(s)
        if m:
            out["fq_codel_dropped"] = int(m.group("drop"))
            out["fq_codel_overlimits"] = int(m.group("ov"))

        m = FQ_CODEL_RE["ecn"].search(s)
        if m:
            out["fq_codel_ecn_mark"] = int(m.group("ecn"))

    # ========== pie ==========
    if "pie" in blocks:
        s = blocks["pie"]

        m = PIE_RE["backlog"].search(s)
        if m:
            out["pie_backlog_bytes"] = int(m.group("b_bytes"))
            out["pie_backlog_pkts"] = int(m.group("b_pkts"))

        m = PIE_RE["drop_over"].search(s)
        if m:
            out["pie_dropped"] = int(m.group("drop"))
            out["pie_overlimits"] = int(m.group("ov"))

        m = PIE_RE["prob"].search(s)
        if m:
            out["pie_prob"] = float(m.group("prob"))

        m = PIE_RE["delay"].search(s)
        if m:
            out["pie_qdelay_ms"] = float(m.group("qd"))

        m = PIE_RE["ecn"].search(s)
        if m:
            out["pie_ecn_mark"] = int(m.group("ecn"))

    # ========== pfifo ==========
    if "pfifo" in blocks:
        s = blocks["pfifo"]

        m = PFIFO_RE["backlog"].search(s)
        if m:
            out["pfifo_backlog_bytes"] = int(m.group("b_bytes"))
            out["pfifo_backlog_pkts"] = int(m.group("b_pkts"))

        m = PFIFO_RE["drop_over"].search(s)
        if m:
            out["pfifo_dropped"] = int(m.group("drop"))
            out["pfifo_overlimits"] = int(m.group("ov"))

    return out


def extract_qdisc_series(qdisc_jsonl: str, out_jsonl: str):
    """
    讀 qdisc_<dev>.jsonl（內含 stdout），抽出 dualpi2 指標成乾淨 jsonl。
    """
    with open(qdisc_jsonl, "r", encoding="utf-8") as r, open(out_jsonl, "w", encoding="utf-8") as w:
        for line in r:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            t = obj.get("t")
            ts = obj.get("ts")
            dev = obj.get("dev")
            rc = obj.get("rc")
            stdout = obj.get("stdout", "") or ""
            row = {
                "t": t,
                "ts": ts,
                "dev": dev,
                "rc": rc,
                "rx_bytes": obj.get("rx_bytes"),
                "tx_bytes": obj.get("tx_bytes"),
            }
            if rc == 0 and stdout:
                row.update(parse_dualpi2_stdout(stdout))
            w.write(json.dumps(row) + "\n")