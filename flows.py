# pre6g/flows.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional
from pre6g.topo import DEFAULT_ACCESS_LIST, DEFAULT_ACCESS_PARAMS, ACCESS_IDX

def load_flow_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if "flows" not in cfg or not isinstance(cfg["flows"], list):
        raise ValueError("flow-file must contain a list field: flows")
    return cfg

def normalize_run_cfg(args, flow_cfg: dict) -> dict:
    # flow-file 可以覆蓋 duration/interval/server_ip
    run = {}
    run["server_ip"] = flow_cfg.get("server_ip", "10.0.40.2")
    run["duration"] = int(flow_cfg.get("duration", args.duration))
    run["interval"] = float(flow_cfg.get("interval", args.interval))
    run["flows"] = flow_cfg["flows"]
    return run

def flow_kind_from_cc(cc: str) -> str:
    # 你也可以在 flow-file 明確寫 kind；這裡只是 fallback
    cc = (cc or "").lower()
    if cc in ("prague", "dctcp"):
        return "l4s"
    return "classic"

def port_for_flow(flow: dict, base: int = 5201) -> int:
    """
    Deterministic unique port per flow:
      base + (ue-1)*100 + access*10 + kind_slot

    kind_slot: classic=0, l4s=1
    - 保證不同 UE / access / classic-vs-l4s 不會撞 port
    - 同時跑多個 flow 不會再出現 control socket unexpected close
    """
    ue = int(flow["ue"])
    access = str(flow["access"]).upper()
    cc = (flow.get("cc", "") or "").lower()
    kind = str(flow.get("kind", flow_kind_from_cc(cc))).lower()

    if access not in ACCESS_IDX:
        raise ValueError(f"bad access: {access}")

    kind_slot = 1 if kind == "l4s" else 0
    return base + (ue - 1) * 100 + ACCESS_IDX[access] * 10 + kind_slot

def build_flow_access_map(flows: list[dict]) -> dict:
    """
    Convert JSON flows into:
        { ue : {server_port : access} }

    Example:
        flows:
          ue1 A classic -> 5201
          ue1 B l4s     -> 5212

        =>
          {1:{5201:"A",5212:"B"}}
    """
    flow_access_map = {}

    for f in flows:
        ue = int(f["ue"])
        access = str(f["access"]).upper()
        port = int(port_for_flow(f))

        flow_access_map.setdefault(ue, {})[port] = access

    return flow_access_map


def tag_for_flow(flow: dict) -> str:
    if "tag" in flow and flow["tag"]:
        return str(flow["tag"])
    # auto tag
    ue = flow["ue"]
    access = flow["access"]
    cc = flow.get("cc", "unknown")
    return f"ue{ue}_{access}_{cc}"

def assign_cports(flow_plan):
    """
    給每個 flow 分配一個固定 client source port（--cport），避免 monitor 誤算 control conn。
    規則：用 tag hash 轉成穩定 port（每次同樣 tag -> 同樣 cport）。
    """
    import hashlib
    used = set()
    for f in flow_plan:
        if "cport" in f and f["cport"]:
            used.add(int(f["cport"]))
            continue

        h = hashlib.md5(f["tag"].encode()).hexdigest()
        base = 40000 + (int(h[:6], 16) % 20000)  # 40000~59999
        cport = base
        while cport in used:
            cport += 1
            if cport > 65000:
                cport = 40000
        f["cport"] = int(cport)
        used.add(int(cport))
    return flow_plan