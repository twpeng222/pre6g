# pre6g/topo.py
from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional

from mininet.node import Node
from mininet.link import TCLink
from mininet.link import Link
import json


DEFAULT_ACCESS_LIST = ("A", "B", "C")
ACCESS_IDX = {a: i for i, a in enumerate(DEFAULT_ACCESS_LIST)}
DEFAULT_ACCESS_PARAMS = {
    "A": {"ue_ap": {"bw": 300, "delay": "2ms"}},
    "B": {"ue_ap": {"bw": 100, "delay": "10ms"}},
    "C": {"ue_ap": {"bw": 50,  "delay": "20ms"}},
}

class LinuxRouter(Node):
    """Enable IP forwarding on the router."""
    def config(self, **params):
        super().config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')

    def terminate(self):
        self.cmd('sysctl -w net.ipv4.ip_forward=0')
        super().terminate()

def ue_access_ip(ue: int, access: str, topo) -> str:
    access_list_local = topo["access_list"]
    idx = access_list_local.index(access)
    base = 10 if access == "A" else 20 if access == "B" else 30
    k = (ue - 1) * len(access_list_local) + idx
    net_ = 4 * k
    return f"10.0.{base}.{net_+2}"

def ue_internal_ip(ue: int, kind: str) -> str:
    # kind: "classic" -> ue{ue}c uses 10.0.(10+ue).2
    #       "l4s"     -> ue{ue}l uses 10.0.(20+ue).2
    if kind == "classic":
        return f"10.0.{10+ue}.2"
    else:
        return f"10.0.{20+ue}.2"

def load_topo_profile(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    access_list = tuple(data.get("access_list", DEFAULT_ACCESS_LIST))
    access_params = data.get("access_params", DEFAULT_ACCESS_PARAMS)

    return access_list, access_params

def build_multiaccess_ue_topo(
    net,
    access_list=("A", "B", "C"),
    access_params=None,
    bottleneck_params=None,
    n_ues: int = 1,
    ue_internal_params=None,
    defaults=None,
):
    """
    NEW Topology (real multi-access):
      ue{i}c, ue{i}l -> ue{i}_shell -(A/B/C parallel links)-> r -> server

    - A/B/C are NOT nodes anymore; they are 3 parallel links between ue_shell and router r.
    - Store link interface names explicitly (because _intf_between breaks with parallel links).
    """

    # ---- defaults ----
    if defaults is None:
        defaults = {
            "bw_edge": 1000, "delay_edge": "0.1ms", "loss_edge": 0, "maxq_edge": 1000,
            "bw_access": 300, "delay_access": "2ms", "loss_access": 0, "maxq_access": 1000,
            "bw_bn": 100, "delay_bn": "10ms", "loss_bn": 0, "maxq_bn": 100,
        }
    if access_params is None:
        access_params = {}
    if bottleneck_params is None:
        bottleneck_params = {}
    if ue_internal_params is None:
        ue_internal_params = {}

    def _mk_link_kwargs(p: dict, d_bw, d_delay, d_loss, d_maxq):
        bw = int(p.get("bw", d_bw))
        delay = str(p.get("delay", d_delay))
        maxq = int(p.get("max_queue", p.get("maxq", d_maxq)))
        loss = p.get("loss", d_loss)

        # Dynamic HTB r2q to keep quantum ~ 3000 bytes (about 2 MTU)
        target_quantum = 3000  # bytes
        r2q = int((bw * 1_000_000 / 8 + target_quantum - 1) // target_quantum)  # ceil
        r2q = max(10, min(r2q, 20000))  # clamp

        kw = dict(cls=TCLink, bw=bw, delay=delay, max_queue_size=maxq, r2q=r2q)
        if loss is not None:
            kw["loss"] = float(loss)
        return kw

    # ---- shared nodes ----
    r = net.addHost("r", cls=LinuxRouter, ip=None)        # single router
    server = net.addHost("server", ip=None)

    # ---- per-UE nodes + links ----
    ues = {}
    linkmap = {"ue_shell_to_r": {}, "r_to_server": None}  # store interface names per UE/per access

    for i in range(1, n_ues + 1):
        ue_c = net.addHost(f"ue{i}c", ip=None)
        ue_l = net.addHost(f"ue{i}l", ip=None)
        ue_shell = net.addHost(f"ue{i}_shell", cls=LinuxRouter, ip=None)

        # 3 parallel access links between ue_shell and router r
        linkmap["ue_shell_to_r"][i] = {}
        for a in access_list:
            ap_cfg = access_params.get(a, {})
            leg = ap_cfg.get("ue_ap", ap_cfg)  # reuse your access_params format
            kw_acc = _mk_link_kwargs(
                leg,
                defaults["bw_access"], defaults["delay_access"], defaults["loss_access"], defaults["maxq_access"]
            )

            lk = net.addLink(ue_shell, r, **kw_acc)
            # IMPORTANT: record interface names on both sides
            linkmap["ue_shell_to_r"][i][a] = {
                "ue_intf": lk.intf1.name if lk.intf1.node == ue_shell else lk.intf2.name,
                "r_intf":  lk.intf1.name if lk.intf1.node == r       else lk.intf2.name,
            }
        
        # ---- internal links (CLEAN: use Link, not TCLink) ----
        lk_c = net.addLink(ue_c, ue_shell, cls=Link)
        lk_l = net.addLink(ue_l, ue_shell, cls=Link)

        if "ue_internal" not in linkmap:
            linkmap["ue_internal"] = {}
        linkmap["ue_internal"][i] = {}

        # classic internal
        linkmap["ue_internal"][i]["c"] = {
            "ue_intf": lk_c.intf1.name if lk_c.intf1.node == ue_c else lk_c.intf2.name,
            "sh_intf": lk_c.intf1.name if lk_c.intf1.node == ue_shell else lk_c.intf2.name,
        }

        # l4s internal
        linkmap["ue_internal"][i]["l"] = {
            "ue_intf": lk_l.intf1.name if lk_l.intf1.node == ue_l else lk_l.intf2.name,
            "sh_intf": lk_l.intf1.name if lk_l.intf1.node == ue_shell else lk_l.intf2.name,
        }

        ues[i] = {"c": ue_c, "l": ue_l, "shell": ue_shell}

    # ---- bottleneck r <-> server ----
    kw_bn = _mk_link_kwargs(
        bottleneck_params,
        defaults["bw_bn"], defaults["delay_bn"], defaults["loss_bn"], defaults["maxq_bn"]
    )
    lk_bn = net.addLink(r, server, **kw_bn)
    linkmap["r_to_server"] = {
        "r_intf": lk_bn.intf1.name if lk_bn.intf1.node == r else lk_bn.intf2.name,
        "server_intf": lk_bn.intf1.name if lk_bn.intf1.node == server else lk_bn.intf2.name,
    }

    return {
        "ues": ues,
        "r": r,
        "server": server,
        "access_list": list(access_list),
        "n_ues": n_ues,
        "params": {
            "defaults": defaults,
            "access_params": access_params,
            "bottleneck_params": bottleneck_params,
            "ue_internal_params": ue_internal_params,
        },
        "linkmap": linkmap,  # <--- 之後配 IP 就靠它（避免 _intf_between 爛掉）
    }