from __future__ import annotations
from typing import Any, Dict


def setup_aqm_on_bottleneck(
    r2,
    bottleneck_intf: str,
    aqm_type: str = "none",
    rate_mbit: int | None = None,
    delay_ms: int = 10,
    burst_kb: int = 64,
    latency_ms: int = 50,
    dualpi2_target_ms: int = 15,
    dualpi2_tupdate_ms: int = 16,
):
    """
    root netem (delay)
      └─ tbf (rate shaping)
            └─ AQM
    """

    dev = bottleneck_intf

    # 清掉原有 qdisc
    r2.cmd(f"tc qdisc del dev {dev} root 2>/dev/null || true")

    # 1️⃣ propagation delay
    r2.cmd(f"tc qdisc add dev {dev} root handle 1: netem delay {delay_ms}ms")

    parent = "parent 1:"

    # 2️⃣ rate shaping
    if rate_mbit is not None and rate_mbit > 0:
        r2.cmd(
            f"tc qdisc add dev {dev} {parent} handle 2: "
            f"tbf rate {rate_mbit}mbit burst {burst_kb}kb latency {latency_ms}ms"
        )
        parent = "parent 2:"

    # 3️⃣ AQM
    if aqm_type is None or aqm_type == "none":
        return "none"

    if aqm_type == "dualpi2":
        r2.cmd(
            f"tc qdisc add dev {dev} {parent} handle 10: dualpi2 "
            f"target {int(dualpi2_target_ms)}ms tupdate {int(dualpi2_tupdate_ms)}ms"
        )
        return "dualpi2"

    if aqm_type == "fq_codel":
        r2.cmd(f"tc qdisc add dev {dev} {parent} handle 10: fq_codel limit 10240 target 50ms interval 150ms ecn")
        return "fq_codel"

    if aqm_type == "pie":
        r2.cmd(f"tc qdisc add dev {dev} {parent} handle 10: pie limit 1000 target 20ms tupdate 30ms ecn")
        return "pie"

    if aqm_type == "pfifo":
        r2.cmd(f"tc qdisc add dev {dev} {parent} handle 10: pfifo limit 1000")
        return "pfifo"

    raise ValueError(f"Unknown aqm_type: {aqm_type}")


def configure_phase1_forwarding_all_ues(topo, return_access: str = "A"):
    """
    For all UE i:
      ueic/ueil default -> ueishell (internal /30)
      r routes internal subnets back via ueishell (deterministic return path via return_access)
      enable ip_forward on ueishell and r
      disable rp_filter on all nodes (multi-homing friendly)
    """
    ues = topo["ues"]
    linkmap = topo["linkmap"]
    r = topo["r"]
    server = topo["server"]
    access_list = topo["access_list"]
    assert return_access in access_list


    # sysctls
    r.cmd("sysctl -w net.ipv4.ip_forward=1 >/dev/null")
    for i in ues:
        ues[i]["shell"].cmd("sysctl -w net.ipv4.ip_forward=1 >/dev/null")

    for n in [r, server]:
        n.cmd("sysctl -w net.ipv4.conf.all.rp_filter=0 >/dev/null")
        n.cmd("sysctl -w net.ipv4.conf.default.rp_filter=0 >/dev/null")

    for i in ues:
        ue_c = ues[i]["c"]
        ue_l = ues[i]["l"]
        ue_sh = ues[i]["shell"]

        for n in [ue_c, ue_l, ue_sh]:
            n.cmd("sysctl -w net.ipv4.conf.all.rp_filter=0 >/dev/null")
            n.cmd("sysctl -w net.ipv4.conf.default.rp_filter=0 >/dev/null")

        # internal /30: ue_shell is .1, ue_c/ue_l is .2 (per your configure_min_ip_only)
        c_net = 10 + i
        l_net = 20 + i

        c_dev = linkmap["ue_internal"][i]["c"]["ue_intf"]
        l_dev = linkmap["ue_internal"][i]["l"]["ue_intf"]

        ue_c.cmd(f"ip route replace default via 10.0.{c_net}.1 dev {c_dev}")
        ue_l.cmd(f"ip route replace default via 10.0.{l_net}.1 dev {l_dev}")

        # deterministic return path (r -> ue internal) via ue_shell on return_access
        idx = access_list.index(return_access)
        base = 10 if return_access == "A" else 20 if return_access == "B" else 30
        k = (i - 1) * len(access_list) + idx
        net = 4 * k
        r_acc_dev = linkmap["ue_shell_to_r"][i][return_access]["r_intf"]
        ue_acc_ip = f"10.0.{base}.{net+2}"  # ue_shell ip on that access

        r.cmd(f"ip route replace 10.0.{c_net}.0/30 via {ue_acc_ip} dev {r_acc_dev}")
        r.cmd(f"ip route replace 10.0.{l_net}.0/30 via {ue_acc_ip} dev {r_acc_dev}")

        # server return routes (server -> internal subnets) via r
        server.cmd(f"ip route replace 10.0.{c_net}.0/30 via 10.0.40.1")
        server.cmd(f"ip route replace 10.0.{l_net}.0/30 via 10.0.40.1")

    r.cmd("ip route flush cache")
    server.cmd("ip route flush cache")

def configure_phase1b_source_pin_all_ues(topo, fallback_access: str = "A"):
    """
    For each ue{i}_shell, install:
      ip rule from <A_ip> -> table 100
      ip rule from <B_ip> -> table 101
      ip rule from <C_ip> -> table 102
    so iperf3 -B <access_ip> pins the flow.
    """
    ues = topo["ues"]
    linkmap = topo["linkmap"]
    access_list = topo["access_list"]
    assert fallback_access in access_list

    table_base = 100
    pref_base = 1000

    for i in ues:
        ue_sh = ues[i]["shell"]

        # clean old rules (safe even if not exist)
        for idx in range(len(access_list)):
            ue_sh.cmd(f"ip rule del pref {pref_base+idx} 2>/dev/null || true")

        for idx, access in enumerate(access_list):
            ue_dev = linkmap["ue_shell_to_r"][i][access]["ue_intf"]

            base = 10 if access == "A" else 20 if access == "B" else 30
            k = (i - 1) * len(access_list) + idx
            net = 4 * k
            r_ip  = f"10.0.{base}.{net+1}"
            ue_ip = f"10.0.{base}.{net+2}"

            table_id = table_base + idx
            pref = pref_base + idx

            ue_sh.cmd(f"ip route flush table {table_id}")
            ue_sh.cmd(f"ip route add default via {r_ip} dev {ue_dev} table {table_id}")
            ue_sh.cmd(f"ip rule add pref {pref} from {ue_ip}/32 table {table_id}")

        # main-table fallback default (so ue_shell itself can reach server without -B)
        idx = access_list.index(fallback_access)
        base = 10 if fallback_access == "A" else 20 if fallback_access == "B" else 30
        k = (i - 1) * len(access_list) + idx
        net = 4 * k
        r_ip = f"10.0.{base}.{net+1}"
        ue_dev = linkmap["ue_shell_to_r"][i][fallback_access]["ue_intf"]
        ue_sh.cmd(f"ip route replace default via {r_ip} dev {ue_dev}")

        ue_sh.cmd("ip route flush cache")

    # optional: print one ue_shell's rules for sanity
    first_i = sorted(list(ues.keys()))[0]
    print(f"[Phase1b] ue{first_i}_shell ip rule:")
    print(ues[first_i]["shell"].cmd("ip rule show"))



def configure_phase2_mark_routing_all_ues(
    topo,
    flow_access_map=None,
    fallback_access: str = "A",
    server_ip: str = "10.0.40.2",
    nft_table_name: str = "ma_mark",
):
    ues = topo["ues"]
    linkmap = topo["linkmap"]
    access_list = topo["access_list"]
    assert fallback_access in access_list
    if flow_access_map is None:
        flow_access_map = {}

    MARK  = {"A": 1, "B": 2, "C": 3}
    TABLE = {"A": 100, "B": 101, "C": 102}
    PREF  = {"A": 1001, "B": 1002, "C": 1003}

    def _r_ip_for(i: int, a: str) -> str:
        idx = access_list.index(a)
        base = 10 if a == "A" else 20 if a == "B" else 30
        k = (i - 1) * len(access_list) + idx
        net = 4 * k
        return f"10.0.{base}.{net+1}"

    def _install_clean_ma_mark_nft(ue_sh, rules_ports_marks):
        import base64, shlex

        # 0) 先刪舊 table（失敗也沒差）
        ue_sh.cmd(f"bash -lc 'nft delete table ip {nft_table_name} 2>/dev/null || true'")

        # 1) 組 per-port 規則（純 nft 語法）
        per_port_lines = ""
        for (dport, mark) in rules_ports_marks:
            per_port_lines += (
                f"add rule ip {nft_table_name} MA_MARK "
                f"ip daddr {server_ip} tcp dport {int(dport)} "
                f"counter meta mark set {int(mark)}\n"
            )

        # 2) nft script：注意不要有奇怪的前置縮排（nft -f 很挑）
        nft_script = (
    f"""add table ip {nft_table_name}

    add chain ip {nft_table_name} prerouting {{
    type filter hook prerouting priority -150; policy accept;
    }}

    add chain ip {nft_table_name} MA_MARK

    # counter 要放在 jump/return 前面（terminal statement 後面不能再接東西）
    add rule ip {nft_table_name} prerouting counter jump MA_MARK

    # reply direction: 直接 return（避免回程被 policy routing 干擾）
    add rule ip {nft_table_name} MA_MARK ct direction reply counter return

    # restore ct mark -> meta mark
    add rule ip {nft_table_name} MA_MARK counter meta mark set ct mark

    # 已經有 mark 就 return
    add rule ip {nft_table_name} MA_MARK meta mark != 0x00000000 counter return

    # 第一次封包：依 dport 決定 meta mark
    {per_port_lines}
    # 把 meta mark 存到 conntrack（後續同一條 flow 都會復用）
    add rule ip {nft_table_name} MA_MARK meta mark != 0x00000000 counter ct mark set meta mark
    """
        )

        tmp = f"/tmp/{nft_table_name}.nft"

        # 3) 用 base64 寫檔（完全不需要處理引號/反斜線/換行）
        b64 = base64.b64encode(nft_script.encode("utf-8")).decode("ascii")
        ue_sh.cmd(f"bash -lc 'echo {shlex.quote(b64)} | base64 -d > {tmp}'")

        # 4) apply
        out = ue_sh.cmd(f"bash -lc 'nft -f {tmp} 2>&1; echo RC:$?'")
        print(f"[nft {ue_sh.name} apply]\n{out}")

        # 5) 列出確認（直接列 chain counter 最有用）
        print(ue_sh.cmd(f"bash -lc 'nft list table ip {nft_table_name} 2>&1 || true'"))
        print(ue_sh.cmd(f"bash -lc 'nft -a list chain ip {nft_table_name} MA_MARK 2>&1 || true'"))


    for i in ues:
        ue_sh = ues[i]["shell"]

        per_ue = flow_access_map.get(i, {}) or {}
        rules_ports_marks = []
        for port, acc in per_ue.items():
            rules_ports_marks.append((int(port), MARK[acc]))

        # ✅ 建議關 rp_filter（避免非對稱路由時被丟）
        ue_sh.cmd("sysctl -w net.ipv4.conf.all.rp_filter=0 >/dev/null 2>&1 || true")
        ue_sh.cmd("sysctl -w net.ipv4.conf.default.rp_filter=0 >/dev/null 2>&1 || true")

        # 1) 清 ip rules / tables
        for a in access_list:
            ue_sh.cmd(f"ip rule del pref {PREF[a]} 2>/dev/null || true")
            ue_sh.cmd(f"ip route flush table {TABLE[a]} 2>/dev/null || true")

        # ✅ 清掉主路由 default，避免 ECMP/殘留
        ue_sh.cmd("ip route del default 2>/dev/null || true")

        # 2) 建 per-access tables + fwmark rules
        for a in access_list:
            ue_dev = linkmap["ue_shell_to_r"][i][a]["ue_intf"]
            r_ip = _r_ip_for(i, a)
            ue_sh.cmd(f"ip route replace default via {r_ip} dev {ue_dev} table {TABLE[a]}")
            ue_sh.cmd(f"ip rule add pref {PREF[a]} fwmark {MARK[a]} lookup {TABLE[a]} 2>/dev/null || true")

        # 3) fallback default route (main table)
        fb_dev = linkmap["ue_shell_to_r"][i][fallback_access]["ue_intf"]
        fb_rip = _r_ip_for(i, fallback_access)
        ue_sh.cmd(f"ip route replace default via {fb_rip} dev {fb_dev}")
        ue_sh.cmd("ip route flush cache")

        # 4) nft
        _install_clean_ma_mark_nft(ue_sh, rules_ports_marks)

        # 5) ✅ 強制 sanity：ip rule 必須真的有 fwmark
        print(f"\n=== ue{i}_shell ip rule sanity ===")
        rules = ue_sh.cmd("ip -o rule show | egrep 'fwmark|lookup 100|lookup 101|lookup 102' || true")
        print(rules if rules.strip() else "(NO fwmark rules seen!)")
        print(ue_sh.cmd("ip route get 10.0.40.2 mark 1 || true"))
        print(ue_sh.cmd("ip route get 10.0.40.2 mark 2 || true"))
        print(ue_sh.cmd("ip route get 10.0.40.2 mark 3 || true"))



def configure_min_ip_only(topo):
    """
    Stage-1: only assign IPs on each link (no routes).
    Goal: verify L2/L3 adjacency on every link.
    """
    r = topo["r"]
    server = topo["server"]
    linkmap = topo["linkmap"]
    n_ues = topo["n_ues"]
    access_list = topo["access_list"]
    ues = topo["ues"]

    def add(n, dev, cidr):
        n.cmd(f"ip addr flush dev {dev}")
        n.cmd(f"ip addr add {cidr} dev {dev}")
        n.cmd(f"ip link set {dev} up")

    # r <-> server (use /30)
    r_dev = linkmap["r_to_server"]["r_intf"]
    s_dev = linkmap["r_to_server"]["server_intf"]
    add(r, r_dev, "10.0.40.1/30")
    add(server, s_dev, "10.0.40.2/30")
    server.cmd(f"ip route replace default via 10.0.40.1 dev {s_dev}")

    # per-UE internal links (use /30 too, simple)
    for i in range(1, n_ues + 1):
        ue_c = ues[i]["c"]
        ue_l = ues[i]["l"]
        ue_sh = ues[i]["shell"]


        # you can still use _intf_between for internal (safe), or build an internal linkmap too.
        c_dev = linkmap["ue_internal"][i]["c"]["ue_intf"]
        sh_c  = linkmap["ue_internal"][i]["c"]["sh_intf"]
        l_dev = linkmap["ue_internal"][i]["l"]["ue_intf"]
        sh_l  = linkmap["ue_internal"][i]["l"]["sh_intf"]


        # c<->shell: 10.0.(10+i).0/30 => shell=.1, c=.2
        add(ue_sh, sh_c, f"10.0.{10+i}.1/30")
        add(ue_c,  c_dev,  f"10.0.{10+i}.2/30")

        # l<->shell: 10.0.(20+i).0/30 => shell=.1, l=.2
        add(ue_sh, sh_l, f"10.0.{20+i}.1/30")
        add(ue_l,  l_dev, f"10.0.{20+i}.2/30")

        if i == 1:
            print("[MAP] UE1 c(sh,ue)=", linkmap["ue_internal"][1]["c"]["sh_intf"], linkmap["ue_internal"][1]["c"]["ue_intf"],
      " l(sh,ue)=", linkmap["ue_internal"][1]["l"]["sh_intf"], linkmap["ue_internal"][1]["l"]["ue_intf"])
            print("[MAP] UE1 A/B/C sh->r:", {a: linkmap["ue_shell_to_r"][1][a]["ue_intf"] for a in access_list})




    # per-UE access links (UE shell <-> r): /30 per (UE,access)
    # block = 4*( (i-1)*len(access_list) + idx )
    for i in range(1, n_ues + 1):
        for idx, a in enumerate(access_list):
            ue_dev = linkmap["ue_shell_to_r"][i][a]["ue_intf"]
            r_dev  = linkmap["ue_shell_to_r"][i][a]["r_intf"]

            base = 10 if a == "A" else 20 if a == "B" else 30
            k = (i - 1) * len(access_list) + idx
            net = 4 * k

            # r = .1, ue = .2
            add(r, topo["linkmap"]["ue_shell_to_r"][i][a]["r_intf"],  f"10.0.{base}.{net+1}/30")
            add(ues[i]["shell"], ue_dev,                              f"10.0.{base}.{net+2}/30")
    ue1sh = topo["ues"][1]["shell"]
    print("[DBG] ue1_shell link state:\n", ue1sh.cmd("ip -br link"))
    print("[DBG] ue1_shell addr:\n", ue1sh.cmd("ip -br a"))
    print("[DBG] ue1_shell eth0 detail:\n", ue1sh.cmd("ip -d link show ue1_shell-eth0"))