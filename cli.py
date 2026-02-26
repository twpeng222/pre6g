# ma3/cli.py
from __future__ import annotations
import sys
import os
import argparse

# 先沿用你舊檔的 main / run_experiment / config 函數
import pre6g.runner as app


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("pre6g")
    # ★把你 newtest_ma3.py 原本 argparse 那段搬過來★
    # 例如：p.add_argument("--flows", ...)
    p.add_argument("--n-ues", type=int, default=1)
    p.add_argument("--no-clean", action="store_true")
    p.add_argument("--duration", type=int, default=12, help="default duration if flow-file not specify")
    p.add_argument("--interval", type=float, default=0.25, help="default monitor interval if flow-file not specify")
    p.add_argument("--flow-file", type=str, required=True, help="JSON file describing flows to run")
    p.add_argument("--topo-file", type=str, required=True, help="JSON file describing topo to run")
    p.add_argument("--qdisc-interval", type=float, default=0.25, help="qdisc monitor interval (sec)")
    p.add_argument("--bn-rate-mbit", type=int, default=20, help="bottleneck rate limit (mbit). use 0 to disable shaping")
    p.add_argument("--bn-burst-kb", type=int, default=64, help="tbf burst in KB (only used when bn-rate-mbit>0)")
    p.add_argument("--bn-delay-ms", type=int, default=10)
    p.add_argument("--bn-latency-ms", type=int, default=50, help="tbf latency (ms), only used when bn-rate-mbit>0")
    p.add_argument("--exp-type", type=str, default="default", help="experiment category (e.g. cwnd_compare, steering_test)")
    p.add_argument("--outdir", type=str, default=None, help="override output directory")
    p.add_argument("--verbose", action="store_true", help="show more info on console")
    p.add_argument("--aqm", type=str, default="none", choices=["none", "dualpi2", "fq_codel", "pie", "pfifo"], help="AQM to apply on shared bottleneck (r->server egress).")
    p.add_argument("--dualpi2-target-ms", type=int, default=15, help="DualPI2 target delay (ms)")
    p.add_argument("--dualpi2-tupdate-ms", type=int, default=16, help="DualPI2 tupdate (ms)")
    p.add_argument("--bn-drp", type=str, default="",
              help="bottleneck DataRate-Pattern. use 'ietf' to enable the paper pattern")
    p.add_argument("--bn-drp-step-s", type=float, default=1.0,
              help="ramp step in seconds (1.0 recommended)")
    return p


def main():
    # 檢查 root 權限
    if os.geteuid() != 0:
        print("請使用 sudo 執行此腳本")
        sys.exit(1)
    
    args = build_parser().parse_args()
    app.main(args)


if __name__ == "__main__":
    main()