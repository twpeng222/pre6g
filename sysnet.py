# pre6g/sysnet.py
from __future__ import annotations
from typing import Any, Dict

def _sh(cmd_host, cmd: str) -> str:
    """run cmd on mininet host, return stdout (strip only right side)."""
    try:
        out = cmd_host.cmd(cmd)
        return out
    except Exception as e:
        return f"[EXC] {e}"

def _intf_rx_tx_bytes(host, intf: str):
    rx = host.cmd(f"cat /sys/class/net/{intf}/statistics/rx_bytes 2>/dev/null").strip()
    tx = host.cmd(f"cat /sys/class/net/{intf}/statistics/tx_bytes 2>/dev/null").strip()
    if rx.isdigit() and tx.isdigit():
        return int(rx), int(tx)
    return None


def _intf_tx_bytes(host, intf: str):
    """
    Return TX bytes for a given interface, or None if parse fails.
    """
    bt = _intf_rx_tx_bytes(host, intf)
    return bt[1] if bt else None


def _tx_bytes(host, intf: str):
    """
    Get TX bytes from: ip -s link show dev <intf>
    Return int or None.
    """
    out = _sh(host,
        f"bash -lc \"ip -s link show dev {intf} 2>/dev/null | awk '/TX:/{{getline; print $1}}'\""
    )
    out = (out or "").strip()
    if out.isdigit():
        return int(out)
    return None
