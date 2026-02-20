# pre6g/logging.py
from __future__ import annotations
import logging
from pathlib import Path

def setup_logging(outdir: str, verbose: bool = False) -> dict:
    """
    - console: 預設只顯示 stage 進度 (INFO)
    - files: logs/run.log (INFO+), logs/debug.log (DEBUG)
    回傳: {"logdir": Path, "stage": logger, "log": logger}
    """
    outdir_p = Path(outdir)
    logdir = outdir_p / "logs"
    logdir.mkdir(parents=True, exist_ok=True)

    # 兩個 logger：stage (乾淨 console) 與 log (詳細檔案)
    stage = logging.getLogger("pre6g.stage")
    log = logging.getLogger("pre6g.log")

    # 避免重複 handler（例如同一個 python process 重跑）
    for lg in (stage, log):
        lg.handlers.clear()
        lg.propagate = False

    stage.setLevel(logging.INFO)
    log.setLevel(logging.DEBUG)

    # ---- console handler：預設只顯示 stage 行 ----
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    stage.addHandler(ch)

    # ---- file handlers：run.log / debug.log ----
    fh_run = logging.FileHandler(logdir / "run.log")
    fh_run.setLevel(logging.INFO)
    fh_run.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"
    ))

    fh_dbg = logging.FileHandler(logdir / "debug.log")
    fh_dbg.setLevel(logging.DEBUG)
    fh_dbg.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"
    ))

    # 詳細 logger 寫檔
    log.addHandler(fh_run)
    log.addHandler(fh_dbg)

    # 也把 stage 同步寫進 run.log，方便對照
    stage.addHandler(fh_run)

    return {"logdir": logdir, "stage": stage, "log": log}
