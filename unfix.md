bottleneck_params change wait

*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.
*** Error: Warning: sch_htb: quantum of class 50001 is big. Consider r2q change.



建議順序（非常重要）
Phase A（現在）

✔ 目錄標準化
✔ stderr 分離
✔ raw 資料搬到 10_raw
✔ run_meta 整併到 00_meta

Phase B（等 analysis pipeline 穩）

✔ 寫 summary.json
✔ 自動畫圖
✔ metrics extraction

Phase C（最後）

✔ 全部 print → logger
✔ logger level 規範
✔ run.log 成為唯一 console 紀錄


pre6g/results/default/20260216/run_20260216_022917_flows_topo_dualpi2_bn20_9ae8



Logging v2：統一輸出目錄（你指定的 results/.../<run_id>）
1) Run 目錄結構（固定）

唯一入口： results/<exp_type>/<YYYYMMDD>/<run_id>/

底下固定長這樣：

00_meta/

run.json：統一 run metadata（唯一真源）

env.json：系統/核心/工具版本（可選）

topo.json：本次實際 topo（展開後，避免只存指向檔）

flows.json：本次實際 flows（展開後）

10_raw/（原始量測，不做不可逆處理）

qdisc_series.jsonl

multi_source/*.jsonl（例如 ue1_shell.jsonl…或你合併後也可放一份 merged）

marks/*.jsonl（你 Phase2 的 nft/txbytes 監測）

pcap/（若有）

20_logs/

run.log / debug.log（沿用你現有 setup_logging()）【

logging

】

21_errors/（stderr 全部來這）

bg/<tag>.err

exceptions.log（python traceback）

30_analysis/（可重算產物）

aligned/（ts alignment 後）

metrics.json（spike / RTT / CE / backlog 指標）

tables/（csv/parquet）

40_figs/

png/*.png

pdf/*.pdf

50_report/

summary.json（自動產生）

report.md（可選：一頁摘要+圖連結）

你現在 run_meta.json 放在根目錄【

experiment

】；v2 會把它併入 00_meta/run.json，並保留向下相容（必要時可同時輸出一份舊檔名，但建議最後移除）。

2) 統一 run metadata（run.json）—論文可追溯必備欄位

00_meta/run.json 建議分 6 塊（你後面要自動 summary/畫圖會很爽）：

identity

run_id, exp_type, date, start_ts_unix, duration_s

inputs

flow_file, topo_file（原檔名）

flows_resolved（展開後 hash/版本）

topo_resolved

network

access_list, access_params

bottleneck: rate/burst/latency, aqm（dualpi2/fq_codel…）

collection

monitors 啟用清單與 interval（multi-source/qdisc/marks/cwnd…）

ts_alignment: reference clock / method

provenance（論文重現關鍵）

git commit / dirty flag

kernel version / sysctl（ECN/CC）

主要工具版本（iperf3, tc, nft）

outputs（run 結束後補寫）

raw 檔案清單（相對路徑）

analysis/figs/summary 產生狀態

3) 自動 summary.json（你目標之一）

50_report/summary.json 只放「看一眼就懂」的結果（可直接貼論文/表格）：

KPI 類：

throughput（mean/p50/p95）

RTT（p50/p95/p99）

queue delay/backlog（p50/p95/p99）

CE marking ratio（mean / peak）

spike 分析（你已驗證的那套）：

spike_count、max_spike_ms、topN spikes（時間點）

run health：

errors count（stderr 有無）、缺檔、解析失敗等

4) analysis pipeline（標準化一條龍）

觸發點： runner.main() 在 run_experiment() 完後呼叫 analyze_run(outdir)。

Pipeline 固定步驟（每步都輸出到 30_analysis/，可重跑）：

ingest: 收集 raw paths（存在性檢查 + 清單寫回 run.json outputs）

align: ts alignment → 30_analysis/aligned/*.parquet(or json)

compute_metrics: 產 metrics.json + tables

render_figs: 自動畫固定圖（下面列）

write_summary: 產 50_report/summary.json

5) 自動圖（論文常用、固定產出）

先做「最必要的 6 張」，每次 run 都會有：

rtt_timeseries（Classic vs L4S / per-flow）

throughput_timeseries（per-flow + per-access）

qdelay/backlog_timeseries（bottleneck qdisc）

ecn_ce_ratio_timeseries（含 dualpi2 / fq_codel 的 CE）

spike_timeline（spike 標記疊在 RTT/qdelay）

access_share_stacked（A/B/C 使用比例）

