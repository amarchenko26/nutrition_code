# jaravel_food — Jaravel (2019) Table 5 on food cells, the healthiness split, and E1–E3

Replicates the shift-share result of Jaravel (2019, QJE, Table V) on food module × price-decile cells, lets the slope differ
by module healthiness (claude_hi 0–3 vs 4–10), and adds three descriptive exhibits: spending growth by price decile (E1),
barcode health by price decile (E2), and spending growth by healthiness tercile (E3). His code is in `jaravel_replication/`.

Data: every intermediate file goes to `interim/variety/jaravel_food/` (`DATA` in `common.py`). Exhibits go to
`figs|tabs/variety/jaravel_food/` in the Overleaf clone by default (follow the git routine in CLAUDE.md), or to any folder with
`--out DIR`; `--extra` also writes the exhibits that are not in the paper.

## Run order

| Step | Script | Reads | Writes (in `interim/variety/jaravel_food/` unless stated) |
|---|---|---|---|
| 0 (Oscar) | `0_collapse_rms_national.py` via `sbatch 0_run_collapse_rms_national.sh` (1 core, 6 GB, 3 min for all years) | `/users/amarche4/data/rms_variety/rms_upc_fips_spending_<y>.parquet` | `/users/amarche4/scratch/jaravel_food/rms_upc_national_<y>.parquet`; copy the 15 files by hand to `rms_national/` (`scp 'amarche4@ssh.ccv.brown.edu:/users/amarche4/scratch/jaravel_food/rms_upc_national_*.parquet' <interim>/variety/jaravel_food/rms_national/`) |
| 1 | `1_build_acs_groups.py` | `raw/acs/usa_00003.csv` | `acs_groups.parquet` |
| 2 | `2_build_instrument.py` | Homescan purchases 2004–06, panelists, step 1 | `instrument_cells.parquet` |
| 3 | `3_build_hms_upc_years.py` | Homescan purchases 2004–20 | `hms_upc_years.parquet` |
| 4 | `4_build_rms_cells.py 2015` and `4_build_rms_cells.py 2020` | steps 0, 2, 3; `raw/products.tsv` | `cells_2006_<END>/analysis_main.parquet` |
| 4b | `4b_check_figure4_slopes.py 2015` (check, prints) | step 4 | — |
| 5 | `5_export_cells_dta.py 2015` and `2020` | step 4; `interim/what_moves/module_groups.parquet` | `cells_2006_<END>/analysis_main.dta` |
| 6 | `do 6_analyze_table5.do <END> <SIG>`, END 2015 / 2020, SIG s57 (main) and s4, s115, s5 (σ checks) | step 5 | `cells_2006_<END>/results_<SIG>.dta` |
| 7 | `do 7_analyze_weak_iv.do` | step 5 | `ar_checks.dta` |
| 8 | `do 8_analyze_decile_inflation.do <END>` | step 5 | `cells_2006_<END>/descriptive_regs.dta` |
| 9 | `9_make_table5_tables.py [--out DIR] [--extra]` | steps 6–8 | `table5_replication.tex`, `table5_health.tex` (+ `table5_health3.tex`, `decile_inflation_health_<END>.tex` with `--extra`) |
| 10 | `10_build_organic_flag.py` | `raw/consumer/Consumer_Panel_Data_<y>.tgz` (products_extra, streamed), `raw/products.tsv` | `organic_upc.parquet` |
| 11 | `11_build_hms_upc_year.py` (about 17 minutes) | Homescan purchases 2004–20, panelists | `hms_upc_year.parquet` |
| 12 | `12_build_upc_health.py` | Syndigo labels, `raw/products.tsv`, step 10 | `upc_health.parquet` |
| 13 | `13_build_e123_data.py` | steps 0, 4, 11, 12 | `e123/*.dta`, `e123/e3_joint.parquet`, `e123/e3_coverage.parquet`, `e123/rms_upc_year_deciles.parquet` |
| 14 | `do 14_analyze_e123.do` | step 13 | `e123/e123_results.dta` |
| 15 | `15_make_e123_exhibits.py [--out DIR] [--extra]` | steps 13–14 | `e1_growth_by_decile.png/.tex`, `e2_health_by_decile.png/.tex`, `e3_growth_by_health.png/.tex` (+ `e3_joint_hms.tex`, `e3_joint_rms.tex` with `--extra`) |
| 16 | `16_check_e1_fixed_bands.py` (check, prints) | steps 0, 11 | — |
| 17 | `do 17_check_e2_module_year.do` (check, prints) | step 13 | — |
| 18 | `18_check_e3_calorie_density.py` (check, prints) | steps 11–13 | — |

Steps 10–12 do not depend on steps 4–9. Run Stata from a folder outside the repo (it writes its log to the current folder),
and Python with `PYTHONDONTWRITEBYTECODE=1`.

## Rules taken from his code (and where ours differ)

* Cells: every year, a module's barcodes are ranked by national unit value per size unit (size1_amount × multi), each barcode
  counted once, after the pooled p1/p99 trim of size1_amount, and cut into ten equal-count deciles. Continuing-product inflation
  from t to t+1 uses year-t deciles; a new barcode enters the decile of its own price in its first year. A module is kept when
  all ten deciles exist in every year and in the 2004–06 Homescan shares.
* New in t: not seen in Homescan 2004..t−1 or RMS 2006..t−1. Exiting in t: not seen in Homescan or RMS after t, up to the
  window's last year.
* σ: he estimates one σ per module (median 5.7); we use 5.7 for every module (4, 5 and 11.5 as checks).
* Food modules only (our RMS extract); 442 modules (2006–15) and 430 (2006–20) against his 860.
* Homescan spending is not net of coupons (purchases_food has no coupon column).
* His columns 6 and 11 (shift-share standard errors, run in R) are not reproduced.
* RMS coverage: the barcode × county files map stores to counties from the 2006 store list only
  (build/variety/build_product_variety.py), so they hold about 100% of RMS food spending through 2011, 94% in 2015, 90% in 2017
  and 67–69% in 2018–2020. Every exhibit that uses RMS through 2019–20 says so in its caption.
