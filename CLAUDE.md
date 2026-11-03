# nutrition_code — project guide

Paper: **"Why Don't Americans Eat Better? Income, Innovation, and Nutrition"** (Marchenko, JMP).
Two parts: (1) consumer/PE — causal effect of income on nutrition (leave-one-out income IV); (2) firm/GE
— product innovation skews toward unhealthy food. Data: Nielsen Consumer Panel (HMS) + RMS scanner,
Syndigo labels, USDA FoodData Central (Branded), ACS.

This file has two parts. **Part 1** is how Claude should work in this repo. **Part 2** is reference
material about the code, data, and variables.

---

# PART 1 — HOW CLAUDE WORKS HERE

## Folder locations

### Code

Code lives in the GitHub repo:

* cdir = `/Users/anyamarchenko/Documents/GitHub/nutrition_code`

Every script goes in one of three stage folders, then a short, descriptive topic subfolder:

* `cdir/clean` — raw → tidy, one subfolder per source (`clean/nielsen`, `clean/syndigo`, `clean/usda`)
* `cdir/build` — analytic panels and variables, one subfolder per task (`build/iv`, `build/hi/usda`)
* `cdir/analyze` — regressions and figures, one subfolder per topic (`analyze/hi/usda`, `analyze/labor_supply`)

Before creating a folder, check whether a topic folder already exists; never make a second folder for the
same topic. Filenames are **verb-first and dataset-labelled** (`clean_syndigo`, `merge_nielsen_syndigo`,
`build_hi_panel`, `analyze_variety_price_index`); one job per script; `1_`/`2_` prefixes for ordered steps.
Code is stored only in code files (`.py`, `.do`, `.sh`). Languages are Python and Stata; there is no R.

### Data

Data lives in Dropbox under `nielsen_data`, never in the repo:

* rdir = `/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw`
* idir = `/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim`
* gdir = `/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/final`

`rdir` is input-only and is never written to (replication). `raw/` holds the consumer-panel `.tgz` files,
RMS `products.tsv` and `rms/`, `syndigo/`, `usda/`, `acs/`, `bls/`, `NHANES/`, `ailments/`,
`price_deflator/`. Panelist tsvs are extracted from inside `Consumer_Panel_Data_{year}.tgz` (tar member
`nielsen_extracts/HMS/…`) — an archive path, not a folder.

All data outputs go to `idir` or `gdir`. Cleaned and built data go to `interim/<dataset or task>/`, named
after the dataset or the task (`interim/syndigo`, `interim/panel_dataset`, `interim/labor_supply`,
`interim/movers`). Panels combined from several sources also live in `interim/` under the task name
(`interim/panel_dataset/panel_hh_year.parquet`). `gdir` holds only regression-ready `.dta` files
(`final_reg_data.dta`, `labor_supply_reg_data.dta`).

### Figures and tables

Figures and tables write straight into the Overleaf project (Dropbox-synced):

* fdir = `/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs`
* tdir = `/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/tabs`
* ndir = `/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/notes` (write-ups)

Put new output into the existing subfolder for its topic (`figs/labor_supply`, `figs/movers`,
`figs/variety/price`, `figs/variety/entry`, `tabs/labor_supply`, `tabs/movers`, …). A new line of work gets its
own subfolder, with sub-subfolders when one topic has several strands. We never want duplicative folders, and we do not want duplicative
work. If a request looks like it duplicates an existing figure or table, ask before proceeding and give
the path of the existing file you think it duplicates.

Figures and tables are overwritten in place, never saved under a new name as a near-duplicate. When Anya
retires an analysis and says delete, delete its figures and tables from Overleaf as well as its code and
tex (clarified 2026-11-02); before deleting, check that no deck or other tex file includes them.

### Paths

All full paths are defined in exactly two files at the repo root:

* `cdir/paths.py` — Python (`cdir`, `data`, `rdir`, `idir`, `gdir`, `overleaf`, `fdir`, `tdir`, `ndir`;
  aliases `BASE`, `OVERLEAF`)
* `cdir/paths.do` — Stata globals with the same names (plus aliases `$figs`, `$tabs`, `$FIGS`, `$TABS`, `$PD`)

No other code file contains a full path. New Python scripts bootstrap the import with:

```python
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
```

New `.do` files start with `do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"` — the one
permitted full path in a `.do` file. Existing scripts (93 of them) still hardcode the Dropbox paths; migrate
a script to `paths.py`/`paths.do` whenever you edit it, and do not add new hardcoded paths.

## Figure style

Every figure:

* Fits a portrait or landscape Overleaf page and occupies the full page width.
* Is easy to read: large text (e.g. size 26) with the plot still legible.
* Never uses yellow (two Stata scripts still use `gold`; replace it when you touch them).
* Uses only colorblind-friendly, mutually distinguishable colors.
* Uses the same colors, fonts, and styles as every other figure in the project. Use the existing scheme:
  Python `#2c5f8a` (blue), `#a63d40` (red), `#fc8d59` (orange), `#999999`/`#cccccc` (grays);
  Stata `navy`, `maroon`, `gray`.

## Working with Overleaf

When a figure is saved to `fdir` or a table to `tdir`, also append an include block for it to
`fdir/claude/claude_fig.tex` or `tdir/claude/claude_tab.tex` (both exist; the main file inputs them when
wanted). The block must satisfy the figure rules above (fits a portrait or landscape page, full width).

Write a caption for each. The caption describes how the $x$ and $y$ variables were built, states the
sample and the regression if any, explains stars or symbols, and ends with the source. Short: at most
3 lines. Describe only; no conclusions ("from this we can see that…"). A reader should understand the
exhibit without reading the paper.

### Table block

```
\begin{table}[htbp]
    \centering
    \textsc{Table} \ref{tab:table_path_here} --- \textsc{Title of Table Here}
    \vspace{.2cm}
    \\
    \maxsizebox*{1.2\linewidth}{1\textheight}{%
    \begin{threeparttable}
        \input{tabs/table_path_here}
        \caption{\footnotesize CAPTION TEXT HERE. ALL CAPTIONS SHOULD BE SUCCINCT AND LET THE READER KNOW WHAT IS DISPLAYED IN THE TABLE WITHOUT HAVING TO READ THE REST OF THE PAPER. DESCRIBE THE SAMPLE, THE REGRESSION IF ANY, ANY SIGNIFICANCE STARS OR OTHER NUMBERS OR SYMBOLS. KEEP IT SHORT. \textit{Source:} SOURCES FOR TABLE}
    \label{tab:table_path_here}
    \end{threeparttable}
    }
\end{table}
```

### Figure block

```
\begin{figure}[htbp]
\centering
    \textsc{Figure} \ref{fig:fig_path} --- \textsc{Figure title}
    \vspace{.2cm}
    \\
    \includegraphics[width=\linewidth]{figs/fig_path.png}
    \caption{\footnotesize This figure provides PUT DESCRIPTION OF FIGURE HERE. INCLUDE DESCRIPTION OF WHAT THE FIGURE CONTAINS SO THAT A READER CAN UNDERSTAND IT WITHOUT READING THE PAPER. DO NOT INCLUDE ANY OPINION OR CONCLUSIONS FROM THE FIGURE, SIMPLY DESCRIBE THE FIGURE. \textit{Source:} SOURCES FOR FIGURE. }
    \label{fig:fig_path}
\end{figure}
```

Paths inside these blocks are relative to the Overleaf root (`tabs/…`, `figs/…`). The notes in `ndir`
instead use `\input{../tabs/…}` and `\graphicspath{{../figs/<topic>/}}`.

**WARNING: the Dropbox sync is fragile.** If Anya has been working in Overleaf recently, the Dropbox copy
may be stale, and anything Claude writes there overwrites her Overleaf edits. So only write inside `figs/`
and `tabs/`, and when writing tex stay inside `claude_fig.tex` and `claude_tab.tex`, unless explicitly
directed otherwise (e.g. a named note in `notes/`). Never compile tex locally; Anya compiles.

Overleaf notes (when directed to write one): no summary text and no interpretation. The note is the figures
and tables Anya asked for, in order, plus a short Definitions section for sample rules and citations that
captions cannot repeat. Each caption carries the regression or definitional information needed to read the
exhibit (sample, outcome, controls, weights, standard errors, what the headers report). Never judge a result
in the note (no "small", "flat", "rules out"); state numbers in the reply to Anya instead (2026-11-02).

## Workflow

1. Make a new branch for the task, or switch to it if it exists. Also make a GitHub issue and log all
   comments on it.
   1. Branch name: plain words describing the task, three words or fewer, separated by underscores
      (`event_study`, `desc_maps`). No job numbers, no internal fix codes.
   2. The issue has the same name as the branch and the label `claude`. Every issue and every comment
      starts with `# From Claude` (GitHub posts under Anya's account, so this marks the text as Claude's).
   3. All work for the task goes on the issue: findings, plans, and progress are posted as issue comments
      as well as in the terminal.
   4. **Status 2026-11-02:** the `gh` CLI is not installed on this Mac, so Claude cannot create issues,
      labels, or comments yet. Until `brew install gh && gh auth login` is run and a `claude` label exists
      on `amarchenko26/nutrition_code`, do the branch step, say the issue step was skipped, and continue.
2. Pull everything.
3. Execute the task.
4. Push everything.
5. Pull on the server (`git pull`). The server is Oscar (`ssh.ccv.brown.edu`, user `amarche4`).
6. If necessary, run the job on the server. The goal is to get the job to complete as fast as possible:
   1. If the job has never run on the server, first run it with 1 core as a small test job (< 1 hour, so
      it starts immediately; it will fail fast) to learn its memory use.
   2. Once memory/cores/time are known, request the smallest allocation that keeps the job running, and
      the shortest time based on experience.
   3. Minimize time, units, and memory so long as the job runs.
   4. Parallelize as much as possible (job arrays, dependencies) for the fastest run.
   The existing SLURM runners are `build/variety/run_*.sh` and `build/innovation/run_*.sh`; they hardcode
   `/users/amarche4/scratch/` for both the script and the log, so the repo location is irrelevant to them.
7. Outputs: this repo has no automatic sync back to Dropbox (the `live_sync.sh` / `push_output.sh` /
   `push_dropbox.sh` scripts from the other project were not ported). Each job should draw its plots right
   after its estimates; outputs are copied back to the Dropbox folders by hand. If a job needs a periodic
   push to Dropbox, build that first and say so.

## Output hygiene

* Claude never saves `.csv` files anywhere but its scratchpad unless explicitly directed. Any `.csv` made
  as part of a workflow goes in scratch, not in our folders. The one exception is a coefficient file that a
  downstream script in the same pipeline reads (e.g. `interim/labor_supply/*.csv` feeding a figure script);
  those stay in `interim/`.
* If asked for figures, do not produce additional `.csv` files; delete intermediate ones once the figures
  and tables exist. Data must be as clean and minimal as possible.
* Claude does not create summary `.tex` files of estimates it computes unless directed to.
* Anya often asks for several variations of the same figure. Before creating figures that are slight
  versions of old ones, ask whether the new code/output should supersede the existing related files.

## Language style

- Do not use "this" or "that" as nouns on their own; put the noun you are referring to (this estimate, that problem).
- Do not use figurative or metaphorical language; use the most straightforward noun for any concept.
- Do not substitute a metaphorical noun or verb for the plain one.
- Do not use intense adverbs ("genuinely advantageous" should be "better").
- Do not use jargon.
- Specifically, do not use project-manager language. Cut phrases like "is the whole story", "load bearing",
  "X is what matters", "this figure lives in", and the like.

---

# PART 2 — PROJECT REFERENCE

## Repo layout (verb-first; reorganized 2026-08-13)

```
paths.py · paths.do   the only files with full paths (added 2026-11-02)
REVIEW_cells_codes_2026-09.md   review of IV cells / occupation codes (data fixes §8)
clean/     raw → tidy, per source
  nielsen/   clean_nielsen · clean_panelist · clean_ailments · validate_ailments · create_sample
  syndigo/   clean_syndigo · merge_nielsen_syndigo         → interim/syndigo_nielsen_merged/syndigo_wide.parquet
  usda/      build_usda_wide                               → interim/usda_nielsen_merged/usda_wide.parquet
build/     analytic panels & variables
  hi/          build_claude_hi · syndigo/(build_hi_panel · add_sodium_to_panel) · usda/(build_hi_usda_panel · export_hi_usda_dta) · full_hei/(build_hei_crosswalk · build_hei_panel)
  iv/          build_iv · build_iv_by_head · build_iv_retiree · build_iv_baseline_occ (occupation frozen at first panel year; Bartik-style assignment) · build_iv_occ_edu (occ×edu cells, no household-size bin — HH male-preferred + per-head variants) · build_iv_predicted (2026-09-29, PREDICTED-SHARE IV: realized year-t cell replaced by out-of-fold transition probabilities from a 96-group table — origin occ bin × origin size bin × cell-defining head's age bin at t−1 (<30/30–44/45–54/55–61/62–66/67+), education held at t−1, 5 household folds w/ fixed seed, coarse occ×age fallback below 50 training pairs; cell incomes for EVERY cell×county×year with build_iv's leave-one-county-out formula, own-cell values asserted == iv_income_fips → interim/panel_dataset/iv_predicted.parquet/.dta: d_iv_pred = d_iv_frozen (within-cell shift of the t−1 cell) + d_iv_expswitch (expected switch gain), $1,000s; p_stay/p_switch/p_notemp/p_realized; fit report → interim/labor_supply/predicted_shares_fit.csv + calib.csv and tabs/labor_supply/predicted_shares_fit.tex) · build_iv_earned (VALIDATION-ONLY two-year-shifted income series, see INCOME TIMING) · build_bls_occ_wage (national OEWS wage instrument for 19_) · build_acs_cell_iv (ACS cell-income instrument for 20_, from raw/acs/usa_00004.csv) · build_county_income_shock · build_bartik_bls_iv · build_bartik_bls_module_iv · build_jaravel_ssiv
  variety/     build_product_variety · build_module_healthiness · build_module_income_elasticity · build_upc_spending · build_price_index (+ run_*.sh)
  innovation/  build_innovation_reg_data · build_upc_first_year_county (+ run_*.sh)
  expenditure/ build_expenditure_panel · build_trips_panel
  prep_time/   build_prep_time_index · build_prep_time_panel
  monthly_cycle/ build_monthly_cycle_panel
analyze/   regressions & figures
  hi/          build_outcome_robustness.do (cross-source robustness table) · syndigo/(build_hi.do: assembly of final_reg_data + main IV regs · figure_hi_over_time · figure_predicted_vs_actual_hi · replicate_figure1) · usda/(build_usda_exhibits.do · build_usda_mechanisms_table.do · verify_hi_usda.do · replicate_figure1_usda · figure_calories_by_income: daily cals per calorie-needs adult equivalent vs income binscatter · figure_gradient_no_retirees) · full_hei/(build_hei_note_results.do · figure_hei_vs_hi · build_composite_baskets: DUA-safe aggregated top/bottom-HEI-decile Atlanta basket comparison for talks) · figure_gradient_by_year.py: per-year income-nutrition gradient (Syndigo + USDA) → figs/gradient_by_year.png
  usda_comparison/ 1_build_comparison_universe · 2_figure_coverage_bars · 3_figure_nutrient_agreement · 4_figure_hi_comparison · 5_figure_coverage_over_time · 6_figure_syndigo_hi_vs_usda_coverage · 7_figure_appendix_hi_agreement (Syndigo-vs-USDA coverage/agreement diagnostics → figs/usda_diagnostics, note notes/syndigo_vs_usda.tex; data interim/usda_comparison)
  variety/     analyze_variety_healthiness (-> figs/variety/entry/: new-product entry shares vs module healthiness, syndigo|usda modes) · analyze_variety_price_index (-> figs/variety/price/: Jaravel/Feenstra price levels by module-healthiness quartile of claude_hi; CES = continuing products only, VA = variety-adjusted; levels are cumulative from the 2007->2008 pair so 2007 = 1 and the first plotted year is 2008 because RMS starts in 2006 and build_price_index.py drops 2006 as left-censored; the ratio figure drops 2008 as a commodity-price outlier; coverage gate pct_coverage >= 1% from module_healthiness_usda since 2026-11-02; HEALTHINESS GROUPS are FIXED claude_hi score ranges 0-2 / 3 / 4-5 / 6-10 (HI_CUTS; until 2026-11-02 pd.qcut on the integer score, whose cut points moved with the gate: the USDA gate's 4 extra modules put score 4 in Q2 and score 5 alone in Q3, making the quartile figure non-monotone; the fixed ranges reproduce the 2026-03-29 grouping); healthy = Q3+Q4 = scores 4-10, unhealthy = Q1+Q2 = 0-3; `--skip-hh` skips the slow household-purchase figures 4-5; figures: price_level_by_healthiness (4 quartiles x CES/VA over time), price_level_healthy_vs_unhealthy (Q3+Q4 vs Q1+Q2 x CES/VA over time, added 2026-11-02), price_level_by_usda_label (REAL, CPI-U deflated 2007 = 1, halves of modules by the USDA-label Allcott index hi_per_100g, spending- and equal-weighted, added 2026-11-02 for obesity.tex), price_ratio_adjusted_vs_not, price_level_ces_vs_va_by_healthiness (+ _step1), price_level_by_healthiness_income, price_gap_income_by_healthiness, price_gap_hh_income_by_healthiness, bundle_cost_by_income). RULE (Anya 2026-11-02): variety work SUPERSEDES existing code and figures in place; never add a parallel version. Figures moved from figs/ root into figs/variety/{price,entry}/ on 2026-11-02 and the six decks in present/ repointed.
  innovation/  analyze_innovation_inequality · innovation.do
  monthly_cycle/ build_monthly_cycle_regs.do
  labor_supply/  1_build_labor_supply_data.do (assembles final/labor_supply_reg_data.dta) · 2_build_head_structure_descriptives.do · [3_build_shock_descriptives.do RETIRED 2026-09-17: full-sample shock descriptives; the note uses the clean-sample versions from 16_ only — the decks still reference its old outputs shock_distribution.png / shock_by_occupation.tex / shock_density_by_group.png, left on Overleaf as orphans] · 4_build_labor_supply_regs.do · 5_build_hi_by_employment.do (main HI result on employed-cell samples)
  labor_supply/ (cont.) 6_build_hi_baseline_shock.do (main IV with baseline-occupation shock assignment → tabs/labor_supply/hi_baseline_shock.tex) · 7_build_hi_no_retirees.do (drops retirees only — not-emp cell & age≥62 — keeping unemp↔emp transitions → hi_no_retirees.tex + hi_no_retirees_pretrend.tex) · 8_build_no_retiree_descriptives.do (shock histogram + shock-by-occupation table on the non-retiree sample) · 9_build_hi_no_retirees_working_iv.do (working-head IV on no-retiree samples) · 10_build_no_retiree_quartile_figs.do (baseline_inc_usda/hei_no_retirees + baseline_inc_metabolic coefplots) · 11_build_hi_working_hours_iv.do (t-1-hours-weighted working-head IV → hi_working_hours[_pretrend].tex, note section 8) · 12_build_clean_sample_results.do (CLEAN SAMPLE core — employed shock cell at BOTH t-1 and t (drops all unemployed incl. job losers at t; retirees mechanically), original male-preferred shock only: hi_clean/pretrend_clean/labor_supply_clean tables + status_transition_matrix_clean) · 13_build_ols_iv_clean.do (OLS vs 2SLS rows → ols_iv_clean.tex) · 15_build_occ_move_grids.do (3×3 occupation-transition grid, origin t-1 → destination t, S/B/W + pooled Δocc-rank×Δinc-tercile grid, reduced forms → interim/labor_supply/occ_move_coefs.csv) · 16_build_clean_shock_descriptives.do (the note's only shock descriptives: shock_by_occupation_clean.tex + shock_distribution_clean.png + shock_density_by_occupation_clean.png; occupation at t, so the Not employed row = job leavers at t) · 18_build_unemployed_hi.do (unemployed HHs — not-emp cell at t, age<62 — as time-vs-income robustness → hi_unemployed.tex; weak F) · 19_build_hi_bls_shock.do (external instrument: Δ national OEWS mean wage of own occupation, build/iv/build_bls_occ_wage.py → hi_clean_bls.tex + pretrend_clean_bls.tex; F≈14-15) · 20_build_hi_acs_shock.do (finer external instrument: ACS occ-bin×educ×size cell mean HH income, build/iv/build_acs_cell_iv.py from IPUMS extract raw/acs/usa_00004.csv → hi_clean_acs.tex + pretrend_clean_acs.tex; F≈76) · 21_build_iv_decomposition.do (REWRITTEN 2026-09-17: per-group TREATMENT EFFECTS — one interacted 2SLS per outcome on the pooled clean sample, Δincome × {still employed at t, job losers <67, entered retirement 67+} instrumented by shock × group, group dummies, pooled hhid FE; Sanderson-Windmeijer F per group from ivreg2 ffirst → iv_decomposition.tex; retirement group has no first stage, F≈0.2) · 22_build_hi_clean_timing.do (2026-09-17: same regressor D.real_income/D.iv, purchase window moved one CALENDAR year at a time — L2D/LD/D/FD/F2D of the z-scored outcome = purchases t−3→t−2 (true placebo) / t−2→t−1 (= the old "pre-trend placebo", contemporaneous in calendar time) / t−1→t (nutrition shifted forward one year) / t→t+1 (main) / t+1→t+2 → hi_clean_timing.tex, esttab fragment/append blocks; the note's preamble defines \sym for fragment tables). PURCHASE-SIDE TIMING CHECK TRIED AND DROPPED (2026-09-17): a HH×month grocery-spending event study around a male head's E→NE transition (monthly_cycle_hh_month.parquet, hhid + calendar year-month FE, never-event controls) shows NO spending response — file years s−1/s/s+1 within ~1% of s−2 — so purchases cannot date the survey; the monthly version also had an unexplained seasonal sawtooth. Scripts/outputs deleted; the income event study remains the timing evidence. · 25_build_mover_design.do (2026-09-18, AKM/Card-Heining-Kline-style MOVERS DESIGN on the clean sample: groups by the occupation-type move t−1→t — stayers / up-movers / down-movers (employed destinations, rank service<blue<white) / job losers (not-emp at t, <67; retirement moves left out); NO household FE, move-type × year FE (occ type t−1 × occ type t × year) + kids/age/comp, county-clustered, pw → movers_groups.tex (weighted means: shock, income change, forward outcome changes t→t+1 and pre-move changes t−3→t−2), movers_symmetry.tex (movers-only 2SLS, Δincome × group instrumented by shock × group, p-value up = down, SW first-stage F per group), movers_2sls.tex (2SLS blocks: movers emp→emp / incl. job losers / stayers / all clean, then the FE ladder on all-clean: + household FE, and the main spec = hi_clean)) · 26_build_predicted_share_iv.do (2026-09-29, PREDICTED-SHARE instrument on the clean sample, spec as 12_; merges iv_predicted.dta, asserts iv_own_lag == L.iv_income_fips and d_iv_pred == frozen + expswitch → predicted_shares_descriptives.tex (SDs/corrs + variance split), first_stage_predicted.tex (D.real_income on current / predicted / each piece / both / predicted + origin-group FE), hi_predicted.tex (current vs predicted, 3 outcomes), hi_predicted_variants.tex (fragment blocks: + origin-group FE grp_id = occ×size×age bin at t−1; heads under 62 at t−1; within-cell piece alone; expected-switch piece alone), pretrend_predicted.tex (F.d_iv_pred), labor_supply_predicted.tex). CLEAN SAMPLE (2026-09-16, spec = Overleaf notes/labor_supply.tex "Sample"): employed occupation cell at t−1 — rules use t−1 ONLY, never status at t (job losers/retirees at t stay in). A retiree = not employed AND average head age (exact, from birth years) ≥67, so the employment rule already excludes retirees and a working 67-year-old stays in (Anya 2026-09-16). 18_'s not-employed sample = not employed at t−1 & age <67 at t−1. Occupation assignment: male head unless absent or armed forces, then female. Kilts occ codes: 1 professional · 2 manager/owner · 3 clerical · 4 sales · 5 craftsman · 6 operative/driver · 7 ARMED FORCES (excluded, no cell) · 8 service · 9 farmer (service/other, final 2026-09-16) · 10 students <30h (not employed in practice → not_emp) · 11 laborers (blue) · 12 not employed · 0 no head; bins = build_iv.OCC_MAP · 17_build_clean_quartile_figs.do · figure_occ_move_heatmaps.py (→ figs/labor_supply/occ_transition_3x3.png + occ_move_ranks.png). Superseded two-shock versions (hours-weighted working-head shock, retired for HH-income cell contamination) archived in z_archive/labor_supply_two_shocks/.
  movers/      OCCUPATION-MOVERS DESIGN (Finkelstein-Gentzkow-Williams 2016 with the Nielsen occupation codes as "places" and HEI as the outcome; note notes/movers.tex, outputs figs/movers + tabs/movers, data interim/movers). REWORKED 2026-11-02 from Anya's inline notes in movers.tex: MAIN SAMPLE = every dated move between employed occupations (codes 1-6, 8, 9, 11; moves into/out of not employed 12 are built with flag notemp but are not the main sample; 7 and 10 dropped), ALL movers via STACKED EVENTS (window = origin spell back to the previous code change + destination spell up to the next; event FE in place of household FE; cluster hhid; a one-move household reduces to FGW's household FE), and ONE-YEAR REVERSALS (A,B,A over three consecutive observed years) set back to A as measurement error (`occ` smoothed, `occ_raw` kept; 17,365 HH-years = 2.0%). 1_build_occupation_movers_panel.py (-> occupation_movers_panel.parquet HH-year panel with mover_type 0 never changes / 1 one E->E move only / 2 E->E mover with other changes / 3 changes but no E->E event; movers_events_panel.parquet/.dta stacked events: 44,582 E->E events in 30,029 HHs, flags single / first_event / reversed / emp2emp / notemp, r = year - (m_file - 1); occupation_means.csv on HH-years outside any r = 0) · 2_figure_occupation_moves_descriptives.py (occupation_histogram, transition_heatmap, top_moves, hei_by_occupation raw + age/year-adjusted, hei_gradient_income_vs_occupation = HEI net of age5/kids/year: 20-bin income binscatter + each occupation's adjusted mean at its mean income with both slopes; occupation_means.tex, moves_summary.tex) · 3_figure_movers_change_vs_delta.py (FGW Figs III-V and hei_change_by_rank_move on the events; pre -3..-1 / post 1..3 windows, matched non-mover cell means in the origin occupation; prints an alternative-window check, no file) · 4_build_movers_event_study.do (FGW eq. 6 with event FE; runs main x {hei_usda, hi_usda, hi}, single, firstmove, notemp -> event_study_coefs.csv + event_study_summary.csv; no tex table) · 6_figure_movers_event_studies.py (event_study_hei.png; event_study_samples.png 2x2 = all moves / not later reversed / first move / single-change; event_study_outcomes.png 4 samples x 3 outcomes; event_study_income.png = income share of the occupation income gap (E->E 0.06 (0.01) vs into/out of not employed 0.25 (0.01)) + HEI per $10k of the gap vs the IV-implied path; other_switches.png) · 8_build_life_event_studies.do (RESTORED 2026-11-02 at Anya's ask: births 3,505 / deaths 3,022 / education increase 5,845 as first-switch household-FE designs with never-changer controls; retirement 9,901 / job loss 8,079 / re-employment 9,606 as the not-employed events of the stacked panel with event FE; HEI points, gap, income jump, IV-implied -> other_switches_coefs/summary.csv; 1_ carries r_birth/birth_grp, r_death/death_grp, r_educ/educ_grp/delta_educ_hei and writes occupation_movers_panel.dta). 2_ also draws hei_by_education_and_occupation_income.png (education bins and occupations: raw / + age5, kids, year / + income ventiles; education range 4.8 / 5.6 / 4.6 pts, occupation 4.1 / 4.1 / 3.4: income explains little of either). RESULT 2026-11-02: post-move share of the origin-destination HEI gap 0.02 (SE 0.04), pre-trend p 0.69, on 44,461 moves / 29,958 HHs (95% CI excludes a share above 0.10); life events: HEI jumps birth -0.01 / death -0.16 / education +0.30 / retirement +0.38 / job loss +0.27 / re-employment -0.38 (SE 0.20-0.32), income falls $9-13k on death, retirement, job loss (IV-implied HEI -0.3 to -0.45); single movers only 0.13 (0.07), first moves 0.05 (0.06), simple indices -0.05 (0.09) USDA and 0.02 (0.07) Syndigo; change-on-move slope 0.005 (0.037); pre-move mover-minus-matched slope 0.26 (0.05) = sorting. Gradients on the same sample/controls: 0.36 HEI pts per $10k of household income (5.0 pts bottom-to-top ventile) vs 0.88 per $10k of occupation mean income (4.1 pts laborer-to-professional). Timing citations in the note: Manual 2024 Panelists file p. 21, FAQ 9 p. 59, Note 3 p. 25 (demographics from the fall-(t-1) survey), Note 2 p. 24 / PDF p. 25 (purchase weeks only). Change-on-move WINDOW kept at -3..-1 / 1..3 pending Anya's say-so (alternatives change N by <4% and the SE by <0.002). RETIRED 2026-11-02 (code deleted; everything is in snapshot commit 29c419d; stale interim outputs moved to interim/movers/z_retired_2026-11-02; their Overleaf figures and tables deleted the same day): 5_build_symmetric_pairs.do, 7_build_movers_decomposition.do (FGW Table II), 8_build_other_switch_event_studies.do (birth/death/retirement/job loss/re-employment), 9_/10_ stacked panel scripts (folded into 1_/4_); figures ever_move_by_tenure, move_symmetry_pairs, event_study_variants/controls/income/more_movers, pairs_mirror, pairs_pooled_fe/samples, occupation_effects, other_switches; tables change_vs_delta_summary, event_study_summary, decomposition, stacked_event_study, pairs_summary, pairs_pooled, other_switches. The retired results (other switches flat; IV-implied HEI drops of 0.4-0.5 not seen; movers' Wald 0.006 SD per $10k vs the IV's 0.035) live in the snapshot commit's movers.tex.
  health/      (2026-11-02, for Overleaf notes/obesity.tex "Thinking out loud"; figures -> figs/health/) figure_cdc_benchmarks.py (cdc_obesity_trend: NHANES measured obesity by cycle 1999-2023 + BRFSS self-reported obesity/overweight 2011-2025; cdc_physical_activity_trend: BRFSS aerobic-guideline share (odd years) + no leisure-time activity; values typed in from data.cdc.gov hn4x-zwk7 and NCHS data briefs 360/508) · figure_hei_gradients_income_education.py (HEI-income binscatter with and without education-bin controls: 0.36 vs 0.24 pts per $10k; and within each education bin: 0.30/0.18/0.22/0.28) · build_obesity_series.py (Nielsen Health Care Survey obesity series, adults 18+, from clean/nielsen/clean_ailments_members.py: self-report vs proxy, age-sex standardized, household series raked to the panel, within-household changes at the 2015/16 and 2018/19 redesigns, 2016-17 bracket-BMI check, BRFSS/NHANES benchmarks -> obesity_series.png + obesity_series_checks.png). AILMENTS SURVEY FACTS (from the format files, 2026-11-02): 2011-2015 one questionnaire per member 13+ (self-reports, age bracket Q7, gender Q14, birth year Q13a_yy; rows = members), response rate 60-66% falling to 48% in 2015; 2016+ one respondent per household: household-level list Q36 ("anyone in your household ... experienced during the past 6 months") + per-member grid (Q31_mem<k>_Ailment<j> 2016-17, Q18_mem<k> 2018, '18 (Q9_A_n)' 2019+ with n = (block-1)*9 + member, blocks in the format file's order) + roster (age, gender, relationship 1 = Myself); 2016-2017 only: member weight/height brackets; list grew 35 -> 49 items in 2016, renumbered 2019 and 2022; 2022 file carries its own Projection weight. The old clean_ailments.py household flags are kept; the member-level rebuild is interim/ailments/ailments_members.parquet + ailments_households.parquet (per-year cache in by_year/; 2019-2022 grids are item-first in the order of the Q36 numbers present in the data, 2023 is member-first; the extractor tests both against the household obesity flag and asserts >95% agreement; the 2023 layout file is named 'Layout'; Q13a_yy birth years in 2011-2015 are unusable, ages come from the Q7 brackets). OBESITY SERIES RESULT (2026-11-03, figs/health/obesity_series.png): adults 18+, age-sex standardized, self-reports: 18.9% (2011) -> 20.4% (2015) | 22.1% (2016, redesign) -> 18.8% (2023); all adult members 20.3% -> 16.8% over 2016-2023; proxy reports run 5-7 pts below self-reports; household any-adult 25.6% -> 27.1% (2015) -> 23.7% (2023), raking to the panel changes it by <0.3 pts; within-household change at the 2015->16 redesign +0.9 pts vs -1.3 in the surrounding pairs. BRFSS rises 27.4 -> 32.8 over the same years. BMI check 2016-17 (bracket weight/height, 126k adults): bracket BMI>=30 share 39.8% = NHANES 39.6%, but the item flags only 42% of them (48% self, 34% proxy), 11% of BMI 25-30 and 2% of BMI<25: the item is a self-identified condition, not a BMI measure, so its level is about half of true obesity and its trend does not track BRFSS.
  nhanes/      analyze_nhanes_diet_stats (WWEIA 2017-18 weighted diet facts for talk/intro: zero-veg shares, top categories by calories/weight; raw .xpt in nielsen_data/raw/nhanes/, auto-redownloads)
utils/     food_filters (shared department/group/module drop lists)
z_archive/ retired: corn/ (separate farm-bill project), explore_coverage.py, fracking.do, labor_supply_two_shocks/
food_deserts_replication/, jaravel_replication/   standalone external replications (left as-is)
```

Cross-folder Python imports use a `sys.path` bootstrap (only in `build/variety/build_module_healthiness.py`
and `build_product_variety.py`, which reach `utils/` and `clean/syndigo/`). `.sh` runners are Oscar/SLURM
cluster jobs hardcoding `/users/amarche4/scratch/` — repo location is irrelevant to them.

Overleaf layout: `0_main.tex` (paper), `figs/`, `tabs/`, `notes/` (labor_supply.tex, movers.tex,
syndigo_vs_usda.tex, hei_simple_vs_full.tex), `present/` (talk decks), `food.bib`.

## Health Index (main outcome)

Allcott et al. (2019): fixed positive score for fruit/veg modules; for all else,
`fiber/29.5 − sugar/32.8 − satfat/17.2 − sodium/2.3 − chol/0.3` per 100 g → per-1,000 kcal →
calorie-weighted to household-year → standardized. Produce/whole-grain flags come from Nielsen
modules (source-independent). Built by `build/hi/build_hi_panel.py` →
`interim/panel_dataset/panel_hh_year.parquet` (column `hi`). USDA-based analog `hi_usda` via
`build_hi_usda_panel.py` (validated: Syndigo-mode reproduces `hi` at corr 1.000). Main IV regressions in
`analyze/hi/build_hi.do` — forward-difference 2SLS, leave-one-out county income instrument, non-movers,
HH/year/kids/age/composition FE, county-clustered SE. UPC harmonization: Nielsen `'0'+zfill(upc,12)`;
external GTINs harmonize by dropping the check digit (`gtin.zfill(14)[:-1]`).

## Labor-supply subsystem (built in `clean/nielsen/clean_panelist.py`, analyzed in `analyze/labor_supply/`)

**Nielsen head/member caveats (shape everything):**
- No single "household head." Two fixed slots `male_head_*`/`female_head_*`; whether a HH reports **1 or 2
  heads is self-reported** at signup (not forced by marital status) — ~37% of HH-years report one head.
- Non-heads appear as `Member_1..7`. `n_members = household_size − n_heads` holds ~always (no adult dropped).
  `Member_N_Relationship_Sex`: 1 Son, 2 Daughter, 3/4 other male/female relative, 5/6 male/female unrelated —
  **there is NO spouse/partner code**.
- **Head employment is an hours bin** (0 none, 1 <30h, 2 30–34h, 3 35+h, 9 not employed);
  **member employment is binary** (1 employed / blank). So a dual-earner couple is measured differently by
  self-reported head count → heads-only measures **undercount second earners in one-head HHs** (a bias,
  likely correlated with gender norms — not just missing data).

**Variables added to `panelists_all_years.parquet`/`.dta`:**
`male_head_status`/`female_head_status` (FT=code3 / PT=codes1,2 / NE=code9); `male/female_head_employed`
(1/0, missing if no such head); `n_heads`; `n_head_earners`; `hh_total_workhours` (summed head hours
{1→20,2→32,3→40,9→0}); `has_recovered_partner`, `recovered_partner_sex`, `n_recovered_partner_earners`;
`n_earners_total` (= head earners + recovered-partner earners — the extensive-margin count that is
**consistent across the 1-vs-2-head split**). Pre-existing (unchanged): `hh_avg_workhours`,
`hh_avg_workhours_if_employed`, `hh_employed`.

**Recovered partner** (proxy for a working spouse a one-head HH declined to mark as head): flagged only in
single-head HHs, a member who is opposite-sex to the head, adult (≥18), non-child (rel. codes 3/4/5/6), and
spouse-aged (within ±15y of head's binned-age midpoint). Kept as a separate flagged measure so it never
contaminates head-only analysis.

**`panelist_heads_long.parquet`:** one row per `household_code × panel_year × role`
(role ∈ male_head/female_head/partner) with `status`, `employed`, `margin`, `status_lag`, `transition`
(e.g. `FT->PT`). Transitions computed **for heads only** (head slots stable across years; member slots are
not). Full-panel counts (2004–2024): `n_heads` 1→445,531 · 2→757,915 HH-yrs; recovered partners 21,376;
long panel 1,982,737 rows.

**INCOME TIMING (resolved 2026-09-10):** Kilts Note 3 officially says the year-t file reports income as of
year-end t-2, but panelists actually report ANNUALIZED CURRENT income at the fall-(t-1) survey — Nielsen
concurs (instructions dropped the prior-year wording in 2011), and our event study proves it: reported
income moves −18.5 log pts in the SAME file year as an employed→NE head transition (2011+: −20.6 then ~0;
pre-2011: −14.1 with a −7.1 echo one file later; NOTHING at +2 files in any era). So file-t income ≈ income
as of fall t-1, contemporaneous with file-t occupation → the paper's survey-timed pairing is correct, with
an effective ~1-year lag vs the panel year. Shifting income two years per Note 3 (the "earned" series:
`household_income[_midpoint]_earned` in clean_panelist.py, `build_iv_earned.py`, `real_income_earned` in
labor-supply 1_) KILLS the first stage (Δ-corr 0.002 vs 0.09) — kept as VALIDATION-ONLY machinery, do not
estimate with it. Write-up: labor_supply.tex "Validating income timing". CALENDAR-TIME READING (2026-09-17): file-t income
= income during calendar t−1, so the file-year change t−1→t is calendar t−2→t−1; the main forward difference (purchases
t→t+1) is a TWO-calendar-year lag, and the "pre-trend placebo" (purchases t−1→t on the file t→t+1 income change) is the
CONTEMPORANEOUS regression, not a placebo. 22_'s timing table gives the true placebo (t−3→t−2) and the one-year-after
alignment (t−1→t = nutrition shifted forward one year).

**DATA FIXES APPLIED 2026-09-16 (see REVIEW_cells_codes_2026-09.md §8):** (1) income codes 27–30
collapsed to one "$100k+" bracket (midpoint 140k) in every year (`clean_panelist.py`); (2) head age =
panel year − birth year, codebook midpoints only as fallback (`build_hi_panel.py`, `clean_panelist.py`
`*_head_age_exact`); the old `age_map` was shifted one bin too high; (3) IV cell income deflated to 2013$
with the same CPI as `real_income`, and `MIN_CELL_N` now counts leave-out donor HOUSEHOLDS (it used to
compare against a weight sum and never bound) (`build_iv.py`, imported by all sibling IV builders);
(4) OCC_MAP: 1–4 white · 5, 6, 11 blue · 8, 9 service/other · 10, 12 not_emp · 7 NaN (armed forces, no cell;
`bin_occupation` then falls back to the female head); ACS instrument bins match. `build_hi_panel.py
--refresh-panelist-vars` rewrites the pass-through panelist columns on the existing HI panel without
reprocessing purchases — use it after any clean_panelist change. Pre-rebuild data copies:
`nielsen_data/z_backup_2026-09-16/`. NOTE: 88% of var(ΔIV) is households switching cells; within a
fixed cell the shock has no first stage among working households (review §5, §6.11).

**Head-specific instruments (built 2026-08-21):** `build/iv/build_iv_by_head.py` builds
`iv_income_male_fips`/`iv_income_female_fips` — leave-one-county-out cell income from each head's OWN
education×occupation cell (vs. `build_iv.py`'s male-preferred HH assignment) →
`interim/panel_dataset/iv_income_by_head.{parquet,dta}`. Spouse-shock corr among couples: 0.52.
The labor-supply pipeline lives in `analyze/labor_supply/` (1_ data assembly →
`final/labor_supply_reg_data.dta`; 2_ head-structure descriptives; 3_ shock descriptives [retired]; 4_ regressions:
HH-level 2SLS moved out of build_hi.do, own-vs-spouse reduced forms on couples, income splits,
opposing-sign shock interactions). Write-up: Overleaf `notes/labor_supply.tex`; outputs in
`figs/labor_supply/`, `tabs/labor_supply/`.
