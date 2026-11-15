# Code review: demographic cells, Nielsen codes, and the income IV

Date: 2026-09-15. Scope: everything between the raw panelist file and the forward-difference 2SLS
(`clean/nielsen/clean_panelist.py` → `build/iv/build_iv.py` and siblings →
`build/hi/syndigo/build_hi_panel.py` → `analyze/hi/syndigo/build_hi.do` →
`analyze/labor_supply/1_` and `12_`). Reference: Kilts *Consumer Panel Dataset Manual 2024*, which
covers 2004–2024 and is the only manual in `raw/consumer/Reference_Documentation` (no older edition
on disk). Every empirical claim below was checked on `panelists_all_years.parquet` (1.20M
household-years, 2004–2024) or on the regression data. Scripts and result tables:
`~/Documents/nutrition_review_2026-09-15/` (`build_iv_variants.py`, `review_sensitivity.do`,
`res_panel*.txt`, `code_crosstabs.txt`). Nothing in the repo was changed.

## Summary

1. The code lists in the pipeline match the 2024 manual, with one exception that matters
   (occupation code 10, section 4.1) and one bug that matters for the retiree rule (head-age
   midpoints, 4.2).
2. Nielsen's demographic codes do **not** change meaning over 2004–2024. The two documented
   exceptions (income codes 27–30 in 2006–09; the 2011 change in income wording) are already
   handled. Two undocumented format quirks exist and are harmless (section 2).
3. The 2026-09-10 occupation fix changed the instrument by very little: old and new
   year-over-year shocks correlate at 0.98 and the raw first-stage correlation moved from 0.092
   to 0.091. The paper's full-sample estimates are unchanged under every coding variant
   (section 6.1). On the no-retiree sample the relabel moved the full-HEI estimate from 0.033**
   to 0.027 (t from 2.1 to 1.6), and that is the whole change since last week's office hours
   (6.8); the USDA simple index stayed significant throughout.
4. What moved on the fully-employed sample was a fragile estimate: under every instrument and
   sample variant nothing on that sample is significant, and the 0.046* was the luckiest of
   several near-identical draws (6.4). The estimate on the no-retiree sample is robust for the
   USDA index and fragile for the full HEI.
5. The structural finding: 88% of the variance of the shock ΔIV comes from households *switching
   cells* between t−1 and t. Within a fixed cell, national cell income has essentially no first
   stage after 2011 (correlation with Δincome −0.001). The instrument's power is cell switching:
   job loss, retirement, births, and education-code noise. Section 5 and 6.6.

## 1. Code lists vs. the 2024 manual

| Variable | Pipeline mapping | Manual (Appendix II) | Verdict |
|---|---|---|---|
| `household_income` 3–26 | midpoints 2,500 … 85,000 (`clean_panelist.py`) | same brackets | OK, identical to Allcott et al.'s `NominalIncome` |
| `household_income` 27 | 140,000 in 2004–05 and 2010+; 112,500 in 2006–09 | "27 = $100,000+ except 2006–09 where 27 = $100–125k" | OK (see 4.3 on whether to collapse) |
| `household_income` 28–30 | 137,500 / 175,000 / 250,000 | exist only 2006–09 | OK; data confirm (0 rows outside 2006–09) |
| `household_size` | bins 1, 2, 3–4, 5+ | codes 1–9 = persons | OK; bins equal Nielsen's own sampling strata |
| `*_head_education` | {1:6, 2:10, 3:12, 4:14, 5:16, 6:18} years | 1 grade school … 6 post-college | OK, identical to Allcott |
| `*_head_employment` | 1 <30h→20, 2 30–34h→32, 3 35+→40, 9 not employed→0 | same | OK (Allcott uses 24 for code 1; immaterial) |
| `*_head_age` in `clean_panelist.py` | HEAD_AGE_MID {1:22 … 8:60, 9:70} | 1 <25, 2 25–29, …, 8 55–64, 9 65+ | OK |
| `*_head_age` in `build_hi_panel.py` | age_map {1:27, 2:32, …, 8:62, 9:67} | same codebook | **Shifted one bin (5 yrs) for codes 1–7; wrong** (4.2) |
| `*_head_occupation` 1–4 | white | 1 professional, 2 manager/owner, 3 clerical, 4 sales | OK |
| 5, 6, 11 | blue | 5 craftsman/foreman, 6 operative/driver, 11 laborer | OK |
| 7, 8, 9 | service/other | 7 armed forces, 8 service, 9 farmer | OK (7 is a judgment call; 6k HH-years) |
| 10 | service (employed) | "Students employed <30 hours" | **Label says employed; the data say not employed** (4.1) |
| 12 | not employed | housewife / retired / unable / unemployed | OK; manual FAQ 14 confirms 12 mixes retired and unemployed |
| `household_composition`, `age_and_presence_of_children` | used only as absorbed FE | — | OK, labels irrelevant |

**Code 10, exclude or not-employed bin?** Recommendation: the not-employed bin. Code-10 heads
look exactly like code-12 heads on every observable (90%+ report no paid work, 0% full-time,
lowest income), so they belong with them: the cell average then describes households with no
working head, which is what code 10 households are. Excluding them instead means dropping 9k
household-years (0.75%) from both the donor pools and the regression, which changes nothing
material: section 6.7 runs both and the estimates differ by 0.002–0.004 with the same F. The bin choice
also keeps the sample rules simple (retiree = not-employed cell and age; fully employed = cell
not in {10, 12}). If you prefer a clean "we only use people with a profession" story, exclude;
the numbers will not notice.

**Decision (2026-09-16): occupation map = armed forces (7) → blue, students (10) → not employed,
laborers (11) → blue.** Recorded here and in section 7; not yet applied to the repo. When
applied: `OCC_MAP` in `build_iv.py` gets `7: 'blue', 10: 'not_emp', 11: 'blue'` (the copies in
`build_iv_baseline_occ.py` and `build_iv_retiree.py` follow), and in every labor-supply .do file
`not_emp_cell = inlist(occ_src, 10, 12)` and the head-works rule `inlist(occ, 1,2,3,4,5,6,7,8,9,11)`.
Section 6.9 has this map's numbers; the 6.7 rows use code 10 → not employed but keep 7 in service.

**Which variables define the cell?** Three, all from `build_iv.py`: household-size bin (1 / 2 /
3–4 / 5+), education bin of the heads' average years of schooling (<12 / 12 / 13–15 / 16+), and
the occupation bin of the male head, female if there is no male head (white / blue / service /
not employed), by year.

**Why three occupation bins rather than the 11 codes?** The bins were chosen to keep cells
populated (the docstring in `build_iv.py` says so), and they mirror Nielsen's own sampling
strata, which use exactly three occupation levels (white collar, blue collar, other). I tested
the alternative in the scratchpad: cells on the raw code (with 10 merged into 12) give a median
of 173 cells per year instead of 64. The median household still has 1,154 leave-out donors, but
farmers, armed forces and laborers have medians of 30, 53 and 81, and 4% of household-years have
fewer than 100. Cell switching rises from 24% to 29% of household-year pairs, because a move
from clerical to sales now counts.

| cells | sample | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F |
|---|---|---|---|---|---|
| 11 codes | full | 0.025 (0.015) | 0.045*** (0.015) | 0.027** (0.012) | 390–402 |
| 4 bins (current) | full | 0.033** (0.016) | 0.049*** (0.014) | 0.028** (0.012) | 381–386 |
| 11 codes | no retirees | 0.019 (0.021) | 0.036 (0.024) | 0.027 (0.018) | 173–175 |
| 4 bins (current) | no retirees | 0.031 (0.023) | 0.046** (0.022) | 0.027 (0.017) | 165–166 |

Finer occupation cells buy no first-stage strength (within-cell income movements are still
tiny, and the extra cells mostly add switching) and cost precision on the no-retiree sample.
Keep the bins.



## 4. Problems found

### 4.1 Occupation code 10 is not an employed category in practice
The manual labels 10 "Students employed <30 hours". In the data, 89–100% of code-10 heads report
`employment = 9` (not employed for pay) in every era, 0% report 35+ hours, their mean age bin is
35–44, and their mean income is $40–50k, the lowest of any code. Code 12 heads look the same.
Whatever the questionnaire says, code 10 behaves as a not-employed category, and the old map
(10 → not_emp) was empirically right. Affected: 2,912 male-head and 6,211 female-head HH-years.
Recommendation: map 10 → not_emp in `build_iv.py` (and the copies in `build_iv_baseline_occ.py`,
`build_iv_retiree.py`), and treat 10 as not employed in the .do files
(`not_emp_cell = inlist(occ_src, 10, 12)`; `male_works = inrange(occ, 1, 9) | occ == 11`).

### 4.2 Head-age midpoints in `build_hi_panel.py` are one bin too high
`age_map = {1:27, 2:32, 3:37, 4:42, 5:47, 6:52, 7:57, 8:62, 9:67}` gives code 2 (ages 25–29) the
value 32 and code 8 (55–64) the value 62. `clean_panelist.py` (HEAD_AGE_MID) and Allcott's
`HomescanHouseholdDataPrep.do` use the correct midpoints {22, 27, 32, 37, 42, 47, 52, 60, 73}.
As an absorbed FE this is a relabelling and harmless. It is not harmless for the **retiree rule**
`avg_age_hh_head >= 62`: under the shifted map a single not-employed head in the 55–64 bin is a
retiree; under correct midpoints only 65+ is. Counts: 350,671 retiree HH-years under the current
map vs 265,756 under codebook midpoints. Only 38% of code-8 heads are actually 62 or older, so
"62" cannot be implemented from the bins.

Better: use the exact birth year, as Allcott does. `male_head_birth`/`female_head_birth` are
valid for 100% of present heads, and 88% of implied ages fall inside the reported age bin (mean
deviation from the midpoint 2.7 years). Then "62" means 62. Section 6.3 shows the clean-sample
estimates under the 65+ reading.

**Decision (2026-09-16): use the exact birth year for every head.** Not yet applied to the repo
(per your instruction, everything so far stays in the scratchpad). The change when you want it:
in `build_hi_panel.py`, each head's age = panel year − birth year, with the codebook midpoint
only when the birth year is missing or implausible, and `avg_age_hh_head` = mean over present
heads; in `clean_panelist.py`, the recovered-partner age window uses the head's exact age. The
regression's absorbed age effect and the retiree rule both read `avg_age_hh_head`, so they follow
once the panel is refreshed. The sensitivity rows below already use exact age (built in the
scratchpad from the same birth-year fields).

### 4.3 The income top bracket is not collapsed across 2006–09, contrary to the docstring
`clean_panelist.py`'s header says codes ≥27 are "collapsed to $100,000+ with a single midpoint for
consistency"; the code does the opposite (112.5k/137.5k/175k/250k in 2006–09, 140k otherwise).
That is exactly Allcott's choice, fine in a cross-sectional design. In a first-difference design
it puts a bracket-driven jump into Δincome for every top-coded household in 2006 (140k →
112.5k/137.5k/175k/250k) and 2010 (back to 140k): 11–14% of households in those years. The
by-year sd of Δreal income is highest in exactly 2006 and 2010 (2.21 and 2.32 vs 1.5–2.0
elsewhere). The same jump sits in the cell means. Reassuringly, the mean midpoint among
top-coded households in 2006–09 is $135–141k, so 140k is the right single value. Section 6.1
shows the estimates barely move when the bracket is collapsed, so this is a consistency fix, not
a result-changing one.

**Decision (2026-09-16): top-code consistently.** Not yet applied to the repo (scratchpad only).
The change when you want it: in `clean_panelist.py`, codes 27–30 all map to "$100,000+" with
midpoint 140,000 in every year, so the income variable has the same bracket structure 2004–2024;
everything downstream (`real_income`, every IV builder) inherits it once rerun. The sensitivity
rows below already use the collapsed bracket on both sides.

Related and growing: the share of households at "$100k+" rises from 9% (2004) to 22% (2020)
and 27% (2024). For those households real income falls mechanically every year (140k nominal
deflated), while the cell mean is nominal (4.5).

### 4.4 The minimum-cell-size rule is inoperative
`iv_cell_n_lo = lo_w` is the leave-out **sum of projection factors**, not a household count.
Projection factors are in the thousands, so `MIN_CELL_N = 30` never binds (0 HH-years dropped).
By actual counts, 1,983 HH-years sit in leave-out cells with fewer than 30 donor households and
19 with fewer than 10, so the consequence is small, but the note describes a rule that does not
exist. Same in `build_iv_by_head.py`, `build_iv_baseline_occ.py`, `build_iv_retiree.py`,
`build_iv_earned.py`. Fix: carry a count alongside the weight sum and threshold on the count.

### 4.5 The instrument is nominal, the endogenous variable is real
`build_iv.py` averages `household_income_midpoint` (nominal); `real_income` is deflated to 2013$.
Year FE absorb the common inflation factor, but in dollars the inflation increment is
proportional to the cell's income level, so ΔIV carries a "richer cells get bigger nominal raises
in high-inflation years" component with no counterpart in Δreal income. Deflating the IV with the
same CPI table is a one-line change; it moves the shock little (corr 0.98) and the estimates
not at all (6.1).

Why the instrument is nominal even though the regression uses real income: the two variables are
built in different scripts from different inputs. `real_income` is created in
`build_hi_panel.py` (line 96) as the bracket midpoint deflated to 2013 dollars. The instrument is
created in `build_iv.py` (line 120) from `household_income_midpoint`, the *undeflated* midpoint
in the panelist file, and `build_iv.py` never sees `real_income`. So the leave-one-out average is
an average of nominal midpoints. It should have been the average of the same deflated series;
deflating inside `build_iv.py` with the same CPI table makes it so.

deflate the exact same way to the exact same CPi and the exact same year the way we do real income. better yet, why not use real_income directly to construct the IV? confused. 

### 4.6 Smaller items
- `build_hi.do` line 519 regresses `male_head_occupation` (a nominal code) on instrumented
  income under a "Compliers" header. The coefficient has no meaning; use indicators.
- `1_build_labor_supply_data.do`: `female_works` missing instead of 0 for male-only households
  from 2011 (section 2). Fix: `replace female_works = 0 if missing(female_head_occupation)`.
- `avg_age_hh_head` fills missing with 45 silently. The birth-year route removes the need.

## 5. Where the shock's variance comes from

Decompose ΔIV_t = [IV(cell_t, t) − IV(cell_{t−1}, t)] + [IV(cell_{t−1}, t) − IV(cell_{t−1}, t−1)],
a switching component plus a within-cell component, on all consecutive HH-year pairs:

| | share of HH-year pairs | sd(ΔIV), $k | share of sum of squares of ΔIV |
|---|---|---|---|
| no cell switch | 76.1% | 4.0 | 11.9% |
| household-size bin changed only | 6.5% | 16.8 | 16.7% |
| education bin changed only | 5.3% | 18.9 | 17.2% |
| occupation bin changed only | 7.6% | 19.2 | 25.5% |
| more than one dimension changed | 4.4% | 26.8 | 28.6% |

The within-cell shock has variance 16 against 111 for ΔIV, and the two correlate at only 0.35.
The most common occupation moves are white→not employed (21% of occupation switches),
white↔blue (26%), not employed→white (12%).

Education switches are mostly noise: for the *same* male head observed in consecutive years the
education code changes in 9.4% of pairs, 4.6% up and 4.8% down, 90% by one category. Adults do
not lose a degree, so the down moves (and by symmetry most up moves) are reporting error; the
manual's FAQ 11 warns about exactly this. Household-size changes are real but are household
choices (births, children leaving, a parent moving in).

Implication: the identifying variation in the current design is overwhelmingly "the household
changed cell", which is why relabelling cells can move estimates, why the decomposition table
shows job losers carrying a third of the estimate, and why the occ-stable and employed samples
lose their first stage. The standard remedy is to freeze the cell: give each household the
change in income of the cell it was in *last year* (re-assigned every year), and let only the
national leave-out cell income move. That is a proper shift-share instrument, immune to job
loss, retirement, births and education-code noise. A stricter variant freezes the cell at the
household's first panel year forever; `build_iv_baseline_occ.py` does that for occupation only.
Sections 6.1–6.2 and 6.6 report the t−1-cell version (formulas under 6.1), and the result is
sobering: it has almost no first stage.

## 6. Sensitivity runs

All: forward-difference 2SLS, non-movers, `[pw=projection_factor]`, absorb(year hhid kids
avg_age_hh_head hh_comp), county-clustered SE, z-scored outcomes, SD per $10k. Stars: * 0.10,
** 0.05, *** 0.01.

**Principle (Anya, 2026-09-16): never condition the sample on status at t.** Occupation and
employment at t are outcomes of the shock (switching cells *is* the shock), so a rule like
"retiree at t or t−1" or "employed at t−1 and t" selects on the realized treatment. Every sample
rule must use t−1 (or earlier) information only. Sections 6.2–6.10 predate this and use rules
with a t component; they are kept as the record of what was run. Section 6.11 is the ladder
with t−1-only rules and supersedes 6.10. The note's own clean sample (`12_`) also uses
`retiree == 0 & L.retiree == 0` and needs the same change when the code is touched (section 7).

**Sample names used below.** "Clean sample" and "no retirees" mean the same thing: a
household-year is dropped if the household is a retiree at t or at t−1, retiree = not-employed
occupation cell and average head age 62+ (binned age map unless "exact age" is stated).
Working-age unemployed households, including job losers, stay in. This is the note's current
definition (Sept 10 final) and applies to 6.2, 6.3, 6.7, 6.9 and ladder rows 1, 2, 3 and 5.
There is **no** employment restriction in it. "9-09 clean" (only in 6.8) additionally required an
employed cell at t−1. "Fully employed" (6.4, ladder rows 4 and 6) requires an employed cell at
both t−1 and t, which drops all unemployed household-years and retirees mechanically.

"All fixes" = the three data fixes applied together: code 10 → not employed (4.1), the IV
deflated with the same CPI as income (4.5), and the top income bracket collapsed to 140k in every
year on both sides, income and IV (4.3). It does not include the age-map fix (4.2), which only
matters where a retiree rule is applied (6.3 does that), nor the cell-count rule (4.4), which
would drop 0.4% of observations. So the "all fixes" rows are what the tables would show if
recommendations 1, 2 and 4 of section 7 were applied.

"Frozen t−1 cell" is a different *instrument*, not a data fix; it is explained under 6.1.

### 6.1 Full sample (paper spec), N ≈ 480k

| instrument | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F |
|---|---|---|---|---|
| old map (pre-fix) | 0.030* (0.016) | 0.051*** (0.014) | 0.031*** (0.011) | 401 |
| current map | 0.033** (0.016) | 0.049*** (0.014) | 0.028** (0.012) | 386 |
| code 10 → not employed | 0.030* (0.016) | 0.047*** (0.014) | 0.028** (0.012) | 403 |
| deflated IV | 0.036** (0.017) | 0.050*** (0.015) | 0.029** (0.012) | 355 |
| collapsed top bracket | 0.035** (0.017) | 0.054*** (0.015) | 0.028** (0.012) | 456 |
| all fixes | 0.035** (0.017) | 0.053*** (0.015) | 0.029** (0.012) | 455 |
| frozen t−1 cell | 0.191 (0.117) | 0.058 (0.136) | 0.178 (0.119) | 10 |
| frozen + fixes | 0.256* (0.151) | 0.163 (0.169) | 0.337** (0.162) | 12 |

Yes: the "all fixes" row is the full-sample paper estimate with the three data fixes applied.
Every data variant lands within 0.005 of the current numbers.

**What "frozen t−1 cell" means, and what the current code does instead.** Write IV(c, t) for
the leave-one-county-out mean income of cell c in year t. The shock in every regression today is
`D.iv_income_fips`, which is

    current:  ΔIV_t = IV(c_t, t) − IV(c_{t−1}, t−1)

where c_t is the cell the household is in *this* year. If the household stays in its cell, this
is the change in its cell's income. If it switches cells at t, the difference also contains the
level gap between the new cell and the old one. A professional whose household retires moves
from a cell averaging roughly $95k to one averaging roughly $55k, so the "shock" is about −$40k
even if nothing happened to either cell's income that year.

The frozen version is

    frozen:   ΔIV_t = IV(c_{t−1}, t) − IV(c_{t−1}, t−1)

that is, the change over the year in the income of the cell the household was in last period.
This is exactly the design you describe in your question: between t−1 and t the household gets
the shock of its t−1 cell; if it switches at t, then between t and t+1 it gets the shock of its t
cell. Cells are re-assigned every year; nobody is stuck in their first cell. The current code
does not do this; it uses the contemporaneous cell, and that is where the switching component in
section 5 comes from. ("Baseline cell", mentioned in section 5 as an alternative, would freeze
the cell at the household's first panel year forever. Only `build_iv_baseline_occ.py` does that,
and only for occupation. It is not what the rows above use.)

The F of 10 is the price: once the level gaps from switching are removed, the only variation
left is national movement in the t−1 cell's income, about $2–4k a year, which barely registers
against bracketed reported income. The point estimates keep the sign and are larger, but with
standard errors of 0.12–0.17 they say little on their own.

Pre-trend placebo (Δz on future Δincome): current IV, USDA −0.004 (0.018), HEI −0.023 (0.014),
F 393; frozen IV, USDA −0.097 (0.076), HEI −0.035 (0.054), F 29.

### 6.2 Clean sample as in the note (no retirees, current age map), N ≈ 313k

"Clean sample" is the sample of `12_build_clean_sample_results.do` and of tables 3–4 of
`labor_supply.tex`: a household-year is dropped if the household is a retiree at t or at t−1,
where retiree = the male-preferred occupation cell is "not employed" (code 12) and the average
head age is 62 or more under the current age map (which, per 4.2, in practice means 55 or more).
Working-age unemployed households stay in.

| instrument | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F |
|---|---|---|---|---|
| old map | 0.025 (0.022) | 0.050** (0.021) | 0.033** (0.016) | 173 |
| current map (= note tables) | 0.031 (0.023) | 0.046** (0.022) | 0.027 (0.017) | 166 |

The "old map" row is today's no-retiree sample with the pre-2026-09-10 instrument, so it isolates
the instrument change with the sample held fixed. The "coding errors" in that instrument were 7
(armed forces) binned as blue and 11 (laborers) binned as service; its third difference, code 10
in the not-employed bin, turns out to have been right (4.1). Both instruments give the same
answer here.
| all fixes | 0.032 (0.024) | 0.051** (0.024) | 0.024 (0.018) | 199 |
| frozen t−1 cell | 0.262* (0.157) | 0.003 (0.164) | 0.095 (0.124) | 7 |

The "current map" row reproduces `hi_clean.tex` exactly (same coefficients, F and N), which
confirms the setup. The "all fixes" row is the answer to "exclude retirees and apply all the
fixes": USDA 0.051**, HEI 0.024 (n.s.). Section 6.3 is the same exercise with the age map
corrected so that "retiree" means 65+; there the USDA estimate is 0.048** and HEI 0.019.

### 6.3 Clean sample with codebook age midpoints (retiree = not-employed cell and 65+), N ≈ 354k

| instrument | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F |
|---|---|---|---|---|
| current map | 0.027 (0.020) | 0.043** (0.018) | 0.020 (0.014) | 267 |
| all fixes | 0.028 (0.020) | 0.048** (0.019) | 0.019 (0.014) | 349 |

Nearly, but 6.3 still defines "retiree" from the binned age codes, so "62" cannot mean 62 there
(it means 65+). The version that does what the rule says, with age computed from the reported
birth year (Allcott's approach, recommendation 3), is below.

### 6.3b Final version: no retirees by exact age (not-employed cell and age 62+ at t or t−1), all fixes

| instrument | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F | N |
|---|---|---|---|---|---|
| current map | 0.026 (0.021) | 0.042** (0.020) | 0.019 (0.015) | 219 | 335,941 |
| all fixes | 0.027 (0.022) | 0.047** (0.022) | 0.017 (0.015) | 278 | 335,941 |

Retiree household-years by definition: 350,671 (current binned map, in practice 55+), 304,276
(exact age 62+), 265,756 (binned codebook midpoints, in practice 65+), 261,651 (exact age 65+).
So yes: the "all fixes" row here is the no-retiree result with every coding issue fixed and the
retiree rule implemented as stated. It is the same picture as 6.2 and 6.3: the USDA simple index
holds at about 0.05 per $10k, the Syndigo index is positive and not significant, and the full HEI
is about 0.02 and not significant. The one thing not fixed in any of these rows is the instrument
itself (section 5).

### 6.4 Fully-employed sample (employed cell at t−1 and t), N ≈ 284k, KP F 103–112

| sample / instrument | Simple (Syndigo) | Simple (USDA) | Full HEI |
|---|---|---|---|
| code 10 employed (current sample), current IV | 0.014 (0.028) | 0.028 (0.029) | 0.032 (0.026) |
| code 10 not employed, current IV | 0.018 (0.030) | 0.024 (0.031) | 0.034 (0.028) |
| code 10 not employed, code-10 IV | 0.015 (0.029) | 0.020 (0.031) | 0.037 (0.027) |
| code 10 not employed, old IV | 0.015 (0.029) | 0.032 (0.030) | 0.046* (0.026) |

### 6.5 Era split, full sample, current IV

| era | Simple (USDA) | Full HEI | KP F |
|---|---|---|---|
| 2005–2010 | 0.076* (0.044) | 0.068** (0.031) | 56 |
| 2011–2020 | 0.035** (0.016) | 0.010 (0.014) | 370 |

Frozen IV by era: 2005–10 F = 13 (USDA 0.099 (0.123), HEI 0.166* (0.100)); 2011–20 F = 1.

Why the first stage is so much stronger after 2011. Two things multiply.

- Sample size: 159k vs 313k household-year pairs. The panel was 40k households in 2004–06 and
  60k from 2007, and 2004 has no lag. The first-stage F scales with N, so this alone roughly
  doubles it.
- The first-stage correlation itself rises from 0.070 to 0.105 (6.6), and among switchers from
  0.124 to 0.159. This does not reopen last week's timing question; it is the same picture read
  by era. The resolution stands: in every era panelists report current income, the income and
  occupation in a file come from the same survey wave, and the survey-timed pairing is right.
  What the event study also showed is that the response is *sharper* after 2011: a head's move
  to not-employed cuts reported income by 20.6 log points in the same file year and ~0 after,
  whereas before 2011 it is 14.1 in the same file year with a 7.1 echo the next year. So pre-2011
  reports were still mostly current, just with a partial one-year lag for about a third of the
  response, plausibly because the instructions then said "as of year-end of the previous
  calendar year" and some panelists followed them. A contemporaneous cell switch therefore
  predicts a larger share of the income change after 2011. The 2006 and 2010 bracket changes
  (4.3) add noise to Δincome in the early era as well.

Combining N and the squared correlation gives a predicted ratio of about 4.5; the observed
ratio is 6.6. The extra comes from the switch-driven first stage being cleaner post-2011. Note
the flip side: the era with the stronger instrument is the era with no HEI effect.

### 6.6 Raw first stage by switcher status (correlation of the shock with Δreal income)

| | 2005–10 | 2011–20 |
|---|---|---|
| all households, current shock | 0.070 | 0.105 |
| cell switchers only (24% of pairs) | 0.124 | 0.159 |
| non-switchers only (the within-cell shock) | 0.025 | 0.010 |
| frozen shock, year-demeaned, all households | 0.015 | −0.001 |

Year by year, the year-demeaned within-cell correlation is between −0.010 and +0.021 in every
year 2005–2020. Within a fixed cell, movements in national cell income do not predict a
household's reported income change; with bracketed income and cell means moving $1–2k a year,
there is little signal to find.

Reading the panels together: the paper's full-sample result is robust to every coding choice.
The no-retiree USDA result is robust; the full-HEI result on the clean samples is fragile
(0.019–0.033, significant in some variants only). On the fully-employed sample nothing is
significant under any variant. The HEI effect lives in 2005–2010. And the instrument's strength
is cell switching, not cell income.

### 6.7 The five rows you asked for

Same specification throughout (forward-difference 2SLS, non-movers, weights, year/household/
kids/age/composition absorbed, county-clustered SE; z-scored outcomes, SD per $10k). "Fixes" here
is the complete set: code 10 → not employed, deflated instrument, collapsed top bracket, and the
cell rule on household counts (drops 0.2% of household-years). "No retirees" in rows 2–5 uses
the exact birth-year age (retiree = not-employed cell and age 62+ at t or t−1); row 1 is the
note's own definition. "Fully employed" = employed occupation cell (not 10 or 12) at t−1 and t,
which excludes retirees mechanically.

| | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F | N |
|---|---|---|---|---|---|
| 1. No retirees, no fixes (note tables, last week) | 0.031 (0.023) | 0.046** (0.022) | 0.027 (0.017) | 166 | 313,812 |
| 2. No retirees, bug/code fixes | 0.028 (0.023) | 0.045** (0.021) | 0.019 (0.015) | 263 | 334,903 |
| 3. No retirees + fully employed, no fixes | 0.018 (0.030) | 0.024 (0.031) | 0.034 (0.028) | 107 | 284,576 |
| 3′. No retirees + fully employed, fixes | 0.022 (0.034) | 0.015 (0.036) | 0.038 (0.030) | 105 | 283,587 |
| 4. No retirees, fixes, cell frozen at t−1 | 1.18 (1.65) | 0.31 (1.01) | 0.71 (1.25) | 0–1 | 335,153 |
| 5. No retirees + fully employed, fixes, cell frozen at t−1 | 0.71 (1.06) | 0.14 (0.72) | 0.54 (0.99) | 0–1 | 283,808 |

Rows 4 and 5 are not estimates in any useful sense: the first-stage F is below 1, so the 2SLS
ratio is noise. This sharpens section 5. On the full sample the frozen-cell instrument had F of
10–12 (6.1); that residual first stage came from the *not-employed* cells, whose income moves
with Social Security and pension adjustments that retiree households actually receive. Once
retirees are out, movements in a working household's cell income have no detectable relation to
its own reported income change. The current instrument's first stage on working households is
cell switching and nothing else.

### 6.8 What changed between last week's office hours and row 1

The two no-retiree tables you had last week, reproduced with the instrument as it was then (old
occupation map, still in `final_reg_data.dta`) and with the instrument as it is now:

| sample | instrument | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F | N |
|---|---|---|---|---|---|---|
| 9-04 "no retirees" (retiree = not-employed cell incl. code 10, age 62+, at t or t−1) | old (as shown) | 0.024 (0.022) | 0.050** (0.021) | 0.033** (0.016) | 173 | 313,535 |
| same sample | current | 0.031 (0.023) | 0.046** (0.022) | 0.027 (0.017) | 166 | 313,535 |
| 9-09 "clean" (no retiree at t, employed cell at t−1) | old (as shown) | 0.018 (0.023) | 0.044* (0.024) | 0.045** (0.020) | 145 | 289,122 |
| same sample | current | 0.022 (0.024) | 0.038 (0.025) | 0.039* (0.022) | 138 | 289,122 |
| 9-10 "no retirees" (row 1 of 6.7) | old | 0.025 (0.022) | 0.050** (0.021) | 0.033** (0.016) | 173 | 313,812 |
| same sample | current | 0.031 (0.023) | 0.046** (0.022) | 0.027 (0.017) | 166 | 313,812 |

The first row of each pair reproduces the table of that date exactly. The sample did not change
(the 9-04 and 9-10 no-retiree samples differ by 277 household-years). What changed is the
instrument: the September 10 relabel of code 7 (armed forces, blue → service) and code 11
(laborers, service → blue), plus code 10 leaving the not-employed bin, which your decision now
reverses. On the no-retiree sample that relabel moves the full-HEI estimate from 0.033 to 0.027
against a standard error of 0.017, that is from t = 2.1 to t = 1.6. The USDA simple index went
from 0.050 to 0.046 and stayed significant. On the 9-09 clean sample the same relabel took HEI
from 0.045** to 0.039* and USDA from 0.044* to 0.038.

So the HEI result on the no-retiree samples was sitting on the 5% line, and a 2% change in the
instrument pushed it over. It was not a robust "significant" last week and it is not a robust
"zero" now: across every instrument variant in this memo the no-retiree HEI estimate lies
between 0.019 and 0.033 with a standard error of 0.015–0.017.

### 6.9 Your occupation map (armed forces → blue, students → not employed, laborers → blue)

Built in the scratchpad with everything else as in the current instrument (nominal midpoints,
uncollapsed bracket, weight-based cell rule). It differs from the *old* map only in code 11
(laborers, service → blue) and from the *new* map in codes 7 and 10.

| | vs old instrument | vs new instrument |
|---|---|---|
| correlation of levels | 0.998 | 0.998 |
| correlation of year-over-year shocks | 0.989 | 0.992 |
| household-years with an identical value | 75% | 44% |
| household-years moved by more than $500 | 17% | 34% |
| raw first-stage correlation (old 0.092, new 0.091) | 0.092 | |

| sample | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F | N |
|---|---|---|---|---|---|
| full (paper spec) | 0.030* (0.016) | 0.048*** (0.014) | 0.027** (0.012) | 404–409 | 480k |
| no retirees (not-employed = 10/12, binned age rule, as in 6.8) | 0.024 (0.022) | 0.045** (0.021) | 0.026 (0.017) | 175 | 313k |

Read against 6.8: on the no-retiree sample the old map gave HEI 0.033** and your map gives 0.026,
and the only difference between the two maps is where 16k laborer household-years sit (1.3% of
the sample). So the movement of the HEI estimate across the significance line traces to that
single relabel, through the blue- and service-cell means it shifts and the switches it creates
or removes. The USDA simple index is 0.045–0.050 and significant under all three maps.

### 6.10 Specification ladder (your occupation map)

Each row changes one thing relative to the row above. Same specification throughout:
forward-difference 2SLS, non-movers, projection weights, year/household/kids/age/composition
absorbed, county-clustered SE; z-scored outcomes, SD per $10k. "No retirees" = the household is
not in the not-employed cell with average head age 62+ at t or t−1; last week's rule used the
binned age map (in practice 55+), the exact-age rule uses the reported birth year. "Fully
employed" = employed occupation cell (not 10 or 12) at both t−1 and t. All rows from 2 down use
your map (armed forces → blue, students → not employed, laborers → blue).

| | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F | N |
|---|---|---|---|---|---|
| 1. Last week: no retirees, old instrument (laborers in service) | 0.024 (0.022) | 0.050** (0.021) | 0.033** (0.016) | 173 | 313,535 |
| 2. + laborers → blue (your map), nothing else changed | 0.024 (0.022) | 0.045** (0.021) | 0.026 (0.017) | 175 | 313,535 |
| 3. + bug/code fixes | 0.028 (0.022) | 0.046** (0.021) | 0.019 (0.015) | 268 | 334,817 |
| 4. + fully employed at t−1 and t | 0.021 (0.033) | 0.021 (0.035) | 0.040 (0.030) | 110 | 283,503 |
GET RID OF THIS SAYS PETER NO CONDITIONING ON T


| 5. Row 3 with the cell frozen at t−1 | 0.97 (1.16) | 0.26 (0.79) | 0.66 (0.97) | 1 | 335,063 |
| 6. Row 4 with the cell frozen at t−1 | 0.65 (0.94) | 0.15 (0.70) | 0.68 (1.14) | 1 | 283,724 |

Row 3 bundles two kinds of fix. Separated (both on your map):

| | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F | N |
|---|---|---|---|---|---|
| 2 + exact-age retiree rule only | 0.022 (0.020) | 0.042** (0.019) | 0.018 (0.014) | 235 | 335,941 |
| 2 + instrument fixes only (deflated, collapsed bracket, count-based cell rule; last week's retiree rule) | 0.031 (0.025) | 0.049** (0.024) | 0.026 (0.017) | 194 | 312,419 |

Reading down the ladder: the USDA simple index is 0.042–0.050 and significant in rows 1–3 and
in both split rows; it loses significance only on the fully-employed sample, where the standard
error nearly doubles. The full HEI moves from 0.033** to 0.026 with the laborer relabel (row 2)
and to 0.018–0.019 once retirees are defined by exact age; it is never significant after row 1.
Rows 5 and 6 have no first stage (F = 1): with retirees out, a working household's own income
change is unrelated to the income movement of the cell it was in last year, so the frozen-cell
2SLS ratios are noise. The instrument's identifying variation on working households is cell
switching (section 5).

### 6.11 Specification ladder with sample rules at t−1 only (supersedes 6.10)

Same specification and outcomes as 6.10. Every sample rule now uses t−1 status only, so
households that retire, lose a job, or switch occupation at t stay in. "Not a retiree at t−1" =
not (not-employed cell and average head age 62+) at t−1; "employed at t−1" = employed occupation
cell (not 10 or 12) at t−1. Rows 2 down use your occupation map.

| | Simple (Syndigo) | Simple (USDA) | Full HEI | KP F | N |
|---|---|---|---|---|---|
| 1. Not a retiree at t−1 (binned age), old instrument | 0.019 (0.018) | 0.041** (0.017) | 0.027** (0.013) | 280 | 329,187 |
| 2. + laborers → blue (your map) | 0.019 (0.018) | 0.038** (0.017) | 0.022 (0.013) | 287 | 329,187 |
| 3. + bug/code fixes (deflated, collapsed bracket, count rule, exact age at t−1) | 0.024 (0.019) | 0.043** (0.019) | 0.020 (0.013) | 322 | 349,455 |
| 4. Employed cell at t−1, fixes | 0.022 (0.019) | 0.033* (0.020) | 0.034** (0.016) | 272 | 300,528 |
| 5. Row 3 with the cell frozen at t−1 | 0.57 (0.55) | 0.11 (0.47) | 0.36 (0.43) | 2 | 349,701 |
| 6. Row 4 with the cell frozen at t−1 | 0.50 (0.69) | 0.17 (0.56) | 0.56 (0.78) | 1 | 300,747 |

Compared with the t-conditioned rules in 6.10: the first stage is stronger everywhere (F 280–320
against 170–270), because the households whose status changes at t are exactly the ones that
carry the switching variation, and they are no longer thrown out. On the employed-at-t−1 sample
(row 4) the full HEI is 0.034** and the USDA index 0.033*, where the "employed at t−1 and t"
sample gave 0.040 (0.030) and 0.021 (0.035) with an F of 110: dropping job losers at t had
removed most of the identifying variation. Rows 5 and 6 still have no first stage; with retirees
out, within-cell income movements do not predict a household's income change (section 5).

The picture is otherwise unchanged. The USDA simple index is 0.033–0.043 and significant in every
row with a first stage; the full HEI is 0.020–0.034 with a standard error of 0.013–0.016,
significant in rows 1 and 4 and not in 2 and 3.

## 8. Applied on 2026-09-16 (after your go-ahead)

The spec is now `labor_supply.tex`, sections "Measurement of key variables" and "Sample",
which supersede the maps and rules discussed above in one place: armed forces (7) are
**excluded** rather than blue collar. Farmers (9) were moved to blue collar for one run and
then returned to service/other (final table in the note, 2026-09-16 evening). Applied:

- `clean/nielsen/clean_panelist.py`: income codes 27–30 → one "$100,000+" bracket, midpoint
  140,000, every year; exact head age from birth year (`*_head_age_exact`), used for the
  recovered-partner window.
- `build/hi/syndigo/build_hi_panel.py`: `avg_age_hh_head` from birth years (codebook midpoints
  only as fallback); new `--refresh-panelist-vars` mode that rewrites the pass-through panelist
  columns on the existing panel without reprocessing purchases.
- `build/iv/build_iv.py` (and, through imports, `_by_head`, `_occ_edu`, `_earned`, `_retiree`,
  `_baseline_occ`): OCC_MAP = 1–4 white; 5, 6, 11 blue; 8, 9 service/other; 10, 12 not employed;
  7 no cell (falls back to the female head); cell income deflated to 2013$; cell rule on
  household counts. `build_acs_cell_iv.py` bins match (military unassigned).
- `analyze/labor_supply/1_`: head "works" = codes 1–6, 8, 9, 11; blank female occupation = 0.
- `analyze/labor_supply/3_, 12_, 13_, 15_, 16_, 17_, 18_, 19_, 20_, 21_`: occupation assigned
  male-unless-absent-or-armed-forces; occupation bins as above; **clean = employed cell at t−1**
  (no rule uses status at t). A retiree is not employed and 67+, so the employment rule already
  excludes retirees and a working 67-year-old stays in (Anya, 2026-09-16; an age-only rule was
  briefly implemented and removed the same day). `18_` = not employed at t−1 and under 67 at t−1;
  `21_` decomposes the clean sample into still-employed vs job losers at t.
- `analyze/hi/usda/figure_gradient_no_retirees.py`: drops age 67+ and armed-forces households.
- Data rebuilt in this order: panelists → panel refresh → six Nielsen instruments + ACS
  instrument → `build_hi.do` assembly (`final_reg_data`) → `1_` (`labor_supply_reg_data`) → the
  note's scripts. Pre-rebuild copies of every overwritten data file are in
  `nielsen_data/z_backup_2026-09-16/`; the pre-September-10 instrument is also saved as
  `~/Documents/nutrition_review_2026-09-15/iv_old_instrument_pre_2026-09-10.dta`.
- Not rerun: the paper-side exhibits in `build_hi.do` after the save line, and the USDA/HEI
  exhibit scripts; they read `final_reg_data.dta`, which is now on the new vintage.

**Final result (main table `hi_clean.tex`, 2026-09-16 17:31; sample = employed cell at t−1, your
final occupation table with farmers in service/other):** Syndigo 0.022 (0.020), USDA 0.032 (0.020),
full HEI 0.034** (0.016), KP F 278, N 299,860; pre-trend placebo 0.027 (0.017), 0.017 (0.020),
−0.001 (0.017), F 261. This equals the reference row (6.11 row 4: 0.022, 0.033*, 0.034**) to
within 0.001; the armed-forces exclusion and the exact-age fixed effects change nothing. An
intermediate run with farmers in blue collar gave USDA 0.037* and HEI 0.032**. An age-only "under 67 at t−1" rule was briefly in place on
2026-09-16 and gave 0.013 / 0.036* / 0.028* with N ≈ 275k; Anya ruled that a working 67-year-old
belongs in the sample, so the rule was removed. Decomposition scripts and results:
`~/Documents/nutrition_review_2026-09-15/scratch/` (`build_iv_decomp.py`, `review_decomp.do`,
`res_decomp.txt`).

**Vintage of the note's other outputs (only `12_` was rerun on the final spec, at Anya's
request):** on the new data but under the interim age-only rule (2026-09-16 17:05–17:18):
`ols_iv_clean`, `shock_by_occupation_clean`, `shock_distribution_clean`, the quartile figures,
`hi_unemployed`, `hi_clean_bls/acs`, `pretrend_clean_bls/acs`, and the section-3 shock descriptives
(`3_`, which has no sample rule and is therefore final). Still on the 2026-09-10 data and sample:
`iv_decomposition`, the occupation-grid figures (`occ_transition_3x3`, `occ_move_ranks`), and the
gradient-excluding-retirees binscatters (2026-09-04). Rerun `13_, 15_–21_`,
`figure_occ_move_heatmaps.py` and `figure_gradient_no_retirees.py` to bring them onto the final
spec; the scripts already carry the final definitions.

## 7. Recommended changes (as of 2026-09-15; items 1–4 applied on 09-16, see section 8)

1. `build_iv.py` and copies: map code 10 → not_emp; threshold on a household count, not a
   weight sum; deflate the cell income with the CPI table before averaging.
2. `clean_panelist.py`: collapse codes ≥27 to one midpoint (140k) in all years, as the docstring
   already claims, or keep the detail and document that 2006 and 2010 carry bracket jumps.
3. `build_hi_panel.py`: replace `age_map` with the codebook midpoints and prefer exact age from
   `*_head_birth` (Allcott's approach); then decide whether "retiree" is 62+ by exact age.
4. Labor-supply .do files: `not_emp_cell = inlist(occ_src, 10, 12)`; `female_works = 0` when the
   female occupation is blank. **And every sample rule must use t−1 status only** (Anya,
   2026-09-16): in `12_`, `13_`, `15_`–`21_` replace `clean = (retiree == 0 & L.retiree == 0)` with
   `clean = (L.retiree == 0)`, and drop the "employed at t−1 and t" sample (`18_`'s unemployed
   sample, defined at t, goes too); an "employed at t−1" sample is the admissible replacement.
   Retiree/employment status at t is an outcome of the shock and must not select the sample.
5. Decide, with Peter, what to do about section 5. Options: (a) frozen-cell (t−1 or baseline)
   instrument as the headline, accepting F ≈ 10 and wide intervals, possibly with multi-year
   differences to build up signal; (b) keep the current instrument but reframe it as a
   transition-driven design and report the decomposition; (c) an external cell-income series
   (ACS, F ≈ 76) as the shift, with frozen Nielsen cells as the share.
6. Rerun `build_hi.do` so `final_reg_data` and the paper tables sit on the same instrument
   vintage as the note (still pending from 2026-09-10).




OKAY, LET'S WRITE EVERYTHING OUT: 

NO RETIREES, NO FIXES ESTIMATES (original from last week): 
NO RETIREES, BUG / CODE FIXES ESTIMATES: 
NO RETIREES, + FULLY EMPLOYED SAMPLE ESTIMATES: 
NO RETIREES, BUG / CODE fixes + CELL AT T-1 ESTIMATES: 
NO RETIREES, BUG / CODE fixes + fully employed + CELL AT T-1 ESTIMATES: 
