// ============================================================================
// build_outcome_robustness.do -- deck robustness table: the main forward-diff
// income IV (m7 spec from build_hi.do) with a DIFFERENT healthiness outcome in
// each column:
//   (1) HI, Syndigo (the paper's simple index)     [f_hi, pre-built]
//   (2) HI, USDA (same formula, USDA labels)       [f_hi_usda, pre-built]
//   (3) HEI-2020, USDA (full index)                [added when build_hei_panel.py ships]
//   (4) Added sugar, USDA (g/1,000 kcal)           [USDA-only field]
//   (5) Total sugars, Syndigo (g/1,000 kcal)
//   (6) Total calories, Syndigo
//   (7) Sodium, Syndigo (g/1,000 kcal)
//
// Comparability: every outcome is standardized (z-score, projection-weighted,
// over the non-mover estimation sample), so all coefficients read as
// "SD per $10k of instrumented income change" and signs are directly
// comparable (expect: + for the indices, - for added sugar/sugar/calories/sodium).
// Spec is identical to m7: forward-diff outcome on lagged instrumented income
// change, non-movers, HH/year/kids/age/composition FE, county-clustered SEs.
//
// Output: tabs/results_outcome_robustness.tex
// ============================================================================

clear all
set graphics off

global TABS "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/tabs"

use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/final/final_reg_data.dta", replace
xtset hhid year
keep if movers_f == 0

// ----------------------------------------------------------------------------
// Standardize each outcome (projection-weighted z-score on estimation sample),
// then build its forward difference. hi / hi_usda are already standardized
// indices; re-scaling them to sample z-scores changes nothing material but
// keeps every column in identical units.
// ----------------------------------------------------------------------------
local outcomes hi hi_usda addsugar_per_1000cal_usda sugar_per_1000cal ///
	total_cals sodium_per_1000cal hei_usda

foreach v of local outcomes {
	cap drop z_`v' fz_`v'
	qui sum `v' [aw = projection_factor]
	gen z_`v' = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// ----------------------------------------------------------------------------
// Forward-diff IV per outcome
// ----------------------------------------------------------------------------
eststo clear
local i = 0
foreach v of local outcomes {
	local ++i
	eststo r`i': ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor], ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
}

// mean share of matched calories carrying an added-sugar value (for the note)
qui sum addsugar_cal_share_usda [aw = projection_factor]
local asg_cov : display %4.0f 100*r(mean)

esttab r1 r2 r7 r3 r4 r5 r6 using "$TABS/results_outcome_robustness.tex", ///
	replace booktabs label ///
	b(3) se(3) ///
	star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("HI (Syndigo)" "HI (USDA)" "HEI-2020 (USDA)" "Added sugar (USDA)" ///
	        "Total sugars (Syn.)" "Calories (Syn.)" "Sodium (Syn.)") ///
	keep(D.real_income) ///
	varlabels(D.real_income "Income (t-1)") ///
	stats(N, fmt(%9.0fc) labels("N")) ///
	nonotes

di "build_outcome_robustness.do complete"
