// ============================================================================
// 13_build_ols_iv_clean.do -- OLS vs 2SLS on the CLEAN SAMPLE (see 12_ for the
// definition: no retirees at t, employed shock cell at t-1), original
// (male-preferred) shock only. Survey-timed income (validated: panelists
// report annualized CURRENT income, not Note 3's t-2 concept -- see 12_). The OLS row is the identical forward-difference
// specification with income NOT instrumented, run on the 2SLS estimation
// sample so the rows are directly comparable.
//
// Output (tabs/labor_supply/):
//   ols_iv_clean.tex
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// ------------------------------------------------------------ clean-sample flag
// employed shock cell at BOTH t-1 and t (see 12_)
// Assigned occupation and SAMPLE exactly as in 12_ (labor_supply.tex
// "Measurement of key variables" / "Sample", 2026-09-16): male head unless
// absent or armed forces (7), then female; employed cell = 1-6, 8, 9, 11;
// clean = employed cell at t-1 (t-1 only). A retiree is not employed and 67+, so the
// employment rule already excludes retirees and a working 67-year-old stays in.
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell     = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte not_emp_cell = inlist(occ_src, 10, 12)                    if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)

// ------------------------------------------------- z-scored nutrition outcomes
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// ============================================================================
// 2SLS, then OLS on the identical estimation sample
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo iv_`v': ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
	gen byte smp_`v' = e(sample)

	eststo ols_`v': reghdfe fz_`v' D.real_income ///
		[pw = projection_factor] if smp_`v' == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
}

// Two coefficient rows in one table via fragment/append.
// \\$ emits a literal \$ into the .tex (a single \$ reaches LaTeX as a bare
// $ because Stata consumes the backslash).
esttab ols_hi ols_hi_usda ols_hei_usda ///
	using "$overleaf/tabs/labor_supply/ols_iv_clean.tex", ///
	replace fragment booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "OLS: $\Delta$ Income (\\$10k)") ///
	mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
	collabels(none) nonotes noobs ///
	prehead("\begin{tabular}{l*{3}{c}}" "\toprule") ///
	posthead("\midrule")

esttab iv_hi iv_hi_usda iv_hei_usda ///
	using "$overleaf/tabs/labor_supply/ols_iv_clean.tex", ///
	append fragment booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "2SLS: $\Delta$ Income (\\$10k)") ///
	nomtitles nonumbers collabels(none) nonotes ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	prefoot("\midrule") ///
	postfoot("\bottomrule" "\end{tabular}")

esttab ols_hi iv_hi ols_hi_usda iv_hi_usda ols_hei_usda iv_hei_usda, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn-OLS" "Syn-IV" "USDA-OLS" "USDA-IV" "HEI-OLS" "HEI-IV") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

di "13_build_ols_iv_clean.do complete"
