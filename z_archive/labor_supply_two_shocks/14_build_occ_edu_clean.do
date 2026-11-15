// ============================================================================
// 14_build_occ_edu_clean.do -- the clean-sample nutrition table (12_'s Table 1)
// with the IV cells redefined as OCCUPATION x EDUCATION only, dropping the
// household-size bin (build/iv/build_iv_occ_edu.py; iv_income_occedu_fips,
// male-preferred assignment). Clean sample and spec as in 12_; survey-timed
// income (validated as effectively current -- see 12_).
//
// Output (tabs/labor_supply/):
//   hi_occ_edu_clean.tex
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// ------------------------------------------------------------ clean-sample flag
// employed shock cell at BOTH t-1 and t (see 12_)
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | occ_src == 0
gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & occ_src != 0
gen byte clean = (not_emp_cell == 0 & L.not_emp_cell == 0) ///
	if !missing(not_emp_cell) & !missing(L.not_emp_cell)

// ------------------------------------------------- z-scored nutrition outcomes
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// ============================================================================
// Nutrition outcomes, clean sample, occ x edu cells
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo `v': ivreghdfe fz_`v' (D.real_income = D.iv_income_occedu_fips) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

// \\$ emits a literal \$ (a single \$ reaches LaTeX as a bare $)
esttab hi hi_usda hei_usda ///
	using "$overleaf/tabs/labor_supply/hi_occ_edu_clean.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\\$10k)") ///
	mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab hi hi_usda hei_usda, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syndigo" "USDA" "HEI") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

di "14_build_occ_edu_clean.do complete"
