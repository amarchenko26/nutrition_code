// ============================================================================
// 18_build_unemployed_hi.do -- the UNEMPLOYED households as a robustness
// check on the nutrition result (Anya 2026-09-10). The clean sample (12_)
// keeps only household-years with an employed shock cell at t-1 and t; this
// script runs the same forward-diff IV on the excluded unemployed:
//
//   "Unemp. at t"  -- not-employed shock cell at t (includes job losers)
//   "Stable unemp."-- not-employed cell at BOTH t-1 and t (the shock is then
//                     within-cell income movement, not the cell switch)
//
// Retirement-age households (avg head age >= 62) are excluded so this is
// about unemployment, not retirement. Hypothesis: unemployed households have
// more time for home production, so if the main nutrition effect were about
// time rather than income, the response here should attenuate or flip sign.
//
// Output (tabs/labor_supply/):
//   hi_unemployed.tex   3 outcomes x 2 samples
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// Assigned occupation as in 12_ (male head unless absent or armed forces,
// then female). Sample rules use t-1 ONLY (labor_supply.tex "Sample",
// 2026-09-16): the unemployed sample is the complement of the clean sample --
// not-employed cell (codes 10/12) at t-1 and average head age under 67 at t-1.
// Status at t (job finders, stayers) is an outcome and does not select.
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & !inlist(occ_src, 0, 7)

gen byte unemp = (L.not_emp_cell == 1 & L.avg_age_hh_head < 67) ///
	if !missing(L.not_emp_cell) & !missing(L.avg_age_hh_head)

// ------------------------------------------------- z-scored nutrition outcomes
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// ============================================================================
// Forward-diff IV on the unemployed samples
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo `v'_u: ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor] if unemp == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

// \\$ emits a literal \$ (a single \$ reaches LaTeX as a bare $)
esttab hi_u hi_usda_u hei_usda_u ///
	using "$overleaf/tabs/labor_supply/hi_unemployed.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\\$10k)") ///
	mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab hi_u hi_usda_u hei_usda_u, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syndigo" "USDA" "HEI") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

di "18_build_unemployed_hi.do complete"
