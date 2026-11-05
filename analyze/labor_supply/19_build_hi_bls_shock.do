// ============================================================================
// 19_build_hi_bls_shock.do -- EXTERNAL-INSTRUMENT robustness for the clean-
// sample nutrition tables (Anya 2026-09-10): instead of instrumenting the
// household's income change with other Nielsen households' cell income,
// instrument it with the change in the national BLS (OEWS) mean annual wage
// of the household's occupation group (build/iv/build_bls_occ_wage.py;
// male-preferred occupation, Nielsen codes 1-9 and 11 mapped to SOC major
// groups). Identification: differential national wage growth across
// occupations; year FE absorb the common trend. Entirely outside Nielsen.
//
// Clean sample and spec as in 12_ (employed shock cell at t-1 and t,
// forward-diff 2SLS, non-movers, HH/year/kids/age/composition FE,
// county-clustered SE, projection-weighted).
//
// Outputs (tabs/labor_supply/):
//   hi_clean_bls.tex        Table 3 analog (nutrition, 3 outcomes)
//   pretrend_clean_bls.tex  Table 4 analog (pre-trend placebos)
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

// --------------------------------------------- BLS occupation wage instrument
merge m:1 occ_src year using "$data/interim/panel_dataset/bls_occ_wage.dta", ///
	keep(master match)
drop _merge
sort hhid year    // the m:1 merge breaks the panel sort; F./D. operators need it
gen bls_w = bls_occ_wage / 10000    // $10k units, matching real_income

// ------------------------------------------------- z-scored nutrition outcomes
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

cap drop f_inc
cap drop f_bls
gen f_inc = F.real_income - real_income
gen f_bls = F.bls_w - bls_w

// ============================================================================
// Table 3 analog: nutrition, clean sample, BLS occupation-wage instrument
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo `v': ivreghdfe fz_`v' (D.real_income = D.bls_w) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

// \\$ emits a literal \$ (a single \$ reaches LaTeX as a bare $)
esttab hi hi_usda hei_usda ///
	using "$overleaf/tabs/labor_supply/hi_clean_bls.tex", ///
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

// ============================================================================
// Table 4 analog: pre-trend placebos, BLS instrument
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo p_`v': ivreghdfe D.z_`v' (f_inc = f_bls) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab p_hi p_hi_usda p_hei_usda ///
	using "$overleaf/tabs/labor_supply/pretrend_clean_bls.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(f_inc) varlabels(f_inc "Future $\Delta$ Income (\\$10k)") ///
	mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab p_hi p_hi_usda p_hei_usda, ///
	keep(f_inc) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syndigo" "USDA" "HEI") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

di "19_build_hi_bls_shock.do complete"
