// ============================================================================
// 20_build_hi_acs_shock.do -- ACS EXTERNAL cell-income instrument robustness
// for the clean-sample nutrition tables: the finer-cells upgrade of 19_'s
// BLS occupation-wage instrument. The instrument is the change in the
// national ACS mean household income of the household's occupation-bin x
// education x size cell (build/iv/build_acs_cell_iv.py; 3 x 4 x 4 cells x
// 2005-2020), built entirely outside Nielsen. Not-employed cells cannot be
// built in ACS (no employment status in the extract), so those household-
// years drop from these columns.
//
// Clean sample (12_: no retirees) and spec as in the main tables.
//
// Outputs (tabs/labor_supply/):
//   hi_clean_acs.tex        Table 3 analog (nutrition, 3 outcomes)
//   pretrend_clean_acs.tex  Table 4 analog (pre-trend placebos)
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
keep if movers_f == 0

// --------------------------------------------- ACS cell-income instrument
ren hhid household_code
ren year panel_year
merge 1:1 household_code panel_year using "$data/interim/panel_dataset/iv_income_acs.dta", ///
	keepusing(iv_income_acs) keep(master match)
drop _merge
ren household_code hhid
ren panel_year year
xtset hhid year

// ------------------------------------------------------------ clean-sample flag
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

cap drop f_inc
cap drop f_acs
gen f_inc = F.real_income - real_income
gen f_acs = F.iv_income_acs - iv_income_acs

// ============================================================================
// Table 3 analog: nutrition, clean sample, ACS cell-income instrument
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo `v': ivreghdfe fz_`v' (D.real_income = D.iv_income_acs) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

// \\$ emits a literal \$ (a single \$ reaches LaTeX as a bare $)
esttab hi hi_usda hei_usda ///
	using "$overleaf/tabs/labor_supply/hi_clean_acs.tex", ///
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
// Table 4 analog: pre-trend placebos, ACS instrument
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo p_`v': ivreghdfe D.z_`v' (f_inc = f_acs) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab p_hi p_hi_usda p_hei_usda ///
	using "$overleaf/tabs/labor_supply/pretrend_clean_acs.tex", ///
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

di "20_build_hi_acs_shock.do complete"
