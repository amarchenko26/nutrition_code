// ============================================================================
// No-retirees tables with the WORKING-HEAD instrument.
//
// Mirrors 7_build_hi_no_retirees.do (same samples: all / no retirees /
// under 65; same z-scored outcomes and m7 spec) but instruments income with
// iv_income_working_fips (built in 1_build_labor_supply_data.do): the
// own-cell shock of the head who works — his if only he works, hers if only
// she works, the average if both work, missing if neither works. Retiree
// defined as in 7_: not-employed shock cell AND avg head age >= 62.
//
// Outputs: tabs/labor_supply/hi_no_retirees_working.tex
//          tabs/labor_supply/hi_no_retirees_working_pretrend.tex
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// Male-preferred occupation (defines the retiree flag, as in 7_)
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | occ_src == 0
gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & occ_src != 0

gen byte retiree = not_emp_cell == 1 & avg_age_hh_head >= 62 ///
	if !missing(not_emp_cell) & !missing(avg_age_hh_head)

gen byte no_retiree = (retiree == 0 & L.retiree == 0) ///
	if !missing(retiree) & !missing(L.retiree)

gen byte under65 = (avg_age_hh_head < 65 & L.avg_age_hh_head < 65) ///
	if !missing(avg_age_hh_head) & !missing(L.avg_age_hh_head)

// z-scored outcomes and their forward differences
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// ============================================================================
// Main effect: 3 outcomes x 3 samples, working-head instrument
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo `v'_all: ivreghdfe fz_`v' (D.real_income = D.iv_income_working_fips) ///
		[pw = projection_factor], ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo `v'_nr: ivreghdfe fz_`v' (D.real_income = D.iv_income_working_fips) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo `v'_u65: ivreghdfe fz_`v' (D.real_income = D.iv_income_working_fips) ///
		[pw = projection_factor] if under65 == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab hi_all hi_nr hi_u65 hi_usda_all hi_usda_nr hi_usda_u65 ///
	hei_usda_all hei_usda_nr hei_usda_u65 ///
	using "$overleaf/tabs/labor_supply/hi_no_retirees_working.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\$10k)") ///
	mtitles("All" "No retir." "Under 65" "All" "No retir." "Under 65" "All" "No retir." "Under 65") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 0 1 0 0 1 0 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab hi_all hi_nr hi_u65 hi_usda_all hi_usda_nr hi_usda_u65 ///
	hei_usda_all hei_usda_nr hei_usda_u65, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn" "Syn-nr" "Syn-u65" "USDA" "USDA-nr" "USDA-u65" ///
	        "HEI" "HEI-nr" "HEI-u65") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

// ============================================================================
// Pre-trend placebo: past outcome change on future income change,
// instrumented with the future change in the working-head IV
// ============================================================================

cap drop f_inc
cap drop f_iv_w
gen f_inc  = F.real_income - real_income
gen f_iv_w = F.iv_income_working_fips - iv_income_working_fips

eststo clear
foreach v of local outcomes {
	eststo p_`v'_all: ivreghdfe D.z_`v' (f_inc = f_iv_w) ///
		[pw = projection_factor], ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo p_`v'_nr: ivreghdfe D.z_`v' (f_inc = f_iv_w) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo p_`v'_u65: ivreghdfe D.z_`v' (f_inc = f_iv_w) ///
		[pw = projection_factor] if under65 == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab p_hi_all p_hi_nr p_hi_u65 p_hi_usda_all p_hi_usda_nr p_hi_usda_u65 ///
	p_hei_usda_all p_hei_usda_nr p_hei_usda_u65 ///
	using "$overleaf/tabs/labor_supply/hi_no_retirees_working_pretrend.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(f_inc) varlabels(f_inc "Future $\Delta$ Income (\$10k)") ///
	mtitles("All" "No retir." "Under 65" "All" "No retir." "Under 65" "All" "No retir." "Under 65") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 0 1 0 0 1 0 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab p_hi_all p_hi_nr p_hi_u65 p_hi_usda_all p_hi_usda_nr p_hi_usda_u65 ///
	p_hei_usda_all p_hei_usda_nr p_hei_usda_u65, ///
	keep(f_inc) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn" "Syn-nr" "Syn-u65" "USDA" "USDA-nr" "USDA-u65" ///
	        "HEI" "HEI-nr" "HEI-u65") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))
