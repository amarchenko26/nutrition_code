// ============================================================================
// 14_build_no_retirees_occ_edu_cells.do -- the no-retirees two-shock nutrition
// table (12_'s Table 1) with the IV cells redefined as OCCUPATION x EDUCATION
// only, dropping the household-size bin (build/iv/build_iv_occ_edu.py):
//
//   (O) male-preferred household occ x edu cell     (iv_income_occedu_fips)
//   (W) working-head shock from each head's OWN occ x edu cell, heads
//       weighted by their share of t-1 hours (weights as in 12_)
//
// Sample and specification identical to 12_: no retirees (not-emp cell & avg
// head age >= 62 at t or t-1 dropped), non-movers, forward-diff 2SLS,
// [pw=projection_factor], absorb(year hhid kids avg_age_hh_head hh_comp),
// county-clustered SE.
//
// Output (tabs/labor_supply/):
//   hi_no_retirees_two_shocks_occedu.tex
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// ---------------------------------------------------------------- retiree flag
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | occ_src == 0
gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & occ_src != 0
gen byte retiree = not_emp_cell == 1 & avg_age_hh_head >= 62 ///
	if !missing(not_emp_cell) & !missing(avg_age_hh_head)
gen byte no_retiree = (retiree == 0 & L.retiree == 0) ///
	if !missing(retiree) & !missing(L.retiree)

// -------------------- hours-weighted working-head IV (W), occ x edu head cells
foreach h in male female {
	gen `h'_hours_bin = cond(`h'_head_employment == 1, 20, ///
	                   cond(`h'_head_employment == 2, 32, ///
	                   cond(`h'_head_employment == 3, 40, ///
	                   cond(`h'_head_employment == 9, 0, .))))
}
gen w_m = L.male_hours_bin / (L.male_hours_bin + L.female_hours_bin) ///
	if male_works == 1 & female_works == 1 ///
	 & !missing(L.male_hours_bin) & !missing(L.female_hours_bin) ///
	 & (L.male_hours_bin + L.female_hours_bin) > 0
replace w_m = 0.5 if male_works == 1 & female_works == 1 & missing(w_m)

gen iv_income_whrs_occedu = .
replace iv_income_whrs_occedu = iv_income_male_occedu_fips   if male_works == 1 & female_works != 1
replace iv_income_whrs_occedu = iv_income_female_occedu_fips if female_works == 1 & male_works != 1
replace iv_income_whrs_occedu = w_m * iv_income_male_occedu_fips ///
	+ (1 - w_m) * iv_income_female_occedu_fips ///
	if male_works == 1 & female_works == 1
label var iv_income_whrs_occedu "Working-head occ x edu cell IV, t-1 hours-share weighted"

// ------------------------------------------------- z-scored nutrition outcomes
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// ============================================================================
// Nutrition outcomes, no retirees, occ x edu cells, old vs hours-weighted
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo `v'_o: ivreghdfe fz_`v' (D.real_income = D.iv_income_occedu_fips) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo `v'_w: ivreghdfe fz_`v' (D.real_income = D.iv_income_whrs_occedu) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

// \\$ emits a literal \$ into the .tex (a single \$ reaches LaTeX as a bare
// $ because Stata consumes the backslash)
esttab hi_o hi_w hi_usda_o hi_usda_w hei_usda_o hei_usda_w ///
	using "$overleaf/tabs/labor_supply/hi_no_retirees_two_shocks_occedu.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\\$10k)") ///
	mtitles("Original" "Hrs-wt." "Original" "Hrs-wt." "Original" "Hrs-wt.") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab hi_o hi_w hi_usda_o hi_usda_w hei_usda_o hei_usda_w, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn-O" "Syn-W" "USDA-O" "USDA-W" "HEI-O" "HEI-W") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

di "14_build_no_retirees_occ_edu_cells.do complete"
