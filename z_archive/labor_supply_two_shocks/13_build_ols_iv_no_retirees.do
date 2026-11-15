// ============================================================================
// 13_build_ols_iv_no_retirees.do -- OLS vs 2SLS comparison on the no-retirees
// sample, mirroring the paper's main-table "OLS estimate / IV estimate" rows:
// three outcomes (Syndigo HI, USDA HI, full HEI) x two shock definitions
// ((O) original male-preferred cell, (W) t-1-hours-weighted working-head, as
// in 12_). The OLS row is the identical forward-difference specification with
// income NOT instrumented, run on each column's 2SLS estimation sample so the
// two rows are directly comparable.
//
// Output (tabs/labor_supply/):
//   ols_iv_no_retirees_two_shocks.tex
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

// ------------------------------------------- hours-weighted working-head IV (W)
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

gen iv_income_whrs_fips = .
replace iv_income_whrs_fips = iv_income_male_fips   if male_works == 1 & female_works != 1
replace iv_income_whrs_fips = iv_income_female_fips if female_works == 1 & male_works != 1
replace iv_income_whrs_fips = w_m * iv_income_male_fips + (1 - w_m) * iv_income_female_fips ///
	if male_works == 1 & female_works == 1

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
	foreach s in o w {
		local ivvar = cond("`s'" == "o", "iv_income_fips", "iv_income_whrs_fips")

		eststo iv_`v'_`s': ivreghdfe fz_`v' (D.real_income = D.`ivvar') ///
			[pw = projection_factor] if no_retiree == 1, ///
			absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
		estadd scalar fs_F = e(widstat)
		gen byte smp_`v'_`s' = e(sample)

		eststo ols_`v'_`s': reghdfe fz_`v' D.real_income ///
			[pw = projection_factor] if smp_`v'_`s' == 1, ///
			absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	}
}

// Two coefficient rows in one table: OLS row (with the column headers), then
// the 2SLS row plus the stats block, via fragment/append.
// \\$ emits a literal \$ into the .tex (a single \$ reaches LaTeX as a bare
// $ because Stata consumes the backslash).
esttab ols_hi_o ols_hi_w ols_hi_usda_o ols_hi_usda_w ols_hei_usda_o ols_hei_usda_w ///
	using "$overleaf/tabs/labor_supply/ols_iv_no_retirees_two_shocks.tex", ///
	replace fragment booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "OLS: $\Delta$ Income (\\$10k)") ///
	mtitles("Original" "Hrs-wt." "Original" "Hrs-wt." "Original" "Hrs-wt.") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) nonotes noobs ///
	prehead("\begin{tabular}{l*{6}{c}}" "\toprule") ///
	posthead("\midrule")

esttab iv_hi_o iv_hi_w iv_hi_usda_o iv_hi_usda_w iv_hei_usda_o iv_hei_usda_w ///
	using "$overleaf/tabs/labor_supply/ols_iv_no_retirees_two_shocks.tex", ///
	append fragment booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "2SLS: $\Delta$ Income (\\$10k)") ///
	nomtitles nonumbers collabels(none) nonotes ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	prefoot("\midrule") ///
	postfoot("\bottomrule" "\end{tabular}")

// console check
esttab ols_hi_o iv_hi_o ols_hi_usda_o iv_hi_usda_o ols_hei_usda_o iv_hei_usda_o, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn-OLS" "Syn-IV" "USDA-OLS" "USDA-IV" "HEI-OLS" "HEI-IV") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))
esttab ols_hi_w iv_hi_w ols_hi_usda_w iv_hi_usda_w ols_hei_usda_w iv_hei_usda_w, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn-OLS" "Syn-IV" "USDA-OLS" "USDA-IV" "HEI-OLS" "HEI-IV") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

di "13_build_ols_iv_no_retirees.do complete"
