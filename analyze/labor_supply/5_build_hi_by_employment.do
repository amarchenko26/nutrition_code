// ============================================================================
// Does the main nutrition result depend on the not-employed cells?
//
// Households whose (male-preferred) occupation is "not employed" receive their
// shock from the household income of other not-employed households — hard to
// interpret as a labor demand shock. This re-runs the paper's main
// forward-difference IV (m7 spec in analyze/hi/syndigo/build_hi.do) on:
//   (1) all households        (baseline, for comparison)
//   (2) employed cells only   (occupation not 10/12 at t and t-1)
//   (3) employed + occupation-stable (same occupation code at t and t-1)
// for three outcomes: simple index (Syndigo), simple index (USDA), full HEI
// (USDA). Outcomes z-scored on the estimation sample so coefficients are
// comparable (SD per $10k), as in analyze/hi/full_hei/build_hei_note_results.do.
//
// Output: tabs/labor_supply/hi_employed_sample.tex
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// Male-preferred occupation (the cell the HH shock is computed from)
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | occ_src == 0

gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & occ_src != 0

// Employed cell in both years the shock spans (t-1 and t)
gen byte employed_cell = (not_emp_cell == 0 & L.not_emp_cell == 0) ///
	if !missing(not_emp_cell) & !missing(L.not_emp_cell)

// Occupation code unchanged from t-1 (kills the cell-switch channel entirely)
gen byte occ_stable_hh = (occ_src == L.occ_src) ///
	if !missing(occ_src) & !missing(L.occ_src) & occ_src != 0

// z-scored outcomes and their forward differences
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// ============================================================================
// Forward-difference IV, m7 spec: 3 outcomes x 3 samples
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo `v'_all: ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor], ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo `v'_emp: ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor] if employed_cell == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo `v'_st: ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor] if employed_cell == 1 & occ_stable_hh == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab hi_all hi_emp hi_st hi_usda_all hi_usda_emp hi_usda_st ///
	hei_usda_all hei_usda_emp hei_usda_st ///
	using "$overleaf/tabs/labor_supply/hi_employed_sample.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\$10k)") ///
	mtitles("All" "Empl." "Occ.-st." "All" "Empl." "Occ.-st." "All" "Empl." "Occ.-st.") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 0 1 0 0 1 0 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab hi_all hi_emp hi_st hi_usda_all hi_usda_emp hi_usda_st ///
	hei_usda_all hei_usda_emp hei_usda_st, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn-all" "Syn-emp" "Syn-st" "USDA-all" "USDA-emp" "USDA-st" ///
	        "HEI-all" "HEI-emp" "HEI-st") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

// ============================================================================
// Working-head shock (built in 1_build_labor_supply_data.do): the household
// receives the own-cell shock of the head who works — his if only he works,
// hers if only she works, the average if both work, none if neither works.
// "Stable" columns: the head(s) receiving the shock keep the same occupation
// code from t-1 to t (which also fixes the working-head configuration), so
// the shock reflects cell income movement, not anyone switching cells.
// ============================================================================

gen byte working_occ_stable = working_assign_stable
replace working_occ_stable = 0 if working_occ_stable < . & male_works == 1 & male_occ_stable != 1
replace working_occ_stable = 0 if working_occ_stable < . & female_works == 1 & female_occ_stable != 1

eststo clear
foreach v of local outcomes {
	eststo w_`v': ivreghdfe fz_`v' (D.real_income = D.iv_income_working_fips) ///
		[pw = projection_factor], ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo ws_`v': ivreghdfe fz_`v' (D.real_income = D.iv_income_working_fips) ///
		[pw = projection_factor] if working_occ_stable == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab w_hi ws_hi w_hi_usda ws_hi_usda w_hei_usda ws_hei_usda ///
	using "$overleaf/tabs/labor_supply/hi_working_shock.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\$10k)") ///
	mtitles("All" "Stable" "All" "Stable" "All" "Stable") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab w_hi ws_hi w_hi_usda ws_hi_usda w_hei_usda ws_hei_usda, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn" "Syn-st" "USDA" "USDA-st" "HEI" "HEI-st") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

// ============================================================================
// Retiree shocks:
//   (a) current instrument, sample = households in the not-employed cell at
//       both t-1 and t (stayers, so no transition years)
//   (b) no-working-head households, instrument rebuilt on a donor pool of
//       no-working-head households only (build/iv/build_iv_retiree.py) — no
//       other households' working spouses in the cell average. Sample is
//       no-working-head at t-1 and t by construction (IV defined only there).
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo r_`v': ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor] if not_emp_cell == 1 & L.not_emp_cell == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo rr_`v': ivreghdfe fz_`v' (D.real_income = D.iv_income_retiree_fips) ///
		[pw = projection_factor], ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab r_hi rr_hi r_hi_usda rr_hi_usda r_hei_usda rr_hei_usda ///
	using "$overleaf/tabs/labor_supply/hi_retiree_shock.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\$10k)") ///
	mtitles("Not-emp cell" "Retiree IV" "Not-emp cell" "Retiree IV" "Not-emp cell" "Retiree IV") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab r_hi rr_hi r_hi_usda rr_hi_usda r_hei_usda rr_hei_usda, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn-ne" "Syn-ret" "USDA-ne" "USDA-ret" "HEI-ne" "HEI-ret") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))
