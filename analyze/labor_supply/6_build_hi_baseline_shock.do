// ============================================================================
// Main nutrition IV with BASELINE-occupation shock assignment.
//
// Instrument: iv_income_baseline_fips (build/iv/build_iv_baseline_occ.py) —
// same leave-one-county-out cell income as the main IV, but each household's
// occupation bin is frozen at its first panel year, so its own job changes /
// retirement can never switch its shock cell. Same m7 spec and z-scoring as
// 5_build_hi_by_employment.do. Columns per outcome:
//   (1) all households with the baseline IV
//   (2) employed at baseline (baseline occ bin is white/blue/service)
//
// Output: tabs/labor_supply/hi_baseline_shock.tex
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
ren hhid household_code
ren year panel_year
merge 1:1 household_code panel_year using "$data/interim/panel_dataset/iv_income_baseline_occ.dta", ///
	keep(master match) nogenerate
ren household_code hhid
ren panel_year year

xtset hhid year
keep if movers_f == 0

gen byte emp_base = inlist(baseline_occ_bin, "white", "blue", "service") ///
	if !inlist(baseline_occ_bin, "", "nan")

// z-scored outcomes and their forward differences
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

eststo clear
foreach v of local outcomes {
	eststo b_`v': ivreghdfe fz_`v' (D.real_income = D.iv_income_baseline_fips) ///
		[pw = projection_factor], ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo be_`v': ivreghdfe fz_`v' (D.real_income = D.iv_income_baseline_fips) ///
		[pw = projection_factor] if emp_base == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab b_hi be_hi b_hi_usda be_hi_usda b_hei_usda be_hei_usda ///
	using "$overleaf/tabs/labor_supply/hi_baseline_shock.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\$10k)") ///
	mtitles("All" "Empl. base" "All" "Empl. base" "All" "Empl. base") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab b_hi be_hi b_hi_usda be_hi_usda b_hei_usda be_hei_usda, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn" "Syn-eb" "USDA" "USDA-eb" "HEI" "HEI-eb") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))
