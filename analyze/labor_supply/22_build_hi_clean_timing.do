// ============================================================================
// 22_build_hi_clean_timing.do -- WHEN does nutrition respond? Same income
// change, outcome window moved one calendar year at a time (Anya 2026-09-17).
//
// Timing (labor_supply.tex "Validating income timing"): the year-t panelist
// file carries the fall-(t-1) survey, and panelists report annualized
// CURRENT income, so file-t income is income during calendar t-1. The file-
// year change D.real_income = m(t) - m(t-1) is therefore the change in income
// between calendar years t-2 and t-1. Purchases are dated by trip: y(t) is
// calendar year t. Hence the main specification (F.y - y on D.m) pairs an
// outcome change TWO calendar years after the income change, and the
// "pre-trend placebo" (D.y on F.m - m) is contemporaneous in calendar time.
//
// This script holds the regressor fixed -- D.real_income instrumented by
// D.iv_income_fips, exactly as in 12_ -- and moves the outcome window:
//   L2D.y = y(t-2) - y(t-3)  purchases t-3 -> t-2  one year BEFORE the income change (true placebo)
//   LD.y  = y(t-1) - y(t-2)  purchases t-2 -> t-1  same calendar years as the income change
//   D.y   = y(t)   - y(t-1)  purchases t-1 -> t    one year after  ("nutrition moved forward one year")
//   FD.y  = y(t+1) - y(t)    purchases t   -> t+1  two years after (main specification, = hi_clean.tex)
//   F2D.y = y(t+2) - y(t+1)  purchases t+1 -> t+2  three years after
//
// Sample and spec as in 12_: clean = employed shock cell at t-1 (t-1 only),
// non-movers, [pw = projection_factor], absorb(year hhid kids avg_age_hh_head
// hh_comp), county-clustered SE. Outcomes z-scored (SD per $10k).
//
// Output (tabs/labor_supply/): hi_clean_timing.tex
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// ------------------------------------------------------------ clean-sample flag
// Assigned occupation and SAMPLE exactly as in 12_ (labor_supply.tex
// "Measurement of key variables" / "Sample", 2026-09-16): male head unless
// absent or armed forces (7), then female; employed cell = 1-6, 8, 9, 11;
// clean = employed cell at t-1 (t-1 only).
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)

// ------------------------------- z-scored outcomes, change at each calendar lag
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v' = (`v' - r(mean)) / r(sd)
	gen dz_m1_`v' = L2D.z_`v'      // purchases t-3 -> t-2
	gen dz_0_`v'  = LD.z_`v'       // purchases t-2 -> t-1
	gen dz_p1_`v' = D.z_`v'        // purchases t-1 -> t
	gen dz_p2_`v' = FD.z_`v'       // purchases t   -> t+1   (main)
	gen dz_p3_`v' = F2D.z_`v'      // purchases t+1 -> t+2
}

local lags m1 0 p1 p2 p3
local lab_m1 "\(t-3 \to t-2\): one year before"
local lab_0  "\(t-2 \to t-1\): same years"
local lab_p1 "\(t-1 \to t\): one year after"
local lab_p2 "\(t \to t+1\): two years after (main)"
local lab_p3 "\(t+1 \to t+2\): three years after"

// ============================================================================
// 2SLS at each lag
// ============================================================================

eststo clear
foreach k of local lags {
	foreach v of local outcomes {
		eststo t`k'_`v': ivreghdfe dz_`k'_`v' (D.real_income = D.iv_income_fips) ///
			[pw = projection_factor] if clean == 1, ///
			absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
		estadd scalar fs_F = e(widstat)
	}
	esttab t`k'_hi t`k'_hi_usda t`k'_hei_usda, ///
		keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
		mtitles("Syndigo" "USDA" "HEI") title("`lab_`k''") ///
		stats(fs_F N, fmt(%9.0fc %9.0fc))
}

// ============================================================================
// One table: a block of rows per outcome window (fragment/append, as in 13_).
// posthead("%")/prefoot("%") replace esttab's default \midrule after the (empty)
// header and before the stats rows with a line LaTeX ignores, so consecutive
// blocks are separated by a single rule. \(t\) is
// used instead of $t$ because "$t" would expand a (nonexistent) global.
// ============================================================================

local file "$overleaf/tabs/labor_supply/hi_clean_timing.tex"
local i = 0
foreach k of local lags {
	local ++i
	if `i' == 1 {
		esttab t`k'_hi t`k'_hi_usda t`k'_hei_usda using "`file'", ///
			replace fragment booktabs ///
			b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
			keep(D.real_income) varlabels(D.real_income "`lab_`k''") ///
			refcat(D.real_income "Purchase window, years relative to the income change:", nolabel) ///
			mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
			collabels(none) nonotes ///
			stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
			prehead("\begin{tabular}{l*{3}{c}}" "\toprule") posthead("\midrule") ///
			prefoot("%") postfoot("\midrule")
	}
	else if `i' < 5 {
		esttab t`k'_hi t`k'_hi_usda t`k'_hei_usda using "`file'", ///
			append fragment booktabs ///
			b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
			keep(D.real_income) varlabels(D.real_income "`lab_`k''") ///
			nomtitles nonumbers collabels(none) nonotes ///
			stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
			posthead("%") prefoot("%") postfoot("\midrule")
	}
	else {
		esttab t`k'_hi t`k'_hi_usda t`k'_hei_usda using "`file'", ///
			append fragment booktabs ///
			b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
			keep(D.real_income) varlabels(D.real_income "`lab_`k''") ///
			nomtitles nonumbers collabels(none) nonotes ///
			stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
			posthead("%") prefoot("%") postfoot("\bottomrule" "\end{tabular}")
	}
}

di "22_build_hi_clean_timing.do complete"
