// ============================================================================
// 21_build_iv_decomposition.do -- the clean-sample IV by employment
// transition at t: a TREATMENT EFFECT for each group (Anya 2026-09-17;
// replaces the earlier means/first-stage/reduced-form/Wald layout).
//
// Groups by the shock cell's status at t, given an employed cell at t-1
// (the clean sample; male-preferred occupation, not-employed = codes 10/12):
//   1  still in an employed cell at t
//   2  not-employed cell at t, average head age under 67  (job losers)
//   3  not-employed cell at t, average head age 67+       (entered retirement)
// Household-years whose cell at t is undefined (armed forces / no head) drop.
//
// One interacted 2SLS per outcome on the pooled clean sample:
//   fz_y = sum_g beta_g (D.real_income x 1[g]) + group dummies + FE,
//   instrumented by (d_iv x 1[g]); exactly identified. beta_g is the group's
// own IV estimate, but identified with the pooled household FE (a separate
// regression per transition group would be identified only off households
// with repeated transitions). Group membership at t is an outcome of the
// shock, so this is a decomposition of the estimate, not a sample split.
//
// Spec as in 12_: non-movers, [pw = projection_factor], absorb(year hhid
// kids avg_age_hh_head hh_comp), county-clustered SE; outcomes z-scored.
// Sanderson-Windmeijer first-stage F per group from ivreg2's ffirst (the
// joint Kleibergen-Paap F is not reported: it is pulled to ~0 by the
// retirement group's instrument, which has no first stage on its own).
//
// Output (tabs/labor_supply/): iv_decomposition.tex
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// Assigned occupation and SAMPLE exactly as in 12_ (labor_supply.tex
// "Measurement of key variables" / "Sample", 2026-09-16): male head unless
// absent or armed forces (7), then female; employed cell = 1-6, 8, 9, 11;
// clean = employed cell at t-1 (t-1 only).
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell     = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte not_emp_cell = inlist(occ_src, 10, 12)                    if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)

// transition groups at t (see header)
gen byte grp = .
replace grp = 1 if clean == 1 & emp_cell == 1
replace grp = 2 if clean == 1 & not_emp_cell == 1 & avg_age_hh_head < 67
replace grp = 3 if clean == 1 & not_emp_cell == 1 & avg_age_hh_head >= 67

local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}
gen d_inc = D.real_income

forvalues g = 1/3 {
	gen byte g`g'  = (grp == `g') if !missing(grp)
	gen d_inc_g`g' = d_inc * g`g'
	gen d_iv_g`g'  = d_iv  * g`g'
}

// weighted share of each group among household-years that enter the regressions
quietly summarize projection_factor if !missing(grp, d_inc, d_iv, fz_hei_usda)
local wtot = r(sum)
forvalues g = 1/3 {
	quietly summarize projection_factor if grp == `g' & !missing(d_inc, d_iv, fz_hei_usda)
	local sh`g' : di %4.1f 100 * r(sum) / `wtot'
	local sh`g' = strtrim("`sh`g''")
}
// \(t\) rather than $t$: "$t" would expand a (nonexistent) global
local lab1 "\(\times\) still employed at \(t\) (`sh1'\%)"
local lab2 "\(\times\) job losers: not employed at \(t\), under 67 (`sh2'\%)"
local lab3 "\(\times\) entered retirement: not employed at \(t\), 67+ (`sh3'\%)"

// ============================================================================
// Interacted 2SLS, one per outcome
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo g_`v': ivreghdfe fz_`v' (d_inc_g1 d_inc_g2 d_inc_g3 = d_iv_g1 d_iv_g2 d_iv_g3) g2 g3 ///
		[pw = projection_factor] if clean == 1 & !missing(grp), ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) ffirst
	estadd scalar fs_F = e(widstat)
	// Sanderson-Windmeijer (older ivreg2: Angrist-Pischke) first-stage F per group
	cap mat drop FS
	mat FS = e(first)
	local rn : rownames FS
	local swrow "SWF"
	if strpos(" `rn' ", " SWF ") == 0 local swrow "APF"
	forvalues g = 1/3 {
		estadd scalar swf`g' = FS[rownumb(FS, "`swrow'"), colnumb(FS, "d_inc_g`g'")]
	}
}

// \\$ emits a literal \$ into the .tex (a single \$ reaches LaTeX as a bare $)
esttab g_hi g_hi_usda g_hei_usda ///
	using "$overleaf/tabs/labor_supply/iv_decomposition.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(d_inc_g1 d_inc_g2 d_inc_g3) order(d_inc_g1 d_inc_g2 d_inc_g3) ///
	refcat(d_inc_g1 "$\Delta$ Income (\\$10k)", nolabel) ///
	varlabels(d_inc_g1 "`lab1'" d_inc_g2 "`lab2'" d_inc_g3 "`lab3'") ///
	mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
	collabels(none) ///
	stats(swf1 swf2 swf3 N, fmt(%9.1fc %9.1fc %9.1fc %9.0fc) ///
	      labels("First-stage F: still employed" "First-stage F: job losers" ///
	             "First-stage F: entered retirement" "N")) ///
	nonotes

esttab g_hi g_hi_usda g_hei_usda, ///
	keep(d_inc_g1 d_inc_g2 d_inc_g3) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syndigo" "USDA" "HEI") ///
	stats(swf1 swf2 swf3 fs_F N, fmt(%9.1fc %9.1fc %9.1fc %9.0fc %9.0fc))

di "21_build_iv_decomposition.do complete"
