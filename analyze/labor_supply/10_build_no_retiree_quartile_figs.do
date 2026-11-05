// ============================================================================
// Baseline-income quartile coefplots: no-retirees versions + metabolic disease.
//
// Mirrors the baseline_inc_usda/baseline_inc_hei figures (forward-diff IV by
// baseline-income quartile, build_usda_exhibits.do block B) on the
// labor-supply data, with retirees dropped as in 7_build_hi_no_retirees.do
// (not-employed shock cell AND avg head age >= 62, at t or t-1). Also makes
// the same figure for any metabolic disease (full sample, in percentage
// points; ailments observed 2011-2019 only) and prints the descriptive
// change in prevalence by quartile.
//
// Outputs (Overleaf figs/):
//   labor_supply/baseline_inc_usda_no_retirees.png
//   labor_supply/baseline_inc_hei_no_retirees.png
//   labor_supply/baseline_inc_metabolic.png
//   health/metabolic_change_by_income_quartile.png  (descriptive 2011->2019
//     change in prevalence by baseline-income quartile -- the IV coefplot above
//     is too noisy to be informative, so the deck uses this descriptive twin)
// ============================================================================

clear all
set graphics off

do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// Retiree flag as in 7_
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | occ_src == 0
gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & occ_src != 0
gen byte retiree = not_emp_cell == 1 & avg_age_hh_head >= 62 ///
	if !missing(not_emp_cell) & !missing(avg_age_hh_head)
gen byte no_retiree = (retiree == 0 & L.retiree == 0) ///
	if !missing(retiree) & !missing(L.retiree)

// z-scored outcomes and forward differences (as in 7_)
foreach v in hi_usda hei_usda {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// Metabolic disease in percentage points
gen metab100 = 100 * any_metabolic_disease
gen f_metab  = F.metab100 - metab100

// Baseline-income quartiles (full sample, as in the originals)
cap drop inc_q baseline_income
bysort hhid (year): gen baseline_income = real_income[1]
xtile inc_q = baseline_income [aw = projection_factor], nquantiles(4)

set graphics on
// warm-up export: first export of a headless batch session can render black
quietly twoway scatteri 0 0, msymbol(none)
quietly graph export "`c(tmpdir)'/warmup.png", replace

// ============================================================================
// (A) Nutrition coefplots, no retirees
// ============================================================================

foreach v in hi_usda hei_usda {
	estimates clear
	forvalues q = 1/4 {
		ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) [pw = projection_factor] ///
			if no_retiree == 1 & inc_q == `q', ///
			absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
		estimates store nr_q`q'
	}
	ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) [pw = projection_factor] ///
		if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	local beta_overall = _b[D.real_income]

	coefplot nr_q*, keep(D.real_income) ///
	    rename(D.real_income = " ") ///
	    xtitle("Income quartile", size(medlarge)) ///
	    ytitle("IV effect on Nutrition (std. dev. per $10k)", size(medlarge)) ///
	    title("Effect of Income on Nutrition by Baseline Income") ///
	    vertical yline(0, lcolor(gray) lpattern(solid)) ciopts(recast(rcap) lwidth(medthick)) ///
	    yline(`beta_overall', lcolor(blue) lpattern(shortdash) lwidth(medthick)) legend(off) ///
	    text(`=`beta_overall'+.015' .57 "Overall", color(blue) size(medlarge)) ///
	    xlabel(.7 "Q1 (Lowest)" .9 "Q2" 1.1 "Q3" 1.3 "Q4 (Highest)", labsize(medlarge)) ///
	    ylabel(, labsize(medlarge))
	local stub = cond("`v'" == "hi_usda", "usda", "hei")
	graph export "$figs/labor_supply/baseline_inc_`stub'_no_retirees.png", replace width(1700)
}

// ============================================================================
// (B) Metabolic disease coefplot (full sample; ailments observed 2011-2019)
// ============================================================================

estimates clear
forvalues q = 1/4 {
	ivreghdfe f_metab (D.real_income = D.iv_income_fips) [pw = projection_factor] ///
		if inc_q == `q', ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estimates store mb_q`q'
}
ivreghdfe f_metab (D.real_income = D.iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
local beta_overall = _b[D.real_income]

coefplot mb_q*, keep(D.real_income) ///
    rename(D.real_income = " ") ///
    xtitle("Income quartile", size(medlarge)) ///
    ytitle("IV effect on any metabolic disease (pp per $10k)", size(medlarge)) ///
    title("Effect of Income on Metabolic Disease by Baseline Income") ///
    vertical yline(0, lcolor(gray) lpattern(solid)) ciopts(recast(rcap) lwidth(medthick)) ///
    yline(`beta_overall', lcolor(blue) lpattern(shortdash) lwidth(medthick)) legend(off) ///
    xlabel(.7 "Q1 (Lowest)" .9 "Q2" 1.1 "Q3" 1.3 "Q4 (Highest)", labsize(medlarge)) ///
    ylabel(, labsize(medlarge))
graph export "$figs/labor_supply/baseline_inc_metabolic.png", replace width(1700)

// Descriptive: prevalence by quartile, first vs last ailment year
table inc_q year if inlist(year, 2011, 2019) [aw = projection_factor], ///
	statistic(mean any_metabolic_disease) nototals

// ============================================================================
// (C) Descriptive change in metabolic-disease prevalence by quartile (figure)
//     Companion to the baseline-income nutrition coefplots: did the quartiles
//     with the biggest nutrition gains (Q1-Q3) also see metabolic disease fall
//     the most?
// ============================================================================

cap mkdir "$figs/health"
preserve
keep if inlist(year, 2011, 2019) & !missing(any_metabolic_disease) & !missing(inc_q)
collapse (mean) prev = any_metabolic_disease [aw = projection_factor], by(inc_q year)
reshape wide prev, i(inc_q) j(year)
gen dpp = 100 * (prev2019 - prev2011)
gen dlab = string(dpp, "%9.1f") + " pp"
list inc_q prev2011 prev2019 dpp

twoway (bar dpp inc_q, barwidth(.62) color("200 50 50%70") lcolor("200 50 50")) ///
       (scatter dpp inc_q, msymbol(none) mlabel(dlab) mlabposition(6) ///
            mlabgap(*2) mlabsize(medlarge) mlabcolor(black)), ///
    yline(0, lcolor(gray)) legend(off) ///
    xtitle("Baseline income quartile", size(medlarge)) ///
    ytitle("Change in share with any metabolic disease" "2011-2019 (pp)", size(medlarge)) ///
    title("Metabolic disease fell most in Q1-Q3", size(large)) ///
    xlabel(1 `""Q1" "(Lowest)""' 2 "Q2" 3 "Q3" 4 `""Q4" "(Highest)""', labsize(medlarge)) ///
    ylabel(-5(1)0, labsize(medlarge)) ///
    graphregion(color(white)) plotregion(margin(medium))
graph export "$figs/health/metabolic_change_by_income_quartile.png", replace width(1700)
restore
