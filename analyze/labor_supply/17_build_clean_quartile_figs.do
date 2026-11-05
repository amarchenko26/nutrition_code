// ============================================================================
// 17_build_clean_quartile_figs.do -- baseline-income quartile coefplots on the
// CLEAN SAMPLE (see 12_: no retirees at t, employed shock cell at t-1).
// Adapted from 10_build_no_retiree_quartile_figs.do block (A); original
// (male-preferred) shock.
//
// Outputs (Overleaf figs/labor_supply/):
//   baseline_inc_usda_clean.png
//   baseline_inc_hei_clean.png
// ============================================================================

clear all
set graphics off

do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

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

// z-scored outcomes and forward differences
foreach v in hi_usda hei_usda {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// Baseline-income quartiles (full sample, as in the originals)
cap drop inc_q baseline_income
bysort hhid (year): gen baseline_income = real_income[1]
xtile inc_q = baseline_income [aw = projection_factor], nquantiles(4)

set graphics on
// warm-up export: first export of a headless batch session can render black
quietly twoway scatteri 0 0, msymbol(none)
quietly graph export "`c(tmpdir)'/warmup.png", replace

foreach v in hi_usda hei_usda {
	estimates clear
	forvalues q = 1/4 {
		ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) [pw = projection_factor] ///
			if clean == 1 & inc_q == `q', ///
			absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
		estimates store cl_q`q'
	}
	ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) [pw = projection_factor] ///
		if clean == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	local beta_overall = _b[D.real_income]

	coefplot cl_q*, keep(D.real_income) ///
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
	graph export "$figs/labor_supply/baseline_inc_`stub'_clean.png", replace width(1700)
}

di "17_build_clean_quartile_figs.do complete"
