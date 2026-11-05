// ============================================================================
// build_hei_exhibits.do -- full-HEI (hei_usda, 0-100) versions of the main
// results table and the baseline-income coefplot, parallel to the Syndigo and
// simple-USDA exhibits (build_hi.do / build_usda_exhibits.do).
//
// hei_usda is on a 0-100 scale, so it is standardized to a z-score over the
// non-mover estimation sample before estimation -- this makes every coefficient
// "SD per $10k", directly comparable to the Syndigo and simple-USDA slides and
// to the HEI note (build_hei_note_results.do; forward-diff = 0.031***).
// ivreghdfe/reghdfe (not reghdfejl) so the file is batch-safe. No table notes.
//
// Inputs : final/final_reg_data.dta (incl. hei_usda)
// Outputs: tabs/results_hei.tex
//          figs/baseline_inc_hei.png
// ============================================================================

clear all
set graphics off

global TABS "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/tabs"
global FIGS "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs"

use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/final/final_reg_data.dta", replace
xtset hhid year

eststo clear
preserve
keep if movers_f == 0

// standardize the 0-100 HEI over the estimation sample; build its differences
qui sum hei_usda [aw = projection_factor]
gen z_hei = (hei_usda - r(mean)) / r(sd)
gen f_hei = F.z_hei - z_hei

// (1) OLS - no controls
eststo h1: reghdfe z_hei real_income [pw = projection_factor], ///
	cluster(fips)

// (2) OLS - with HH fixed effects
eststo h2: reghdfe z_hei real_income [pw = projection_factor], ///
	absorb(year hhid kids hh_comp avg_age_hh_head) cluster(fips)

// (6) First difference
eststo h6: ivreghdfe D.z_hei (D.real_income=D.iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (7) Forward difference (headline)
eststo h7: ivreghdfe f_hei (D.real_income=D.iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (8) Pre-trend placebo
eststo h8: ivreghdfe D.z_hei (f_inc = f_iv) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

esttab h1 h2 h6 h7 h8 using "$TABS/results_hei.tex", ///
	replace booktabs label ///
	b(3) se(3) ///
	star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("OLS" "OLS+HHFE" "First Diff" "Fwd Diff" "Pre-trends") ///
	keep(real_income D.real_income f_inc) ///
	varlabels(real_income "Income (t)" D.real_income "Income (t-1)" f_inc "Income (t+1)") ///
	stats(N r2, fmt(%9.0fc %9.4f) labels("N" "R-squared")) ///
	nonotes

// ============================================================================
// baseline_inc_hei.png : IV effect by baseline-income quartile
// ============================================================================
cap drop inc_q baseline_income
bysort hhid (year): gen baseline_income = real_income[1]
xtile inc_q = baseline_income [aw=projection_factor], nquantiles(4)

set graphics on

forvalues q = 1/4 {
	ivreghdfe f_hei (D.real_income=D.iv_income_fips) [pw = projection_factor] ///
		if movers==0 & inc_q == `q', ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estimates store ivh_q`q'
}

// overall forward-diff HEI effect for the reference line
ivreghdfe f_hei (D.real_income=D.iv_income_fips) [pw = projection_factor] if movers_f == 0, ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
local beta_overall = _b[D.real_income]

coefplot ivh_q*, keep(D.real_income) ///
    rename(D.real_income = " ") ///
    xtitle("Income quartile", size(medlarge)) ytitle("IV effect on Nutrition (std. dev. per $10k)", size(medlarge)) ///
    title("Effect of Income on Nutrition by Baseline Income (Full HEI)") ///
    vertical yline(0, lcolor(gray) lpattern(solid)) ciopts(recast(rcap) lwidth(medthick)) ///
    yline(`beta_overall', lcolor(blue) lpattern(shortdash) lwidth(medthick)) legend(off) ///
    text(`=`beta_overall'+.015' .57 "Overall", color(blue) size(medlarge)) ///
    xlabel(.7 "Q1 (Lowest)" .9 "Q2" 1.1 "Q3" 1.3 "Q4 (Highest)", labsize(medlarge)) ///
    ylabel(, labsize(medlarge))
graph export "$FIGS/baseline_inc_hei.png", replace width(1700)

// ============================================================================
// IV binscatter, forward diff + pre-trend (iv_binscatter_hei.png)
// ============================================================================
cap drop Dhat_income fhat_income r_fhei r_Dhat r_dhei r_fhat

reghdfe D.real_income D.iv_income_fips [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
predict Dhat_income, xb

reghdfe f_inc f_iv [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
predict fhat_income, xb

reghdfe f_hei [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_fhei, resid

reghdfe Dhat_income [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_Dhat, resid

reghdfe D.z_hei [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_dhei, resid

reghdfe fhat_income [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_fhat, resid

binscatter r_fhei r_Dhat [aw = projection_factor], ///
    nquantiles(15) ///
    xtitle("{&Delta} Instrumented Income Year Prior", size(medlarge)) ///
    ytitle("{&Delta} Nutrition Year After (Full HEI)", size(medlarge)) ///
    title("Forward Difference", size(large)) ///
    xlabel(, labsize(medlarge)) ylabel(, labsize(medlarge)) ///
    lcolor(navy) mcolor(navy) ///
    name(gh1, replace)

binscatter r_dhei r_fhat [aw = projection_factor], ///
    nquantiles(15) ///
    xtitle("{&Delta} Instrumented Income Year After", size(medlarge)) ///
    ytitle("{&Delta} Nutrition Year Prior (Full HEI)", size(medlarge)) ///
    title("Pre-trend", size(large)) ///
    xlabel(, labsize(medlarge)) ylabel(, labsize(medlarge)) ///
    lcolor(maroon) mcolor(maroon) ///
    name(gh2, replace)

graph combine gh1 gh2, cols(2) ycommon xsize(7) ysize(3.5)
graph export "$FIGS/iv_binscatter_hei.png", replace width(2400)

restore
di "build_hei_exhibits.do complete"
