// ============================================================================
// build_usda_exhibits.do -- USDA-HI versions of every deck exhibit in build_hi.do.
//
// Parallel outputs with an _usda suffix; NOTHING from the Syndigo set is
// overwritten. Mirrors build_hi.do specs exactly (same samples, FE, weights,
// clustering), with one deliberate deviation: reghdfejl (Julia) is replaced by
// ivreghdfe/reghdfe so the file is batch-safe (same convention as
// verify_hi_usda.do; coefficients match to 4 decimals, verified 8/2026).
//
// Inputs : final/final_reg_data.dta  (assembled by build_hi.do, incl. USDA cols)
// Outputs: tabs/results_usda.tex, tabs/results_robust_usda.tex
//          figs/baseline_inc_usda.png, figs/baseline_inc_blank_usda.png
//          figs/iv_binscatter_usda.png
//          figs/corr_{diab,ob,chol,any}_hi_usda.png, figs/corr_diab_hi2_usda.png,
//          figs/corr_inc_hi_usda.png
// ============================================================================

clear all
set graphics off

global TABS "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/tabs"
global FIGS "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs"

use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/final/final_reg_data.dta", replace
xtset hhid year

// ============================================================================
// (A) Main model set m1-m8, outcome = hi_usda
// ============================================================================
eststo clear
preserve
keep if movers_f == 0

// (1) OLS - no controls
eststo u1: reghdfe hi_usda real_income [pw = projection_factor], ///
	cluster(fips)

// (2) OLS - with HH fixed effects
eststo u2: reghdfe hi_usda real_income [pw = projection_factor], ///
	absorb(year hhid kids hh_comp avg_age_hh_head) cluster(fips)

// (3) First stage
eststo u3: reghdfe real_income iv_income_fips [pw = projection_factor], ///
	absorb(year hhid kids hh_comp avg_age_hh_head) vce(cluster zip_code)

// (4) 2SLS FIPS
eststo u4: ivreghdfe hi_usda (real_income=iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
local beta_overall = _b[real_income]

// (5) Small-share HHs IV
eststo u5: ivreghdfe hi_usda (real_income=iv_income_zip) [pw = projection_factor] ///
	if cell_zip_share < 0.25, ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (6) First difference
eststo u6: ivreghdfe D.hi_usda (D.real_income=D.iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (7) Forward difference (headline)
eststo u7: ivreghdfe f_hi_usda (D.real_income=D.iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (8) Pre-trend placebo
eststo u8: ivreghdfe D.hi_usda (f_inc = f_iv) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

esttab u1 u2 u6 u7 u8 using "$TABS/results_usda.tex", ///
	replace booktabs label ///
	b(3) se(3) ///
	star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("OLS" "OLS+HHFE" "First Diff" "Fwd Diff" "Pre-trends") ///
	keep(real_income D.real_income f_inc) ///
	varlabels(real_income "Income (t)" D.real_income "Income (t-1)" f_inc "Income (t+1)") ///
	stats(N r2, fmt(%9.0fc %9.4f) labels("N" "R-squared")) ///
	nonotes

esttab u4 u5 using "$TABS/results_robust_usda.tex", ///
	replace booktabs label ///
	b(3) se(3) ///
	star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("2SLS" "2SLS Small Share") ///
	keep(real_income) ///
	varlabels(real_income "Income (t)") ///
	stats(N r2, fmt(%9.0fc %9.4f) labels("N" "R-squared")) ///
	nonotes

// ============================================================================
// (B) IV effect by baseline-income quartile (baseline_inc_usda.png)
// ============================================================================
cap drop inc_q baseline_income
bysort hhid (year): gen baseline_income = real_income[1]
xtile inc_q = baseline_income [aw=projection_factor], nquantiles(4)

set graphics on 

forvalues q = 1/4 {
	ivreghdfe f_hi_usda (D.real_income=D.iv_income_fips) [pw = projection_factor] ///
		if movers==0 & inc_q == `q', ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estimates store ivu_q`q'
}

// Get main coefficient
ivreghdfe f_hi_usda (D.real_income=D.iv_income_fips) [pw = projection_factor] if movers_f == 0, ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
local beta_overall = _b[D.real_income]

coefplot ivu_q*, keep(D.real_income) ///
    rename(D.real_income = " ") ///
    xtitle("Income quartile", size(medlarge)) ytitle("IV effect on Nutrition (std. dev. per $10k)", size(medlarge)) ///
    title("Effect of Income on Nutrition by Baseline Income", ) ///
    vertical yline(0, lcolor(gray) lpattern(solid)) ciopts(recast(rcap) lwidth(medthick)) ///
    yline(`beta_overall', lcolor(blue) lpattern(shortdash) lwidth(medthick)) legend(off) ///
    text(`=`beta_overall'+.015' .57 "Overall", color(blue) size(medlarge)) ///
    xlabel(.7 "Q1 (Lowest)" .9 "Q2" 1.1 "Q3" 1.3 "Q4 (Highest)", labsize(medlarge)) ///
    ylabel(, labsize(medlarge))
graph export "$FIGS/baseline_inc_usda.png", replace width(1700)


// ============================================================================
// (C) IV binscatter, forward diff + pre-trend (iv_binscatter_usda.png)
// ============================================================================
cap drop Dhat_income fhat_income r_fhiu r_Dhat r_dhiu r_fhat

reghdfe D.real_income D.iv_income_fips [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
predict Dhat_income, xb

reghdfe f_inc f_iv [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
predict fhat_income, xb

reghdfe f_hi_usda [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_fhiu, resid

reghdfe Dhat_income [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_Dhat, resid

reghdfe D.hi_usda [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_dhiu, resid

reghdfe fhat_income [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_fhat, resid

binscatter r_fhiu r_Dhat [aw = projection_factor], ///
    nquantiles(15) ///
    xtitle("{&Delta} Instrumented Income Year Prior", size(medlarge)) ///
    ytitle("{&Delta} Nutrition Year After (USDA)", size(medlarge)) ///
    title("Forward Difference", size(large)) ///
    xlabel(, labsize(medlarge)) ylabel(, labsize(medlarge)) ///
    lcolor(navy) mcolor(navy) ///
    name(gu1, replace)

binscatter r_dhiu r_fhat [aw = projection_factor], ///
    nquantiles(15) ///
    xtitle("{&Delta} Instrumented Income Year After", size(medlarge)) ///
    ytitle("{&Delta} Nutrition Year Prior (USDA)", size(medlarge)) ///
    title("Pre-trend", size(large)) ///
    xlabel(, labsize(medlarge)) ylabel(, labsize(medlarge)) ///
    lcolor(maroon) mcolor(maroon) ///
    name(gu2, replace)

graph combine gu1 gu2, cols(2) ycommon xsize(7) ysize(3.5)
graph export "$FIGS/iv_binscatter_usda.png", replace width(2400)

restore

// ============================================================================
// (D) Health-outcome binscatters vs USDA HI (corr_*_usda.png)
// ============================================================================
preserve

collapse obesity n_dietary_conditions hypertension heart_disease diabetes_type1 ///
	diabetes_type2 cholesterol any_metabolic_disease hi_usda real_income ///
	avg_age_hh_head hh_comp [pw=projection_factor], by(hhid)

#delimit ;
binscatter diabetes_type2 hi_usda,
    n(50) msymbol(O) linetype(lfit)
    xtitle("Nutrition (USDA)")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("$FIGS/corr_diab_hi_usda.png") replace;

binscatter obesity hi_usda,
    n(50) msymbol(O) linetype(lfit)
    xtitle("Nutrition (USDA)")
    ytitle("Obesity (any HH member)")
    savegraph("$FIGS/corr_ob_hi_usda.png") replace;

binscatter cholesterol hi_usda,
    n(50) msymbol(O) linetype(lfit)
    xtitle("Nutrition (USDA)")
    ytitle("Cholesterol (any HH member)")
    savegraph("$FIGS/corr_chol_hi_usda.png") replace;

binscatter any_metabolic_disease hi_usda,
    n(50) msymbol(O) linetype(lfit)
    xtitle("Nutrition (USDA)")
    ytitle("Any Metabolic Disease (any HH member)")
    savegraph("$FIGS/corr_any_hi_usda.png") replace;

binscatter diabetes_type2 hi_usda,
    n(40) controls(real_income) msymbol(O) linetype(qfit)
    xtitle("HH Nutrition (USDA, controlling for income)")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("$FIGS/corr_diab_hi2_usda.png") replace;

binscatter hi_usda real_income,
    n(50) msymbol(O) linetype(qfit)
    xtitle("HH income $1000s")
    ytitle("HH Nutrition (USDA)")
    savegraph("$FIGS/corr_inc_hi_usda.png") replace;
#delimit cr

restore

di "build_usda_exhibits.do complete"
