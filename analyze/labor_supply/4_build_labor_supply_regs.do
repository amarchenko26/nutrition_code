// ============================================================================
// Labor supply responses to income shocks.
//
// A. Household-level 2SLS (moved from analyze/hi/syndigo/build_hi.do):
//    forward diff of HH labor supply on instrumented income change.
// B. Head-specific shocks (reduced form), couples: does the male head's own
//    shock move his labor supply, and his wife's? And vice versa.
// C. Same, split by baseline income (below/above median).
// D. Opposing-sign spouse shocks: is the labor supply / nutrition response
//    different when the two heads' shocks disagree in sign?
// E. Single mothers (one female head, kids under 18): own shock only.
//
// Spec mirrors the main HI regressions: [pw = projection_factor], non-movers,
// absorb(year hhid kids avg_age_hh_head hh_comp), SE clustered on county.
//
// Outputs (Overleaf tabs/labor_supply/):
//   hh_labor_supply_iv.tex · own_cross_shocks.tex · own_cross_shocks_stable.tex
//   own_cross_by_income.tex · opposing_shocks.tex · single_mother_shocks.tex
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year

// ============================================================================
// A. Household-level income effect on labor supply (2SLS, moved from build_hi.do)
// ============================================================================

eststo clear

eststo a_hours: ivreghdfe f_hh_avg_workhours (D.real_income = D.iv_income_fips) ///
	[pw = projection_factor] if movers_f == 0, ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
quietly summarize hh_avg_workhours if e(sample)
estadd scalar mean_y = r(mean)

eststo a_hours_emp: ivreghdfe f_hh_avg_workhours_emp (D.real_income = D.iv_income_fips) ///
	[pw = projection_factor] if movers_f == 0, ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
quietly summarize hh_avg_workhours_if_employed if e(sample)
estadd scalar mean_y = r(mean)

eststo a_emp: ivreghdfe f_hh_employed (D.real_income = D.iv_income_fips) ///
	[pw = projection_factor] if movers_f == 0, ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
quietly summarize hh_employed if e(sample)
estadd scalar mean_y = r(mean)

eststo a_male: ivreghdfe f_male_head_employed (D.real_income = D.iv_income_fips) ///
	[pw = projection_factor] if movers_f == 0, ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
quietly summarize male_head_employed if e(sample)
estadd scalar mean_y = r(mean)

eststo a_female: ivreghdfe f_female_head_employed (D.real_income = D.iv_income_fips) ///
	[pw = projection_factor] if movers_f == 0, ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
quietly summarize female_head_employed if e(sample)
estadd scalar mean_y = r(mean)

esttab a_hours a_hours_emp a_emp a_male a_female ///
	using "$overleaf/tabs/labor_supply/hh_labor_supply_iv.tex", ///
	replace booktabs label ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\$10k)") ///
	mtitles("Avg hours" "Hours $|$ emp." "Share employed" "Male employed" "Female employed") ///
	collabels(none) ///
	stats(mean_y N, fmt(%9.2fc %9.0fc) labels("Mean outcome" "N")) ///
	nonotes

esttab a_hours a_hours_emp a_emp a_male a_female, keep(D.real_income) b(3) se(3) ///
	mtitles("hours" "hours|emp" "emp" "male emp" "fem emp") star(* 0.10 ** 0.05 *** 0.01)

// ============================================================================
// B. Head-specific shocks, own vs spouse (reduced form)
//    Sample: two-head couples (both shocks defined by construction of sample)
// ============================================================================

eststo clear
foreach y in male_head_hours male_head_employed female_head_hours female_head_employed {
	eststo b_`y': reghdfe f_`y' d_iv_male d_iv_female ///
		[pw = projection_factor] if movers_f == 0 & n_heads == 2, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	quietly summarize `y' if e(sample)
	estadd scalar mean_y = r(mean)
}

esttab b_male_head_hours b_male_head_employed b_female_head_hours b_female_head_employed ///
	using "$overleaf/tabs/labor_supply/own_cross_shocks.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(d_iv_male d_iv_female) ///
	varlabels(d_iv_male "Male head shock (\$10k)" d_iv_female "Female head shock (\$10k)") ///
	mtitles("Hours" "Employed" "Hours" "Employed") ///
	mgroups("Male head outcome" "Female head outcome", pattern(1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(mean_y N, fmt(%9.2fc %9.0fc) labels("Mean outcome" "N")) ///
	nonotes

esttab b_*, keep(d_iv_male d_iv_female) b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("M hours" "M emp" "F hours" "F emp")

// Robustness: both heads' occupations unchanged from t-1, so the shock is
// movement in the cell's income, not the head switching cells (job loss moves
// a head into the not-employed cell, which mechanically lowers their cell income)
eststo clear
foreach y in male_head_hours male_head_employed female_head_hours female_head_employed {
	eststo bs_`y': reghdfe f_`y' d_iv_male d_iv_female ///
		[pw = projection_factor] if movers_f == 0 & n_heads == 2 ///
		& male_occ_stable == 1 & female_occ_stable == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	quietly summarize `y' if e(sample)
	estadd scalar mean_y = r(mean)
}

esttab bs_male_head_hours bs_male_head_employed bs_female_head_hours bs_female_head_employed ///
	using "$overleaf/tabs/labor_supply/own_cross_shocks_stable.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(d_iv_male d_iv_female) ///
	varlabels(d_iv_male "Male head shock (\$10k)" d_iv_female "Female head shock (\$10k)") ///
	mtitles("Hours" "Employed" "Hours" "Employed") ///
	mgroups("Male head outcome" "Female head outcome", pattern(1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(mean_y N, fmt(%9.2fc %9.0fc) labels("Mean outcome" "N")) ///
	nonotes

esttab bs_*, keep(d_iv_male d_iv_female) b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("M hours" "M emp" "F hours" "F emp")

// ============================================================================
// C. Own vs spouse responses by baseline income (below/above median)
// ============================================================================

eststo clear
foreach y in male_head_hours female_head_hours {
	eststo c_`y'_lo: reghdfe f_`y' d_iv_male d_iv_female ///
		[pw = projection_factor] if movers_f == 0 & n_heads == 2 & inc_q <= 2, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	eststo c_`y'_hi: reghdfe f_`y' d_iv_male d_iv_female ///
		[pw = projection_factor] if movers_f == 0 & n_heads == 2 & inc_q >= 3, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
}

esttab c_male_head_hours_lo c_male_head_hours_hi c_female_head_hours_lo c_female_head_hours_hi ///
	using "$overleaf/tabs/labor_supply/own_cross_by_income.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(d_iv_male d_iv_female) ///
	varlabels(d_iv_male "Male head shock (\$10k)" d_iv_female "Female head shock (\$10k)") ///
	mtitles("Low income" "High income" "Low income" "High income") ///
	mgroups("Male head hours" "Female head hours", pattern(1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(N, fmt(%9.0fc) labels("N")) ///
	nonotes

esttab c_*, keep(d_iv_male d_iv_female) b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("M hrs lo" "M hrs hi" "F hrs lo" "F hrs hi")

// ============================================================================
// D. Opposing-sign spouse shocks: differential response when shocks disagree
//    Interactions: does a dollar of shock move labor supply / nutrition more
//    (or less) when the spouses' shocks have opposite signs?
// ============================================================================

// How common is each sign combination?
tab shock_combo if movers_f == 0 & n_heads == 2

eststo clear
foreach y in male_head_hours female_head_hours hh_total_workhours hi {
	eststo d_`y': reghdfe f_`y' c.d_iv_male##i.opp_sign c.d_iv_female##i.opp_sign ///
		[pw = projection_factor] if movers_f == 0 & n_heads == 2, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	quietly summarize `y' if e(sample)
	estadd scalar mean_y = r(mean)
}

esttab d_male_head_hours d_female_head_hours d_hh_total_workhours d_hi ///
	using "$overleaf/tabs/labor_supply/opposing_shocks.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(d_iv_male d_iv_female 1.opp_sign 1.opp_sign#c.d_iv_male 1.opp_sign#c.d_iv_female) ///
	varlabels(d_iv_male "Male head shock (\$10k)" ///
	          d_iv_female "Female head shock (\$10k)" ///
	          1.opp_sign "Opposite-sign shocks" ///
	          1.opp_sign#c.d_iv_male "Male shock $\times$ opposite sign" ///
	          1.opp_sign#c.d_iv_female "Female shock $\times$ opposite sign") ///
	mtitles("Male hours" "Female hours" "HH hours" "Health Index") ///
	collabels(none) ///
	stats(mean_y N, fmt(%9.2fc %9.0fc) labels("Mean outcome" "N")) ///
	nonotes

esttab d_*, keep(d_iv_male d_iv_female 1.opp_sign 1.opp_sign#c.d_iv_male 1.opp_sign#c.d_iv_female) ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) mtitles("M hrs" "F hrs" "HH hrs" "HI")

// ============================================================================
// E. Single mothers: one female head, children under 18 present
//    (kids code 9 = no children under 18). Own shock only — no spouse.
//    Second pair of columns: occupation unchanged from t-1.
// ============================================================================

eststo clear
foreach y in female_head_hours female_head_employed {
	eststo sm_`y': reghdfe f_`y' d_iv_female ///
		[pw = projection_factor] if movers_f == 0 & n_heads == 1 ///
		& !missing(female_head_employed) & kids != 9, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	quietly summarize `y' if e(sample)
	estadd scalar mean_y = r(mean)
	eststo sms_`y': reghdfe f_`y' d_iv_female ///
		[pw = projection_factor] if movers_f == 0 & n_heads == 1 ///
		& !missing(female_head_employed) & kids != 9 & female_occ_stable == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	quietly summarize `y' if e(sample)
	estadd scalar mean_y = r(mean)
}

esttab sm_female_head_hours sm_female_head_employed sms_female_head_hours sms_female_head_employed ///
	using "$overleaf/tabs/labor_supply/single_mother_shocks.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(d_iv_female) ///
	varlabels(d_iv_female "Own shock (\$10k)") ///
	mtitles("Hours" "Employed" "Hours" "Employed") ///
	mgroups("All single mothers" "Occupation-stable", pattern(1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(mean_y N, fmt(%9.2fc %9.0fc) labels("Mean outcome" "N")) ///
	nonotes

esttab sm_female_head_hours sm_female_head_employed sms_female_head_hours sms_female_head_employed, ///
	keep(d_iv_female) b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Hours" "Emp" "Hours-st" "Emp-st")
