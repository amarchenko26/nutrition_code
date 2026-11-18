// ============================================================================
// build_iv_health.do -- the income IV on reported health conditions (Anya's note in 0_main.tex, 2026-11-18: "regress the obesity,
// type 2 diabetes, and hypertension on the income iv regression itself")
//
// Main specification of analyze/labor_supply/12_ : households employed at t-1 that stay in their county; change in the outcome
// from t to t+1 on the change in income from t-1 to t ($10k), instrumented with the change in leave-one-county-out group income;
// household, state-by-year, children, head age and household composition fixed effects; projection weights; clustered on county.
// Placebo: the same outcome from t-3 to t-2.
// Outcomes: 100 if the household reports the condition for any member in the Health Care Survey of that year (household flags of
// clean/nielsen/clean_ailments.py: obesity = "Obesity / Overweight", diabetes_type2, hypertension), 0 if it answered the survey
// and does not, missing if it did not answer. The survey starts in 2011; it changed from one questionnaire per member to one
// respondent per household in 2016 (state-by-year fixed effects take out the level change).
// Output: interim/what_moves/iv_health.csv (read by build_iv_health_table.py), printed as well.
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use "$gdir/labor_supply_reg_data.dta", clear
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
sort hhid year
xtset hhid year
gen byte emp_cell = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)
egen year_state = group(year fips_state_code)
cap drop d_iv
cap drop d_inc
gen double d_inc = D.real_income
gen double d_iv = D.iv_income_fips / 10
local FE "year_state hhid kids avg_age_hh_head hh_comp"
local base "movers_f == 0 & clean == 1"
tab year if !missing(obesity)
foreach v in obesity diabetes_type2 hypertension {
	gen double h_`v' = 100 * `v'
	gen double m_`v' = F.h_`v' - h_`v'
	gen double b_`v' = L2.h_`v' - L3.h_`v'
}

file open fh using "$idir/what_moves/iv_health.csv", write replace
file write fh "outcome,window,b,se,f,n,mean" _n
file close fh
cap program drop run_one
program define run_one
	args v window yvar levvar cond fe
	capture ivreghdfe `yvar' (d_inc = d_iv) [pw = projection_factor] if `cond', absorb(`fe') cluster(fips)
	if _rc == 0 {
		local b = _b[d_inc]
		local se = _se[d_inc]
		local f = e(widstat)
		local n = e(N)
		quietly sum `levvar' [aw = projection_factor] if e(sample)
		file open fh using "$idir/what_moves/iv_health.csv", write append
		file write fh "`v',`window'," (`b') "," (`se') "," (`f') "," (`n') "," (r(mean)) _n
		file close fh
		di "`v' `window': " %9.4f `b' " (" %8.4f `se' ") F " %6.1f `f' " N " `n' " mean " %9.3f r(mean)
	}
	else di as error "`v' `window' failed, rc " _rc
end

foreach v in obesity diabetes_type2 hypertension {
	run_one `v' main m_`v' h_`v' "`base'" "`FE'"
	run_one `v' before b_`v' h_`v' "`base'" "`FE'"
}
di "build_iv_health.do complete"
