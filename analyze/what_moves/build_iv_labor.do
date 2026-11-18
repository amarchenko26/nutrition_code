// ============================================================================
// build_iv_labor.do -- the income IV on the heads' labor supply (Anya 2026-11-18: a table below the health tables of 0_main.tex,
// for the mechanisms question: when income changes, do the heads work more or fewer hours?)
//
// Main specification of analyze/labor_supply/12_ (the 0.031 SD HEI estimate): households employed at t-1 that stay in their county;
// the change in the outcome on the change in income from t-1 to t ($10k), instrumented with the change in leave-one-county-out
// group income; household, state-by-year, children, head age and household composition fixed effects; projection weights;
// clustered on county. Three windows for the outcome: t-1 to t (the same file years as the income change; income and employment
// are reported in the same survey), t to t+1 (the window of the nutrition outcomes) and t-3 to t-2 (placebo).
// Outcomes (clean/nielsen/clean_panelist.py): hh_avg_workhours = weekly hours averaged over the heads present, 0 for a head who
// does not work (hours bins: under 30 = 20, 30-34 = 32, 35 or more = 40); emp100 = 100 x the share of the heads present who work;
// hh_avg_workhours_if_employed = weekly hours averaged over the heads who work (missing when none works).
// The labor-supply note's table (12_, tabs/labor_supply/labor_supply_clean.tex) is the t to t+1 window with year fixed effects.
// Output: interim/what_moves/iv_labor.csv (read by build_iv_labor_table.py), printed as well.
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
local base "movers_f == 0 & clean == 1 & avg_age_hh_head + 1 < 60"          // default sample since 2026-11-18 (Anya): heads' average age under 60 at t+1
gen double emp100 = 100 * hh_employed
foreach v in hh_avg_workhours emp100 hh_avg_workhours_if_employed {
	gen double c_`v' = `v' - L.`v'
	gen double m_`v' = F.`v' - `v'
	gen double b_`v' = L2.`v' - L3.`v'
}

file open fh using "$idir/what_moves/iv_labor.csv", write replace
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
		file open fh using "$idir/what_moves/iv_labor.csv", write append
		file write fh "`v',`window'," (`b') "," (`se') "," (`f') "," (`n') "," (r(mean)) _n
		file close fh
		di "`v' `window': " %9.4f `b' " (" %8.4f `se' ") F " %6.1f `f' " N " `n' " mean " %9.3f r(mean)
	}
	else di as error "`v' `window' failed, rc " _rc
end

foreach v in hh_avg_workhours emp100 hh_avg_workhours_if_employed {
	run_one `v' same c_`v' L.`v' "`base'" "`FE'"
	run_one `v' main m_`v' `v' "`base'" "`FE'"
	run_one `v' before b_`v' `v' "`base'" "`FE'"
}
di "build_iv_labor.do complete"
