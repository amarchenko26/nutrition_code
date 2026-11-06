// ============================================================================
// build_iv_suboutcomes.do -- the income IV on the parts of the diet, in natural units
// (what-moves review, 2026-11-05).
//
// Specification of analyze/labor_supply/12_ (tabs/labor_supply/hi_clean.tex): clean sample, non-movers, change in
// income from t-1 to t ($10k) instrumented with the change in the leave-one-county-out cell income; household,
// state-by-year, kids, age and composition fixed effects; projection weights; clustered on county. Two windows for
// each outcome: the change from t to t+1 (the main regression) and the level at t+1 minus the level at t-1.
//
// Outcomes come from interim/movers/occupation_movers_panel.dta (analyze/movers/1_).
// Output: interim/what_moves/iv_suboutcomes.csv
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use "$idir/movers/occupation_movers_panel.dta", clear
keep hhid year cals_ae_day upf_share produce_share produce_spend_share sugar_1000 sodium_1000 whole_share soda_spend_share
tempfile subs
save `subs'

use "$gdir/labor_supply_reg_data.dta", clear
merge 1:1 hhid year using `subs', keep(master match) nogen
xtset hhid year
keep if movers_f == 0

gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)
egen year_state = group(year fips_state_code)

file open fh using "$idir/what_moves/iv_suboutcomes.csv", write replace
file write fh "outcome,window,b,se,f,n,mean,sd" _n
foreach v in hei_usda sugar_1000 sodium_1000 upf_share produce_share produce_spend_share soda_spend_share whole_share cals_ae_day {
	quietly summarize `v' [aw = projection_factor] if clean == 1
	local m = r(mean)
	local s = r(sd)
	gen double y_main  = F.`v' - `v'
	gen double y_level = F.`v' - L.`v'
	foreach w in main level {
		capture ivreghdfe y_`w' (D.real_income = D.iv_income_fips) [pw = projection_factor] if clean == 1, ///
			absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
		if _rc == 0 {
			file write fh "`v',`w'," (_b[D.real_income]) "," (_se[D.real_income]) "," (e(widstat)) "," (e(N)) ",`m',`s'" _n
			di "`v' `w': " %7.3f _b[D.real_income] " (" %6.3f _se[D.real_income] ") F " %5.0f e(widstat) " N " e(N)
		}
		else di as error "`v' `w' failed, rc " _rc
	}
	drop y_main y_level
}
file close fh
