// ============================================================================
// build_policy_shocks.do -- two large outside shocks to what food costs and to
// time at home (what-moves-nutrition review, 2026-11-05).
//
// WIC FOOD PACKAGE, OCTOBER 2009. The revised WIC package added cash vouchers for
// fruit and vegetables ($6-10 a month), whole-grain bread and cereal and low-fat
// milk, and cut juice and cheese. Who is compared with whom: households on WIC in
// the 2007 or 2008 panel file against households with children under 6 and income
// per person under $13,000 that report no WIC in those files, before and after the
// change, with each household's own level removed. Status is fixed before the
// change, so households leaving WIC stay in the treated group. Panel years 2005-2013;
// 2008 omitted. Year, age-bin, kids and composition dummies; projection weights;
// SE clustered on household.
//
// COVID, 2020. Who is compared with whom: households whose heads held a job that can
// be done at home (white-collar occupation bin in 2018-2019) against households in
// blue-collar and service jobs in those years, and households with school-age children
// against households without them, in 2020 relative to 2019, with each household's own
// level removed. Sample: employed occupation cell in 2019.
// Outputs (interim/what_moves/): policy_shock_coefs.csv
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

pq use household_code panel_year wic_indicator_current household_income_midpoint household_size age_and_presence_of_children using "$idir/panelists/panelists_all_years.parquet", clear
ren household_code hhid
ren panel_year year
destring wic_indicator_current, replace force
gen byte wic = wic_indicator_current == 1
destring age_and_presence_of_children, replace force
gen byte young_p = inlist(age_and_presence_of_children, 1, 4, 5, 7)
gen double ipc = household_income_midpoint / household_size
bys hhid: egen byte wic_base = max(cond(inlist(year, 2007, 2008), wic, 0))
bys hhid: egen byte low_base = max(cond(inlist(year, 2007, 2008), young_p & ipc <= 13000, 0))
keep if year == 2008
keep hhid wic_base low_base
tempfile base
save `base'

use "$idir/movers/occupation_movers_panel.dta", clear
keep hhid year occ projection_factor age5 kids hh_comp hh_size real_income hei_usda hi hi_usda cals_ae_day upf_share produce_share sugar_1000 sodium_1000 soda_spend_share whole_share produce_spend_share
merge m:1 hhid using `base', keep(master match) nogen
tempfile panel
save `panel'

cap file close fh
file open fh using "$idir/what_moves/policy_shock_coefs.csv", write replace
file write fh "design,outcome,term,b,se,N,n_treated,n_control" _n
file close fh

// ---------------------------------------------------------------- WIC
keep if inrange(year, 2005, 2013) & (wic_base == 1 | low_base == 1)
gen byte treat = wic_base == 1
bys hhid: gen byte _f = _n == 1
quietly count if _f & treat == 1
local nt = r(N)
quietly count if _f & treat == 0
local nc = r(N)
di "WIC: treated households `nt', control households `nc'"
forvalues y = 2005/2013 {
	if `y' == 2008 continue
	gen byte t_`y' = treat * (year == `y')
}
gen byte post = year >= 2010
gen byte tpost = treat * post
foreach v in hei_usda produce_share produce_spend_share whole_share sugar_1000 upf_share cals_ae_day real_income {
	quietly reghdfe `v' t_* [pw = projection_factor], absorb(hhid year age5 kids hh_comp) cluster(hhid)
	local N = e(N)
	file open fh using "$idir/what_moves/policy_shock_coefs.csv", write append
	forvalues y = 2005/2013 {
		if `y' == 2008 {
			file write fh "wic,`v',2008,0,0,`N',`nt',`nc'" _n
			continue
		}
		file write fh "wic,`v',`y',`=_b[t_`y']',`=_se[t_`y']',`N',`nt',`nc'" _n
	}
	file close fh
	quietly reghdfe `v' tpost [pw = projection_factor] if year != 2009, absorb(hhid year age5 kids hh_comp) cluster(hhid)
	file open fh using "$idir/what_moves/policy_shock_coefs.csv", write append
	file write fh "wic,`v',post,`=_b[tpost]',`=_se[tpost]',`=e(N)',`nt',`nc'" _n
	file close fh
	di "WIC `v': treated x 2010-2013 = " %8.3f _b[tpost] " (" %6.3f _se[tpost] ")"
}

// ---------------------------------------------------------------- COVID
use `panel', clear
xtset hhid year
gen byte white = inrange(occ, 1, 4)
gen byte blueserv = inlist(occ, 5, 6, 8, 9, 11)
gen byte emp19 = (white | blueserv) if year == 2019
bys hhid: egen byte emp_2019 = max(emp19)
gen byte w19 = white if year == 2019
bys hhid: egen byte white_2019 = max(w19)
gen byte hk = (kids != 9) if year == 2019
bys hhid: egen byte kids_2019 = max(hk)
keep if inrange(year, 2016, 2020)
preserve
keep if emp_2019 == 1
bys hhid: gen byte _f = _n == 1
quietly count if _f & white_2019 == 1
local nt = r(N)
quietly count if _f & white_2019 == 0
local nc = r(N)
di "COVID telework: white-collar `nt', other `nc'"
forvalues y = 2016/2020 {
	if `y' == 2019 continue
	gen byte t_`y' = white_2019 * (year == `y')
}
foreach v in hei_usda upf_share cals_ae_day produce_share sugar_1000 sodium_1000 real_income {
	quietly reghdfe `v' t_* [pw = projection_factor], absorb(hhid year age5 kids hh_comp) cluster(hhid)
	file open fh using "$idir/what_moves/policy_shock_coefs.csv", write append
	forvalues y = 2016/2020 {
		if `y' == 2019 {
			file write fh "covid_white,`v',2019,0,0,`=e(N)',`nt',`nc'" _n
			continue
		}
		file write fh "covid_white,`v',`y',`=_b[t_`y']',`=_se[t_`y']',`=e(N)',`nt',`nc'" _n
	}
	file close fh
	di "COVID white-collar `v': 2020 vs 2019 = " %8.3f _b[t_2020] " (" %6.3f _se[t_2020] ")"
}
restore
// all households: the national 2019 -> 2020 change, with each household's own level removed
preserve
gen byte y2020 = year == 2020
keep if inlist(year, 2019, 2020)
foreach v in hei_usda upf_share cals_ae_day produce_share sugar_1000 sodium_1000 {
	quietly reghdfe `v' y2020 [pw = projection_factor], absorb(hhid age5 kids hh_comp) cluster(hhid)
	file open fh using "$idir/what_moves/policy_shock_coefs.csv", write append
	file write fh "covid_all,`v',2020,`=_b[y2020]',`=_se[y2020]',`=e(N)',.,." _n
	file close fh
	di "COVID all `v': 2020 vs 2019 = " %8.3f _b[y2020] " (" %6.3f _se[y2020] ")"
}
restore
di "build_policy_shocks.do complete"
