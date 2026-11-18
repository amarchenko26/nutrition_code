// ============================================================================
// build_iv_shopping.do -- the income IV on shopping (Anya 2026-11-15, note under the health table of 0_main.tex)
//
// Main specification of analyze/labor_supply/12_: households employed at t-1 that stay in their county; change in the outcome from
// t to t+1 on the change in income from t-1 to t ($10k), instrumented with the change in leave-one-county-out group income;
// household, state-by-year, children, head age and household composition fixed effects; projection weights; clustered on county.
// Outcomes from build/expenditure/build_shopping_panel.py. Placebo: the same outcome from t-3 to t-2.
// Spending and trips are household totals and the instrument moves with household size, so those two use households of the same
// size at t-1, t and t+1, as calories do in the robustness table. Main-store switches are already changes: switched = 100 if the
// main store (RMS store with the most food spending) at t+1 differs from the one at t; farther = 100 if it switched to a store
// farther from the household's ZIP; the placebo uses t-3 and t-2. The store measures (distance, home ZIP, shoppers' ZIP income,
// Health Index of what the store sells) are fixed per store, so their change from t to t+1 is nonzero only for households that
// switch (or change ZIP). Distance capped at 100 km. switched_retailer (main retailer, any store) is written but not tabulated.
// Output: interim/what_moves/iv_shopping.csv (read by build_iv_shopping_table.py), printed as well.
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use "$gdir/labor_supply_reg_data.dta", clear
ren hhid household_code
ren year panel_year
merge 1:1 household_code panel_year using "$idir/panel_dataset/shopping_hh_year.dta", keep(master match) nogen
ren household_code hhid
ren panel_year year
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
gen byte same_size = hh_size == L.hh_size & hh_size == F.hh_size if !missing(hh_size, L.hh_size, F.hh_size)
local FE "year_state hhid kids avg_age_hh_head hh_comp"
local base "movers_f == 0 & clean == 1"

replace dist_km = min(dist_km, 100) if !missing(dist_km)
gen double samezip = 100 * same_zip
gen double sw_main = 100 * (F.main_store != main_store) if !missing(F.main_store, main_store)
gen double sw_before = 100 * (L2.main_store != L3.main_store) if !missing(L2.main_store, L3.main_store)
gen double far_main = 100 * (F.dist_km > dist_km & F.main_store != main_store) if !missing(F.dist_km, dist_km, F.main_store, main_store)
gen double far_before = 100 * (L2.dist_km > L3.dist_km & L2.main_store != L3.main_store) if !missing(L2.dist_km, L3.dist_km, L2.main_store, L3.main_store)
gen double swr_main = 100 * (F.main_retailer != main_retailer) if !missing(F.main_retailer, main_retailer)
gen double swr_before = 100 * (L2.main_retailer != L3.main_retailer) if !missing(L2.main_retailer, L3.main_retailer)
foreach v in spend_month trips_week sh_grocery sh_discount sh_club sh_drug sh_conv samezip dist_km store_zipinc store_hi {
	gen double m_`v' = F.`v' - `v'
	gen double b_`v' = L2.`v' - L3.`v'
}

file open fh using "$idir/what_moves/iv_shopping.csv", write replace
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
		file open fh using "$idir/what_moves/iv_shopping.csv", write append
		file write fh "`v',`window'," (`b') "," (`se') "," (`f') "," (`n') "," (r(mean)) _n
		file close fh
		di "`v' `window': " %9.4f `b' " (" %8.4f `se' ") F " %6.1f `f' " N " `n' " mean " %9.3f r(mean)
	}
	else di as error "`v' `window' failed, rc " _rc
end

foreach v in spend_month trips_week {
	run_one `v' main m_`v' `v' "`base' & same_size == 1" "`FE'"
	run_one `v' before b_`v' `v' "`base' & same_size == 1" "`FE'"
	// all households of the main specification, whatever their size (Anya's note in 0_main.tex, 2026-11-18)
	run_one `v'_all main m_`v' `v' "`base'" "`FE'"
	run_one `v'_all before b_`v' `v' "`base'" "`FE'"
}
foreach v in sh_grocery sh_discount sh_club sh_drug sh_conv samezip dist_km store_zipinc store_hi {
	run_one `v' main m_`v' `v' "`base'" "`FE'"
	run_one `v' before b_`v' `v' "`base'" "`FE'"
}
run_one switched main sw_main sw_main "`base'" "`FE'"
run_one switched before sw_before sw_before "`base'" "`FE'"
run_one farther main far_main far_main "`base'" "`FE'"
run_one farther before far_before far_before "`base'" "`FE'"
run_one switched_retailer main swr_main swr_main "`base'" "`FE'"
run_one switched_retailer before swr_before swr_before "`base'" "`FE'"
di "build_iv_shopping.do complete"
