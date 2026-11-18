// ============================================================================
// build_iv_time.do -- the income IV on the time intensity of the groceries (Anya 2026-11-15: a table after the diet-parts table)
//
// Main specification of the paper (tab:hi_clean): households employed at t-1 that stay in their county, heads' average age under 60
// at t+1 (the default sample since 2026-11-18);
// change in the outcome from t to t+1 on the change in income from t-1 to t ($10k), instrumented with the change in
// leave-one-county-out group income; household, state-by-year, children, head age and household composition fixed effects;
// projection weights; clustered on county. Placebo: the same outcome from t-3 to t-2.
// Outcomes from build/prep_time/build_prep_time_panel.py (interim/panel_dataset/panel_prep_time.dta), over the spending on modules
// with a preparation score (condiments, sauces, toppings and sweeteners have none): prep_time_raw = spending-weighted preparation
// score (1-9); sh_open, sh_little, sh_cook = % of that spending on open-and-eat (score 1), little preparation (2-3) and cooking (4-9)
// modules; sh_rte = ready-to-eat meals, sh_open_rte = open and eat or ready to eat, sh_scratch = scratch ingredients (module lists in
// the builder); prep_min_1000kcal = minutes of cooking per 1,000 kcal bought, from minutes per barcode (build/prep_time/
// prep_minutes_rules.py: rules on Nielsen's product attributes; four label servings per cooking session, one session for packages
// cooked whole), a ratio, so on all households (since 2026-11-18; it replaces prep_min_ae_day, the module-score minutes per adult
// equivalent per day); prep_min_1000kcal_serv4 and prep_min_1000kcal_once = the same with four servings per session for every
// barcode and with one session per package for every barcode (checks, in the csv only); prep_min_1000kcal_same = the main measure
// on households of the same size at t-1, t and t+1 (Anya 2026-11-18); ing_per_trip = distinct ingredient barcodes
// (NOVA group 1 or 2) per food shopping trip, on households of the same size at t-1, t and t+1 (a larger household buys more items
// on a trip).
// Output: interim/what_moves/iv_time.csv (read by build_iv_time_table.py), printed as well.
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use "$gdir/labor_supply_reg_data.dta", clear
ren hhid household_code
ren year panel_year
merge 1:1 household_code panel_year using "$idir/panel_dataset/panel_prep_time.dta", keepusing(prep_time_raw sh_open sh_little sh_cook sh_rte sh_open_rte sh_scratch prep_min_1000kcal prep_min_1000kcal_serv4 prep_min_1000kcal_once ing_per_trip) keep(master match) nogen
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
local base "movers_f == 0 & clean == 1 & avg_age_hh_head + 1 < 60"          // default sample since 2026-11-18 (Anya): heads' average age under 60 at t+1
foreach v in prep_time_raw sh_open sh_little sh_cook sh_rte sh_open_rte sh_scratch prep_min_1000kcal prep_min_1000kcal_serv4 prep_min_1000kcal_once ing_per_trip {
	gen double m_`v' = F.`v' - `v'
	gen double b_`v' = L2.`v' - L3.`v'
}

file open fh using "$idir/what_moves/iv_time.csv", write replace
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
		file open fh using "$idir/what_moves/iv_time.csv", write append
		file write fh "`v',`window'," (`b') "," (`se') "," (`f') "," (`n') "," (r(mean)) _n
		file close fh
		di "`v' `window': " %9.4f `b' " (" %8.4f `se' ") F " %6.1f `f' " N " `n' " mean " %9.3f r(mean)
	}
	else di as error "`v' `window' failed, rc " _rc
end

foreach v in prep_time_raw sh_open sh_little sh_cook sh_rte sh_open_rte sh_scratch prep_min_1000kcal prep_min_1000kcal_serv4 prep_min_1000kcal_once {
	run_one `v' main m_`v' `v' "`base'" "`FE'"
	run_one `v' before b_`v' `v' "`base'" "`FE'"
}
// minutes of cooking per 1,000 kcal on households of the same size at t-1, t and t+1 as well (Anya 2026-11-18)
run_one prep_min_1000kcal_same main m_prep_min_1000kcal prep_min_1000kcal "`base' & same_size == 1" "`FE'"
run_one prep_min_1000kcal_same before b_prep_min_1000kcal prep_min_1000kcal "`base' & same_size == 1" "`FE'"
run_one ing_per_trip main m_ing_per_trip ing_per_trip "`base' & same_size == 1" "`FE'"
run_one ing_per_trip before b_ing_per_trip ing_per_trip "`base' & same_size == 1" "`FE'"
di "build_iv_time.do complete"
