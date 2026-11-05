clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
use "$gdir/labor_supply_reg_data.dta", clear
xtset hhid year
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)
quietly summarize hei_usda [aw = projection_factor]
gen z = (hei_usda - r(mean)) / r(sd)
gen fz = F.z - z
egen year_state = group(year fips_state_code)
cap drop d_iv
gen d_iv = D.iv_income_fips / 10
cap drop d_inc
gen d_inc = D.real_income
bys hhid: gen byte _first = _n == 1
quietly count if _first
local nhh = r(N)
quietly count if _first & movers_f == 1
di "GEO households that ever change county: " r(N) " of `nhh' (" %4.1f 100 * r(N) / `nhh' "%)"
quietly count if clean == 1 & !missing(fz, d_inc, d_iv)
local n_all = r(N)
quietly count if clean == 1 & !missing(fz, d_inc, d_iv) & movers_f == 0
di "GEO clean-sample observations with the main regression's variables: non-movers " r(N) ", all households " `n_all' " (+" %4.1f 100 * (`n_all' / r(N) - 1) "%)"
ivreghdfe fz (d_inc = d_iv) [pw = projection_factor] if clean == 1 & movers_f == 0, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
di "GEO non-movers only: beta = " %7.4f _b[d_inc] " (" %6.4f _se[d_inc] ")  F = " %5.0f e(widstat) "  N = " e(N)
ivreghdfe fz (d_inc = d_iv) [pw = projection_factor] if clean == 1, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
di "GEO all households:   beta = " %7.4f _b[d_inc] " (" %6.4f _se[d_inc] ")  F = " %5.0f e(widstat) "  N = " e(N)
di "iv_geo_movers complete"
