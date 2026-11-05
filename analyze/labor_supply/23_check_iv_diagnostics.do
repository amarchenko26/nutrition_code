clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
use "$gdir/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)
quietly summarize hei_usda [aw = projection_factor]
local sd = r(sd)
gen z = (hei_usda - r(mean)) / r(sd)
gen fz = F.z - z
egen year_state = group(year fips_state_code)
cap drop d_iv
gen d_iv = D.iv_income_fips / 10
cap drop d_inc
gen d_inc = D.real_income
// cell components for the switch type
gen byte occ_bin = cond(inrange(occ_src, 1, 4), 1, cond(inlist(occ_src, 5, 6, 11), 2, cond(inlist(occ_src, 8, 9), 3, cond(inlist(occ_src, 10, 12), 4, .))))
gen byte educ_bin = cond(hh_avg_yrsofschool < 12, 1, cond(hh_avg_yrsofschool < 13, 2, cond(hh_avg_yrsofschool < 16, 3, 4))) if !missing(hh_avg_yrsofschool)
gen byte size_bin = cond(hh_size <= 1, 1, cond(hh_size <= 2, 2, cond(hh_size <= 4, 3, 4))) if !missing(hh_size)
gen byte sw_occ = occ_bin != L.occ_bin if !missing(occ_bin, L.occ_bin)
gen byte sw_edu = educ_bin != L.educ_bin if !missing(educ_bin, L.educ_bin)
gen byte sw_size = size_bin != L.size_bin if !missing(size_bin, L.size_bin)
gen byte stype = .
replace stype = 1 if sw_occ == 0 & sw_edu == 0 & sw_size == 0
replace stype = 2 if sw_occ == 1 & sw_edu == 0 & sw_size == 0
replace stype = 3 if sw_occ == 0 & sw_edu == 0 & sw_size == 1
replace stype = 4 if sw_occ == 0 & sw_edu == 1 & sw_size == 0
replace stype = 5 if sw_occ + sw_edu + sw_size >= 2
label define stype 1 "no switch" 2 "occupation only" 3 "size only" 4 "education only" 5 "two or more"
label values stype stype

di _n "HEI SD = `sd' (0.035 SD = " 0.035 * `sd' " points)"
di _n "==== A. baseline (12_): forward change t->t+1, hhid + state-year FE"
ivreghdfe fz (d_inc = d_iv) [pw = projection_factor] if clean == 1, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
di "A  beta = " _b[d_inc] " (" _se[d_inc] ")  F = " e(widstat)

di _n "==== B1. same without the household FE"
ivreghdfe fz (d_inc = d_iv) [pw = projection_factor] if clean == 1, absorb(year_state kids avg_age_hh_head hh_comp) cluster(fips)
di "B1 beta = " _b[d_inc] " (" _se[d_inc] ")  F = " e(widstat)

di _n "==== B2. same with year FE only (no household, no state-year)"
ivreghdfe fz (d_inc = d_iv) [pw = projection_factor] if clean == 1, absorb(year kids avg_age_hh_head hh_comp) cluster(fips)
di "B2 beta = " _b[d_inc] " (" _se[d_inc] ")  F = " e(widstat)

di _n "==== B3. LEVELS: z(t+1) on income(t), instrument iv(t), household + state-year FE (the movers-style specification)"
ivreghdfe F.z (real_income = iv_income_fips) [pw = projection_factor] if clean == 1, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
di "B3 beta = " _b[real_income] " (" _se[real_income] ")  F = " e(widstat)

di _n "==== C. IV IN EVENT TIME: level of z at t+k minus level at t-1, same regressor (income change t-1 -> t), same FE"
gen c_m3 = L3.z - L.z
gen c_m2 = L2.z - L.z
gen c_0  = z - L.z
gen c_p1 = F.z - L.z
gen c_p2 = F2.z - L.z
gen c_p3 = F3.z - L.z
foreach k in m3 m2 0 p1 p2 p3 {
	quietly ivreghdfe c_`k' (d_inc = d_iv) [pw = projection_factor] if clean == 1, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	di "C  level at k=`k' minus t-1: beta = " %7.4f _b[d_inc] " (" %6.4f _se[d_inc] ")  N = " e(N) "  F = " %5.0f e(widstat)
}

di _n "==== D. reduced form and first stage by switch type (instrument x type), same FE"
reghdfe fz c.d_iv#i.stype i.stype [pw = projection_factor] if clean == 1, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
matrix RF = e(b)
forvalues t = 1/5 {
	local rf_`t' = _b[`t'.stype#c.d_iv]
	local rfse_`t' = _se[`t'.stype#c.d_iv]
}
reghdfe d_inc c.d_iv#i.stype i.stype [pw = projection_factor] if clean == 1, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
forvalues t = 1/5 {
	local fs_`t' = _b[`t'.stype#c.d_iv]
	local fsse_`t' = _se[`t'.stype#c.d_iv]
	local lab : label stype `t'
	quietly count if clean == 1 & stype == `t' & !missing(fz, d_inc, d_iv)
	di "D  `lab': N = " r(N) "  reduced form (SD per $10k of cell shock) = " %7.4f `rf_`t'' " (" %6.4f `rfse_`t'' ")  first stage = " %6.3f `fs_`t'' " (" %5.3f `fsse_`t'' ")  implied IV = " %7.3f `rf_`t'' / `fs_`t'' " SD per $10k"
}

di _n "==== E. 2SLS by switch type: income change x type instrumented by shock x type (one regression)"
ivreghdfe fz (c.d_inc#i.stype = c.d_iv#i.stype) i.stype [pw = projection_factor] if clean == 1, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
forvalues t = 1/5 {
	local lab : label stype `t'
	di "E  `lab': beta = " %7.4f _b[`t'.stype#c.d_inc] " (" %6.4f _se[`t'.stype#c.d_inc] ")"
}
di "iv_diagnostics complete"
