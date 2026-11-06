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

di _n "HEI SD = `sd' points"
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
di _n "==== F. the same households in every row: observed t-1, t and t+1"
gen byte samp3 = clean == 1 & !missing(L.z, z, F.z, d_inc, d_iv)
foreach k in fz c_0 c_p1 {
	quietly ivreghdfe `k' (d_inc = d_iv) [pw = projection_factor] if samp3 == 1, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	di "F  `k': beta = " %7.4f _b[d_inc] " (" %6.4f _se[d_inc] ")  N = " e(N) "  F = " %5.0f e(widstat)
}
gen byte samp5 = clean == 1 & !missing(L.z, z, F.z, F2.z, F3.z, d_inc, d_iv)
foreach k in c_0 c_p1 c_p2 c_p3 {
	quietly ivreghdfe `k' (d_inc = d_iv) [pw = projection_factor] if samp5 == 1, absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	di "F5 `k': beta = " %7.4f _b[d_inc] " (" %6.4f _se[d_inc] ")  N = " e(N) "  F = " %5.0f e(widstat)
}

di _n "==== G. long differences: households observed 8+ years apart, first two years against last two"
preserve
bys hhid (year): gen int _n1 = _n
bys hhid (year): gen int _nN = _N - _n
gen byte _early = _n1 <= 2
gen byte _late = _nN <= 1
keep if _early | _late
collapse (mean) z real_income iv_income_fips hh_size avg_age_hh_head (min) y0 = year (max) y1 = year (firstnm) fips projection_factor year_state [aw = projection_factor], by(hhid _late)
reshape wide z real_income iv_income_fips hh_size avg_age_hh_head y0 y1 fips projection_factor year_state, i(hhid) j(_late)
keep if y01 - y00 >= 8
gen dz = (z1 - z0)
gen dinc = real_income1 - real_income0
gen div = (iv_income_fips1 - iv_income_fips0) / 10
gen dsize = hh_size1 - hh_size0
gen byte age0 = floor(avg_age_hh_head0 / 5)
quietly reghdfe dz dinc [pw = projection_factor0], absorb(y00 y01 age0) cluster(hhid)
di "G  OLS: long change in z-HEI on long change in income ($10k): " %7.4f _b[dinc] " (" %6.4f _se[dinc] ")  N = " e(N)
quietly reghdfe dz dinc dsize [pw = projection_factor0], absorb(y00 y01 age0) cluster(hhid)
di "G  OLS + change in household size: " %7.4f _b[dinc] " (" %6.4f _se[dinc] ");  per person " %7.4f _b[dsize] " (" %6.4f _se[dsize] ")"
quietly ivreghdfe dz (dinc = div) [pw = projection_factor0], absorb(y00 y01 age0) cluster(fips0)
di "G  IV (long change in cell income): " %7.4f _b[dinc] " (" %6.4f _se[dinc] ")  F = " %6.1f e(widstat) "  N = " e(N)
restore
di _n "==== H. by quartile of the household's income in its first panel year (the 17_ grouping): change after, levels, before"
cap drop inc_q baseline_income
bysort hhid (year): gen baseline_income = real_income[1]
xtile inc_q = baseline_income [aw = projection_factor], nquantiles(4)
cap drop c_b
gen c_b = L2.z - L3.z
forvalues q = 1/4 {
	foreach k in fz c_0 c_p1 c_b {
		quietly ivreghdfe `k' (d_inc = d_iv) [pw = projection_factor] if clean == 1 & inc_q == `q', absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
		di "H  Q`q' `k': beta = " %7.4f _b[d_inc] " (" %6.4f _se[d_inc] ")  N = " e(N) "  F = " %5.0f e(widstat)
	}
}
di "iv_diagnostics complete"
