// ============================================================================
// 15_build_occ_transition_grid.do -- does the shock move households ACROSS
// occupation types, or make them richer WITHIN their occupation?
//
// Grid regressions on the no-retirees sample: the outcome is the dummy
// 1{occupation type = t & income tercile = b} at time t (the post period);
// the treatment is the shock from t-1 to t, in $10k (reduced form). The
// sample conditions on the household's occupation type at t-1 (the pre
// period) = blue collar (and separately white collar / service). Because the
// 12 cells partition the sample, the coefficients sum to zero within a panel:
// positive = the shock moves households INTO that cell, negative = out.
//
//   occupation type (male-preferred head occ, as in the IV cells):
//     1 white collar (codes 1-4)   2 blue collar (5-7)
//     3 service (8, 9, 11)         4 not employed (10, 12)
//   income tercile: real_income within year, weighted, on the base sample
//   shocks: (O) original male-preferred cell shock (d_iv);
//           (W) t-1-hours-weighted working-head shock (as in 12_)
//
// Spec: reduced form, year/kids/age/composition FE (NO household FE -- the
// outcome is a level and the sample conditions on the lagged state),
// non-movers, county-clustered SE, projection-weighted.
//
// Output: interim/labor_supply/occ_transition_coefs.csv
//         (heatmap figure: figure_occ_transition_heatmap.py)
// ============================================================================

clear all

global data "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// ---------------------------------------------------------------- retiree flag
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | occ_src == 0
gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & occ_src != 0
gen byte retiree = not_emp_cell == 1 & avg_age_hh_head >= 62 ///
	if !missing(not_emp_cell) & !missing(avg_age_hh_head)
gen byte no_retiree = (retiree == 0 & L.retiree == 0) ///
	if !missing(retiree) & !missing(L.retiree)

// ------------------------------------------- hours-weighted working-head shock
foreach h in male female {
	gen `h'_hours_bin = cond(`h'_head_employment == 1, 20, ///
	                   cond(`h'_head_employment == 2, 32, ///
	                   cond(`h'_head_employment == 3, 40, ///
	                   cond(`h'_head_employment == 9, 0, .))))
}
gen w_m = L.male_hours_bin / (L.male_hours_bin + L.female_hours_bin) ///
	if male_works == 1 & female_works == 1 ///
	 & !missing(L.male_hours_bin) & !missing(L.female_hours_bin) ///
	 & (L.male_hours_bin + L.female_hours_bin) > 0
replace w_m = 0.5 if male_works == 1 & female_works == 1 & missing(w_m)

gen iv_income_whrs_fips = .
replace iv_income_whrs_fips = iv_income_male_fips   if male_works == 1 & female_works != 1
replace iv_income_whrs_fips = iv_income_female_fips if female_works == 1 & male_works != 1
replace iv_income_whrs_fips = w_m * iv_income_male_fips + (1 - w_m) * iv_income_female_fips ///
	if male_works == 1 & female_works == 1

gen d_iv_whrs = D.iv_income_whrs_fips / 10   // $10k units, matching d_iv

// ------------------------------------------------------ occupation type at t
gen byte occ_type = .
replace occ_type = 1 if inlist(occ_src, 1, 2, 3, 4)
replace occ_type = 2 if inlist(occ_src, 5, 6, 7)
replace occ_type = 3 if inlist(occ_src, 8, 9, 11)
replace occ_type = 4 if inlist(occ_src, 10, 12)
label define occ_type_lbl 1 "White collar" 2 "Blue collar" 3 "Service" 4 "Not employed"
label values occ_type occ_type_lbl
gen byte Locc_type = L.occ_type

// --------------------- income terciles within year, weighted, on base sample
gen byte smp_base = no_retiree == 1 & !missing(occ_type) & !missing(real_income)
gen byte inc_bin = .
levelsof year, local(yrs)
foreach y of local yrs {
	// the first panel year has no lag, so no_retiree (hence smp_base) is
	// never 1 there -- skip years with too few eligible observations
	quietly count if smp_base & year == `y'
	if r(N) < 30 continue
	cap drop _tmpb
	quietly xtile _tmpb = real_income [aw = projection_factor] ///
		if smp_base & year == `y', nquantiles(3)
	quietly replace inc_bin = _tmpb if smp_base & year == `y'
}
cap drop _tmpb

// ------------------------------------------------------ 12 exhaustive dummies
forvalues t = 1/4 {
	forvalues b = 1/3 {
		gen byte cell_`t'_`b' = (occ_type == `t' & inc_bin == `b') if smp_base & !missing(inc_bin)
	}
}

// ============================================================================
// Grid regressions: 2 shocks x 3 baseline types x 12 cells
// ============================================================================

cap mkdir "$data/interim/labor_supply"

tempfile results
postfile grid str8 shock byte base_type byte occ_type byte inc_bin ///
	double(b se p ymean) long N using `results', replace

foreach s in orig whrs {
	local shockvar = cond("`s'" == "orig", "d_iv", "d_iv_whrs")
	foreach base in 2 1 3 {    // blue, white, service at t-1
		forvalues t = 1/4 {
			forvalues b = 1/3 {
				quietly reghdfe cell_`t'_`b' `shockvar' [pw = projection_factor] ///
					if smp_base & !missing(inc_bin) & Locc_type == `base', ///
					absorb(year kids avg_age_hh_head hh_comp) cluster(fips)
				local beta = _b[`shockvar']
				local se   = _se[`shockvar']
				local p    = 2 * ttail(e(df_r), abs(`beta' / `se'))
				local n    = e(N)
				quietly summarize cell_`t'_`b' [aw = projection_factor] if e(sample)
				post grid ("`s'") (`base') (`t') (`b') ///
					(`beta') (`se') (`p') (r(mean)) (`n')
			}
		}
		di "done: shock `s', baseline `base'"
	}
}
postclose grid

use `results', clear
export delimited using "$data/interim/labor_supply/occ_transition_coefs.csv", replace

di "15_build_occ_transition_grid.do complete"
