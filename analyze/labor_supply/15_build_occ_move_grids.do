// ============================================================================
// 15_build_occ_move_grids.do -- does the shock move households ACROSS
// occupations? Two grids on the CLEAN SAMPLE (12_: employed shock cell at
// both t-1 and t), ORIGINAL shock (d_iv):
//
// (A) 3x3 OCCUPATION TRANSITION GRID: condition on the occupation type at
//     t-1 (white/blue/service); the outcome is the dummy for each occupation
//     type at t. With the clean sample the three destinations are exhaustive,
//     so within each origin the coefficients sum to zero. Income upgrading
//     is invisible here by design -- this isolates occupation transitions.
//
// (B) RANK-CHANGE GRID (everyone pooled): dummy for (change in occupation
//     rank) x (change in income tercile), t-1 -> t. Ranks by mean household
//     income: service (1) < blue (2) < white (3); changes are in [-2, +2].
//     Income terciles within year, weighted. (0,0) = stayers.
//
// Reduced form: each cell dummy on the t-1 -> t shock in $10k. No household
// FE (level outcome conditional on the lagged state); year/kids/age/
// composition FE, non-movers, county-clustered SE, projection-weighted.
//
// Output: interim/labor_supply/occ_move_coefs.csv
//         (figures: figure_occ_move_heatmaps.py)
// ============================================================================

clear all

global data "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// ------------------------------------------------------------ clean-sample flag
// employed shock cell at BOTH t-1 and t (see 12_)
// Assigned occupation and SAMPLE exactly as in 12_ (labor_supply.tex
// "Measurement of key variables" / "Sample", 2026-09-16): male head unless
// absent or armed forces (7), then female; employed cell = 1-6, 8, 9, 11;
// clean = employed cell at t-1 (t-1 only). A retiree is not employed and 67+, so the
// employment rule already excludes retirees and a working 67-year-old stays in.
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell     = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte not_emp_cell = inlist(occ_src, 10, 12)                    if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)

// ------------------------------------------------------ occupation type & rank
// occupation types = the IV cell bins (build_iv.OCC_MAP): armed forces (7) has no type
gen byte occ_type = .
replace occ_type = 1 if inlist(occ_src, 1, 2, 3, 4)
replace occ_type = 2 if inlist(occ_src, 5, 6, 11)
replace occ_type = 3 if inlist(occ_src, 8, 9)
replace occ_type = 4 if inlist(occ_src, 10, 12)
label define occ_type_lbl 1 "White collar" 2 "Blue collar" 3 "Service/other" 4 "Not employed"
label values occ_type occ_type_lbl
gen byte Locc_type = L.occ_type

// rank by mean household income: not employed 0 < service 1 < blue 2 < white 3
gen byte occ_rank = cond(occ_type == 4, 0, ///
                    cond(occ_type == 3, 1, ///
                    cond(occ_type == 2, 2, ///
                    cond(occ_type == 1, 3, .))))
gen byte Locc_rank = L.occ_rank

// --------------------------- income terciles within year (weighted; grid B)
gen byte smp_bin = !missing(occ_type) & !missing(real_income)
gen byte inc3 = .
levelsof year, local(yrs)
foreach y of local yrs {
	quietly count if smp_bin & year == `y'
	if r(N) < 30 continue
	cap drop _t3
	quietly xtile _t3 = real_income [aw = projection_factor] ///
		if smp_bin & year == `y', nquantiles(3)
	quietly replace inc3 = _t3 if smp_bin & year == `y'
}
cap drop _t3
gen byte Linc3 = L.inc3

cap mkdir "$data/interim/labor_supply"
tempfile results
postfile grid str8 gridtype byte origin_occ int row int col ///
	double(b se p ymean) long N using `results', replace

// ============================================================================
// (A) 3x3 occupation transition grid: origin at t-1 -> destination at t
// ============================================================================

forvalues d = 1/4 {
	gen byte dest_`d' = (occ_type == `d') if !missing(occ_type)
}

foreach o in 3 2 1 {          // service, blue, white at t-1
	forvalues d = 1/4 {
		quietly reghdfe dest_`d' d_iv [pw = projection_factor] ///
			if clean == 1 & Locc_type == `o' & !missing(occ_type), ///
			absorb(year kids avg_age_hh_head hh_comp) cluster(fips)
		local beta = _b[d_iv]
		local se   = _se[d_iv]
		local pv   = 2 * ttail(e(df_r), abs(`beta' / `se'))
		local n    = e(N)
		quietly summarize dest_`d' [aw = projection_factor] if e(sample)
		post grid ("occ3x3") (`o') (`d') (`o') ///
			(`beta') (`se') (`pv') (r(mean)) (`n')
	}
	di "done: origin occupation `o'"
}

// ============================================================================
// (B) rank-change grid, everyone pooled
// ============================================================================

gen d_occ = occ_rank - Locc_rank
replace d_occ = -2 if d_occ < -2 & !missing(d_occ)
replace d_occ =  2 if d_occ >  2 & !missing(d_occ)
gen d_inc = inc3 - Linc3

forvalues r = 1/5 {                // r-3 = d_occ of -2..+2
	forvalues c = 1/5 {            // c-3 = d_inc of -2..+2
		gen byte rk_`r'_`c' = (d_occ == `r' - 3 & d_inc == `c' - 3) ///
			if !missing(d_occ) & !missing(d_inc)
	}
}

forvalues r = 1/5 {
	forvalues c = 1/5 {
		quietly reghdfe rk_`r'_`c' d_iv [pw = projection_factor] ///
			if clean == 1 & !missing(d_occ) & !missing(d_inc), ///
			absorb(year kids avg_age_hh_head hh_comp) cluster(fips)
		local beta = _b[d_iv]
		local se   = _se[d_iv]
		local pv   = 2 * ttail(e(df_r), abs(`beta' / `se'))
		local n    = e(N)
		quietly summarize rk_`r'_`c' [aw = projection_factor] if e(sample)
		post grid ("ranks") (0) (`=`r'-3') (`=`c'-3') ///
			(`beta') (`se') (`pv') (r(mean)) (`n')
	}
	di "done: rank row `=`r'-3'"
}

postclose grid

use `results', clear
export delimited using "$data/interim/labor_supply/occ_move_coefs.csv", replace

di "15_build_occ_move_grids.do complete"
