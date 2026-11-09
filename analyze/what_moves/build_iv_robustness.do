// ============================================================================
// build_iv_robustness.do -- the income IV under other fixed effects, timing, samples and instruments, and
// where it comes from (Anya's comments on the what-moves section of 0_main.tex, 2026-11-06).
//
// Base specification = analyze/labor_supply/12_: change in the outcome from t to t+1 on the change in income from
// t-1 to t ($10k), instrumented with the change in leave-one-county-out cell income; clean sample (employed cell at
// t-1), non-movers; household, state-by-year, kids, age and composition fixed effects; projection weights; clustered
// on county. Every row also has the test on purchases before the income change, t-3 to t-2 (window `before`).
// Outcomes in natural units: HEI points, ultra-processed and produce shares of calories (%), calories per adult
// equivalent per day. Calories always use households of the same size at t-1, t, t+1, because the instrument moves
// with size (build_iv_suboutcomes.do).
//
// Rows (spec column):
//   main; hh_year (household + year FE); no_hhfe (state-by-year FE only); year_only (year FE only);
//   levels (outcome at t+1 on income at t, instrument = cell income at t, household + state-by-year FE);
//   twoyear (t-1 to t+1 on the same income change); level_t (t-1 to t on the same income change);
//   same_size; geo_movers (households changing county kept); stayers (cell unchanged t-1 to t); switchers (cell changed);
//   switch_noNE (cell changed, and not into not employed); no_size_bin (occupation x education cells);
//   predicted (predicted-share instrument, labor_supply/26_); baseline_occ (occupation frozen at first panel year);
//   bls (national OEWS wage of the occupation); acs (ACS cell income);
//   k2, k3, k5, k8 (k-year change in the outcome ending at t+1 on the k-year change in income and instrument ending
//   at t, employed at t-k);
//   q1-q4 (quartile of income in the household's first panel year);
//   ql1-ql4 (quartile of income at t-1, cut within each year among non-movers, projection-weighted; mode qlag).
//
// `do build_iv_robustness.do <outcome>` writes interim/what_moves/iv_robustness_<outcome>.csv.
// `do build_iv_robustness.do <outcome> qlag` replaces only the ql1-ql4 rows of that file and prints how the t-1 quartiles
// compare with the first-year quartiles (same quartile, income change, group-income change).
// `do build_iv_robustness.do unpack` writes interim/what_moves/iv_unpack.csv: for the main regression, the exact
// shares of the numerator (outcome on the group-income change) and of the denominator (own income on the group-income
// change) by type of cell change, after the fixed effects; and the year-by-year path for stayers and switchers.
// `do build_iv_robustness.do zero` writes interim/what_moves/iv_zero_first_stage.csv.
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
local part "`1'"
if "`part'" == "" local part hei_usda

use "$idir/movers/occupation_movers_panel.dta", clear
keep hhid year cals_ae_day upf_share produce_share sugar_1000
tempfile subs
save `subs'
use "$gdir/labor_supply_reg_data.dta", clear
merge 1:1 hhid year using `subs', keep(master match) nogen
merge 1:1 hhid year using "$idir/what_moves/price_per_kcal.dta", keep(master match) nogen keepusing(price_1000)
ren hhid household_code
ren year panel_year
merge 1:1 household_code panel_year using "$idir/panel_dataset/iv_income_acs.dta", keepusing(iv_income_acs) keep(master match) nogen
merge 1:1 household_code panel_year using "$idir/panel_dataset/iv_predicted.dta", keepusing(d_iv_pred) keep(master match) nogen
merge 1:1 household_code panel_year using "$idir/panel_dataset/iv_income_baseline_occ.dta", keepusing(iv_income_baseline_fips) keep(master match) nogen
ren household_code hhid
ren panel_year year
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
merge m:1 occ_src year using "$idir/panel_dataset/bls_occ_wage.dta", keep(master match) nogen
sort hhid year
xtset hhid year
gen byte emp_cell = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)
egen year_state = group(year fips_state_code)
cap drop d_iv
cap drop d_inc
gen d_inc = D.real_income
gen d_iv  = D.iv_income_fips / 10
gen d_ivx = D.iv_income_occedu_fips / 10
gen d_ivb = D.iv_income_baseline_fips / 10
gen d_acs = D.iv_income_acs / 10
gen d_bls = D.bls_occ_wage / 10000
gen d_prd = d_iv_pred / 10
// type of cell change t-1 to t, the bins of build_iv.py
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
replace stype = 5 if sw_occ + sw_edu + sw_size >= 2 & !missing(sw_occ, sw_edu, sw_size)
gen byte stayer = stype == 1 if !missing(stype)
gen byte into_ne = occ_bin == 4 & L.occ_bin != 4 if !missing(occ_bin, L.occ_bin)
gen byte same_size = hh_size == L.hh_size & hh_size == F.hh_size if !missing(hh_size, L.hh_size, F.hh_size)
cap drop inc_q baseline_income
bysort hhid (year): gen baseline_income = real_income[1]
xtile inc_q = baseline_income [aw = projection_factor] if movers_f == 0, nquantiles(4)
sort hhid year
local FE "year_state hhid kids avg_age_hh_head hh_comp"

// ---------------------------------------------------------------- where the estimate comes from
if "`part'" == "unpack" {
	cap file close fh
	file open fh using "$idir/what_moves/iv_unpack.csv", write replace
	file write fh "table,outcome,window,group,a,b,c,d,e,f" _n
	file close fh
	foreach v in hei_usda upf_share {
		gen double y_main = F.`v' - `v'
		gen double y_t = `v' - L.`v'
		gen double y_two = F.`v' - L.`v'
		foreach w in main t two {
			// one sample per window, no missing regressors; FE residuals, then the shares of the two covariances
			preserve
			keep if movers_f == 0 & clean == 1 & !missing(y_`w', d_inc, d_iv, stype, into_ne, projection_factor)
			quietly reghdfe y_`w' [aw = projection_factor], absorb(`FE') residuals(ry) keepsingletons
			quietly reghdfe d_inc [aw = projection_factor], absorb(`FE') residuals(rx) keepsingletons
			quietly reghdfe d_iv [aw = projection_factor], absorb(`FE') residuals(rz) keepsingletons
			keep if !missing(ry, rx, rz)
			gen double num = projection_factor * rz * ry
			gen double den = projection_factor * rz * rx
			gen double zz = projection_factor * rz * rz
			quietly summarize num
			local Tn = r(sum)
			quietly summarize den
			local Td = r(sum)
			quietly summarize zz
			local Tz = r(sum)
			di "`v' `w': pooled IV from residuals = " `Tn' / `Td'
			forvalues g = 0/6 {
				if `g' == 0 local cond "1"
				if inrange(`g', 1, 5) local cond "stype == `g'"
				if `g' == 6 local cond "stype > 1 & into_ne == 1"
				quietly summarize num if `cond'
				local n = r(sum)
				quietly summarize den if `cond'
				local d = r(sum)
				quietly summarize zz if `cond'
				local z = r(sum)
				quietly count if `cond'
				local N = r(N)
				file open fh using "$idir/what_moves/iv_unpack.csv", write append
				file write fh "shares,`v',`w',`g'," (`N') "," (`z' / `Tz') "," (`d' / `Td') "," (`n' / `Tn') "," (`n' / `d') "," (`Tn' / `Td') _n
				file close fh
				di "  group `g': N " `N' "  share of instrument variance " %5.3f `z' / `Tz' "  of own-income link " %5.3f `d' / `Td' "  of outcome link " %5.3f `n' / `Tn' "  own ratio " %7.3f `n' / `d'
			}
			restore
		}
		// year by year, on households observed t-1 through t+1 (same households in each column), own income and outcome on the group-income change
		gen double c_m3 = L3.`v' - L.`v'
		gen double c_m2 = L2.`v' - L.`v'
		gen double c_0  = `v' - L.`v'
		gen double c_p1 = F.`v' - L.`v'
		gen double c_p2 = F2.`v' - L.`v'
		gen byte samp3 = movers_f == 0 & clean == 1 & !missing(L.`v', `v', F.`v', d_inc, d_iv, stayer)
		foreach gg in stayers switchers {
			local cond = cond("`gg'" == "stayers", "stayer == 1", "stayer == 0")
			foreach k in m3 m2 0 p1 p2 {
				quietly reghdfe c_`k' d_iv [pw = projection_factor] if samp3 == 1 & `cond', absorb(`FE') cluster(fips)
				local rf = _b[d_iv]
				local rfse = _se[d_iv]
				local rN = e(N)
				quietly ivreghdfe c_`k' (d_inc = d_iv) [pw = projection_factor] if samp3 == 1 & `cond', absorb(`FE') cluster(fips)
				file open fh using "$idir/what_moves/iv_unpack.csv", write append
				file write fh "path,`v',`k',`gg'," (`rf') "," (`rfse') "," (_b[d_inc]) "," (_se[d_inc]) "," (e(widstat)) "," (`rN') _n
				file close fh
				di "`v' `gg' t+`k': outcome per $10k of group income " %7.3f `rf' " (" %6.3f `rfse' "); per $10k of own income " %7.3f _b[d_inc] " (" %6.3f _se[d_inc] ") F " %5.0f e(widstat) " N " `rN'
			}
			quietly reghdfe d_inc d_iv [pw = projection_factor] if samp3 == 1 & `cond', absorb(`FE') cluster(fips)
			file open fh using "$idir/what_moves/iv_unpack.csv", write append
			file write fh "first,`v',all,`gg'," (_b[d_iv]) "," (_se[d_iv]) ",,,," (e(N)) _n
			file close fh
			di "`v' `gg': own income per dollar of group income " %6.3f _b[d_iv] " (" %5.3f _se[d_iv] ")"
		}
		drop y_main y_t y_two c_m3 c_m2 c_0 c_p1 c_p2 samp3
	}
	di "build_iv_robustness.do unpack complete"
	exit
}

// ---------------------------------------------------------------- households whose own income does not follow the group
// `do build_iv_robustness.do zero`: outcome windows on the group-income change with no income regressor, for households whose group does
// not change (own income does not move with group income among them, so any response here is not coming through own income) and for
// households whose group changes. Output: interim/what_moves/iv_zero_first_stage.csv
if "`part'" == "zero" {
	cap file close fh
	file open fh using "$idir/what_moves/iv_zero_first_stage.csv", write replace
	file write fh "outcome,window,group,b,se,n" _n
	file close fh
	foreach v in hei_usda upf_share produce_share sugar_1000 {
		gen double y_main = F.`v' - `v'
		gen double y_t = `v' - L.`v'
		gen double y_two = F.`v' - L.`v'
		gen double y_before = L2.`v' - L3.`v'
		foreach gg in stayers switchers {
			local cond = cond("`gg'" == "stayers", "stayer == 1", "stayer == 0")
			foreach w in main t two before {
				quietly reghdfe y_`w' d_iv [pw = projection_factor] if movers_f == 0 & clean == 1 & `cond', absorb(`FE') cluster(fips)
				file open fh using "$idir/what_moves/iv_zero_first_stage.csv", write append
				file write fh "`v',`w',`gg'," (_b[d_iv]) "," (_se[d_iv]) "," (e(N)) _n
				file close fh
				di "`v' `gg' `w': per $10k of group income " %7.3f _b[d_iv] " (" %6.3f _se[d_iv] ")  N " e(N)
			}
		}
		drop y_main y_t y_two y_before
	}
	quietly reghdfe d_inc d_iv [pw = projection_factor] if movers_f == 0 & clean == 1 & stayer == 1, absorb(`FE') cluster(fips)
	file open fh using "$idir/what_moves/iv_zero_first_stage.csv", write append
	file write fh "own_income,first,stayers," (_b[d_iv]) "," (_se[d_iv]) "," (e(N)) _n
	file close fh
	di "own income per dollar of group income, group unchanged: " %7.4f _b[d_iv] " (" %6.4f _se[d_iv] ")"
	quietly reghdfe d_inc d_iv [pw = projection_factor] if movers_f == 0 & clean == 1 & stayer == 0, absorb(`FE') cluster(fips)
	file open fh using "$idir/what_moves/iv_zero_first_stage.csv", write append
	file write fh "own_income,first,switchers," (_b[d_iv]) "," (_se[d_iv]) "," (e(N)) _n
	file close fh
	di "own income per dollar of group income, group changed: " %7.4f _b[d_iv] " (" %6.4f _se[d_iv] ")"
	di "build_iv_robustness.do zero complete"
	exit
}

// ---------------------------------------------------------------- one outcome, every row
local v "`part'"
local mode "`2'"
global OUT "$idir/what_moves/iv_robustness_`v'.csv"
cap file close fh
if "`mode'" != "qlag" {
	file open fh using "$OUT", write replace
	file write fh "outcome,spec,window,b,se,f,n" _n
	file close fh
}
cap program drop run_iv
program define run_iv
	args v spec window yvar xvar zvar cond fe
	capture ivreghdfe `yvar' (`xvar' = `zvar') [pw = projection_factor] if `cond', absorb(`fe') cluster(fips)
	if _rc == 0 {
		file open fh using "$OUT", write append
		file write fh "`v',`spec',`window'," (_b[`xvar']) "," (_se[`xvar']) "," (e(widstat)) "," (e(N)) _n
		file close fh
		di "`v' `spec' `window': " %8.3f _b[`xvar'] " (" %7.3f _se[`xvar'] ") F " %6.1f e(widstat) " N " e(N)
	}
	else di as error "`v' `spec' `window' failed, rc " _rc
end

gen double y_main = F.`v' - `v'
gen double y_before = L2.`v' - L3.`v'
gen double y_two = F.`v' - L.`v'
gen double y_t = `v' - L.`v'
gen double y_lev = F.`v'
// calories: households of the same size only, in every row
local base "movers_f == 0 & clean == 1"
if "`v'" == "cals_ae_day" local base "`base' & same_size == 1"

// quartile of income at t-1: cut points within each year among non-movers, projection-weighted
if "`mode'" == "qlag" {
	preserve
	import delimited using "$OUT", clear varnames(1) stringcols(_all)
	drop if substr(spec, 1, 2) == "ql"
	export delimited using "$OUT", replace
	restore
	gen byte inc_qy = .
	levelsof year, local(yrs)
	foreach y of local yrs {
		_pctile real_income [aw = projection_factor] if year == `y' & movers_f == 0, nquantiles(4)
		replace inc_qy = 1 + (real_income > r(r1)) + (real_income > r(r2)) + (real_income > r(r3)) if year == `y' & movers_f == 0 & !missing(real_income)
	}
	gen byte inc_ql = L.inc_qy
	gen double inc_lag = L.real_income
	gen byte cmp = `base' & !missing(y_main, d_inc, d_iv)
	tab inc_q inc_ql [aw = projection_factor] if cmp, cell nofreq
	gen byte same_q = inc_q == inc_ql if cmp & !missing(inc_q, inc_ql)
	sum same_q [aw = projection_factor]
	tabstat inc_lag d_inc d_iv [aw = projection_factor] if cmp, by(inc_q) statistics(mean n)
	tabstat inc_lag d_inc d_iv [aw = projection_factor] if cmp, by(inc_ql) statistics(mean n)
	foreach w in main before {
		forvalues q = 1/4 {
			run_iv `v' ql`q' `w' y_`w' d_inc d_iv "`base' & inc_ql == `q'" "`FE'"
		}
	}
	di "build_iv_robustness.do `v' qlag complete"
	exit
}

foreach w in main before {
	run_iv `v' main `w' y_`w' d_inc d_iv "`base'" "`FE'"
	run_iv `v' hh_year `w' y_`w' d_inc d_iv "`base'" "year hhid kids avg_age_hh_head hh_comp"
	run_iv `v' no_hhfe `w' y_`w' d_inc d_iv "`base'" "year_state kids avg_age_hh_head hh_comp"
	run_iv `v' year_only `w' y_`w' d_inc d_iv "`base'" "year kids avg_age_hh_head hh_comp"
	if "`v'" != "cals_ae_day" run_iv `v' same_size `w' y_`w' d_inc d_iv "`base' & same_size == 1" "`FE'"
	if "`v'" != "cals_ae_day" run_iv `v' geo_movers `w' y_`w' d_inc d_iv "clean == 1" "`FE'"
	if "`v'" == "cals_ae_day" run_iv `v' geo_movers `w' y_`w' d_inc d_iv "clean == 1 & same_size == 1" "`FE'"
	run_iv `v' stayers `w' y_`w' d_inc d_iv "`base' & stayer == 1" "`FE'"
	run_iv `v' switchers `w' y_`w' d_inc d_iv "`base' & stayer == 0" "`FE'"
	run_iv `v' switch_noNE `w' y_`w' d_inc d_iv "`base' & stayer == 0 & into_ne == 0" "`FE'"
	run_iv `v' no_size_bin `w' y_`w' d_inc d_ivx "`base'" "`FE'"
	run_iv `v' predicted `w' y_`w' d_inc d_prd "`base'" "`FE'"
	run_iv `v' baseline_occ `w' y_`w' d_inc d_ivb "`base'" "`FE'"
	run_iv `v' bls `w' y_`w' d_inc d_bls "`base'" "`FE'"
	run_iv `v' acs `w' y_`w' d_inc d_acs "`base'" "`FE'"
	forvalues q = 1/4 {
		run_iv `v' q`q' `w' y_`w' d_inc d_iv "`base' & inc_q == `q'" "`FE'"
	}
}
run_iv `v' twoyear main y_two d_inc d_iv "`base'" "`FE'"
run_iv `v' level_t main y_t d_inc d_iv "`base'" "`FE'"
forvalues q = 1/4 {
	run_iv `v' q`q' twoyear y_two d_inc d_iv "`base' & inc_q == `q'" "`FE'"
}
// levels: outcome at t+1 on income at t
capture ivreghdfe y_lev (real_income = iv_income_fips) [pw = projection_factor] if `base', absorb(`FE') cluster(fips)
if _rc == 0 {
	file open fh using "$OUT", write append
	file write fh "`v',levels,main," (_b[real_income]) "," (_se[real_income]) "," (e(widstat)) "," (e(N)) _n
	file close fh
	di "`v' levels: " %8.3f _b[real_income] " (" %7.3f _se[real_income] ") F " %6.1f e(widstat) " N " e(N)
}
// k-year changes: employed at t-k; outcome from t+1-k to t+1, income and instrument from t-k to t. Before window: the same k-year income change on the one-year change t-k-2 to t-k-1
foreach k in 2 3 5 8 {
	local km1 = `k' - 1
	gen double dk_inc = real_income - L`k'.real_income
	gen double dk_iv = (iv_income_fips - L`k'.iv_income_fips) / 10
	gen double dk_y = F.`v' - L`km1'.`v'
	gen byte clean_k = L`k'.emp_cell == 1
	gen byte size_k = hh_size == L`k'.hh_size & hh_size == F.hh_size
	local bk "movers_f == 0 & clean_k == 1"
	if "`v'" == "cals_ae_day" local bk "`bk' & size_k == 1"
	run_iv `v' k`k' main dk_y dk_inc dk_iv "`bk'" "`FE'"
	drop dk_inc dk_iv dk_y clean_k size_k
}
di "build_iv_robustness.do `v' complete"
