// ============================================================================
// build_local_price_regs.do -- do households whose local market makes healthy
// food relatively cheaper eat better? (Anya's "what moves nutrition" review,
// 2026-11-05). `do build_local_price_regs.do county` (main), `... zip3` (check);
// a second argument `test` runs on a 2% household sample.
//   county  RMS scanner price index by county x year 2008-2020
//           (build_county_price_index.py: chained CES log price change since
//           2007 at fixed 2008 module shares; rel_hi_ces = healthy minus
//           unhealthy, rel_upf_ces = mostly ultra-processed minus not,
//           rel_prep_ces = little preparation minus cooking; _va = variety-
//           adjusted) and its shift-share instrument z_rel_* (2008 shares x
//           national module price change, county left out, chained from 2008).
//   zip3    3-digit-zip market x year price LEVEL relative to the national
//           basket from the panelists' own purchases, 2004-2020
//           (build_local_price_index.py).
// Comparisons: the same household in years when its market's relative price is
// lower vs higher, against other households in the same year (household and year
// FE; kids, composition, 5-year age-bin dummies); the same with state-by-year FE;
// the shift-share IV (with and without county baseline HEI and income x year);
// long differences across markets (first three vs last three years) and within
// households present in both windows; a placebo of the later price change on the
// diet change BEFORE 2008; store-side spending ratios on the same prices. Zip3
// adds the comparison ACROSS markets (no household FE). Projection weights; SE
// clustered on market; geographic non-movers. HEI comes from hei_usda_panel.
// Output: interim/what_moves/local_price_regs_<county|zip3>.csv,
//         local_price_longdiff_<county|zip3>.dta
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
local which "`1'"
if "`which'" == "" local which county
local test "`2'"
global OUTCSV "$idir/what_moves/local_price_regs_`which'.csv"

cap program drop write_row
program define write_row
	args index outcome price spec b se N nunits fsF fsb fsse
	file open fh using "$OUTCSV", write append text
	file write fh "`index',`outcome',`price',`spec',`b',`se',`N',`nunits',`fsF',`fsb',`fsse'" _n
	file close fh
end

cap file close fh
file open fh using "$OUTCSV", write replace text
file write fh "index,outcome,price,spec,b,se,N,n_units,fs_F,fs_b,fs_se" _n
file close fh

local pairs "hei_usda:hi hei_usda:upf hei_usda:prep upf_share:upf prep_time_raw:prep hi_usda:hi produce_share:hi sugar_per_1000cal_usda:hi cals_ae_day:hi"
local ys "hei_usda hi_usda produce_share sugar_per_1000cal_usda cals_ae_day upf_share prep_time_raw"
local prices "hi upf prep"
local FE "hhid year age5 kids hh_comp"

// ---------------------------------------------------------------- household panel
use hhid year fips zip_code hi_usda sugar_per_1000cal_usda prep_time_raw projection_factor kids hh_comp avg_age_hh_head fips_state_code movers_f real_income hh_avg_yrsofschool ///
	using "$gdir/labor_supply_reg_data.dta", clear
keep if movers_f == 0
if "`test'" == "test" keep if mod(hhid, 50) == 0
merge 1:1 hhid year using "$idir/what_moves/local_price_outcomes.dta", keep(master match) nogenerate
gen long county = real(fips)
gen long zip3 = floor(real(zip_code) / 100)
gen int state = real(fips_state_code)
gen age5 = floor(avg_age_hh_head / 5) * 5
egen year_state = group(year state)
tempfile hh
save `hh'

if "`which'" == "county" {
	// -------------------------------------------------- county index: variation, store-side regressions
	use "$idir/what_moves/county_price_index.dta", clear
	rename fips county
	xtset county year
	foreach p of local prices {
		gen F_rel_`p' = F.rel_`p'_ces
		gen L_rel_`p' = L.rel_`p'_ces
		quietly reghdfe rel_`p'_ces, absorb(county year) residuals(_r)
		quietly summarize _r
		local s1 = r(sd)
		drop _r
		quietly reghdfe z_rel_`p' if !missing(rel_`p'_ces), absorb(county year) residuals(_r)
		quietly summarize _r
		di "VAR county rel_`p'_ces: SD net of county and year means " %6.4f `s1' "; instrument z_rel_`p' " %6.4f r(sd)
		drop _r
		reghdfe rel_`p'_ces z_rel_`p' [aw = spend_total], absorb(county year) cluster(county)
		di "FSC county-level first stage rel_`p'_ces on z: " %7.3f _b[z_rel_`p'] " (" %6.3f _se[z_rel_`p'] ")"
		reghdfe lspend_`p' rel_`p'_ces [aw = spend_total], absorb(county year) cluster(county)
		write_row county lspend_`p' rel_`p'_ces store_ols `=_b[rel_`p'_ces]' `=_se[rel_`p'_ces]' `=e(N)' . . . .
		di "STORE lspend_`p' on rel_`p'_ces: " %8.4f _b[rel_`p'_ces] " (" %7.4f _se[rel_`p'_ces] ")  N = " e(N)
		ivreghdfe lspend_`p' (rel_`p'_ces = z_rel_`p') [aw = spend_total], absorb(county year) cluster(county)
		write_row county lspend_`p' rel_`p'_ces store_iv `=_b[rel_`p'_ces]' `=_se[rel_`p'_ces]' `=e(N)' . `=e(widstat)' . .
		di "STORE IV lspend_`p': " %8.4f _b[rel_`p'_ces] " (" %7.4f _se[rel_`p'_ces] ")  F = " %6.1f e(widstat)
	}
	keep county year rel_* z_rel_* F_rel_* L_rel_*
	tempfile idx
	save `idx'

	// -------------------------------------------------- pre-period (before any price data): household changes 2004-05 -> 2006-07
	use `hh', clear
	keep if inrange(year, 2004, 2007) & !missing(county)
	gen byte win = cond(year <= 2005, 1, 2)
	collapse (mean) `ys' projection_factor (first) county, by(hhid win)
	reshape wide `ys' projection_factor, i(hhid county) j(win)
	foreach v of local ys {
		gen d0_`v' = `v'2 - `v'1
	}
	gen w0 = (projection_factor1 + projection_factor2) / 2
	gen byte one = 1
	keep if !missing(w0)
	collapse (mean) d0_* (rawsum) n0 = one w0 [pw = w0], by(county)
	keep if n0 >= 10
	di "PLACEBO sample: " _N " counties with 10+ households observed in both 2004-05 and 2006-07"
	tempfile pre
	save `pre'

	// -------------------------------------------------- county baseline HEI and income, 2004-2007
	use `hh', clear
	keep if inrange(year, 2004, 2007) & !missing(county)
	collapse (mean) base_hei = hei_usda base_inc = real_income [pw = projection_factor], by(county)
	tempfile base
	save `base'

	use `hh', clear
	merge m:1 county using `base', keep(master match) nogenerate
	merge m:1 county year using `idx', keep(match) nogenerate
	forvalues t = 2009/2020 {
		gen double bh_`t' = base_hei * (year == `t')
		gen double bi_`t' = base_inc * (year == `t')
	}
	local unit county
	local first 2008
	local last 2020
}
else {
	// -------------------------------------------------- zip3 index
	use "$idir/what_moves/county_price_index.dta", clear
	rename fips county
	keep county year rel_hi_ces rel_upf_ces rel_prep_ces
	rename (rel_hi_ces rel_upf_ces rel_prep_ces) (c_rel_hi c_rel_upf c_rel_prep)
	tempfile cidx
	save `cidx'

	pq use using "$idir/what_moves/local_price_index.parquet", clear
	keep if n_households >= 30
	rename market zip3
	destring zip3, replace
	rename (rel_hi rel_upf rel_prep) (rel_hi_ces rel_upf_ces rel_prep_ces)
	foreach p of local prices {
		quietly egen double _my = mean(rel_`p'_ces), by(year)
		quietly gen double _dw = rel_`p'_ces - _my
		quietly summarize _dw
		local sdb = r(sd)
		quietly reghdfe rel_`p'_ces, absorb(zip3 year) residuals(_r)
		quietly summarize _r
		di "VAR zip3 rel_`p'_ces: SD across markets within a year " %6.4f `sdb' "; net of market and year means " %6.4f r(sd)
		drop _r _my _dw
	}
	keep zip3 year rel_*
	tempfile idx
	save `idx'

	use `hh', clear
	merge m:1 zip3 year using `idx', keep(match) nogenerate
	merge m:1 county year using `cidx', keep(master match) nogenerate
	local unit zip3
	local first 2004
	local last 2020
}

quietly count
di "SAMPLE `which': household-years matched to a price index: " r(N)
quietly egen byte _t = tag(`unit')
quietly count if _t
di "SAMPLE `which': markets: " r(N)
drop _t
quietly egen byte _t = tag(hhid)
quietly count if _t
di "SAMPLE `which': households: " r(N)
drop _t
foreach v of local ys {
	quietly summarize `v' [aw = projection_factor]
	di "MEAN `v': " %9.3f r(mean) "  SD " %9.3f r(sd)
}
foreach p of local prices {
	quietly reghdfe rel_`p'_ces [pw = projection_factor], absorb(`FE') residuals(_r)
	quietly summarize _r [aw = projection_factor]
	di "VAR `which' rel_`p'_ces: identifying SD on the household sample (net of household, year, age, kids, composition) " %6.4f r(sd)
	drop _r
}

// ---------------------------------------------------------------- household regressions
foreach pr of local pairs {
	gettoken y p : pr, parse(":")
	local p = subinstr("`p'", ":", "", .)
	local x rel_`p'_ces
	reghdfe `y' `x' [pw = projection_factor], absorb(`FE') cluster(`unit')
	quietly egen byte _m = tag(`unit') if e(sample)
	quietly count if _m
	local nm = r(N)
	drop _m
	write_row `which' `y' `x' hh_year `=_b[`x']' `=_se[`x']' `=e(N)' `nm' . . .
	di "REG `which' `y' on `x' (hh + year): " %8.4f _b[`x'] " (" %7.4f _se[`x'] ")  N = " e(N) "  markets = `nm'"
	reghdfe `y' `x' [pw = projection_factor], absorb(hhid year_state age5 kids hh_comp) cluster(`unit')
	write_row `which' `y' `x' hh_stateyear `=_b[`x']' `=_se[`x']' `=e(N)' `nm' . . .
	di "REG `which' `y' on `x' (hh + state-year): " %8.4f _b[`x'] " (" %7.4f _se[`x'] ")  N = " e(N)

	if "`which'" == "zip3" {
		reghdfe `y' `x' [pw = projection_factor], absorb(year age5 kids hh_comp) cluster(zip3)
		write_row zip3 `y' `x' cross `=_b[`x']' `=_se[`x']' `=e(N)' `nm' . . .
		di "CROSS `y' on `x' (across markets): " %8.4f _b[`x'] " (" %7.4f _se[`x'] ")"
		reghdfe `y' `x' real_income hh_avg_yrsofschool [pw = projection_factor], absorb(year age5 kids hh_comp) cluster(zip3)
		write_row zip3 `y' `x' cross_x `=_b[`x']' `=_se[`x']' `=e(N)' `nm' . . .
		di "CROSS `y' on `x' (across markets, income and schooling held equal): " %8.4f _b[`x'] " (" %7.4f _se[`x'] ")"
	}
	if "`which'" == "county" {
		ivreghdfe `y' (`x' = z_rel_`p') [pw = projection_factor], absorb(`FE') cluster(county)
		local F = e(widstat)
		local b2 = _b[`x']
		local se2 = _se[`x']
		local N2 = e(N)
		local fsb .
		local fsse .
		if "`y'" == "hei_usda" {
			quietly reghdfe `x' z_rel_`p' [pw = projection_factor] if e(sample), absorb(`FE') cluster(county)
			local fsb = _b[z_rel_`p']
			local fsse = _se[z_rel_`p']
			di "FS household sample `x' on z_rel_`p': " %7.3f `fsb' " (" %6.3f `fsse' ")  F = " %6.1f `F'
		}
		write_row county `y' `x' iv `b2' `se2' `N2' . `F' `fsb' `fsse'
		di "IV  county `y' on `x' (shift-share): " %8.4f `b2' " (" %7.4f `se2' ")  F = " %6.1f `F'
		ivreghdfe `y' bh_* bi_* (`x' = z_rel_`p') [pw = projection_factor], absorb(`FE') cluster(county)
		write_row county `y' `x' iv_base `=_b[`x']' `=_se[`x']' `=e(N)' . `=e(widstat)' . .
		di "IV  county `y' on `x' (+ baseline x year): " %8.4f _b[`x'] " (" %7.4f _se[`x'] ")  F = " %6.1f e(widstat)
		if inlist("`y'", "hei_usda", "upf_share") {
			reghdfe `y' rel_`p'_va [pw = projection_factor], absorb(`FE') cluster(county)
			write_row county `y' rel_`p'_va hh_year_va `=_b[rel_`p'_va]' `=_se[rel_`p'_va]' `=e(N)' . . . .
			di "REG county `y' on variety-adjusted rel_`p'_va: " %8.4f _b[rel_`p'_va] " (" %7.4f _se[rel_`p'_va] ")"
		}
		if ("`y'" == "hei_usda" & "`p'" == "hi") | ("`y'" == "upf_share" & "`p'" == "upf") {
			reghdfe `y' F_rel_`p' `x' L_rel_`p' [pw = projection_factor], absorb(`FE') cluster(county)
			write_row county `y' `x' dyn_lead `=_b[F_rel_`p']' `=_se[F_rel_`p']' `=e(N)' . . . .
			write_row county `y' `x' dyn_cur `=_b[`x']' `=_se[`x']' `=e(N)' . . . .
			write_row county `y' `x' dyn_lag `=_b[L_rel_`p']' `=_se[L_rel_`p']' `=e(N)' . . . .
			di "DYN county `y': next year's price " %8.4f _b[F_rel_`p'] " (" %7.4f _se[F_rel_`p'] "), this year's " %8.4f _b[`x'] " (" %7.4f _se[`x'] "), last year's " %8.4f _b[L_rel_`p'] " (" %7.4f _se[L_rel_`p'] ")"
			reghdfe `y' `x' [pw = projection_factor] if !inlist(state, 1, 12, 22, 42), absorb(`FE') cluster(county)
			write_row county `y' `x' hh_year_noexit `=_b[`x']' `=_se[`x']' `=e(N)' . . . .
			di "REG county `y' on `x' (without AL, FL, LA, PA, the retailer exits): " %8.4f _b[`x'] " (" %7.4f _se[`x'] ")"
			ivreghdfe `y' (`x' = z_rel_`p') [pw = projection_factor], absorb(`FE') cluster(state)
			write_row county `y' `x' iv_stcl `=_b[`x']' `=_se[`x']' `=e(N)' . `=e(widstat)' . .
			di "IV  county `y' on `x' (SE clustered on state): " %8.4f _b[`x'] " (" %7.4f _se[`x'] ")  F = " %6.1f e(widstat)
		}
	}
}

// validate the two price measures against each other
if "`which'" == "zip3" {
	foreach p of local prices {
		reghdfe rel_`p'_ces c_rel_`p' [pw = projection_factor], absorb(hhid year) cluster(zip3)
		write_row zip3 rel_`p'_ces c_rel_`p' validate `=_b[c_rel_`p']' `=_se[c_rel_`p']' `=e(N)' . . . .
		di "VALID zip3 unit-value rel_`p' on county RMS rel_`p', within household: " %7.3f _b[c_rel_`p'] " (" %6.3f _se[c_rel_`p'] ")  N = " e(N)
	}
}

// ---------------------------------------------------------------- long differences across markets
local zs ""
if "`which'" == "county" local zs "z_rel_hi z_rel_upf z_rel_prep"
local rels "rel_hi_ces rel_upf_ces rel_prep_ces"
gen byte period = cond(inrange(year, `first', `first' + 2), 1, cond(inrange(year, `last' - 2, `last'), 2, .))
preserve
drop if missing(period)
gen byte one = 1
collapse (mean) `ys' `rels' `zs' (rawsum) nraw = one (sum) n = one [pw = projection_factor], by(`unit' period)
reshape wide `ys' `rels' `zs' nraw n, i(`unit') j(period)
keep if nraw1 >= 20 & nraw2 >= 20
foreach v in `ys' `rels' `zs' {
	gen d_`v' = `v'2 - `v'1
}
gen w = n1 + n2
if "`which'" == "county" merge 1:1 county using `pre', keep(master match) nogenerate
foreach p of local prices {
	quietly summarize d_rel_`p'_ces [aw = w]
	di "VAR `which' long difference in rel_`p'_ces (`first'-`=`first'+2' to `=`last'-2'-`last'): weighted mean " %6.3f r(mean) ", SD " %6.3f r(sd) " over " r(N) " markets"
}
foreach pr of local pairs {
	gettoken y p : pr, parse(":")
	local p = subinstr("`p'", ":", "", .)
	local x rel_`p'_ces
	regress d_`y' d_`x' [aw = w], robust
	write_row `which' `y' `x' longdiff `=_b[d_`x']' `=_se[d_`x']' `=e(N)' `=e(N)' . . .
	di "LONGDIFF `which' `y' on `x': " %8.4f _b[d_`x'] " (" %7.4f _se[d_`x'] ")  markets = " e(N)
	if "`which'" == "county" {
		regress d_`y' d_z_rel_`p' [aw = w], robust
		write_row county `y' z_rel_`p' longdiff_rf `=_b[d_z_rel_`p']' `=_se[d_z_rel_`p']' `=e(N)' `=e(N)' . . .
		di "LONGDIFF RF `y' on the instrument: " %8.4f _b[d_z_rel_`p'] " (" %7.4f _se[d_z_rel_`p'] ")"
		ivregress 2sls d_`y' (d_`x' = d_z_rel_`p') [aw = w], vce(robust)
		local b2 = _b[d_`x']
		local se2 = _se[d_`x']
		local N2 = e(N)
		regress d_`x' d_z_rel_`p' [aw = w] if e(sample), robust
		local F = (_b[d_z_rel_`p'] / _se[d_z_rel_`p'])^2
		write_row county `y' `x' longdiff_iv `b2' `se2' `N2' `N2' `F' `=_b[d_z_rel_`p']' `=_se[d_z_rel_`p']'
		di "LONGDIFF IV `y' on `x': " %8.4f `b2' " (" %7.4f `se2' ")  first stage " %6.3f _b[d_z_rel_`p'] " (" %6.3f _se[d_z_rel_`p'] ") F = " %6.1f `F'
		regress d0_`y' d_z_rel_`p' [aw = w0], robust
		write_row county `y' z_rel_`p' placebo_pre_z `=_b[d_z_rel_`p']' `=_se[d_z_rel_`p']' `=e(N)' `=e(N)' . . .
		di "PLACEBO `y' change 2004-05 to 2006-07 on the later instrument change: " %8.4f _b[d_z_rel_`p'] " (" %7.4f _se[d_z_rel_`p'] ")  counties = " e(N)
		regress d0_`y' d_`x' [aw = w0], robust
		write_row county `y' `x' placebo_pre_x `=_b[d_`x']' `=_se[d_`x']' `=e(N)' `=e(N)' . . .
		di "PLACEBO `y' change 2004-05 to 2006-07 on the later price change: " %8.4f _b[d_`x'] " (" %7.4f _se[d_`x'] ")"
	}
}
save "$idir/what_moves/local_price_longdiff_`which'.dta", replace
restore

// ---------------------------------------------------------------- long differences within households in both windows
preserve
drop if missing(period)
collapse (mean) `ys' `rels' `zs' projection_factor (first) cl = `unit', by(hhid period)
reshape wide `ys' `rels' `zs' projection_factor cl, i(hhid) j(period)
keep if !missing(cl1, cl2)
foreach v in `ys' `rels' `zs' {
	gen d_`v' = `v'2 - `v'1
}
gen w = (projection_factor1 + projection_factor2) / 2
di "SAMPLE `which': households in both long-difference windows: " _N
foreach pr of local pairs {
	gettoken y p : pr, parse(":")
	local p = subinstr("`p'", ":", "", .)
	local x rel_`p'_ces
	regress d_`y' d_`x' [pw = w], cluster(cl1)
	write_row `which' `y' `x' longdiff_hh `=_b[d_`x']' `=_se[d_`x']' `=e(N)' . . . .
	di "LONGDIFF-HH `which' `y' on `x': " %8.4f _b[d_`x'] " (" %7.4f _se[d_`x'] ")  households = " e(N)
	if "`which'" == "county" {
		ivregress 2sls d_`y' (d_`x' = d_z_rel_`p') [pw = w], vce(cluster cl1)
		local b2 = _b[d_`x']
		local se2 = _se[d_`x']
		local N2 = e(N)
		regress d_`x' d_z_rel_`p' [pw = w] if e(sample), cluster(cl1)
		local F = (_b[d_z_rel_`p'] / _se[d_z_rel_`p'])^2
		write_row county `y' `x' longdiff_hh_iv `b2' `se2' `N2' . `F' `=_b[d_z_rel_`p']' `=_se[d_z_rel_`p']'
		di "LONGDIFF-HH IV `y' on `x': " %8.4f `b2' " (" %7.4f `se2' ")  first stage " %6.3f _b[d_z_rel_`p'] " F = " %6.1f `F'
	}
}
restore
di "build_local_price_regs.do (`which') complete"
