// ============================================================================
// build_local_price_regs.do -- do households whose local market makes healthy
// food relatively cheaper eat better? (Anya's "what moves nutrition" review,
// 2026-11-05). Two local price measures:
//   MAIN   county x year group price levels from the RMS scanner price index
//          (build_county_price_index.py: chained CES log price levels, 2007 = 0,
//          by module group with fixed county module weights; rel_hi_ces = healthy
//          minus unhealthy, rel_upf_ces = mostly ultra-processed minus not,
//          rel_prep_ces = little preparation minus cooking; _va = variety-
//          adjusted) with a SHIFT-SHARE INSTRUMENT z_rel_* = the same index built
//          from the NATIONAL module price changes, so a county's predicted
//          relative price moves only through its fixed module mix. 2008-2020.
//   CHECK  3-digit-zip market x year unit-value index from the panelists' own
//          purchases (build_local_price_index.py), 2004-2020, run when the
//          do-file is called with the argument `zip3`.
// Comparison: the same household in years when its market's healthy food is
// relatively cheaper vs dearer, against other households in the same year
// (household and year FE; kids, composition, 5-year age-bin dummies); a
// state-by-year version; the IV version; and long differences across markets
// (first three vs last three years). Outcomes: full HEI, simple USDA index,
// produce share of calories, sugar g per 1,000 kcal, calories per adult
// equivalent per day (on the healthy-unhealthy price); ultra-processed share
// (on the UPF price); basket preparation-time score (on the prep price).
// Projection weights; SE clustered on market; geographic non-movers.
// Output: interim/what_moves/local_price_regs.csv; county long differences in
// local_price_longdiff_county.dta (zip3: _zip3)
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
local which "`1'"
if "`which'" == "" local which county

local pairs "hei_usda:hi hi_usda:hi produce_share:hi sugar_per_1000cal_usda:hi cals_ae_day:hi upf_share:upf prep_time:prep"

cap program drop write_row
program define write_row
	args index outcome price spec b se N nunits fsF fsb
	file open fh using "$idir/what_moves/local_price_regs.csv", write append text
	file write fh "`index',`outcome',`price',`spec',`b',`se',`N',`nunits',`fsF',`fsb'" _n
	file close fh
end

if "`which'" == "county" {
	cap file close fh
	file open fh using "$idir/what_moves/local_price_regs.csv", write replace text
	file write fh "index,outcome,price,spec,b,se,N,n_units,fs_F,fs_b" _n
	file close fh
	use "$idir/what_moves/county_price_index.dta", clear
	keep if !missing(rel_hi_ces)
	foreach v in rel_hi_ces rel_upf_ces rel_prep_ces z_rel_hi {
		quietly egen double _my = mean(`v'), by(year)
		quietly gen double _dw = `v' - _my
		quietly summarize _dw
		local sdb = r(sd)
		quietly egen double _mm = mean(`v'), by(fips)
		quietly gen double _dm = `v' - _mm
		quietly summarize _dm
		di "VAR county `v': SD across counties within a year " %6.3f `sdb' "; within a county over time " %6.3f r(sd)
		drop _my _dw _mm _dm
	}
	tempfile idx
	save `idx'
	use hhid year fips hei_usda hi_usda sugar_per_1000cal_usda prep_time projection_factor kids hh_comp avg_age_hh_head fips_state_code movers_f ///
		using "$gdir/labor_supply_reg_data.dta", clear
	keep if movers_f == 0
	merge m:1 fips year using `idx', keep(match) nogenerate
	local unit fips
	local first 2008
	local last 2020
}
else {
	pq use using "$idir/what_moves/local_price_index.parquet", clear
	keep if n_households >= 30
	rename market zip3
	rename (rel_hi rel_upf rel_prep) (rel_hi_ces rel_upf_ces rel_prep_ces)
	foreach v in rel_hi_ces rel_upf_ces rel_prep_ces {
		quietly egen double _my = mean(`v'), by(year)
		quietly gen double _dw = `v' - _my
		quietly summarize _dw
		local sdb = r(sd)
		quietly egen double _mm = mean(`v'), by(zip3)
		quietly gen double _dm = `v' - _mm
		quietly summarize _dm
		di "VAR zip3 `v': SD across markets within a year " %6.3f `sdb' "; within a market over time " %6.3f r(sd)
		drop _my _dw _mm _dm
	}
	tempfile idx
	save `idx'
	use hhid year zip_code hei_usda hi_usda sugar_per_1000cal_usda prep_time projection_factor kids hh_comp avg_age_hh_head fips fips_state_code movers_f ///
		using "$gdir/labor_supply_reg_data.dta", clear
	keep if movers_f == 0
	gen int zip3 = floor(zip_code / 100)
	merge m:1 zip3 year using `idx', keep(match) nogenerate
	local unit zip3
	local first 2004
	local last 2020
}
merge 1:1 hhid year using "$idir/what_moves/nova_outcomes.dta", keep(master match) nogenerate
gen age5 = floor(avg_age_hh_head / 5) * 5
egen year_state = group(year fips_state_code)
quietly count
di "`which': household-years matched to a price index: " r(N)
quietly egen byte _t = tag(`unit')
quietly count if _t
di "`which': markets: " r(N)
drop _t

foreach pr of local pairs {
	gettoken y p : pr, parse(":")
	local p = subinstr("`p'", ":", "", .)
	local x rel_`p'_ces
	foreach spec in hh_year hh_stateyear {
		local fe = cond("`spec'" == "hh_year", "hhid year age5 kids hh_comp", "hhid year_state age5 kids hh_comp")
		reghdfe `y' `x' [pw = projection_factor], absorb(`fe') cluster(`unit')
		quietly egen byte _m = tag(`unit') if e(sample)
		quietly count if _m
		local nm = r(N)
		drop _m
		write_row `which' `y' `x' `spec' `=_b[`x']' `=_se[`x']' `=e(N)' `nm' . .
		di "REG `which' `y' on `x' (`spec'): " %8.4f _b[`x'] " (" %7.4f _se[`x'] ")  N = " e(N) "  markets = `nm'"
	}
	if "`which'" == "county" {
		ivreghdfe `y' (`x' = z_rel_`p') [pw = projection_factor], absorb(hhid year age5 kids hh_comp) cluster(`unit')
		local F = e(widstat)
		local b2 = _b[`x']
		local se2 = _se[`x']
		local N2 = e(N)
		quietly reghdfe `x' z_rel_`p' [pw = projection_factor] if e(sample), absorb(hhid year age5 kids hh_comp) cluster(`unit')
		write_row `which' `y' `x' iv `b2' `se2' `N2' . `F' `=_b[z_rel_`p']'
		di "IV  `which' `y' on `x' (shift-share): " %8.4f `b2' " (" %7.4f `se2' ")  first stage " %6.3f _b[z_rel_`p'] " F = " %6.1f `F'
		if "`y'" == "hei_usda" {
			reghdfe `y' rel_`p'_va [pw = projection_factor], absorb(hhid year age5 kids hh_comp) cluster(`unit')
			write_row `which' `y' rel_`p'_va hh_year_va `=_b[rel_`p'_va]' `=_se[rel_`p'_va]' `=e(N)' . . .
			di "REG `which' `y' on the variety-adjusted relative price: " %8.4f _b[rel_`p'_va] " (" %7.4f _se[rel_`p'_va] ")"
		}
	}
}

// ---------------------------------------------------------------- long differences across markets
preserve
gen byte period = cond(inrange(year, `first', `first' + 2), 1, cond(inrange(year, `last' - 2, `last'), 2, .))
drop if missing(period)
local zs ""
if "`which'" == "county" local zs "z_rel_hi z_rel_upf z_rel_prep"
collapse (mean) hei_usda hi_usda produce_share sugar_per_1000cal_usda cals_ae_day upf_share prep_time rel_hi_ces rel_upf_ces rel_prep_ces `zs' (count) n = hei_usda [pw = projection_factor], by(`unit' period)
reshape wide hei_usda hi_usda produce_share sugar_per_1000cal_usda cals_ae_day upf_share prep_time rel_hi_ces rel_upf_ces rel_prep_ces `zs' n, i(`unit') j(period)
foreach v in hei_usda hi_usda produce_share sugar_per_1000cal_usda cals_ae_day upf_share prep_time rel_hi_ces rel_upf_ces rel_prep_ces `zs' {
	gen d_`v' = `v'2 - `v'1
}
gen w = n1 + n2
quietly summarize d_rel_hi_ces
di "VAR `which' long difference in rel_hi_ces across markets: SD " %6.3f r(sd) " over " r(N) " markets"
foreach pr of local pairs {
	gettoken y p : pr, parse(":")
	local p = subinstr("`p'", ":", "", .)
	local x rel_`p'_ces
	regress d_`y' d_`x' [aw = w], robust
	write_row `which' `y' `x' longdiff `=_b[d_`x']' `=_se[d_`x']' `=e(N)' `=e(N)' . .
	di "LONGDIFF `which' `y' on `x': " %8.4f _b[d_`x'] " (" %7.4f _se[d_`x'] ")  markets = " e(N)
	if "`which'" == "county" {
		ivregress 2sls d_`y' (d_`x' = d_z_rel_`p') [aw = w], vce(robust)
		local b2 = _b[d_`x']
		local se2 = _se[d_`x']
		local N2 = e(N)
		quietly regress d_`x' d_z_rel_`p' [aw = w], robust
		local F = (_b[d_z_rel_`p'] / _se[d_z_rel_`p'])^2
		write_row `which' `y' `x' longdiff_iv `b2' `se2' `N2' `N2' `F' `=_b[d_z_rel_`p']'
		di "LONGDIFF IV `which' `y' on `x': " %8.4f `b2' " (" %7.4f `se2' ")  first stage " %6.3f _b[d_z_rel_`p'] " F = " %6.1f `F'
	}
}
save "$idir/what_moves/local_price_longdiff_`which'.dta", replace
restore
di "build_local_price_regs.do (`which') complete"
