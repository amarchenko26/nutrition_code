// ============================================================================
// build_local_price_timing.do -- do local relative prices follow the local diet, or the other way around?
// (Anya's comment on the local-price section, 2026-11-06.)
//
// Same county price index and household panel as build_local_price_regs.do (county x year 2008-2020, relative price
// = cumulative log change, healthy minus unhealthy or ultra-processed minus other; z_rel = 2008 shares x national
// module price changes, county left out).
//   A. Household outcome on the relative price two years ahead, next year, this year, last year and two years ago, all
//      in one regression (household, year, age, kids, composition FE; clustered on county). Run on the actual price and
//      on the predicted price. A coefficient on next year's price, holding this year's price fixed, is the
//      association between this year's diet and next year's price change.
//   B. County-year: next year's change in the relative price on this year's mean household change in the outcome
//      (households observed in both years, 20+ per county-year; county and year FE; clustered on county), and next
//      year's mean household change in the outcome on this year's change in the relative price.
//   C. The shift-share IV dropping the modules that move the instrument most (build_local_price_top_modules.py).
//   D. Size of the variation: standard deviations of the two prices net of county and year means.
// Output: interim/what_moves/local_price_timing.csv
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
global OUTCSV "$idir/what_moves/local_price_timing.csv"
cap file close fh
file open fh using "$OUTCSV", write replace
file write fh "part,outcome,price,term,b,se,N,F" _n
file close fh
cap program drop wr
program define wr
	args part outcome price term b se N F
	file open fh using "$OUTCSV", write append
	file write fh "`part',`outcome',`price',`term',`b',`se',`N',`F'" _n
	file close fh
end

use "$idir/what_moves/county_price_index.dta", clear
rename fips county
xtset county year
foreach p in rel_hi_ces rel_upf_ces z_rel_hi z_rel_upf {
	gen F2_`p' = F2.`p'
	gen F1_`p' = F.`p'
	gen L1_`p' = L.`p'
	gen L2_`p' = L2.`p'
	gen d_`p' = D.`p'
	gen Fd_`p' = F.`p' - `p'
}
foreach p in rel_hi_ces rel_upf_ces z_rel_hi z_rel_upf {
	quietly reghdfe `p' [aw = spend_total], absorb(county year) residuals(_r)
	quietly summarize _r [aw = spend_total]
	wr D sd `p' net_of_fe `=r(sd)' . `=r(N)' .
	di "D `p' SD net of county and year means " %6.4f r(sd)
	drop _r
	quietly summarize d_`p' [aw = spend_total]
	wr D sd `p' annual_change `=r(sd)' . `=r(N)' .
	di "D `p' SD of annual change " %6.4f r(sd)
	quietly summarize `p' [aw = spend_total] if year == 2020
	wr D sd `p' sd_2020 `=r(sd)' . `=r(N)' .
	di "D `p' SD across counties in 2020 " %6.4f r(sd) "  mean " %6.4f r(mean)
}
keep county year rel_* z_rel_* F2_* F1_* L1_* L2_* d_* Fd_* spend_total
tempfile idx
save `idx'

use hhid year fips sugar_per_1000cal_usda projection_factor kids hh_comp avg_age_hh_head fips_state_code movers_f using "$gdir/labor_supply_reg_data.dta", clear
keep if movers_f == 0
merge 1:1 hhid year using "$idir/what_moves/local_price_outcomes.dta", keep(master match) nogenerate
gen long county = real(fips)
gen age5 = floor(avg_age_hh_head / 5) * 5
merge m:1 county year using `idx', keep(match) nogenerate
xtset hhid year
local FE "hhid year age5 kids hh_comp"

// ---------------------------------------------------------------- A. leads and lags
local pairs "hei_usda:rel_hi_ces hei_usda:z_rel_hi upf_share:rel_upf_ces upf_share:z_rel_upf sugar_per_1000cal_usda:rel_hi_ces sugar_per_1000cal_usda:z_rel_hi"
foreach pr of local pairs {
	gettoken y p : pr, parse(":")
	local p = subinstr("`p'", ":", "", .)
	quietly reghdfe `y' F2_`p' F1_`p' `p' L1_`p' L2_`p' [pw = projection_factor], absorb(`FE') cluster(county)
	local N = e(N)
	foreach t in F2 F1 L1 L2 {
		wr A `y' `p' `t' `=_b[`t'_`p']' `=_se[`t'_`p']' `N' .
	}
	wr A `y' `p' cur `=_b[`p']' `=_se[`p']' `N' .
	quietly test F2_`p' F1_`p'
	wr A `y' `p' p_leads `=r(p)' . `N' .
	di "A `y' on `p': two ahead " %7.3f _b[F2_`p'] " (" %6.3f _se[F2_`p'] ") next " %7.3f _b[F1_`p'] " (" %6.3f _se[F1_`p'] ") this " %7.3f _b[`p'] " (" %6.3f _se[`p'] ") last " %7.3f _b[L1_`p'] " (" %6.3f _se[L1_`p'] ") two ago " %7.3f _b[L2_`p'] " (" %6.3f _se[L2_`p'] ") joint p leads " %5.3f r(p) " N " `N'
}

// ---------------------------------------------------------------- C. shift-share dropping the modules that move the instrument most
preserve
merge m:1 county year using "$idir/what_moves/local_price_top_modules.dta", keep(master match) nogenerate
foreach pr in "hei_usda hi" "upf_share upf" "sugar_per_1000cal_usda hi" {
	tokenize `pr'
	local y `1'
	local p `2'
	foreach k in 1 3 5 10 {
		capture ivreghdfe `y' (rel_`p'_ces = z_`p'_ex`k') [pw = projection_factor], absorb(`FE') cluster(county)
		if _rc == 0 {
			wr C `y' rel_`p'_ces drop_top`k' `=_b[rel_`p'_ces]' `=_se[rel_`p'_ces]' `=e(N)' `=e(widstat)'
			di "C `y' IV dropping the top `k' modules from the instrument: " %7.3f _b[rel_`p'_ces] " (" %6.3f _se[rel_`p'_ces] ") F " %6.1f e(widstat)
		}
	}
	ivreghdfe `y' (rel_`p'_ces = z_rel_`p') [pw = projection_factor], absorb(`FE') cluster(county)
	wr C `y' rel_`p'_ces drop_top0 `=_b[rel_`p'_ces]' `=_se[rel_`p'_ces]' `=e(N)' `=e(widstat)'
}
restore

// ---------------------------------------------------------------- B. county-year: which moves first
foreach y in hei_usda upf_share {
	gen double dy_`y' = `y' - L.`y'
}
gen byte one = 1
collapse (mean) dy_hei_usda dy_upf_share (rawsum) n = one [pw = projection_factor], by(county year)
keep if n >= 20
merge 1:1 county year using `idx', keep(match) nogenerate
xtset county year
gen Fdy_hei_usda = F.dy_hei_usda
gen Fdy_upf_share = F.dy_upf_share
foreach pr in "hei_usda rel_hi_ces" "upf_share rel_upf_ces" {
	tokenize `pr'
	local y `1'
	local p `2'
	quietly reghdfe Fd_`p' dy_`y' [aw = n], absorb(county year) cluster(county)
	wr B `y' `p' price_next_on_diet_now `=_b[dy_`y']' `=_se[dy_`y']' `=e(N)' .
	di "B next year's change in `p' on this year's change in `y': " %8.5f _b[dy_`y'] " (" %7.5f _se[dy_`y'] ")  N " e(N)
	quietly reghdfe d_`p' dy_`y' [aw = n], absorb(county year) cluster(county)
	wr B `y' `p' price_now_on_diet_now `=_b[dy_`y']' `=_se[dy_`y']' `=e(N)' .
	di "B this year's change in `p' on this year's change in `y': " %8.5f _b[dy_`y'] " (" %7.5f _se[dy_`y'] ")"
	quietly reghdfe Fdy_`y' d_`p' [aw = n], absorb(county year) cluster(county)
	wr B `y' `p' diet_next_on_price_now `=_b[d_`p']' `=_se[d_`p']' `=e(N)' .
	di "B next year's change in `y' on this year's change in `p': " %8.4f _b[d_`p'] " (" %7.4f _se[d_`p'] ")"
}
di "build_local_price_timing.do complete"
