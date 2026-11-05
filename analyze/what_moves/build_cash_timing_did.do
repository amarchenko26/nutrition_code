// ============================================================================
// build_cash_timing_did.do -- does the healthiness of grocery purchases move
// when cash arrives? Two within-year income-timing designs with a comparison
// group (the May 2026 monthly-cycle figures had none):
//   EITC: households eligible for the EITC (low income with children, flag from
//         build/monthly_cycle/build_monthly_cycle_panel.py) vs ineligible
//         households, shopping days in the refund window vs other shopping days
//         of January-March. Since the 2017 PATH Act the IRS holds EITC refunds
//         until Feb 15 and most arrive Feb 22 - Mar 1, so the window is
//         Feb 22 - Mar 7 for 2017-2020; for 2004-2016 refunds arrived within two
//         weeks of filing, mostly late January to mid February, window Jan 29 - Feb 18.
//   SNAP: households with income per capita <= $13k (the panel's SNAP-eligible
//         proxy) vs others, by week of the month (benefits arrive days 1-10 in
//         most states), week 1 = days 1-7 omitted.
// Outcome = shares of a shopping day's grocery spending in healthy modules
// (claude_hi >= threshold), produce modules and unhealthy modules, and the day's
// spending; household and calendar-date fixed effects; projection weights; SE
// clustered on household. Data: interim/panel_dataset/monthly_cycle_hh_day.parquet
// (household x shopping day, 2004-2020).
// Outputs (interim/what_moves/): cash_timing_coefs.csv
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
cap mkdir "$idir/what_moves"

pq use using "$idir/panel_dataset/monthly_cycle_hh_day.parquet", clear
describe purchase_date month day_of_month year eitc_eligible snap_eligible
keep if inlist(month, 1, 2, 3) | snap_eligible < .
gen double hs = healthy_share * 100
gen double ps = produce_share * 100
gen double us = unhealthy_share * 100
gen double spend = spend_total
// calendar date as a Stata date
// purchase_date is a %tc datetime (milliseconds); dofc() turns it into a day
gen double dt = dofc(purchase_date)
format dt %td
gen int doy = doy(dt)

cap file close fh
file open fh using "$idir/what_moves/cash_timing_coefs.csv", write replace text
file write fh "design,years,outcome,term,b,se,N,n_treated_hh" _n
file close fh

// ---------------------------------------------------------------- EITC
foreach era in late early {
	if "`era'" == "late" {
		local yrs "inrange(year, 2017, 2020)"
		local win "(month == 2 & day_of_month >= 22) | (month == 3 & day_of_month <= 7)"
		local ylab "2017-2020"
	}
	else {
		local yrs "inrange(year, 2004, 2016)"
		local win "(month == 1 & day_of_month >= 29) | (month == 2 & day_of_month <= 18)"
		local ylab "2004-2016"
	}
	preserve
	keep if inlist(month, 1, 2, 3) & `yrs' & !missing(eitc_eligible)
	gen byte window = `win'
	gen byte tw = eitc_eligible * window
	quietly egen byte _t = tag(household_code) if eitc_eligible == 1
	quietly count if _t == 1
	local nt = r(N)
	foreach y in hs ps us spend {
		reghdfe `y' tw window [pw = projection_factor], absorb(household_code dt) cluster(household_code)
		file open fh using "$idir/what_moves/cash_timing_coefs.csv", write append text
		file write fh "eitc,`ylab',`y',eligible x window,`=_b[tw]',`=_se[tw]',`=e(N)',`nt'" _n
		file close fh
		di "EITC `ylab' `y': eligible x window = " %7.3f _b[tw] " (" %6.3f _se[tw] ")  N = " e(N) "  eligible households " `nt'
	}
	// daily path for the figure: eligible x day-of-year dummies, Jan 10-31 as the base (late era only)
	if "`era'" == "late" {
		gen int d14 = max(31 + 1, min(31 + 28 + 20, doy))     // Feb 1 .. Mar 20 (bins at the ends)
		quietly summarize d14
		local lo = r(min)
		local hi = r(max)
		forvalues k = `lo'/`hi' {
			quietly gen byte e_`k' = eitc_eligible * (d14 == `k')
		}
		drop e_`=31 + 28 + 1'   // Mar 1 omitted: the day the refunds have arrived (base)
		reghdfe hs e_* [pw = projection_factor], absorb(household_code dt) cluster(household_code)
		file open fh using "$idir/what_moves/cash_timing_coefs.csv", write append text
		forvalues k = `lo'/`hi' {
			if `k' == `=31 + 28 + 1' {
				file write fh "eitc_daily,2017-2020,hs,`k',0,0,`=e(N)',`nt'" _n
				continue
			}
			file write fh "eitc_daily,2017-2020,hs,`k',`=_b[e_`k']',`=_se[e_`k']',`=e(N)',`nt'" _n
		}
		file close fh
	}
	restore
}

// ---------------------------------------------------------------- SNAP week of month
preserve
keep if !missing(snap_eligible) & !missing(week_of_month)
quietly egen byte _t = tag(household_code) if snap_eligible == 1
quietly count if _t == 1
local nt = r(N)
forvalues w = 2/4 {
	gen byte sw`w' = snap_eligible * (week_of_month == `w')
}
foreach y in hs ps us spend {
	reghdfe `y' sw2 sw3 sw4 [pw = projection_factor], absorb(household_code dt) cluster(household_code)
	file open fh using "$idir/what_moves/cash_timing_coefs.csv", write append text
	forvalues w = 2/4 {
		file write fh "snap,2004-2020,`y',eligible x week `w',`=_b[sw`w']',`=_se[sw`w']',`=e(N)',`nt'" _n
	}
	file close fh
	di "SNAP `y': week2 " %7.3f _b[sw2] " (" %6.3f _se[sw2] ")  week3 " %7.3f _b[sw3] " (" %6.3f _se[sw3] ")  week4 " %7.3f _b[sw4] " (" %6.3f _se[sw4] ")  N = " e(N)
}
restore
di "build_cash_timing_did.do complete"
