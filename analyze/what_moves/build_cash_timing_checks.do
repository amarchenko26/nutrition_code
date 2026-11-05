// ============================================================================
// build_cash_timing_checks.do -- is the EITC-window result in build_cash_timing_did.do about cash or
// about the calendar? (2026-11-05). The 2004-2016 refund window, Jan 29 - Feb 18, holds the Super Bowl and
// Valentine's Day, and refund-eligible households all have children. Two checks of the unhealthy and healthy
// shares and spending:
//   same comparison group: refund-eligible households against INELIGIBLE HOUSEHOLDS WITH CHILDREN;
//   placebo: the OLD window in 2017-2020, when the PATH Act holds refunds until after Feb 15 and most
//   arrive after Feb 22, so cash no longer arrives in that window. A result there is the calendar.
// Output: interim/what_moves/cash_timing_checks.csv
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
pq use household_code purchase_date month day_of_month year eitc_eligible has_children projection_factor healthy_share unhealthy_share produce_share spend_total using "$idir/panel_dataset/monthly_cycle_hh_day.parquet", clear
keep if inlist(month, 1, 2, 3) & !missing(eitc_eligible)
gen double hs = healthy_share * 100
gen double us = unhealthy_share * 100
gen double ps = produce_share * 100
gen double spend = spend_total
gen double dt = dofc(purchase_date)
compress
gen byte oldwin = (month == 1 & day_of_month >= 29) | (month == 2 & day_of_month <= 18)
gen byte newwin = (month == 2 & day_of_month >= 22) | (month == 3 & day_of_month <= 7)
cap file close fh
file open fh using "$idir/what_moves/cash_timing_checks.csv", write replace
file write fh "check,years,window,control,outcome,b,se,N" _n
file close fh
cap program drop one
program define one
	args check years win ctl cond
	gen byte tw = eitc_eligible * `win'
	foreach y in hs us ps spend {
		quietly reghdfe `y' tw `win' [pw = projection_factor] if `cond', absorb(household_code dt) cluster(household_code)
		file open fh using "$idir/what_moves/cash_timing_checks.csv", write append
		file write fh "`check',`years',`win',`ctl',`y',`=_b[tw]',`=_se[tw]',`=e(N)'" _n
		file close fh
		di "`check' `years' `win' `ctl' `y': " %7.3f _b[tw] " (" %6.3f _se[tw] ")"
	}
	drop tw
end
one "kids_control" "2004-2016" oldwin "with_children" "inrange(year, 2004, 2016) & has_children == 1"
one "placebo_oldwindow" "2017-2020" oldwin "all" "inrange(year, 2017, 2020)"
one "placebo_oldwindow" "2017-2020" oldwin "with_children" "inrange(year, 2017, 2020) & has_children == 1"
one "kids_control" "2017-2020" newwin "with_children" "inrange(year, 2017, 2020) & has_children == 1"
di "build_cash_timing_checks.do complete"
