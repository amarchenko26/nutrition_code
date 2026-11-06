// ============================================================================
// build_time_events.do -- do households change what they buy when the heads'
// time changes? Event studies of head employment changes (Anya 2026-11-05,
// "what moves nutrition" review). Panel: build_time_events_panel.py (one-year
// reversals of the employment state smoothed, first change only, window cut
// before the next change).
//
// Movers of five kinds against households whose heads' employment never changes:
//   1 retirement  2 job loss  3 re-employment  4 hours cut  5 hours increase
// One regression per outcome with all five sets of relative-year dummies:
//   y_it = a_i + tau_t + sum_e sum_{r != -1} beta_{e,r} 1[evt_i = e, r(i,t) = r] + x_it'b + e_it
// r binned at -5 and +5; household and year FE, 5-year age bins, kids and
// composition FE; projection weights; SE clustered on household. beta_{e,r} is
// the change in the outcome r years from the event for movers of kind e,
// relative to the year before it, net of what happened to the never-changers.
//
// Specs (the spec column of the outputs):
//   main      as above, all outcomes
//   balanced  movers observed in every year r = -2..2 only; all never-changers
//   samecell  year FE replaced by year x baseline employment state x 10-year age
//             bin, plus years-in-panel FE: movers against never-changers who
//             started in the same employment state and age group
//   male, female  main with the event dummies split by which head changes
//
// `do build_time_events.do price` (2026-11-06) runs main and samecell for the price paid per 1,000 kcal, which are not in the
// outcome list of the other groups; `do build_time_events.do pricebal` runs balanced for price_1000 (the figure's dashed line).
// Run one spec group per Stata process: do build_time_events.do main (or balanced, samecell, byhead);
// outputs interim/what_moves/time_events_coefs_<group>.csv and time_events_summary_<group>.csv, which
// figure_time_events.py combines into time_events_coefs.csv and time_events_summary.csv.
// ============================================================================

clear all
set more off
local group "`1'"
if "`group'" == "" local group main
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
global group "`group'"

use "$idir/what_moves/time_events_panel.dta", clear
gen rb = max(-5, min(5, r)) if evt > 0
egen year_cell = group(year basecell)

global elist 1 2 3 4 5
local elab_1 "retirement"
local elab_2 "job loss"
local elab_3 "re-employment"
local elab_4 "hours cut"
local elab_5 "hours increase"
forvalues e = 1/5 {
	global elab_`e' "`elab_`e''"
}

local dlist
forvalues e = 1/5 {
	forvalues k = -5/5 {
		if `k' == -1 continue
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		quietly gen byte d`e'_`s' = (evt == `e' & rb == `k')
		local dlist `dlist' d`e'_`s'
	}
}
global dlist `dlist'
local whodlist
foreach w in 1 2 {
	forvalues e = 1/5 {
		forvalues k = -5/5 {
			if `k' == -1 continue
			local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
			quietly gen byte w`w'd`e'_`s' = (evt == `e' & who == `w' & rb == `k')
			local whodlist `whodlist' w`w'd`e'_`s'
		}
	}
}
global whodlist `whodlist'
bys hhid: gen byte _first = _n == 1

cap file close fh
file open fh using "$idir/what_moves/time_events_coefs_${group}.csv", write replace
file write fh "spec,outcome,event,r,b,se,N" _n
file close fh
file open fh using "$idir/what_moves/time_events_summary_${group}.csv", write replace
file write fh "spec,outcome,event,n_movers,n_controls,jump,jump_se,jump12,jump12_se,step01,step01_se,lead2,lead2_se,pre,pre_se,p_pretrend,mean_pre,N" _n
file close fh

cap program drop write_event
program define write_event
	args spec y e prefix cond N
	quietly count if _first & evt == `e' & `cond'
	local nm = r(N)
	quietly count if _first & evt == 0
	local nc = r(N)
	file open fh using "$idir/what_moves/time_events_coefs_${group}.csv", write append
	forvalues k = -5/5 {
		if `k' == -1 {
			file write fh "`spec',`y',${elab_`e'},-1,0,0,`N'" _n
			continue
		}
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		file write fh "`spec',`y',${elab_`e'},`k',`=_b[`prefix'`e'_`s']',`=_se[`prefix'`e'_`s']',`N'" _n
	}
	file close fh
	quietly lincom (`prefix'`e'_p1 + `prefix'`e'_p2 + `prefix'`e'_p3) / 3
	local jump = r(estimate)
	local jump_se = r(se)
	quietly lincom (`prefix'`e'_p1 + `prefix'`e'_p2) / 2
	local j12 = r(estimate)
	local j12_se = r(se)
	quietly lincom (`prefix'`e'_p0 + `prefix'`e'_p1) / 2
	local st = r(estimate)
	local st_se = r(se)
	local l2 = _b[`prefix'`e'_m2]
	local l2_se = _se[`prefix'`e'_m2]
	quietly lincom (`prefix'`e'_m4 + `prefix'`e'_m3 + `prefix'`e'_m2) / 3
	local pre = r(estimate)
	local pre_se = r(se)
	quietly test `prefix'`e'_m4 `prefix'`e'_m3 `prefix'`e'_m2
	local pp = r(p)
	quietly summarize `y' [aw = projection_factor] if evt == `e' & r == -1 & `cond'
	local mpre = r(mean)
	file open fh using "$idir/what_moves/time_events_summary_${group}.csv", write append
	file write fh "`spec',`y',${elab_`e'},`nm',`nc',`jump',`jump_se',`j12',`j12_se',`st',`st_se',`l2',`l2_se',`pre',`pre_se',`pp',`mpre',`N'" _n
	file close fh
	di "`spec' / `y' / ${elab_`e'}: post(1..3) " %8.3f `jump' " (" %7.3f `jump_se' ")  post(1,2) " %8.3f `j12' " (" %7.3f `j12_se' ")  r=-2 " %8.3f `l2' " (" %7.3f `l2_se' ")  pre p " %5.3f `pp' "  mean " %8.2f `mpre' "  movers `nm'"
end

local outcomes hei_usda prep_time_raw upf_share cals_ae_day produce_share sugar_1000 n_trips spend_ae real_income nonfood_ae food_share_trip

// ------------------------------------------------------------ price paid per calorie
if "`group'" == "price" {
	foreach y in price_1000 price_upf_1000 price_other_1000 {
		di _n "==== price `y'"
		quietly reghdfe `y' $dlist [pw = projection_factor], absorb(hhid year age5 kids hh_comp) cluster(hhid)
		local N = e(N)
		forvalues e = 1/5 {
			write_event main `y' `e' d 1 `N'
		}
		quietly reghdfe `y' $dlist [pw = projection_factor], absorb(hhid year_cell tenure age5 kids hh_comp) cluster(hhid)
		local N = e(N)
		forvalues e = 1/5 {
			write_event samecell `y' `e' d 1 `N'
		}
	}
}

if "`group'" == "pricebal" {
	quietly reghdfe price_1000 $dlist [pw = projection_factor] if evt == 0 | balanced == 1, absorb(hhid year age5 kids hh_comp) cluster(hhid)
	local N = e(N)
	forvalues e = 1/5 {
		write_event balanced price_1000 `e' d "balanced == 1" `N'
	}
}

// ------------------------------------------------------------ main
if "`group'" == "main" {
	foreach y of local outcomes {
		di _n "==== main `y'"
		quietly reghdfe `y' $dlist [pw = projection_factor], absorb(hhid year age5 kids hh_comp) cluster(hhid)
		local N = e(N)
		forvalues e = 1/5 {
			write_event main `y' `e' d 1 `N'
		}
	}
}

// ------------------------------------------------------------ balanced window
if "`group'" == "balanced" {
	foreach y of local outcomes {
		di _n "==== balanced `y'"
		quietly reghdfe `y' $dlist [pw = projection_factor] if evt == 0 | balanced == 1, absorb(hhid year age5 kids hh_comp) cluster(hhid)
		local N = e(N)
		forvalues e = 1/5 {
			write_event balanced `y' `e' d "balanced == 1" `N'
		}
	}
}

// ------------------------------------------------------------ same starting state and age, years in panel
if "`group'" == "samecell" {
	foreach y of local outcomes {
		di _n "==== samecell `y'"
		quietly reghdfe `y' $dlist [pw = projection_factor], absorb(hhid year_cell tenure age5 kids hh_comp) cluster(hhid)
		local N = e(N)
		forvalues e = 1/5 {
			write_event samecell `y' `e' d 1 `N'
		}
	}
}

// ------------------------------------------------------------ by which head changes
if "`group'" == "byhead" {
	foreach y in hei_usda prep_time_raw upf_share cals_ae_day n_trips spend_ae real_income nonfood_ae food_share_trip {
		di _n "==== by head `y'"
		quietly reghdfe `y' $whodlist [pw = projection_factor], absorb(hhid year age5 kids hh_comp) cluster(hhid)
		local N = e(N)
		forvalues e = 1/5 {
			write_event male `y' `e' w1d "who == 1" `N'
			write_event female `y' `e' w2d "who == 2" `N'
		}
	}
}
di "build_time_events.do `group' complete"
