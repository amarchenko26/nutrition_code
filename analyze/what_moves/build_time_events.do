// ============================================================================
// build_time_events.do -- do households change what they buy when the heads'
// time changes? Event studies of head employment changes (Anya 2026-11-05,
// "what moves nutrition" review). Panel: build_time_events_panel.py.
//
// Movers of five kinds (first change of the heads' hours bins, window cut before
// the next change) against households whose heads' employment never changes:
//   1 retirement  2 job loss  3 re-employment  4 hours cut  5 hours increase
// One regression per outcome with all five sets of relative-year dummies:
//   y_it = a_i + tau_t + sum_e sum_{r != -1} beta_{e,r} 1[evt_i = e, r(i,t) = r] + x_it'b + e_it
// r binned at -5 and +5; household and year FE, 5-year age bins, kids and
// composition FE; projection weights; SE clustered on household. beta_{e,r} is
// the change in the outcome r years from the event for movers of kind e,
// relative to the year before it, net of what happened to the never-changers.
//
// Outputs (interim/what_moves/): time_events_coefs.csv, time_events_summary.csv
// ============================================================================

clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use "$idir/what_moves/time_events_panel.dta", clear
gen rb = max(-5, min(5, r)) if evt > 0

local dlist
forvalues e = 1/5 {
	forvalues k = -5/5 {
		if `k' == -1 continue
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		gen byte d`e'_`s' = (evt == `e' & rb == `k')
		local dlist `dlist' d`e'_`s'
	}
}
bys hhid: gen byte _first = _n == 1
forvalues e = 0/5 {
	quietly count if _first & evt == `e'
	local n_`e' = r(N)
}
di "households: controls `n_0', retirement `n_1', job loss `n_2', re-employment `n_3', hours cut `n_4', hours increase `n_5'"

cap file close fh
file open fh using "$idir/what_moves/time_events_coefs.csv", write replace
file write fh "outcome,event,r,b,se,N" _n
file close fh
file open fh using "$idir/what_moves/time_events_summary.csv", write replace
file write fh "outcome,event,n_movers,n_controls,jump,jump_se,pre,pre_se,p_pretrend,mean_pre,N" _n
file close fh

local elab_1 "retirement"
local elab_2 "job loss"
local elab_3 "re-employment"
local elab_4 "hours cut"
local elab_5 "hours increase"

foreach y in hei_usda prep_time_raw upf_share cals_ae_day produce_share sugar_1000 n_trips spend_ae real_income {
	di _n "==== `y'"
	reghdfe `y' `dlist' [pw = projection_factor], absorb(hhid year age5 kids hh_comp) cluster(hhid)
	local N = e(N)
	forvalues e = 1/5 {
		file open fh using "$idir/what_moves/time_events_coefs.csv", write append
		forvalues k = -5/5 {
			if `k' == -1 {
				file write fh "`y',`elab_`e'',-1,0,0,`N'" _n
				continue
			}
			local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
			file write fh "`y',`elab_`e'',`k',`=_b[d`e'_`s']',`=_se[d`e'_`s']',`N'" _n
		}
		file close fh
		quietly lincom (d`e'_p1 + d`e'_p2 + d`e'_p3) / 3
		local jump = r(estimate)
		local jump_se = r(se)
		quietly lincom (d`e'_m4 + d`e'_m3 + d`e'_m2) / 3
		local pre = r(estimate)
		local pre_se = r(se)
		quietly test d`e'_m4 d`e'_m3 d`e'_m2
		local pp = r(p)
		quietly summarize `y' [aw = projection_factor] if evt == `e' & r == -1
		local mpre = r(mean)
		file open fh using "$idir/what_moves/time_events_summary.csv", write append
		file write fh "`y',`elab_`e'',`n_`e'',`n_0',`jump',`jump_se',`pre',`pre_se',`pp',`mpre',`N'" _n
		file close fh
		di "`y' / `elab_`e'': post (1..3) = " %8.3f `jump' " (" %7.3f `jump_se' ")  pre (-4..-2) = " %8.3f `pre' " (" %7.3f `pre_se' ") p = " %5.3f `pp' "  mean at r=-1 = " %8.2f `mpre'
	}
}
di "build_time_events.do complete"
