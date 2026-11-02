// ============================================================================
// 4_build_movers_event_study.do -- Finkelstein-Gentzkow-Williams (2016)
// event study with OCCUPATIONS as the "places" and nutrition as the outcome,
// every dated move between employed occupations as an event
// (1_build_occupation_movers_panel.py -> interim/movers/movers_events_panel).
//
//   y_et = a_e + tau_t + sum_r theta_r delta_e 1[r(e,t)=r]
//        + sum_r rho_r 1[r(e,t)=r] + x_it'b + e_et
//
// a_e event fixed effects (a household appears once per move; with one move
// this is FGW's household fixed effect); delta_e = ybar_dest - ybar_orig;
// r = 0 the calendar year of the move (partially treated), r = -1 omitted,
// r binned at -7 and +7; x = 5-year age bins, kids and composition FE;
// projection-weighted; SE clustered on household. theta_r for r >= 1 is the
// share of the origin-destination gap that the mover's own nutrition closes.
//
// Runs: main (all employed-to-employed moves) for HEI, the simple USDA index
// and the simple Syndigo index; HEI on moves not later reversed, on first
// moves only and on single-change households (the old design); HEI on moves
// into or out of not employed; household income scaled by the occupation
// income gap (the wage validation); HEI per $10k of the occupation income gap.
//
// Outputs (interim/movers/): event_study_coefs.csv, event_study_summary.csv
// ============================================================================

clear all
set more off

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global out      "$data/interim/movers"

use "$out/movers_events_panel.dta", clear

gen rb = max(-7, min(7, r))
global rlist
forvalues k = -7/7 {
	if `k' == -1 continue
	local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
	gen byte d_`s' = (rb == `k')
	global rlist $rlist `s'
}

cap file close fh
file open fh using "$out/event_study_coefs.csv", write replace
file write fh "spec,outcome,r,b,se,N,n_events,n_households" _n
file close fh
file open fh using "$out/event_study_summary.csv", write replace
file write fh "spec,outcome,jump,jump_se,jump_late,jump_late_se,pre,pre_se,p_pretrend,N,n_events,n_households" _n
file close fh

// es_run "spec" "outcome" "sample condition" ["delta variable"]
// (delta defaults to delta_<outcome>; pass delta_real_income to scale by the occupation INCOME gap)
cap program drop es_run
program define es_run
	args spec outcome cond dvar
	if "`dvar'" == "" local dvar delta_`outcome'
	local ddlist
	local dlist
	foreach s of global rlist {
		cap drop dd_`s'
		quietly gen dd_`s' = d_`s' * `dvar'
		local ddlist `ddlist' dd_`s'
		local dlist  `dlist'  d_`s'
	}
	di _n "==== `spec' / `outcome' : `cond'"
	reghdfe `outcome' `ddlist' `dlist' [pw = projection_factor] if `cond', ///
		absorb(event_id year age5 kids hh_comp) cluster(hhid)
	local N = e(N)
	quietly egen byte _te = tag(event_id) if e(sample)
	quietly count if _te == 1
	local nev = r(N)
	quietly egen byte _th = tag(hhid) if e(sample)
	quietly count if _th == 1
	local nhh = r(N)
	drop _te _th
	file open fh using "$out/event_study_coefs.csv", write append
	forvalues k = -7/7 {
		if `k' == -1 {
			file write fh "`spec',`outcome',-1,0,0,`N',`nev',`nhh'" _n
			continue
		}
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		file write fh "`spec',`outcome',`k',`=_b[dd_`s']',`=_se[dd_`s']',`N',`nev',`nhh'" _n
	}
	file close fh
	quietly lincom (dd_p1 + dd_p2 + dd_p3) / 3
	local jump = r(estimate)
	local jump_se = r(se)
	quietly lincom (dd_p4 + dd_p5 + dd_p6) / 3
	local jl = r(estimate)
	local jl_se = r(se)
	quietly lincom (dd_m4 + dd_m3 + dd_m2) / 3
	local pre = r(estimate)
	local pre_se = r(se)
	quietly test dd_m4 dd_m3 dd_m2
	local pp = r(p)
	file open fh using "$out/event_study_summary.csv", write append
	file write fh "`spec',`outcome',`jump',`jump_se',`jl',`jl_se',`pre',`pre_se',`pp',`N',`nev',`nhh'" _n
	file close fh
	di "`spec'/`outcome': jump (1..3) = `jump' (`jump_se'); late (4..6) = `jl' (`jl_se'); pre (-4..-2) = `pre' (`pre_se'), p = `pp'; events `nev', households `nhh'"
end

es_run "main"      "hei_usda" "emp2emp == 1"
es_run "main"      "hi_usda"  "emp2emp == 1"
es_run "main"      "hi"       "emp2emp == 1"
// the other three samples, all three outcomes (figure event_study_outcomes, 4 x 3)
foreach o in hei_usda hi_usda hi {
	es_run "noreversal" "`o'" "emp2emp == 1 & reversed == 0"
	es_run "firstmove"  "`o'" "emp2emp == 1 & first_event == 1"
	es_run "single"     "`o'" "emp2emp == 1 & single == 1"
}
es_run "notemp"    "hei_usda" "notemp == 1"
// income: theta_r = share of the cross-sectional occupation INCOME gap realized (household income, $10k 2013$)
es_run "income"        "real_income" "emp2emp == 1"
es_run "income_notemp" "real_income" "notemp == 1"
es_run "income_single" "real_income" "emp2emp == 1 & single == 1"
// HEI scaled by the occupation INCOME gap: theta_r = HEI points per $10k of cross-sectional income gap
es_run "hei_per_inc" "hei_usda"    "emp2emp == 1" "delta_real_income"

di "4_build_movers_event_study.do complete"
