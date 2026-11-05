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
// Since 2026-11-05 also, for the Movers section of notes/labor_supply.tex: ALL
// MOVES pooled (E->E plus into/out of not employed) and EMPLOYED ORIGIN (origin
// != 12, the movers analogue of the IV's employed-at-t-1 clean sample) for the
// three objects.
//
// Outputs (interim/movers/): event_study_coefs.csv, event_study_summary.csv
// ============================================================================

clear all
set more off

// `do 4_build_movers_event_study.do retailer` runs the same outcomes on main-retailer switches (1_ --places retailer).
// `do 4_build_movers_event_study.do county` runs the COUNTY-MOVERS version (1_ --places county,
// place effects: outputs county_event_study_coefs.csv / _summary.csv).
// `do 4_build_movers_event_study.do income` runs the INCOME-MOVERS version (1_ --places income,
// within-year income quintiles as the places, Anya 2026-11-05): same programs, the income
// events panel, outputs income_event_study_coefs.csv / income_event_study_summary.csv.
local places "`1'"
if "`places'" == "" local places occupation

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global out      "$data/interim/movers"
global prefix = cond("`places'" == "income", "income_", cond("`places'" == "county", "county_", cond("`places'" == "retailer", "retailer_", "")))
global coefs "$out/${prefix}event_study_coefs.csv"
global summ  "$out/${prefix}event_study_summary.csv"

use "$out/${prefix}movers_events_panel.dta", clear

gen rb = max(-7, min(7, r))
global rlist
forvalues k = -7/7 {
	if `k' == -1 continue
	local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
	gen byte d_`s' = (rb == `k')
	global rlist $rlist `s'
}

cap file close fh
file open fh using "$coefs", write replace
file write fh "spec,outcome,r,b,se,N,n_events,n_households" _n
file close fh
file open fh using "$summ", write replace
file write fh "spec,outcome,jump,jump_se,jump_late,jump_late_se,pre,pre_se,p_pretrend,N,n_events,n_households" _n
file close fh

// es_run "spec" "outcome" "sample condition" ["delta variable"] ["controls"]
// (delta defaults to delta_<outcome>; pass delta_real_income to scale by the occupation INCOME gap;
//  controls default to age5 kids hh_comp, pass "none" for event and year FE only -- Anya 2026-11-05)
cap program drop es_run
program define es_run
	args spec outcome cond dvar fe
	if "`dvar'" == "" local dvar delta_`outcome'
	if "`fe'" == "" local fe age5 kids hh_comp
	if "`fe'" == "none" local fe
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
		absorb(event_id year `fe') cluster(hhid)
	local N = e(N)
	quietly egen byte _te = tag(event_id) if e(sample)
	quietly count if _te == 1
	local nev = r(N)
	quietly egen byte _th = tag(hhid) if e(sample)
	quietly count if _th == 1
	local nhh = r(N)
	drop _te _th
	file open fh using "$coefs", write append
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
	file open fh using "$summ", write append
	file write fh "`spec',`outcome',`jump',`jump_se',`jl',`jl_se',`pre',`pre_se',`pp',`N',`nev',`nhh'" _n
	file close fh
	di "`spec'/`outcome': jump (1..3) = `jump' (`jump_se'); late (4..6) = `jl' (`jl_se'); pre (-4..-2) = `pre' (`pre_se'), p = `pp'; events `nev', households `nhh'"
end

// ---------------------------------------------------------------- county movers (place effects, 2026-11-05)
// `do 4_... county`: every county change (1_ --places county), theta_r = share of the
// origin-destination COUNTY gap in the outcome that the mover's own outcome closes.
// Timing differs from the occupation run: r = 0 is file year m_file (see 1_), the year the
// household's trips move to the new location.
if inlist("`places'", "county", "retailer") {
	foreach o in hei_usda hi_usda upf_share produce_share sugar_1000 soda_spend_share {
		es_run "all" "`o'" "1"
	}
	es_run "income_all" "real_income" "1"
	di "4_build_movers_event_study.do (`places' movers) complete"
	exit
}

// ---------------------------------------------------------------- income movers
if "`places'" == "income" {
	es_run "all"                 "hei_usda"    "1"
	es_run "income_all"          "real_income" "1"
	es_run "hei_per_inc_all"     "hei_usda"    "1" "delta_real_income"
	es_run "all_nox"             "hei_usda"    "1" "delta_hei_usda"    "none"
	es_run "income_all_nox"      "real_income" "1" "delta_real_income" "none"
	es_run "hei_per_inc_all_nox" "hei_usda"    "1" "delta_real_income" "none"
	di "4_build_movers_event_study.do (income movers) complete"
	exit
}

// ---------------------------------------------------------------- occupation movers
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

// ALL MOVES pooled (between employed occupations, and into or out of not employed), and the
// movers analogue of the IV's clean sample, EMPLOYED ORIGIN: origin an employed occupation,
// destination another employed occupation or not employed (moves out of not employed
// excluded) -- labor_supply.tex "Movers" section (Anya 2026-11-05)
es_run "all"                 "hei_usda"    "1"
es_run "emporig"             "hei_usda"    "origin != 12"
es_run "income_all"          "real_income" "1"
es_run "income_emporig"      "real_income" "origin != 12"
es_run "hei_per_inc_all"     "hei_usda"    "1"            "delta_real_income"
es_run "hei_per_inc_emporig" "hei_usda"    "origin != 12" "delta_real_income"
// the same six WITHOUT the age / kids / composition controls (event and year FE only)
es_run "all_nox"                 "hei_usda"    "1"            "delta_hei_usda"   "none"
es_run "emporig_nox"             "hei_usda"    "origin != 12" "delta_hei_usda"   "none"
es_run "income_all_nox"          "real_income" "1"            "delta_real_income" "none"
es_run "income_emporig_nox"      "real_income" "origin != 12" "delta_real_income" "none"
es_run "hei_per_inc_all_nox"     "hei_usda"    "1"            "delta_real_income" "none"
es_run "hei_per_inc_emporig_nox" "hei_usda"    "origin != 12" "delta_real_income" "none"
// NO plain event study (rho_r alone, by direction of the move) on this panel: every
// household is a mover, so a linear trend in r is collinear with the year effects and
// the event FE and is pinned only by the binned tails -- tried 2026-11-05 for up-moves
// from an employed occupation: post(1..3) = 0.36 / 0.54 / 0.42 HEI points with r binned
// at +/-4 / +/-7 / +/-10 and -0.22 without year FE. A level change by direction needs
// non-mover controls (3_'s matched non-movers, or 8_'s never-changer design).

di "4_build_movers_event_study.do complete"
