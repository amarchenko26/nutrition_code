// ============================================================================
// build_diagnosis_events.do -- does a household change what it buys after a
// member is diagnosed with a diet-related condition? (what moves nutrition,
// 2026-11-05). Reads interim/what_moves/diagnosis_panel.dta
// (build_diagnosis_panel.py).
//
// For each condition (type-2 diabetes, hypertension, high cholesterol, heart
// disease, obesity) the event is the first survey year the household reports
// it after two or more earlier survey years without it; controls are
// households that answered the survey in two or more years and never report
// it. Event households and controls, 2008-2020:
//   y_it = a_i + tau_t + sum_{r != -1} beta_r 1[r(i,t) = r] + x_it'b + e_it
// r = year - first-report year, binned at -3 and +3; household and year FE,
// 5-year age bins, kids and composition FE; projection-weighted; SE clustered
// on household. Post = mean of beta_1..beta_3, pre = mean of beta_-3, beta_-2.
// Regimes: all events (first report 2013-2019); early = first report
// 2013-2015 (one questionnaire per member); late = 2017-2019 (one household
// respondent, list of conditions for anyone in the household); 2016, the
// redesign year, is in "all" only.
//
// Outputs (interim/what_moves/): diagnosis_events_coefs.csv,
// diagnosis_events_summary.csv
// ============================================================================

clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use "$idir/what_moves/diagnosis_panel.dta", clear
keep if year >= 2008

cap file close fh
file open fh using "$idir/what_moves/diagnosis_events_coefs.csv", write replace
file write fh "condition,regime,outcome,r,b,se,N" _n
file close fh
file open fh using "$idir/what_moves/diagnosis_events_summary.csv", write replace
file write fh "condition,regime,outcome,n_events,n_controls,post,post_se,pre,pre_se,p_pretrend,N" _n
file close fh

cap program drop es_cond
program define es_cond
	args cond regime outcome
	// sample: controls (grp = 0) and events of the regime (grp = 1, first-report year window)
	tempvar insamp ev
	gen byte `ev' = grp_`cond' == 1
	if "`regime'" == "early" replace `ev' = `ev' & inrange(m_`cond', 2013, 2015)
	if "`regime'" == "late"  replace `ev' = `ev' & inrange(m_`cond', 2017, 2019)
	gen byte `insamp' = (grp_`cond' == 0) | `ev'
	cap drop rb d_*
	gen rb = max(-3, min(3, r_`cond')) if `ev'
	local dlist
	foreach k in -3 -2 0 1 2 3 {
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		gen byte d_`s' = (rb == `k') & `ev'
		local dlist `dlist' d_`s'
	}
	reghdfe `outcome' `dlist' [pw = projection_factor] if `insamp', absorb(hhid year age5 kids hh_comp) cluster(hhid)
	local N = e(N)
	quietly egen byte _t1 = tag(hhid) if e(sample) & `ev'
	quietly count if _t1 == 1
	local nev = r(N)
	quietly egen byte _t0 = tag(hhid) if e(sample) & grp_`cond' == 0
	quietly count if _t0 == 1
	local nct = r(N)
	drop _t1 _t0
	file open fh using "$idir/what_moves/diagnosis_events_coefs.csv", write append
	foreach k in -3 -2 -1 0 1 2 3 {
		if `k' == -1 {
			file write fh "`cond',`regime',`outcome',-1,0,0,`N'" _n
			continue
		}
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		file write fh "`cond',`regime',`outcome',`k',`=_b[d_`s']',`=_se[d_`s']',`N'" _n
	}
	file close fh
	quietly lincom (d_p1 + d_p2 + d_p3) / 3
	local post = r(estimate)
	local post_se = r(se)
	quietly lincom (d_m3 + d_m2) / 2
	local pre = r(estimate)
	local pre_se = r(se)
	quietly test d_m3 d_m2
	local pp = r(p)
	file open fh using "$idir/what_moves/diagnosis_events_summary.csv", write append
	file write fh "`cond',`regime',`outcome',`nev',`nct',`post',`post_se',`pre',`pre_se',`pp',`N'" _n
	file close fh
	di "`cond' / `regime' / `outcome': post = " %7.3f `post' " (" %6.3f `post_se' ")  pre = " %7.3f `pre' " (" %6.3f `pre_se' ") p = " %5.2f `pp' "  events `nev', controls `nct'"
end

local outcomes hei_usda sugar_1000 sodium_1000 soda_share cals_ae_day upf_share produce_share
foreach c in diabetes hypertension cholesterol heart obesity {
	foreach y of local outcomes {
		es_cond `c' all `y'
	}
}
foreach c in diabetes hypertension obesity {
	foreach y in hei_usda sugar_1000 cals_ae_day soda_share {
		es_cond `c' early `y'
		es_cond `c' late  `y'
	}
}
di "build_diagnosis_events.do complete"
