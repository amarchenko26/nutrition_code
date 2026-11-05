// ============================================================================
// build_diagnosis_events.do -- does a household change what it buys after a
// member is diagnosed with a diet-related condition? (what moves nutrition,
// 2026-11-05). Reads interim/what_moves/diagnosis_panel.dta
// (build_diagnosis_panel.py).
//
// Event households, one regression per condition, event type and outcome,
// against households that answered the survey in 2+ years and never report the
// condition, 2004-2020:
//   y_it = a_i + tau_t + sum_{r != -1} beta_r 1[r(i,t) = r] + x_it'b + e_it
// Clock: the survey is fielded in January-March of its file year, so a diagnosis
// first seen in survey year s happened in about calendar year s-1; r = year -
// (s - 1). r = 0 is the diagnosis year (partly before, partly after), r = -1 the
// last full year before it (omitted), r = 1 the first full year after it; r
// binned at -5 and +6. Household and year FE, 5-year age bins, kids and
// composition FE; projection-weighted; SE clustered on household.
//   post = mean of beta_1..beta_3, late = mean of beta_4..beta_6, pre = mean of
//   beta_-4..beta_-2, diff = post - pre, p_pretrend = joint test of beta_-4..-2.
// Event types (regime):
//   dated         a member says "diagnosed less than a year ago" in the 2011-2015
//                 survey, every member selecting the condition that year says so,
//                 no report of it in an earlier survey
//   first         first survey report after 2+ earlier survey years without it,
//                 first report 2013-2019
//   first_conf    first, and the household reports it again in its next survey
//   first_unconf  first, and the household does not report it in its next survey
//   first_early   first report 2013-2015 (one questionnaire per member)
//   first_late    first report 2017-2019 (one household respondent)
//
// `do build_diagnosis_events.do A` runs one part (A B C D, for parallel runs);
// outputs interim/what_moves/diagnosis_parts/coefs_<part>.csv, summary_<part>.csv,
// combined by figure_diagnosis_events.py. With no argument, all parts in sequence.
// ============================================================================

clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

local parts "`1'"
if "`parts'" == "" local parts "A B C D"

use "$idir/what_moves/diagnosis_panel.dta", clear
cap mkdir "$idir/what_moves/diagnosis_parts"

cap program drop es_cond
program define es_cond
	args cond regime outcome part
	tempvar ev insamp s
	if substr("`regime'", 1, 5) == "first" gen double `s' = m_`cond'
	else gen double `s' = t_`cond'
	gen byte `ev' = !missing(`s')
	if "`regime'" == "first_conf"   replace `ev' = `ev' & conf_`cond' == 1
	if "`regime'" == "first_unconf" replace `ev' = `ev' & conf_`cond' == 0
	if "`regime'" == "first_early"  replace `ev' = `ev' & inrange(`s', 2013, 2015)
	if "`regime'" == "first_late"   replace `ev' = `ev' & inrange(`s', 2017, 2019)
	gen byte `insamp' = (grp_`cond' == 0) | `ev'
	cap drop rr rb d_*
	gen rr = year - (`s' - 1) if `ev'
	gen rb = max(-5, min(6, rr)) if `ev'
	local dlist
	foreach k in -5 -4 -3 -2 0 1 2 3 4 5 6 {
		local n = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		gen byte d_`n' = (rb == `k') & `ev'
		local dlist `dlist' d_`n'
	}
	quietly reghdfe `outcome' `dlist' [pw = projection_factor] if `insamp', absorb(hhid year age5 kids hh_comp) cluster(hhid)
	local N = e(N)
	quietly egen byte _t1 = tag(hhid) if e(sample) & `ev'
	quietly count if _t1 == 1
	local nev = r(N)
	quietly egen byte _t0 = tag(hhid) if e(sample) & grp_`cond' == 0
	quietly count if _t0 == 1
	local nct = r(N)
	drop _t1 _t0
	local b0 = _b[d_p0]
	local b0se = _se[d_p0]
	file open fh using "$idir/what_moves/diagnosis_parts/coefs_`part'.csv", write append
	foreach k in -5 -4 -3 -2 -1 0 1 2 3 4 5 6 {
		if `k' == -1 {
			file write fh "`cond',`regime',`outcome',-1,0,0,`N'" _n
			continue
		}
		local n = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		file write fh "`cond',`regime',`outcome',`k',`=_b[d_`n']',`=_se[d_`n']',`N'" _n
	}
	file close fh
	quietly lincom (d_p1 + d_p2 + d_p3) / 3
	local post = r(estimate)
	local post_se = r(se)
	quietly lincom (d_p4 + d_p5 + d_p6) / 3
	local late = r(estimate)
	local late_se = r(se)
	quietly lincom (d_m4 + d_m3 + d_m2) / 3
	local pre = r(estimate)
	local pre_se = r(se)
	quietly lincom (d_p1 + d_p2 + d_p3) / 3 - (d_m4 + d_m3 + d_m2) / 3
	local diff = r(estimate)
	local diff_se = r(se)
	quietly test d_m4 d_m3 d_m2
	local pp = r(p)
	quietly summarize `outcome' [aw = projection_factor] if `ev' & rr == -1
	local mean_m1 = r(mean)
	file open fh using "$idir/what_moves/diagnosis_parts/summary_`part'.csv", write append
	file write fh "`cond',`regime',`outcome',`nev',`nct',`mean_m1',`b0',`b0se',`post',`post_se',`late',`late_se',`pre',`pre_se',`diff',`diff_se',`pp',`N'" _n
	file close fh
	di "RES `cond' / `regime' / `outcome': r0 = " %7.3f `b0' " (" %6.3f `b0se' ") post = " %7.3f `post' " (" %6.3f `post_se' ") late = " %7.3f `late' " (" %6.3f `late_se' ") pre = " %7.3f `pre' " (" %6.3f `pre_se' ") p = " %5.2f `pp' " events `nev' controls `nct' mean `mean_m1'"
end

local conds   diabetes hypertension cholesterol heart obesity
local main    hei_usda sugar_1000 sodium_1000 soda_share cals_ae_day upf_share produce_share
local core    hei_usda sugar_1000 cals_ae_day
local records n_trips spend_total
local prod    reg_soda_oz_wk diet_share sweetener_any diet_candy_any

foreach part of local parts {
	file open fh using "$idir/what_moves/diagnosis_parts/coefs_`part'.csv", write replace
	file write fh "condition,regime,outcome,r,b,se,N" _n
	file close fh
	file open fh using "$idir/what_moves/diagnosis_parts/summary_`part'.csv", write replace
	file write fh "condition,regime,outcome,n_events,n_controls,mean_m1,r0,r0_se,post,post_se,late,late_se,pre,pre_se,diff,diff_se,p_pretrend,N" _n
	file close fh
	if "`part'" == "A" {
		foreach c of local conds {
			foreach y in `main' `records' {
				es_cond `c' dated `y' `part'
			}
		}
	}
	if "`part'" == "B" {
		foreach c of local conds {
			foreach y in `main' `records' {
				es_cond `c' first `y' `part'
			}
		}
	}
	if "`part'" == "C" {
		foreach c of local conds {
			foreach y of local core {
				es_cond `c' first_conf `y' `part'
				es_cond `c' first_unconf `y' `part'
			}
		}
		foreach r in dated first first_conf {
			foreach y of local prod {
				es_cond diabetes `r' `y' `part'
			}
		}
	}
	if "`part'" == "D" {
		foreach c of local conds {
			foreach y of local core {
				es_cond `c' first_early `y' `part'
				es_cond `c' first_late `y' `part'
			}
		}
		foreach c in hypertension cholesterol heart obesity {
			foreach y of local prod {
				es_cond `c' dated `y' `part'
			}
		}
	}
}
di "build_diagnosis_events.do complete"
