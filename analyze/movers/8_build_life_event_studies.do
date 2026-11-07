// ============================================================================
// 8_build_life_event_studies.do -- the event study for OTHER switches in a
// household's life: a birth, the death of a head, an education increase,
// retirement, job loss and re-employment. HEI in points.
// Reads interim/movers/occupation_movers_panel.dta (births, deaths, education)
// and movers_events_panel.dta (the employment switches), both from 1_.
//
//   y_it = a_i + tau_t + sum_r beta_r 1[r(i,t)=r] + x_it'b + e_it
// r = -1 omitted, r binned at -5 and +5 (FGW timing: r = 0 is the calendar year
// of the switch); year FE, 5-year age bins, kids and composition FE except the
// one the switch itself changes (births: kids dropped; deaths: composition
// dropped); projection-weighted; SE clustered on household.
//   birth / death / educ_up: household FE; a mover is a household whose state
//     changes once (first consecutive-year change, panel cut before the next
//     change); the comparison group is the households whose state never changes.
//   retirement / jobloss / reemploy: every dated move between an employed
//     occupation and not employed (12) in the stacked event panel, with EVENT FE
//     (the design of 4_); retirement = into not employed with average head age
//     62+ in the first year after the move, job loss = the same under 62,
//     re-employment = out of not employed.
// Gap = mean HEI of the destination state minus the origin state (occupation
// means for the employment switches; state means on never-changers for the
// others); share = jump / gap. For the employment switches FGW's specification
// (theta_r x delta_e + rho_r) is run as well. Each switch is also run with
// household income (2013 dollars, $10k) as the outcome; its post-switch jump
// times the income IV's estimate (SD per $10k, in HEI points) is the HEI jump the
// IV implies.
//
// Outputs (interim/movers/): other_switches_coefs.csv, other_switches_summary.csv
// ============================================================================

clear all
set more off

do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
global out  "$idir/movers"

import delimited "$idir/labor_supply/iv_main_estimates.csv", clear
quietly summarize b if outcome == "hei_usda"
local iv_sd = r(mean)                   // income IV, full HEI, clean sample: SD per $10k (analyze/labor_supply/12_)
quietly summarize sd if outcome == "hei_usda"
global iv_beta_pts = `iv_sd' * r(mean)  // the same estimate in HEI points, with the SD 12_ standardizes with

use "$out/occupation_movers_panel.dta", clear
di "IV coefficient in HEI points per 10k: $iv_beta_pts"

// ------------------------------------ state gaps for births and deaths
cap program drop state_gap
program define state_gap, rclass
	args statevar cond
	preserve
	keep if `cond' & !missing(hei_usda, `statevar')
	collapse (mean) hei = hei_usda [pw = projection_factor], by(`statevar' year)
	collapse (mean) hei, by(`statevar')
	sort `statevar'
	list
	local g = hei[2] - hei[1]
	restore
	return scalar gap = `g'
end
state_gap young_child "birth_grp == 0 & analysis == 1"
global gap_birth = r(gap)
state_gap death_state "death_grp == 0 & analysis == 1"
global gap_death = r(gap)
di "gap birth = $gap_birth; gap death = $gap_death"

cap file close fh
file open fh using "$out/other_switches_coefs.csv", write replace
file write fh "design,spec,r,b,se,N,n_movers" _n
file close fh
file open fh using "$out/other_switches_summary.csv", write replace
file write fh "design,label,n_movers,age_post,gap,jump,jump_se,share,share_se,pre,pre_se,p_pretrend,theta_jump,theta_se,inc_jump,inc_jump_se,iv_implied,N" _n
file close fh

// sw_run "design" "r variable" "sample condition" "absorb list" "gap: a number, or a variable whose mover mean is the gap" fgw(0/1) "label" "unit id for counting movers"
cap program drop sw_run
program define sw_run
	args design rvar cond fe gap fgw label unit
	cap drop rb
	cap drop _mv
	cap drop d_*
	cap drop dd_*
	gen rb = max(-5, min(5, `rvar')) if !missing(`rvar')
	gen byte _mv = !missing(rb)
	local dlist
	forvalues k = -5/5 {
		if `k' == -1 continue
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		gen byte d_`s' = (rb == `k') & _mv
		local dlist `dlist' d_`s'
	}
	di _n "==== `design': `cond' | absorb(`fe')"
	reghdfe hei_usda `dlist' [pw = projection_factor] if `cond', absorb(`fe') cluster(hhid)
	local N = e(N)
	quietly egen byte _tag = tag(`unit') if e(sample) & _mv
	quietly count if _tag == 1
	local nmov = r(N)
	drop _tag
	quietly summarize avg_age_hh_head [aw = projection_factor] if e(sample) & `rvar' == 1
	local age = r(mean)
	cap confirm variable `gap'
	if _rc == 0 {
		quietly summarize `gap' [aw = projection_factor] if e(sample) & _mv
		local gap = r(mean)
	}
	file open fh using "$out/other_switches_coefs.csv", write append
	forvalues k = -5/5 {
		if `k' == -1 {
			file write fh "`design',points,-1,0,0,`N',`nmov'" _n
			continue
		}
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		file write fh "`design',points,`k',`=_b[d_`s']',`=_se[d_`s']',`N',`nmov'" _n
	}
	file close fh
	quietly lincom (d_p1 + d_p2 + d_p3) / 3
	local jump = r(estimate)
	local jump_se = r(se)
	local share = `jump' / `gap'
	local share_se = `jump_se' / abs(`gap')
	quietly lincom (d_m4 + d_m3 + d_m2) / 3
	local pre = r(estimate)
	local pre_se = r(se)
	quietly test d_m4 d_m3 d_m2
	local pp = r(p)
	// realized household income change on the switch, and the HEI jump the income IV implies from it
	reghdfe real_income `dlist' [pw = projection_factor] if `cond', absorb(`fe') cluster(hhid)
	file open fh using "$out/other_switches_coefs.csv", write append
	forvalues k = -5/5 {
		if `k' == -1 {
			file write fh "`design',income,-1,0,0,`=e(N)',`nmov'" _n
			continue
		}
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		file write fh "`design',income,`k',`=_b[d_`s']',`=_se[d_`s']',`=e(N)',`nmov'" _n
	}
	file close fh
	quietly lincom (d_p1 + d_p2 + d_p3) / 3
	local ij = r(estimate)
	local ij_se = r(se)
	local implied = $iv_beta_pts * `ij'
	local tj = .
	local tj_se = .
	if `fgw' == 1 {
		local ddlist
		foreach v of local dlist {
			gen d`v' = `v' * delta_hei_usda
			local ddlist `ddlist' d`v'
		}
		reghdfe hei_usda `ddlist' `dlist' [pw = projection_factor] if `cond', absorb(`fe') cluster(hhid)
		file open fh using "$out/other_switches_coefs.csv", write append
		forvalues k = -5/5 {
			if `k' == -1 {
				file write fh "`design',theta,-1,0,0,`=e(N)',`nmov'" _n
				continue
			}
			local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
			file write fh "`design',theta,`k',`=_b[dd_`s']',`=_se[dd_`s']',`=e(N)',`nmov'" _n
		}
		file close fh
		quietly lincom (dd_p1 + dd_p2 + dd_p3) / 3
		local tj = r(estimate)
		local tj_se = r(se)
	}
	file open fh using "$out/other_switches_summary.csv", write append
	file write fh "`design',`label',`nmov',`age',`gap',`jump',`jump_se',`share',`share_se',`pre',`pre_se',`pp',`tj',`tj_se',`ij',`ij_se',`implied',`N'" _n
	file close fh
	di "`label': movers `nmov', age `age', gap `gap', jump `jump' (`jump_se'), share `share', pre-trend p `pp', theta `tj' (`tj_se'), income jump `ij' (`ij_se'), IV-implied `implied'"
end

// ---------------------------------------------- births, deaths, education (household FE)
sw_run "birth" "r_birth" "analysis == 1 & ((birth_grp == 1 & !missing(r_birth)) | birth_grp == 0)" "hhid year age5 hh_comp" "$gap_birth" 0 "Birth" "hhid"
sw_run "death" "r_death" "analysis == 1 & ((death_grp == 1 & !missing(r_death)) | death_grp == 0)" "hhid year age5 kids"    "$gap_death" 0 "Death of a head" "hhid"
sw_run "educ"  "r_educ"  "analysis == 1 & ((educ_grp == 1 & !missing(r_educ)) | educ_grp == 0)"    "hhid year age5 kids hh_comp" "delta_educ_hei" 0 "Education increase" "hhid"

// ---------------------------------------------- employment switches (stacked events, event FE)
use "$out/movers_events_panel.dta", clear
bysort event_id: egen age_move = max(cond(r == 1, avg_age_hh_head, .))
gen byte emp_origin = inlist(origin, 1, 2, 3, 4, 5, 6, 8, 9, 11)
gen byte emp_dest   = inlist(dest, 1, 2, 3, 4, 5, 6, 8, 9, 11)
local fe_ev "event_id year age5 kids hh_comp"
sw_run "retirement" "r" "notemp == 1 & emp_origin & dest == 12 & age_move >= 62" "`fe_ev'" "delta_hei_usda" 1 "Retirement" "event_id"
sw_run "jobloss"    "r" "notemp == 1 & emp_origin & dest == 12 & age_move <  62" "`fe_ev'" "delta_hei_usda" 1 "Job loss"   "event_id"
sw_run "reemploy"   "r" "notemp == 1 & origin == 12 & emp_dest"                   "`fe_ev'" "delta_hei_usda" 1 "Re-employment" "event_id"

di "8_build_life_event_studies.do complete"
