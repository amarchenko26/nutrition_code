// ============================================================================
// 8_build_other_switch_event_studies.do -- the FGW event study for OTHER
// switches: births, deaths of a head, retirement, job loss, re-employment.
// Reads interim/movers/occupation_movers_panel (1_).
//
// For each switch a "mover" is a household whose state changes once (first
// change, panel cut before the state's next change) and the comparison group
// is the households whose state never changes:
//   y_it = a_i + tau_t + sum_r beta_r 1[r(i,t)=r] + x_it'b + e_it
// HEI in points; r = -1 omitted, r binned at -5 and +5 (FGW timing: r = 0 is
// the calendar year of the switch); household and year FE, 5-year age bins,
// plus kids and composition FE except the one the switch itself changes
// (births: kids dropped; deaths: composition dropped); projection-weighted;
// SE clustered on household. Gap = mean HEI of the destination state minus
// the origin state (occupation means for the employment switches; state means
// on never-changers, by year then averaged, for births and deaths);
// share = jump / gap. For the employment switches FGW's own specification
// (theta_r x delta_i + rho_r) is run as well.
//
// Switches:
//   birth       no child under 6 -> child under 6, household size up
//   death       two heads -> one head, widowed
//   retirement  employed occupation -> not employed, average head age 62+ at the move
//   jobloss     employed occupation -> not employed, under 62
//   reemploy    not employed -> employed occupation
// The employment switches use the occupation movers of 1_ (first move, panel
// cut before a second move) and all occupation non-movers as controls.
//
// For each switch the same event study is also run with household income
// (2013 dollars, $10k) as the outcome; its post-switch jump times the income IV's
// 0.035 SD per $10k (in HEI points) is the HEI jump the IV implies.
//
// Outputs (interim/movers/): other_switches_coefs.csv, other_switches_summary.csv
// Outputs (tabs/movers/): other_switches.tex
// ============================================================================

clear all
set more off

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition"
global out      "$data/interim/movers"
global tabs     "$overleaf/tabs/movers"

use "$out/occupation_movers_panel.dta", clear

bysort hhid: egen age_move = max(cond(year == m_file, avg_age_hh_head, .))
gen byte emp_origin = inlist(origin, 1, 2, 3, 4, 5, 6, 8, 9, 11)
gen byte emp_dest   = inlist(dest, 1, 2, 3, 4, 5, 6, 8, 9, 11)
gen byte occ_mover  = inlist(mover_type, 1, 2) & analysis == 1 & !missing(r)
quietly summarize hei_usda if analysis == 1
global iv_beta_pts = 0.035 * r(sd)     // income IV, full HEI, clean sample: 0.035 SD per $10k, in HEI points
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
state_gap young_child "birth_grp == 0"
local gap_birth = r(gap)
state_gap death_state "death_grp == 0"
local gap_death = r(gap)
di "gap birth = `gap_birth'; gap death = `gap_death'"

cap file close fh
file open fh using "$out/other_switches_coefs.csv", write replace
file write fh "design,spec,r,b,se,N,n_movers" _n
file close fh
file open fh using "$out/other_switches_summary.csv", write replace
file write fh "design,label,n_movers,age_post,gap,jump,jump_se,share,share_se,pre,pre_se,p_pretrend,theta_jump,theta_se,inc_jump,inc_jump_se,iv_implied,N" _n
file close fh

// sw_run "design" "r variable" "sample condition" "absorb list" "gap (. = mean delta among movers)" fgw(0/1) "label"
cap program drop sw_run
program define sw_run
	args design rvar cond fe gap fgw label
	// (cap drop is atomic across a varlist: one cap drop per variable set;
	//  min()/max() ignore missing, so the if is needed to keep never-changers missing)
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
	quietly egen byte _tag = tag(hhid) if e(sample) & _mv
	quietly count if _tag == 1
	local nmov = r(N)
	drop _tag
	quietly summarize avg_age_hh_head [aw = projection_factor] if e(sample) & `rvar' == 1
	local age = r(mean)
	if "`gap'" == "." {
		quietly summarize delta_hei_usda [aw = projection_factor] if e(sample) & _mv
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
	local tj ""
	local tj_se ""
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
	if "`tj'" == "" {
		local tj = .
		local tj_se = .
	}
	matrix OS = nullmat(OS) \ (`nmov', `age', `gap', `jump', `jump_se', `share', `tj', `tj_se', `pp', `ij', `ij_se', `implied')
	di "`label': movers `nmov', age `age', gap `gap', jump `jump' (`jump_se'), share `share', pre-trend p `pp', theta `tj' (`tj_se'), income jump `ij' (`ij_se'), IV-implied `implied'"
end

local fe_main "hhid year age5 kids hh_comp"
cap matrix drop OS
sw_run "birth"      "r_birth" "(birth_grp == 1 & !missing(r_birth)) | birth_grp == 0" "hhid year age5 hh_comp" "`gap_birth'" 0 "Birth"
sw_run "death"      "r_death" "(death_grp == 1 & !missing(r_death)) | death_grp == 0" "hhid year age5 kids"    "`gap_death'" 0 "Death of a head"
sw_run "retirement" "r" "analysis == 1 & ((occ_mover & emp_origin & dest == 12 & age_move >= 62) | mover_type == 0)" "`fe_main'" "." 1 "Retirement"
sw_run "jobloss"    "r" "analysis == 1 & ((occ_mover & emp_origin & dest == 12 & age_move <  62) | mover_type == 0)" "`fe_main'" "." 1 "Job loss"
sw_run "reemploy"   "r" "analysis == 1 & ((occ_mover & origin == 12 & emp_dest) | mover_type == 0)"                  "`fe_main'" "." 1 "Re-employment"

matrix rownames OS = "Birth" "Death of a head" "Retirement, 62+" "Job loss, under 62" "Re-employment"
matrix colnames OS = "Movers" "Age" "Gap" "Jump" "SE" "Share" "FGW theta" "SE" "p pre-trend" "Income jump" "SE" "IV-implied"
esttab matrix(OS, fmt(%12.0fc 1 2 2 2 2 2 2 2 2 2 2)) using "$tabs/other_switches.tex", ///
	replace booktabs nomtitles nonotes collabels(none) ///
	prehead("\begin{tabular}{l*{12}{c}}" "\toprule") ///
	posthead(" & Movers & Age & Gap & Jump & SE & Share & FGW $\theta$ & SE & p pre-trend & Income jump & SE & IV-implied jump \\" "\midrule") ///
	postfoot("\bottomrule" "\end{tabular}")
esttab matrix(OS, fmt(%12.0fc 1 2 2 2 2 2 2 2 2 2 2))

di "8_build_other_switch_event_studies.do complete"
