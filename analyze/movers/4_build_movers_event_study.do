// ============================================================================
// 4_build_movers_event_study.do -- Finkelstein-Gentzkow-Williams (2016)
// event study and additive decomposition with OCCUPATIONS as the "places"
// and nutrition as the outcome. Reads interim/movers/occupation_movers_panel
// (1_build_occupation_movers_panel.py).
//
// Event study (FGW eq. 6), single movers only:
//   y_it = a_i + tau_t + sum_r theta_r delta_i 1[r(i,t)=r]
//        + sum_r rho_r 1[r(i,t)=r] + x_it'b + e_it
// delta_i = ybar_dest - ybar_orig (occupation means of the outcome);
// relative year r = 0 is the calendar year of the move (partially treated),
// r = -1 omitted, r binned at -7 and +7; x_it = 5-year age bins, kids and
// composition FE; SE clustered on household; projection-weighted.
// theta_r for r >= 1 is the share of the origin-destination gap that the
// mover's own nutrition closes -- FGW's place share.
//
// The additive decomposition (FGW Table II) is 7_build_movers_decomposition.do.
//
// Outputs (interim/movers/): event_study_coefs.csv, event_study_summary.csv
// Outputs (tabs/movers/): event_study_summary.tex
// ============================================================================

clear all
set more off
set seed 20260930

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition"
global out      "$data/interim/movers"
global tabs     "$overleaf/tabs/movers"

use "$out/occupation_movers_panel.dta", clear
keep if analysis == 1

// age at the move (file year m) for the working-age variant
bysort hhid: egen age_move = max(cond(year == m_file, avg_age_hh_head, .))
egen cell_year = group(educ_bin size_bin year)
egen year_state = group(year state)

// ------------------------------------------------- relative-year dummies
gen rb = max(-7, min(7, r)) if inlist(mover_type, 1, 2)
global rlist
forvalues k = -7/7 {
	if `k' == -1 continue
	local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
	gen byte d_`s' = (rb == `k') & !missing(rb)
	global rlist $rlist `s'
}

cap file close fh
file open fh using "$out/event_study_coefs.csv", write replace
file write fh "spec,outcome,r,b,se,N,n_movers" _n
file close fh
file open fh using "$out/event_study_summary.csv", write replace
file write fh "spec,outcome,jump,jump_se,jump_late,jump_late_se,pre,pre_se,p_pretrend,N,n_movers" _n
file close fh

// es_run "spec" "outcome" "sample condition" "absorb list" "pw|none" "row label" ["delta variable"]
// (delta defaults to delta_<outcome>; pass delta_real_income to scale by the occupation INCOME gap)
cap program drop es_run
program define es_run
	args spec outcome cond fe wt label dvar
	if "`dvar'" == "" local dvar delta_`outcome'
	local ddlist
	local dlist
	foreach s of global rlist {
		cap drop dd_`s'
		quietly gen dd_`s' = d_`s' * `dvar'
		local ddlist `ddlist' dd_`s'
		local dlist  `dlist'  d_`s'
	}
	local w ""
	if "`wt'" == "pw" local w "[pw = projection_factor]"
	di _n "==== `spec' / `outcome' : `cond' | absorb(`fe') `w'"
	reghdfe `outcome' `ddlist' `dlist' `w' if `cond', absorb(`fe') cluster(hhid)
	quietly egen byte _tag = tag(hhid) if e(sample) & !missing(rb)
	quietly count if _tag == 1
	local nmov = r(N)
	drop _tag
	local N = e(N)
	file open fh using "$out/event_study_coefs.csv", write append
	forvalues k = -7/7 {
		if `k' == -1 {
			file write fh "`spec',`outcome',-1,0,0,`N',`nmov'" _n
			continue
		}
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		file write fh "`spec',`outcome',`k',`=_b[dd_`s']',`=_se[dd_`s']',`N',`nmov'" _n
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
	file write fh "`spec',`outcome',`jump',`jump_se',`jl',`jl_se',`pre',`pre_se',`pp',`N',`nmov'" _n
	file close fh
	matrix ES = nullmat(ES) \ (`jump', `jump_se', `jl', `jl_se', `pre', `pre_se', `pp', `N', `nmov')
	global esrows `"${esrows} "`label'""'
	di "jump (r=1..3) = `jump' (`jump_se'); late (4..6) = `jl' (`jl_se'); pre (-4..-2) = `pre' (`pre_se'), p = `pp'; movers = `nmov'"
end

local fe_main "hhid year age5 kids hh_comp"
global esrows

// ------------------------------------------------------------ main runs
es_run "main" "hei_usda" "mover_type == 1" "`fe_main'" "pw" "HEI, single movers (main)"
es_run "main" "hi_usda"  "mover_type == 1" "`fe_main'" "pw" "Simple index (USDA)"
es_run "main" "hi"       "mover_type == 1" "`fe_main'" "pw" "Simple index (Syndigo)"
// variants on HEI
es_run "emp2emp"       "hei_usda" "mover_type == 1 & origin != 12 & dest != 12"      "`fe_main'" "pw" "HEI, employed to employed"
es_run "notemp"        "hei_usda" "mover_type == 1 & (origin == 12 | dest == 12)"    "`fe_main'" "pw" "HEI, into/out of not employed"
es_run "under62"       "hei_usda" "mover_type == 1 & age_move < 62"                  "`fe_main'" "pw" "HEI, under 62 at the move"
es_run "pure"          "hei_usda" "mover_type == 1 & pure_move == 1"                 "`fe_main'" "pw" "HEI, educ/size bins unchanged"
es_run "multi"         "hei_usda" "inlist(mover_type, 1, 2)"                         "`fe_main'" "pw" "HEI, including multiple movers"
es_run "withnonmovers" "hei_usda" "inlist(mover_type, 0, 1)"                         "`fe_main'" "pw" "HEI, movers and non-movers"
es_run "unweighted"    "hei_usda" "mover_type == 1"                                  "`fe_main'" "none" "HEI, unweighted"
es_run "yearfe_only"   "hei_usda" "mover_type == 1"                                  "hhid year" "pw" "HEI, household and year FE only"
es_run "cellyear"      "hei_usda" "mover_type == 1"                                  "hhid cell_year age5 kids hh_comp" "pw" "HEI, education x size x year FE"
// the income IV's exact control set (12_build_clean_sample_results.do): exact age FE, and year x state FE
es_run "ivcontrols"    "hei_usda" "mover_type == 1" "hhid year kids avg_age_hh_head hh_comp"       "pw" "HEI, IV controls (exact age)"
es_run "ivcontrols_ys" "hei_usda" "mover_type == 1" "hhid year_state kids avg_age_hh_head hh_comp" "pw" "HEI, IV controls + year x state"
// income "first stage" of the move: theta_r = share of the cross-sectional occupation income gap realized
es_run "income"        "real_income" "mover_type == 1" "`fe_main'" "pw" "Income, share of income gap"
// HEI scaled by the occupation INCOME gap: theta_r = HEI points per $10k of cross-sectional income gap
es_run "hei_per_inc"   "hei_usda" "mover_type == 1" "`fe_main'" "pw" "HEI per 10k of income gap" "delta_real_income"

matrix colnames ES = "Jump 1-3" "SE" "Jump 4-6" "SE" "Pre -4..-2" "SE" "p pre-trend" "N" "Movers"
local rn
foreach r of global esrows {
	local rn `"`rn' "`r'""'
}
matrix rownames ES = `rn'
esttab matrix(ES, fmt(3 3 3 3 3 3 2 %12.0fc %12.0fc)) using "$tabs/event_study_summary.tex", ///
	replace booktabs nomtitles nonotes collabels(none) ///
	prehead("\begin{tabular}{l*{9}{c}}" "\toprule") ///
	posthead(" & Jump 1--3 & SE & Jump 4--6 & SE & Pre $-4$..$-2$ & SE & p pre-trend & N & Movers \\" "\midrule") ///
	postfoot("\bottomrule" "\end{tabular}")
esttab matrix(ES, fmt(3 3 3 3 3 3 2 %12.0fc %12.0fc))

di "4_build_movers_event_study.do complete"

