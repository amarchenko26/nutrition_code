// ============================================================================
// 10_build_stacked_event_study.do -- more movers for the FGW event study:
// (a) the first move of every household (panel cut before its second move),
// (b) every move stacked as its own event (9_build_stacked_moves_panel.py).
// Same specification as 4_: y_it = a_e + tau_t + sum_r theta_r delta_e 1[r]
// + sum_r rho_r 1[r] + x_it'b + e_it, with EVENT fixed effects a_e in place of
// household fixed effects (a household appears once per move), r binned at
// -7 and +7, x = 5-year age bins, kids, composition FE; projection-weighted;
// SE clustered on household. HEI only.
//
// Samples (each for employed-to-employed moves, then for all moves):
//   single movers          the household's only change (= 4_ "main"/"emp2emp")
//   first move             the household's first change, cut before the second
//   all moves, stacked     every change
//   excluding reversed     drops moves whose next change returns to the origin code
//   two or more years      drops moves with fewer than two post years in the new code
//
// Outputs (interim/movers/): stacked_event_study_coefs.csv, stacked_event_study_summary.csv
// Outputs (tabs/movers/): stacked_event_study.tex
// ============================================================================

clear all
set more off

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition"
global out      "$data/interim/movers"
global tabs     "$overleaf/tabs/movers"

use "$out/stacked_moves_panel.dta", clear

gen rb = max(-7, min(7, r)) if !missing(r)
global rlist
local dlist
local ddlist
forvalues k = -7/7 {
	if `k' == -1 continue
	local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
	gen byte d_`s' = (rb == `k')
	gen dd_`s' = d_`s' * delta_hei_usda
	global rlist $rlist `s'
	local dlist `dlist' d_`s'
	local ddlist `ddlist' dd_`s'
}
global dlist `dlist'
global ddlist `ddlist'

cap file close fh
file open fh using "$out/stacked_event_study_coefs.csv", write replace
file write fh "spec,sample,r,b,se,N,n_events,n_households" _n
file close fh
file open fh using "$out/stacked_event_study_summary.csv", write replace
file write fh "spec,sample,label,jump,jump_se,jump_late,jump_late_se,pre,pre_se,p_pretrend,N,n_events,n_households" _n
file close fh

// st_run "spec" "sample" "condition" "label"
cap program drop st_run
program define st_run
	args spec sample cond label
	di _n "==== `spec' / `sample': `cond'"
	reghdfe hei_usda $ddlist $dlist [pw = projection_factor] if `cond', ///
		absorb(event_id year age5 kids hh_comp) cluster(hhid)
	local N = e(N)
	quietly egen byte _te = tag(event_id) if e(sample)
	quietly count if _te == 1
	local nev = r(N)
	quietly egen byte _th = tag(hhid) if e(sample)
	quietly count if _th == 1
	local nhh = r(N)
	drop _te _th
	file open fh using "$out/stacked_event_study_coefs.csv", write append
	forvalues k = -7/7 {
		if `k' == -1 {
			file write fh "`spec',`sample',-1,0,0,`N',`nev',`nhh'" _n
			continue
		}
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		file write fh "`spec',`sample',`k',`=_b[dd_`s']',`=_se[dd_`s']',`N',`nev',`nhh'" _n
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
	file open fh using "$out/stacked_event_study_summary.csv", write append
	file write fh "`spec',`sample',`label',`jump',`jump_se',`jl',`jl_se',`pre',`pre_se',`pp',`N',`nev',`nhh'" _n
	file close fh
	matrix ST = nullmat(ST) \ (`jump', `jump_se', `jl', `jl_se', `pre', `pre_se', `pp', `nev', `nhh', `N')
	di "`label': jump `jump' (`jump_se'); late `jl' (`jl_se'); pre `pre' (`pre_se'), p `pp'; events `nev', households `nhh'"
end

local file "$tabs/stacked_event_study.tex"
local rn `" "Single movers" "First move, cut before second" "All moves, stacked" "All moves, excluding reversed" "All moves, 2+ years in new code" "'
foreach smp in emp2emp all {
	cap matrix drop ST
	if "`smp'" == "emp2emp" {
		local base "emp2emp == 1"
		local block "Employed to employed moves"
	}
	else {
		local base "1"
		local block "All moves"
	}
	st_run "single"      "`smp'" "`base' & single == 1"      "Single movers"
	st_run "firstmove"   "`smp'" "`base' & first_event == 1" "First move cut before the second"
	st_run "stacked"     "`smp'" "`base'"                    "All moves stacked"
	st_run "noreversal"  "`smp'" "`base' & reversed == 0"    "All moves excluding reversed"
	st_run "persist2"    "`smp'" "`base' & n_post >= 2"      "All moves with two or more years in the new code"
	matrix rownames ST = `rn'
	matrix colnames ST = "Jump 1-3" "SE" "Jump 4-6" "SE" "Pre" "SE" "p pre-trend" "Events" "Households" "N"
	// one table, a block per sample (fragment/append as in labor_supply/25_)
	if "`smp'" == "emp2emp" {
		esttab matrix(ST, fmt(3 3 3 3 3 3 2 %12.0fc %12.0fc %12.0fc)) using "`file'", ///
			replace fragment booktabs nomtitles nonotes collabels(none) ///
			prehead("\begin{tabular}{l*{10}{c}}" "\toprule" ///
			        " & Jump 1--3 & SE & Jump 4--6 & SE & Pre $-4$..$-2$ & SE & p pre-trend & Events & Households & N \\" ///
			        "\midrule" "\multicolumn{11}{l}{\emph{`block'}} \\") ///
			posthead("%") prefoot("%") postfoot("\midrule")
	}
	else {
		esttab matrix(ST, fmt(3 3 3 3 3 3 2 %12.0fc %12.0fc %12.0fc)) using "`file'", ///
			append fragment booktabs nomtitles nonotes collabels(none) ///
			prehead("\multicolumn{11}{l}{\emph{`block'}} \\") ///
			posthead("%") prefoot("%") postfoot("\bottomrule" "\end{tabular}")
	}
	esttab matrix(ST, fmt(3 3 3 3 3 3 2 %12.0fc %12.0fc %12.0fc))
}

di "10_build_stacked_event_study.do complete"
