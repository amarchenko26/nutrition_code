// ============================================================================
// 5_build_symmetric_pairs.do -- movers from occupation A to B against movers
// from B to A within education x household-size cells (the "times -1" test).
// Reads interim/movers/occupation_movers_panel (1_).
//
// For a pair {A, B} with A the lower-HEI occupation, "up" movers go A -> B and
// "down" movers go B -> A. Event study with separate relative-year dummies
// for each direction:
//   y_it = a_i + lambda_{c(i,t),t} + sum_r theta^up_r 1[r, up]
//        + sum_r theta^dn_r 1[r, down] + e_it
// r = -1 omitted, r binned at -5 and +5; lambda = cell-year FE with cell =
// education bin x size bin (variants: year only; + state; + county); household
// FE; non-movers in A or B are the controls that identify the cell-year FE;
// SE clustered on household; projection-weighted. Movers are "pure" moves
// (education and size bins unchanged from file year m-1 to m) unless noted.
// If occupation B causes a change in nutrition of g_B - g_A, theta^up jumps
// by g_B - g_A and theta^dn by -(g_B - g_A): theta^up and -theta^dn should
// trace the same line, flat before the move.
//
// Pooled version: all pairs at once (up = move to the higher-HEI occupation),
// all non-movers as controls.
//
// Outputs (interim/movers/): pairs_coefs.csv, pairs_summary.csv
// Outputs (tabs/movers/): pairs_summary.tex, pairs_pooled.tex
// ============================================================================

clear all
set more off

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition"
global out      "$data/interim/movers"
global tabs     "$overleaf/tabs/movers"

use "$out/occupation_movers_panel.dta", clear
keep if analysis == 1 & inlist(mover_type, 0, 1)

label define occlbl 1 "Professional" 2 "Manager/owner" 3 "Clerical" 4 "Sales" ///
	5 "Craftsman" 6 "Operative/driver" 8 "Service" 9 "Farmer" 11 "Laborer" 12 "Not employed"

egen cell_year        = group(educ_bin size_bin year)
egen cell_state_year  = group(educ_bin size_bin state year)
egen cell_county_year = group(educ_bin size_bin county year)

gen byte up = (delta_hei_usda > 0) if mover_type == 1
gen A = cond(up == 1, origin, dest) if mover_type == 1
gen B = cond(up == 1, dest, origin) if mover_type == 1
gen pair = A * 100 + B if mover_type == 1
gen gap_AB = abs(delta_hei_usda) if mover_type == 1     // ybar_B - ybar_A

gen rb = max(-5, min(5, r)) if mover_type == 1
global rl
forvalues k = -5/5 {
	if `k' == -1 continue
	local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
	gen byte u_`s' = (rb == `k') & up == 1
	gen byte d_`s' = (rb == `k') & up == 0
	global rl $rl `s'
}
global ulist
global dlist
foreach s of global rl {
	global ulist $ulist u_`s'
	global dlist $dlist d_`s'
}

cap file close fh
file open fh using "$out/pairs_coefs.csv", write replace
file write fh "block,pair,A,B,spec,direction,r,b,se,N,n_up,n_dn" _n
file close fh
file open fh using "$out/pairs_summary.csv", write replace
file write fh "block,pair,A,B,spec,gap,jump_up,jump_up_se,jump_dn,jump_dn_se,p_mirror,p_pretrend,N,n_up,n_dn" _n
file close fh

// pair_run "block" "pair" "A" "B" "spec" "cond" "fe"
cap program drop pair_run
program define pair_run
	args block pr A B spec cond fe
	di _n "==== `block' pair `pr' (`A' -> `B') spec `spec': `cond' | absorb(hhid `fe')"
	reghdfe hei_usda $ulist $dlist [pw = projection_factor] if `cond', absorb(hhid `fe') cluster(hhid)
	local N = e(N)
	quietly egen byte _tu = tag(hhid) if e(sample) & up == 1
	quietly egen byte _td = tag(hhid) if e(sample) & up == 0
	quietly count if _tu == 1
	local nu = r(N)
	quietly count if _td == 1
	local nd = r(N)
	drop _tu _td
	file open fh using "$out/pairs_coefs.csv", write append
	forvalues k = -5/5 {
		if `k' == -1 {
			file write fh "`block',`pr',`A',`B',`spec',up,-1,0,0,`N',`nu',`nd'" _n
			file write fh "`block',`pr',`A',`B',`spec',down,-1,0,0,`N',`nu',`nd'" _n
			continue
		}
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		file write fh "`block',`pr',`A',`B',`spec',up,`k',`=_b[u_`s']',`=_se[u_`s']',`N',`nu',`nd'" _n
		file write fh "`block',`pr',`A',`B',`spec',down,`k',`=_b[d_`s']',`=_se[d_`s']',`N',`nu',`nd'" _n
	}
	file close fh
	quietly lincom (u_p1 + u_p2 + u_p3) / 3
	local ju = r(estimate)
	local ju_se = r(se)
	quietly lincom (d_p1 + d_p2 + d_p3) / 3
	local jd = r(estimate)
	local jd_se = r(se)
	quietly test u_p1 + u_p2 + u_p3 + d_p1 + d_p2 + d_p3 = 0
	local pm = r(p)
	quietly test u_m4 u_m3 u_m2 d_m4 d_m3 d_m2
	local pp = r(p)
	quietly summarize gap_AB if e(sample) & up == 1
	local gap = r(mean)
	file open fh using "$out/pairs_summary.csv", write append
	file write fh "`block',`pr',`A',`B',`spec',`gap',`ju',`ju_se',`jd',`jd_se',`pm',`pp',`N',`nu',`nd'" _n
	file close fh
	matrix PS = nullmat(PS) \ (`nu', `nd', `gap', `ju', `ju_se', -`jd', `jd_se', `pm', `pp')
	di "jump up = `ju' (`ju_se'); jump down = `jd' (`jd_se'); mirror p = `pm'; pre-trend p = `pp'"
end

// ----------------------------------------------- the most common pairs
preserve
keep if mover_type == 1 & pure_move == 1
bysort hhid: keep if _n == 1
gen byte dn = 1 - up
collapse (sum) n_up = up n_dn = dn, by(pair A B)
gen n_min = min(n_up, n_dn)
gen n_tot = n_up + n_dn
gsort -n_tot
keep if n_min >= 100
keep if _n <= 8
list
local npairs = _N
forvalues i = 1/`npairs' {
	local pr_`i' = pair[`i']
	local A_`i'  = A[`i']
	local B_`i'  = B[`i']
}
restore

// ---------------------------------------------- per pair, cell-year FE
cap matrix drop PS
local rn
forvalues i = 1/`npairs' {
	local cond "(pair == `pr_`i'' & pure_move == 1) | (mover_type == 0 & inlist(occ, `A_`i'', `B_`i''))"
	pair_run "pairs" "`pr_`i''" "`A_`i''" "`B_`i''" "cell" "`cond'" "cell_year"
	local nA : label occlbl `A_`i''
	local nB : label occlbl `B_`i''
	local rn `"`rn' "`nA' / `nB'""'
}
matrix rownames PS = `rn'
matrix colnames PS = "N up" "N down" "Gap" "Jump up" "SE" "-(Jump down)" "SE" "p mirror" "p pre-trend"
esttab matrix(PS, fmt(%12.0fc %12.0fc 2 2 2 2 2 2 2)) using "$tabs/pairs_summary.tex", ///
	replace booktabs nomtitles nonotes collabels(none) ///
	prehead("\begin{tabular}{l*{9}{c}}" "\toprule") ///
	posthead(" & N up & N down & Gap & Jump up & SE & $-$(Jump down) & SE & p mirror & p pre-trend \\" "\midrule") ///
	postfoot("\bottomrule" "\end{tabular}")
esttab matrix(PS, fmt(%12.0fc %12.0fc 2 2 2 2 2 2 2))

// ------------------------------------------ per pair, other FE variants
foreach spec in year cellstate cellcounty cell_allmoves {
	if "`spec'" == "year"          local fe "year"
	if "`spec'" == "cellstate"     local fe "cell_state_year"
	if "`spec'" == "cellcounty"    local fe "cell_county_year"
	if "`spec'" == "cell_allmoves" local fe "cell_year"
	forvalues i = 1/`npairs' {
		if "`spec'" == "cell_allmoves" local cond "pair == `pr_`i'' | (mover_type == 0 & inlist(occ, `A_`i'', `B_`i''))"
		else local cond "(pair == `pr_`i'' & pure_move == 1) | (mover_type == 0 & inlist(occ, `A_`i'', `B_`i''))"
		pair_run "pairs" "`pr_`i''" "`A_`i''" "`B_`i''" "`spec'" "`cond'" "`fe'"
	}
}

// ------------------------------------------------------- pooled, all pairs
cap matrix drop PS
local base "(mover_type == 1 & pure_move == 1) | mover_type == 0"
pair_run "pooled" "0" "0" "0" "year"       "`base'" "year"
pair_run "pooled" "0" "0" "0" "cell"       "`base'" "cell_year"
pair_run "pooled" "0" "0" "0" "cellx"      "`base'" "cell_year age5 kids hh_comp"
pair_run "pooled" "0" "0" "0" "cellstate"  "`base'" "cell_state_year"
pair_run "pooled" "0" "0" "0" "cellcounty" "`base'" "cell_county_year"
pair_run "pooled" "0" "0" "0" "emp2emp"    "(mover_type == 1 & pure_move == 1 & origin != 12 & dest != 12) | (mover_type == 0 & occ != 12)" "cell_year"
pair_run "pooled" "0" "0" "0" "notemp"     "(mover_type == 1 & pure_move == 1 & (origin == 12 | dest == 12)) | mover_type == 0" "cell_year"
pair_run "pooled" "0" "0" "0" "allmoves"   "mover_type <= 1" "cell_year"
pair_run "pooled" "0" "0" "0" "moversonly" "mover_type == 1 & pure_move == 1" "cell_year"

// (matrix stripe names are limited to 32 characters)
matrix rownames PS = "Year FE" "Cell x year FE" "Cell x year + age/kids/comp FE" ///
	"Cell x state x year FE" "Cell x county x year FE" "Employed to employed only" ///
	"Into/out of not employed only" "All moves (not only pure)" "Movers only, no non-movers"
matrix colnames PS = "N up" "N down" "Gap" "Jump up" "SE" "-(Jump down)" "SE" "p mirror" "p pre-trend"
esttab matrix(PS, fmt(%12.0fc %12.0fc 2 2 2 2 2 2 2)) using "$tabs/pairs_pooled.tex", ///
	replace booktabs nomtitles nonotes collabels(none) ///
	prehead("\begin{tabular}{l*{9}{c}}" "\toprule") ///
	posthead(" & N up & N down & Mean gap & Jump up & SE & $-$(Jump down) & SE & p mirror & p pre-trend \\" "\midrule") ///
	postfoot("\bottomrule" "\end{tabular}")
esttab matrix(PS, fmt(%12.0fc %12.0fc 2 2 2 2 2 2 2))

di "5_build_symmetric_pairs.do complete"
