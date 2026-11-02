// ============================================================================
// 7_build_movers_decomposition.do -- FGW (2016) Table II additive decomposition
// with occupations as places: y_ijt = a_i + g_j + tau_t + x_it'b + e_ijt on
// non-movers + single movers outside relative year 0 (reghdfe, occupation FE
// saved); x_it = 5-year age bins, kids, composition FE; projection-weighted.
// For occupation sets R (high HEI) and R' (low HEI):
//   share due to occupation = (g_R - g_R') / (ybar_R - ybar_R'),
// household-year weighted; SEs from 50 household-cluster bootstraps (HEI only).
// Reads interim/movers/occupation_movers_panel (1_).
//
// Outputs (interim/movers/): occupation_effects_<outcome>.csv (ybar_j, g_j,
//   ybar*_j = ybar_j - g_j, weight), decomp_reps.dta, decomposition.csv
// Outputs (tabs/movers/): decomposition.tex
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

// ============================================================================
// Additive decomposition (FGW Table II) + occupation effects
// ============================================================================
keep if inlist(mover_type, 0, 1) & (missing(r) | r != 0)
tempfile base
save `base'

// occupation sets, high (R) and low (L), ordered by mean HEI:
// 1 49.4 > 2 47.8 > 12 47.1 > 3 46.9 > 4 46.6 > 8 46.4 > 9 46.1 > 5 45.4 > 11 44.8 > 6 44.8
global R_1 "1, 2, 12, 3, 4"
global L_1 "8, 9, 5, 11, 6"
global R_2 "1, 2, 12"
global L_2 "5, 11, 6"
global R_3 "1"
global L_3 "6"
global R_4 "1, 2, 3, 4"
global L_4 "5, 6, 11"
global R_5 "1"
global L_5 "12"
global R_6 "1, 2"
global L_6 "5, 6"
global nP = 6

cap postutil clear
tempname ph
postfile `ph' str12 outcome int rep int part double(overall place hh share_place) ///
	using "$out/decomp_reps.dta", replace

cap program drop decomp_once
program define decomp_once
	args outcome rep idvar ph
	cap drop g_fe
	reghdfe `outcome' [pw = projection_factor], absorb(`idvar' year age5 kids hh_comp g_fe=occ) ///
		cluster(hhid)
	preserve
	keep if e(sample)
	collapse (mean) ybar = `outcome' gam = g_fe (rawsum) n = projection_factor ///
		[pw = projection_factor], by(occ)
	gen ystar = ybar - gam
	if `rep' == 0 {
		list
		export delimited occ ybar gam ystar n using "$out/occupation_effects_`outcome'.csv", replace
	}
	forvalues p = 1/$nP {
		quietly summarize ybar [aw = n] if inlist(occ, ${R_`p'})
		local yR = r(mean)
		quietly summarize gam  [aw = n] if inlist(occ, ${R_`p'})
		local gR = r(mean)
		quietly summarize ybar [aw = n] if inlist(occ, ${L_`p'})
		local yL = r(mean)
		quietly summarize gam  [aw = n] if inlist(occ, ${L_`p'})
		local gL = r(mean)
		local overall = `yR' - `yL'
		local place   = `gR' - `gL'
		local hh      = `overall' - `place'
		post `ph' ("`outcome'") (`rep') (`p') (`overall') (`place') (`hh') (`place' / `overall')
	}
	restore
end

foreach v in hei_usda hi_usda hi {
	decomp_once `v' 0 hhid `ph'
}

// household-cluster bootstrap for HEI
forvalues b = 1/50 {
	di _n "---- bootstrap rep `b'"
	use `base', clear
	bsample, cluster(hhid) idcluster(bid)
	quietly decomp_once hei_usda `b' bid `ph'
}
postclose `ph'

// ---------------------------------------------------------- table
use "$out/decomp_reps.dta", clear
export delimited using "$out/decomposition.csv", replace
keep if outcome == "hei_usda"
bysort part: egen se_share = sd(cond(rep > 0, share_place, .))
bysort part: egen se_place = sd(cond(rep > 0, place, .))
keep if rep == 0
sort part
matrix D = J(7, $nP, .)
forvalues p = 1/$nP {
	matrix D[1, `p'] = overall[`p']
	matrix D[2, `p'] = place[`p']
	matrix D[3, `p'] = hh[`p']
	matrix D[4, `p'] = share_place[`p']
	matrix D[5, `p'] = se_share[`p']
	matrix D[6, `p'] = 1 - share_place[`p']
	matrix D[7, `p'] = se_share[`p']
}
matrix rownames D = "Overall" "Due to occupation" "Due to households" "Share due to occupation" "(SE)" "Share due to households" "(SE)"
matrix colnames D = "Above/below median" "Top/bottom 3" "Prof./operative" "White/blue collar" "Prof./not employed" "Prof.+mgr/craft+oper."
esttab matrix(D, fmt(2 2 2 2 2 2 2)) using "$tabs/decomposition.tex", ///
	replace booktabs nomtitles nonotes collabels(none) ///
	prehead("\begin{tabular}{l*{$nP}{c}}" "\toprule") ///
	posthead(" & Above/below & Top 3 / & Professional / & White / & Professional / & Prof.+manager / \\" ///
	         " & median & bottom 3 & operative & blue collar & not employed & craftsman+operative \\" "\midrule" ///
	         "\multicolumn{7}{l}{\emph{Difference in mean HEI}} \\") ///
	postfoot("\bottomrule" "\end{tabular}")
esttab matrix(D, fmt(3))

di "7_build_movers_decomposition.do complete"
