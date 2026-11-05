// ============================================================================
// 25_build_mover_design.do -- the AKM-style movers design on the CLEAN SAMPLE
// (Anya 2026-09-18): the instrument is identified off households that switch
// occupation cells, who may be selected. Following Card-Heining-Kline, look at
// movers by the direction and size of the move, and identify off movers only.
//
// Groups (clean sample = employed occupation type at t-1; types as in 15_:
// white 3 > blue 2 > service 1 by mean household income, not employed 0):
//   Stayers      same employed type at t
//   Up-movers    employed type at t, higher rank than at t-1
//   Down-movers  employed type at t, lower rank than at t-1
//   Job losers   not-employed cell at t, average head age under 67
// Moves into the not-employed cell at 67+ (retirement) are left out.
//
// Shock d_iv = D.iv_income_fips/10 (the instrument, $10k); income change
// d_inc = D.real_income ($10k); outcomes z-scored, forward change F.z - z
// (main window) and the pre-move change L2D.z (purchases t-3 -> t-2).
//
// Spec: NO household FE (a household moves rarely, so within-household
// variation among movers is repeat movers only); instead move-type x year
// FE (occupation type at t-1 x type at t x year), plus kids, age and
// composition FE; non-movers; [pw = projection_factor]; SE clustered on
// county. Within a move type the instrument varies through the education
// and size dimensions of the destination cell and the leave-out county.
//
// Outputs (tabs/labor_supply/):
//   movers_groups.tex    weighted means by group: shock, income change,
//                        forward outcome changes, pre-move outcome changes
//   movers_symmetry.tex  movers only: 2SLS with the income change interacted
//                        with up / down / job-loser groups; p-value of
//                        up = down; Sanderson-Windmeijer first-stage F per group
//   movers_2sls.tex      2SLS by sample: movers (employed to employed),
//                        movers incl. job losers, stayers, all clean; then
//                        the FE ladder on the full clean sample: move-type x
//                        year FE + household FE, and the main specification
//                        (household + year FE, no move-type FE, = hi_clean)
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// ------------------------------------------- assigned occupation, clean flag
// exactly as in 12_ / 15_ (labor_supply.tex "Measurement" / "Sample")
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)

gen byte occ_type = .
replace occ_type = 1 if inlist(occ_src, 1, 2, 3, 4)
replace occ_type = 2 if inlist(occ_src, 5, 6, 11)
replace occ_type = 3 if inlist(occ_src, 8, 9)
replace occ_type = 4 if inlist(occ_src, 10, 12)
label define occ_type_lbl 1 "White collar" 2 "Blue collar" 3 "Service/other" 4 "Not employed"
label values occ_type occ_type_lbl
gen byte Locc_type = L.occ_type
gen byte occ_rank  = cond(occ_type == 4, 0, cond(occ_type == 3, 1, cond(occ_type == 2, 2, cond(occ_type == 1, 3, .))))
gen byte Locc_rank = L.occ_rank

// ------------------------------------------------------------------- groups
gen byte grp = .
replace grp = 0 if clean == 1 & inrange(occ_type, 1, 3) & occ_type == Locc_type
replace grp = 1 if clean == 1 & inrange(occ_type, 1, 3) & occ_rank > Locc_rank
replace grp = 2 if clean == 1 & inrange(occ_type, 1, 3) & occ_rank < Locc_rank
replace grp = 3 if clean == 1 & occ_type == 4 & avg_age_hh_head < 67
label define grp_lbl 0 "Stayers" 1 "Up-movers" 2 "Down-movers" 3 "Job losers"
label values grp grp_lbl

// --------------------------------------------------------------- variables
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'        // purchases t -> t+1 (main window)
	gen pz_`v' = L2D.z_`v'              // purchases t-3 -> t-2 (before the income change)
}
gen d_inc = D.real_income

egen mt_year = group(Locc_type occ_type year)   // move type x year

tab grp [aw = projection_factor] if !missing(d_inc, d_iv, fz_hei_usda)

// ============================================================================
// Table A: weighted means by group
// ============================================================================

matrix M = J(4, 9, .)
local r = 1
forvalues g = 0/3 {
	quietly count if grp == `g' & !missing(d_inc, d_iv, fz_hei_usda)
	matrix M[`r', 1] = r(N)
	quietly summarize d_iv  [aw = projection_factor] if grp == `g' & !missing(d_inc, fz_hei_usda)
	matrix M[`r', 2] = r(mean)
	quietly summarize d_inc [aw = projection_factor] if grp == `g' & !missing(d_iv, fz_hei_usda)
	matrix M[`r', 3] = r(mean)
	local c = 4
	foreach v of local outcomes {
		quietly summarize fz_`v' [aw = projection_factor] if grp == `g' & !missing(d_inc, d_iv)
		matrix M[`r', `c'] = r(mean)
		local ++c
	}
	foreach v of local outcomes {
		quietly summarize pz_`v' [aw = projection_factor] if grp == `g' & !missing(d_inc, d_iv)
		matrix M[`r', `c'] = r(mean)
		local ++c
	}
	local ++r
}
matrix rownames M = "Stayers" "Up-movers" "Down-movers" "Job losers"
matrix colnames M = "N" "Shock" "Income" "Syndigo" "USDA" "HEI" "Syndigo " "USDA " "HEI "

esttab matrix(M, fmt(%12.0fc 2 2 3 3 3 3 3 3)) ///
	using "$overleaf/tabs/labor_supply/movers_groups.tex", ///
	replace booktabs nomtitles nonotes ///
	prehead("\begin{tabular}{l*{9}{c}}" "\toprule" ///
	        " & & & & \multicolumn{3}{c}{Change after, \(t \to t+1\)} & \multicolumn{3}{c}{Change before, \(t-3 \to t-2\)} \\" ///
	        "\cmidrule(lr){5-7} \cmidrule(lr){8-10}")

esttab matrix(M, fmt(%12.0fc 2 2 3 3 3 3 3 3))

// ============================================================================
// Table B: movers only, income change interacted with the move group
// ============================================================================

forvalues g = 1/3 {
	gen d_inc_g`g' = d_inc * (grp == `g')
	gen d_iv_g`g'  = d_iv  * (grp == `g')
}
// \(t\) rather than $t$: "$t" would expand a (nonexistent) global
local lab1 "\(\times\) up-movers"
local lab2 "\(\times\) down-movers"
local lab3 "\(\times\) job losers"

eststo clear
foreach v of local outcomes {
	eststo s_`v': ivreghdfe fz_`v' (d_inc_g1 d_inc_g2 d_inc_g3 = d_iv_g1 d_iv_g2 d_iv_g3) ///
		[pw = projection_factor] if inrange(grp, 1, 3), ///
		absorb(mt_year kids avg_age_hh_head hh_comp) cluster(fips) ffirst
	quietly test d_inc_g1 = d_inc_g2
	estadd scalar p_updown = r(p)
	cap mat drop FS
	mat FS = e(first)
	local rn : rownames FS
	local swrow "SWF"
	if strpos(" `rn' ", " SWF ") == 0 local swrow "APF"
	forvalues g = 1/3 {
		estadd scalar swf`g' = FS[rownumb(FS, "`swrow'"), colnumb(FS, "d_inc_g`g'")]
	}
}

// \\$ emits a literal \$ into the .tex (a single \$ reaches LaTeX as a bare $)
esttab s_hi s_hi_usda s_hei_usda ///
	using "$overleaf/tabs/labor_supply/movers_symmetry.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(d_inc_g1 d_inc_g2 d_inc_g3) order(d_inc_g1 d_inc_g2 d_inc_g3) ///
	refcat(d_inc_g1 "$\Delta$ Income (\\$10k)", nolabel) ///
	varlabels(d_inc_g1 "`lab1'" d_inc_g2 "`lab2'" d_inc_g3 "`lab3'") ///
	mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
	collabels(none) ///
	stats(p_updown swf1 swf2 swf3 N, fmt(%9.2f %9.1fc %9.1fc %9.1fc %9.0fc) ///
	      labels("p-value: up-movers = down-movers" "First-stage F: up-movers" ///
	             "First-stage F: down-movers" "First-stage F: job losers" "N")) ///
	nonotes

esttab s_hi s_hi_usda s_hei_usda, ///
	keep(d_inc_g1 d_inc_g2 d_inc_g3) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syndigo" "USDA" "HEI") ///
	stats(p_updown swf1 swf2 swf3 N, fmt(%9.3f %9.1fc %9.1fc %9.1fc %9.0fc))

// ============================================================================
// Table C: 2SLS by sample under the movers specification (no household FE)
// ============================================================================

local samples emp all stay clean cleanhh main
local cond_emp     "inlist(grp, 1, 2)"
local cond_all     "inrange(grp, 1, 3)"
local cond_stay    "grp == 0"
local cond_clean   "clean == 1 & !missing(mt_year)"
local cond_cleanhh "clean == 1 & !missing(mt_year)"
local cond_main    "clean == 1 & !missing(mt_year)"
local fe_emp     "mt_year kids avg_age_hh_head hh_comp"
local fe_all     "mt_year kids avg_age_hh_head hh_comp"
local fe_stay    "mt_year kids avg_age_hh_head hh_comp"
local fe_clean   "mt_year kids avg_age_hh_head hh_comp"
local fe_cleanhh "mt_year hhid kids avg_age_hh_head hh_comp"
local fe_main    "year hhid kids avg_age_hh_head hh_comp"
local lab_emp     "Movers, employed to employed"
local lab_all     "Movers including job losers"
local lab_stay    "Stayers"
local lab_clean   "All, clean sample"
local lab_cleanhh "All, clean sample, household FE added"
local lab_main    "All, clean sample, main specification"

eststo clear
foreach s of local samples {
	foreach v of local outcomes {
		eststo c`s'_`v': ivreghdfe fz_`v' (d_inc = d_iv) ///
			[pw = projection_factor] if `cond_`s'', ///
			absorb(`fe_`s'') cluster(fips)
		estadd scalar fs_F = e(widstat)
	}
	esttab c`s'_hi c`s'_hi_usda c`s'_hei_usda, ///
		keep(d_inc) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
		mtitles("Syndigo" "USDA" "HEI") title("`lab_`s''") ///
		stats(fs_F N, fmt(%9.0fc %9.0fc))
}

// one table, a block per sample (fragment/append as in 22_; posthead/prefoot
// "%" are comment lines LaTeX ignores, so blocks are separated by one rule)
local file "$overleaf/tabs/labor_supply/movers_2sls.tex"
local i = 0
foreach s of local samples {
	local ++i
	if `i' == 1 {
		esttab c`s'_hi c`s'_hi_usda c`s'_hei_usda using "`file'", ///
			replace fragment booktabs ///
			b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
			keep(d_inc) varlabels(d_inc "`lab_`s''") ///
			refcat(d_inc "$\Delta$ Income (\\$10k), sample:", nolabel) ///
			mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
			collabels(none) nonotes ///
			stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
			prehead("\begin{tabular}{l*{3}{c}}" "\toprule") posthead("\midrule") ///
			prefoot("%") postfoot("\midrule")
	}
	else if `i' < 6 {
		esttab c`s'_hi c`s'_hi_usda c`s'_hei_usda using "`file'", ///
			append fragment booktabs ///
			b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
			keep(d_inc) varlabels(d_inc "`lab_`s''") ///
			nomtitles nonumbers collabels(none) nonotes ///
			stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
			posthead("%") prefoot("%") postfoot("\midrule")
	}
	else {
		esttab c`s'_hi c`s'_hi_usda c`s'_hei_usda using "`file'", ///
			append fragment booktabs ///
			b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
			keep(d_inc) varlabels(d_inc "`lab_`s''") ///
			nomtitles nonumbers collabels(none) nonotes ///
			stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
			posthead("%") prefoot("%") postfoot("\bottomrule" "\end{tabular}")
	}
}

di "25_build_mover_design.do complete"
