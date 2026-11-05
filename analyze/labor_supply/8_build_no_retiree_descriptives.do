// ============================================================================
// Shock descriptives EXCLUDING RETIREES — companion to 7_build_hi_no_retirees.
//
// Retiree defined as in 7_: shock cell is not-employed (occ 10/12) AND avg
// head age >= 62; a household-year is dropped if it is a retiree observation
// at t-1 or t. Remakes, on the non-retiree sample:
//   figs/labor_supply/shock_distribution_no_retirees.png
//   tabs/labor_supply/shock_by_occupation_no_retirees.tex
// (the Not employed row that remains is the young not-employed: unemployed,
// homemakers, students-age households — not retirees)
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year

// Occupation bin of the shock cell (male head, female fallback) — as in 3_
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | occ_src == 0

gen byte occ_bin = .
replace occ_bin = 1 if inrange(occ_src, 1, 4)
replace occ_bin = 2 if inrange(occ_src, 5, 7)
replace occ_bin = 3 if inlist(occ_src, 8, 9, 11)
replace occ_bin = 4 if inlist(occ_src, 10, 12)
label define occ_bin_lbl 1 "White collar" 2 "Blue collar" 3 "Service/other" 4 "Not employed"
label values occ_bin occ_bin_lbl

// Retiree: not-employed shock cell at retirement age (same as 7_)
gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & occ_src != 0
gen byte retiree = not_emp_cell == 1 & avg_age_hh_head >= 62 ///
	if !missing(not_emp_cell) & !missing(avg_age_hh_head)
keep if retiree == 0 & L.retiree == 0

gen byte neg_shock = (d_iv < 0) if !missing(d_iv)

// ============================================================================
// Table: shocks by occupation bin, non-retirees
// ============================================================================

quietly count if !missing(d_iv)
local N_all = r(N)

matrix S = J(5, 5, .)
local r = 1
forvalues g = 1/4 {
	quietly summarize d_iv if occ_bin == `g'
	matrix S[`r',1] = r(N)
	matrix S[`r',2] = 100 * r(N) / `N_all'
	matrix S[`r',3] = r(mean)
	matrix S[`r',4] = r(sd)
	quietly summarize neg_shock if occ_bin == `g'
	matrix S[`r',5] = 100 * r(mean)
	local ++r
}
quietly summarize d_iv
matrix S[5,1] = r(N)
matrix S[5,2] = 100
matrix S[5,3] = r(mean)
matrix S[5,4] = r(sd)
quietly summarize neg_shock
matrix S[5,5] = 100 * r(mean)

matrix rownames S = "White collar" "Blue collar" "Service/other" "Not employed" "Total"
matrix colnames S = "N" "Share" "Mean" "SD" "Share neg."

esttab matrix(S, fmt(%12.0fc 1 3 3 1)) using "$overleaf/tabs/labor_supply/shock_by_occupation_no_retirees.tex", ///
	replace booktabs nomtitles nonotes

esttab matrix(S, fmt(%12.0fc 1 3 3 1))

// ============================================================================
// Figure: distribution of the shock, non-retirees
// ============================================================================

// warm-up export: the first export of a headless batch session can render a
// black canvas; this throwaway absorbs it
quietly twoway scatteri 0 0, msymbol(none)
quietly graph export "`c(tmpdir)'/warmup.png", replace

quietly summarize neg_shock
local pctneg : display %4.1f 100 * r(mean)

histogram d_iv if inrange(d_iv, -1.5, 1.5), percent width(0.05) ///
	xline(0, lcolor(black) lwidth(medthick)) ///
	xtitle("Income shock ($10k)", size(medlarge)) ///
	ytitle("Percent of HH-years", size(medlarge)) ///
	xlabel(-1.5(0.5)1.5, labsize(medlarge)) ylabel(, labsize(medlarge)) ///
	note("`pctneg'% of shocks are negative. Display truncated at ±$15k.", size(medium))

graph export "$overleaf/figs/labor_supply/shock_distribution_no_retirees.png", replace width(2400)
