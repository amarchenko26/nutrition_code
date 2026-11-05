// ============================================================================
// 16_build_clean_shock_descriptives.do -- shock descriptives on the CLEAN
// SAMPLE (see 12_: employed shock cell at t-1). The only shock descriptives
// in labor_supply.tex: the full-sample versions (3_build_shock_descriptives.do)
// were retired 2026-09-17 because the not-employed cell is no longer a
// state a household can start in.
//
// Outputs:
//   tabs/labor_supply/shock_by_occupation_clean.tex
//   figs/labor_supply/shock_distribution_clean.png
//   figs/labor_supply/shock_density_by_occupation_clean.png
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year

// Occupation bin of the shock cell (male head, female fallback) — as in 3_
// Assigned occupation and SAMPLE exactly as in 12_ (labor_supply.tex
// "Measurement of key variables" / "Sample", 2026-09-16): male head unless
// absent or armed forces (7), then female; employed cell = 1-6, 8, 9, 11;
// clean = employed cell at t-1 (t-1 only). A retiree is not employed and 67+, so the
// employment rule already excludes retirees and a working 67-year-old stays in.
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell     = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte not_emp_cell = inlist(occ_src, 10, 12)                    if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)

// occupation bins at t = the IV cell bins (build_iv.OCC_MAP); armed forces has none.
// On the clean sample the "Not employed" row is households that left employment at t.
gen byte occ_bin = .
replace occ_bin = 1 if inrange(occ_src, 1, 4)
replace occ_bin = 2 if inlist(occ_src, 5, 6, 11)
replace occ_bin = 3 if inlist(occ_src, 8, 9)
replace occ_bin = 4 if inlist(occ_src, 10, 12)
label define occ_bin_lbl 1 "White collar" 2 "Blue collar" 3 "Service/other" 4 "Not employed"
label values occ_bin occ_bin_lbl

keep if clean == 1

gen byte neg_shock = (d_iv < 0) if !missing(d_iv)

// ============================================================================
// Table: shocks by occupation bin, clean sample
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

esttab matrix(S, fmt(%12.0fc 1 3 3 1)) using "$overleaf/tabs/labor_supply/shock_by_occupation_clean.tex", ///
	replace booktabs nomtitles nonotes

esttab matrix(S, fmt(%12.0fc 1 3 3 1))

// ============================================================================
// Figure: distribution of the shock, clean sample
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

graph export "$overleaf/figs/labor_supply/shock_distribution_clean.png", replace width(2400)

// ============================================================================
// Figure: shock density by occupation bin at t, clean sample. Employed bins
// only: the not-employed bin at t is the job leavers, whose shock is the cell
// switch itself (see the table above).
// ============================================================================

twoway (kdensity d_iv if occ_bin == 1 & inrange(d_iv, -1.5, 1.5), lwidth(thick)) ///
       (kdensity d_iv if occ_bin == 2 & inrange(d_iv, -1.5, 1.5), lwidth(thick)) ///
       (kdensity d_iv if occ_bin == 3 & inrange(d_iv, -1.5, 1.5), lwidth(thick)), ///
	xline(0, lcolor(gray)) ///
	xtitle("Income shock ($10k)", size(medlarge)) ytitle("Density", size(medlarge)) ///
	legend(order(1 "White collar" 2 "Blue collar" 3 "Service/other") ///
	       rows(1) position(6) size(medlarge)) ///
	xlabel(-1.5(0.5)1.5, labsize(medlarge)) ylabel(, labsize(medlarge))

graph export "$overleaf/figs/labor_supply/shock_density_by_occupation_clean.png", replace width(2400)

di "16_build_clean_shock_descriptives.do complete"
