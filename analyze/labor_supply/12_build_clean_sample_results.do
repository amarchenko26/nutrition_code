// ============================================================================
// 12_build_clean_sample_results.do -- the core tables of labor_supply.tex
// section 4 on the CLEAN SAMPLE (labor_supply.tex "Sample", 2026-09-16):
//
//   clean = employed shock cell at t-1 (codes 1-6, 8, 9, 11). A retiree is
//           not employed and 67+, so no age rule is needed. Rules use t-1 ONLY: employment/occupation at
//           t is an outcome of the shock and never selects the sample, so job
//           losers and retirees-at-t stay in. Armed forces (7) define no cell.
//
// Shock: ORIGINAL definition only (male-preferred occupation cell,
// iv_income_fips). The hours-weighted working-head shock is retired: each
// head's "own cell" average is HOUSEHOLD income, so a blue-collar wife's
// cell is contaminated by the white-collar husbands of other blue-collar
// wives' households. The male-preferred cell is the primary-breadwinner
// assumption instead.
//
// INCOME TIMING (resolved 2026-09-10): despite Kilts Note 3's official
// wording (income "as of year-end of the previous calendar year", i.e.
// t-2), panelists actually report ANNUALIZED CURRENT income at the fall
// t-1 survey -- Nielsen says so themselves (instructions dropped the
// prior-year wording in 2011), and our event study confirms it (reported
// income moves -18.5 log pts in the SAME file year as an employed->NE
// transition, ~0 two files later). File-t income and file-t occupation are
// both fall-t-1 measurements, so the survey-timed pairing below is the
// contemporaneous one (effective ~1-year lag vs the panel year). Shifting
// income by two years per Note 3 kills the first stage (corr 0.002) --
// see the note's "Validating income timing" subsection. The earned-series
// variables remain in the reg data for that validation only.
//
// Spec everywhere: forward-diff 2SLS, non-movers, [pw=projection_factor],
// absorb(year_state hhid kids avg_age_hh_head hh_comp) for the nutrition tables
// (state-by-year FE since 2026-09-18; 13_/17_/18_/22_ aligned 2026-11-05), absorb(year ...)
// for labor supply; county-clustered SE.
//
// Outputs (tabs/labor_supply/):
//   hi_clean.tex                     nutrition, 3 outcomes
//   pretrend_clean.tex               pre-trend placebos
//   labor_supply_clean.tex           hours + employment outcomes
//   status_transition_matrix_clean.tex  head FT/PT/NE transition matrix
// ============================================================================

clear all

do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// ------------------------------------------------------------ clean-sample flag
// Assigned occupation: the male head's unless he is absent (0/missing) or in
// the armed forces (7), then the female head's (labor_supply.tex
// "Measurement of key variables", 2026-09-16). Employed cell = codes 1-6, 8,
// 9, 11; 10 (students) and 12 are not employed; 7 defines no cell.
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell     = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte not_emp_cell = inlist(occ_src, 10, 12)                    if !missing(occ_src) & !inlist(occ_src, 0, 7)
// SAMPLE (labor_supply.tex "Sample", 2026-09-16). Rules use t-1 ONLY -- status
// at t is an outcome of the shock and never selects the sample: employed cell
// at t-1. A retiree is not employed and 67+, so this already excludes retirees;
// a working 67-year-old stays in.
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)

// ------------------------------------------------- z-scored nutrition outcomes
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

// pre-trend pieces (f_inc may already exist in the reg data)
cap drop f_inc
cap drop f_iv
gen f_inc = F.real_income - real_income
gen f_iv  = F.iv_income_fips - iv_income_fips

// \\$ emits a literal \$ into the .tex (a single \$ reaches LaTeX as a bare
// $ because Stata consumes the backslash)

egen year_state = group(year fips_state_code)

// ============================================================================
// Table 1: nutrition outcomes, clean sample
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo `v': ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

// the estimates the movers figures compare with (analyze/movers 6_ and 8_ read this file)
file open fh using "$idir/labor_supply/iv_main_estimates.csv", write replace
file write fh "outcome,b,se,f,n" _n
foreach v of local outcomes {
	estimates restore `v'
	file write fh "`v'," (_b[D.real_income]) "," (_se[D.real_income]) "," (e(widstat)) "," (e(N)) _n
}
file close fh

esttab hi hi_usda hei_usda ///
	using "$overleaf/tabs/labor_supply/hi_clean.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\\$10k)") ///
	mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab hi hi_usda hei_usda, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syndigo" "USDA" "HEI") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

// ============================================================================
// Table 2: pre-trend placebos
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo p_`v': ivreghdfe D.z_`v' (f_inc = f_iv) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab p_hi p_hi_usda p_hei_usda ///
	using "$overleaf/tabs/labor_supply/pretrend_clean.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(f_inc) varlabels(f_inc "Future $\Delta$ Income (\\$10k)") ///
	mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab p_hi p_hi_usda p_hei_usda, ///
	keep(f_inc) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syndigo" "USDA" "HEI") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

// ============================================================================
// Table 3: labor supply (hours worked, is working), clean sample
// ============================================================================

eststo clear
local lsvars hh_avg_workhours hh_avg_workhours_emp hh_employed
local lslvls hh_avg_workhours hh_avg_workhours_if_employed hh_employed
forvalues j = 1/3 {
	local y : word `j' of `lsvars'
	local lv : word `j' of `lslvls'

	eststo ls_`j': ivreghdfe f_`y' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
	quietly summarize `lv' [aw = projection_factor] if e(sample)
	estadd scalar mean_y = r(mean)
}

esttab ls_1 ls_2 ls_3 ///
	using "$overleaf/tabs/labor_supply/labor_supply_clean.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\\$10k)") ///
	mtitles("Avg weekly hours" "Hours $|$ employed" "Share heads employed") ///
	collabels(none) ///
	stats(mean_y fs_F N, fmt(%9.2fc %9.0fc %9.0fc) labels("Mean outcome" "First-stage F" "N")) ///
	nonotes

esttab ls_1 ls_2 ls_3, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Hours" "Hrs|emp" "Employed") ///
	stats(mean_y fs_F N, fmt(%9.2fc %9.0fc %9.0fc))

// ============================================================================
// Table 4: head FT/PT/NE transition matrix (clean sample, weighted)
//   Rows: status at t-1; columns: status at t (row percentages).
// ============================================================================

foreach h in male female {
	gen byte st_`h' = 1 * (`h'_head_status == "FT") ///
	                + 2 * (`h'_head_status == "PT") ///
	                + 3 * (`h'_head_status == "NE") if `h'_head_status != ""
	replace st_`h' = . if st_`h' == 0
	gen byte Lst_`h' = L.st_`h'
}

file open fh using "$overleaf/tabs/labor_supply/status_transition_matrix_clean.tex", write replace text
file write fh "\begin{tabular}{llcccc}" _n
file write fh "\toprule" _n
file write fh " & & \multicolumn{3}{c}{Status at \(t\)} & \\ \cmidrule(lr){3-5}" _n
file write fh " & Status at \(t-1\) & Full-time & Part-time & Not employed & Share of rows \\" _n
file write fh "\midrule" _n

local rowlab1 "Full-time"
local rowlab2 "Part-time"
local rowlab3 "Not employed"
foreach h in male female {
	local H = cond("`h'" == "male", "Male heads", "Female heads")
	quietly summarize projection_factor if clean == 1 & !missing(Lst_`h') & !missing(st_`h')
	local grand = r(sum)
	forvalues r = 1/3 {
		quietly summarize projection_factor if clean == 1 & Lst_`h' == `r' & !missing(st_`h')
		local rowtot = r(sum)
		local rowsh : di %9.1f 100 * `rowtot' / `grand'
		local rowsh = strtrim("`rowsh'")
		local cells ""
		forvalues c = 1/3 {
			quietly summarize projection_factor if clean == 1 & Lst_`h' == `r' & st_`h' == `c'
			local pct : di %9.1f 100 * r(sum) / `rowtot'
			local pct = strtrim("`pct'")
			local cells "`cells' & `pct'"
		}
		local lead = cond(`r' == 1, "`H'", "")
		file write fh "`lead' & `rowlab`r''`cells' & `rowsh' \\" _n
	}
	if "`h'" == "male" file write fh "\midrule" _n
}
file write fh "\bottomrule" _n
file write fh "\end{tabular}" _n
file close fh

di "12_build_clean_sample_results.do complete"
