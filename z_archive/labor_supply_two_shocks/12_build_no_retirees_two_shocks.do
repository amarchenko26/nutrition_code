// ============================================================================
// 12_build_no_retirees_two_shocks.do -- the no-retirees core of the labor
// supply write-up (Anya 2026-09-04: "drop retirees from the entire analysis"),
// with every main object re-made under two shock definitions:
//
//   (O) "old" shock: male-preferred occupation cell    (iv_income_fips)
//   (W) working-head shock, each head weighted by their share of the
//       household's t-1 hours (as in 11_: I work 10 hrs, spouse works 30
//       -> my weight .25; one working head -> weight 1; none -> no shock)
//
// Retiree (as in 7_): not-employed shock cell (occ 10/12) AND avg head age
// >= 62. A household-year enters the sample only if not-retiree at t-1 and t.
// Spec everywhere: forward-diff 2SLS, non-movers, [pw=projection_factor],
// absorb(year hhid kids avg_age_hh_head hh_comp), county-clustered SE.
//
// Outputs (tabs/labor_supply/):
//   hi_no_retirees_two_shocks.tex        nutrition (3 outcomes x 2 shocks)
//   pretrend_no_retirees_two_shocks.tex  pre-trend placebos, same layout
//   labor_supply_no_retirees.tex         hours + employment (3 outcomes x 2 shocks)
//   status_transition_matrix.tex         head FT/PT/NE year-over-year
//                                        transition matrix (no-retirees sample)
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition"

use "$data/final/labor_supply_reg_data.dta", clear
xtset hhid year
keep if movers_f == 0

// ---------------------------------------------------------------- retiree flag
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | occ_src == 0
gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & occ_src != 0
gen byte retiree = not_emp_cell == 1 & avg_age_hh_head >= 62 ///
	if !missing(not_emp_cell) & !missing(avg_age_hh_head)
gen byte no_retiree = (retiree == 0 & L.retiree == 0) ///
	if !missing(retiree) & !missing(L.retiree)

// ------------------------------------------- hours-weighted working-head IV (W)
foreach h in male female {
	gen `h'_hours_bin = cond(`h'_head_employment == 1, 20, ///
	                   cond(`h'_head_employment == 2, 32, ///
	                   cond(`h'_head_employment == 3, 40, ///
	                   cond(`h'_head_employment == 9, 0, .))))
}
gen w_m = L.male_hours_bin / (L.male_hours_bin + L.female_hours_bin) ///
	if male_works == 1 & female_works == 1 ///
	 & !missing(L.male_hours_bin) & !missing(L.female_hours_bin) ///
	 & (L.male_hours_bin + L.female_hours_bin) > 0
replace w_m = 0.5 if male_works == 1 & female_works == 1 & missing(w_m)

gen iv_income_whrs_fips = .
replace iv_income_whrs_fips = iv_income_male_fips   if male_works == 1 & female_works != 1
replace iv_income_whrs_fips = iv_income_female_fips if female_works == 1 & male_works != 1
replace iv_income_whrs_fips = w_m * iv_income_male_fips + (1 - w_m) * iv_income_female_fips ///
	if male_works == 1 & female_works == 1
label var iv_income_whrs_fips "Working-head cell income IV, t-1 hours-share weighted"

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
cap drop f_iv_whrs
gen f_inc     = F.real_income - real_income
gen f_iv      = F.iv_income_fips - iv_income_fips
gen f_iv_whrs = F.iv_income_whrs_fips - iv_income_whrs_fips

// ============================================================================
// Table 1: nutrition outcomes, no retirees, old vs hours-weighted shock
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo `v'_o: ivreghdfe fz_`v' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo `v'_w: ivreghdfe fz_`v' (D.real_income = D.iv_income_whrs_fips) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab hi_o hi_w hi_usda_o hi_usda_w hei_usda_o hei_usda_w ///
	using "$overleaf/tabs/labor_supply/hi_no_retirees_two_shocks.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\$10k)") ///
	mtitles("Original" "Hrs-wt." "Original" "Hrs-wt." "Original" "Hrs-wt.") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab hi_o hi_w hi_usda_o hi_usda_w hei_usda_o hei_usda_w, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn-O" "Syn-W" "USDA-O" "USDA-W" "HEI-O" "HEI-W") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

// ============================================================================
// Table 2: pre-trend placebos, same layout
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo p_`v'_o: ivreghdfe D.z_`v' (f_inc = f_iv) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)

	eststo p_`v'_w: ivreghdfe D.z_`v' (f_inc = f_iv_whrs) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab p_hi_o p_hi_w p_hi_usda_o p_hi_usda_w p_hei_usda_o p_hei_usda_w ///
	using "$overleaf/tabs/labor_supply/pretrend_no_retirees_two_shocks.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(f_inc) varlabels(f_inc "Future $\Delta$ Income (\$10k)") ///
	mtitles("Original" "Hrs-wt." "Original" "Hrs-wt." "Original" "Hrs-wt.") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.0fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab p_hi_o p_hi_w p_hi_usda_o p_hi_usda_w p_hei_usda_o p_hei_usda_w, ///
	keep(f_inc) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn-O" "Syn-W" "USDA-O" "USDA-W" "HEI-O" "HEI-W") ///
	stats(fs_F N, fmt(%9.0fc %9.0fc))

// ============================================================================
// Table 3: labor supply (hours worked, is working), no retirees, both shocks
// ============================================================================

eststo clear
local lsvars hh_avg_workhours hh_avg_workhours_emp hh_employed
local lslvls hh_avg_workhours hh_avg_workhours_if_employed hh_employed
forvalues j = 1/3 {
	local y : word `j' of `lsvars'
	local lv : word `j' of `lslvls'

	eststo ls_`j'_o: ivreghdfe f_`y' (D.real_income = D.iv_income_fips) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
	quietly summarize `lv' [aw = projection_factor] if e(sample)
	estadd scalar mean_y = r(mean)

	eststo ls_`j'_w: ivreghdfe f_`y' (D.real_income = D.iv_income_whrs_fips) ///
		[pw = projection_factor] if no_retiree == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
	quietly summarize `lv' [aw = projection_factor] if e(sample)
	estadd scalar mean_y = r(mean)
}

esttab ls_1_o ls_1_w ls_2_o ls_2_w ls_3_o ls_3_w ///
	using "$overleaf/tabs/labor_supply/labor_supply_no_retirees.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\$10k)") ///
	mtitles("Original" "Hrs-wt." "Original" "Hrs-wt." "Original" "Hrs-wt.") ///
	mgroups("Avg weekly hours" "Hours $|$ employed" "Share heads employed", pattern(1 0 1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(mean_y fs_F N, fmt(%9.2fc %9.0fc %9.0fc) labels("Mean outcome" "First-stage F" "N")) ///
	nonotes

esttab ls_1_o ls_1_w ls_2_o ls_2_w ls_3_o ls_3_w, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Hrs-O" "Hrs-W" "HrsE-O" "HrsE-W" "Emp-O" "Emp-W") ///
	stats(mean_y fs_F N, fmt(%9.2fc %9.0fc %9.0fc))

// ============================================================================
// Table 4: head FT/PT/NE transition matrix (no-retirees sample, weighted)
//   Rows: status at t-1; columns: status at t (row percentages). Heads must
//   exist in both years. "Share of rows" = weighted share of head-years in
//   each origin state.
// ============================================================================

foreach h in male female {
	gen byte st_`h' = 1 * (`h'_head_status == "FT") ///
	                + 2 * (`h'_head_status == "PT") ///
	                + 3 * (`h'_head_status == "NE") if `h'_head_status != ""
	replace st_`h' = . if st_`h' == 0
	gen byte Lst_`h' = L.st_`h'
}

file open fh using "$overleaf/tabs/labor_supply/status_transition_matrix.tex", write replace text
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
	// grand total for origin-state shares
	quietly summarize projection_factor if no_retiree == 1 & !missing(Lst_`h') & !missing(st_`h')
	local grand = r(sum)
	forvalues r = 1/3 {
		quietly summarize projection_factor if no_retiree == 1 & Lst_`h' == `r' & !missing(st_`h')
		local rowtot = r(sum)
		local rowsh : di %9.1f 100 * `rowtot' / `grand'
		local rowsh = strtrim("`rowsh'")
		local cells ""
		forvalues c = 1/3 {
			quietly summarize projection_factor if no_retiree == 1 & Lst_`h' == `r' & st_`h' == `c'
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

di "12_build_no_retirees_two_shocks.do complete"
