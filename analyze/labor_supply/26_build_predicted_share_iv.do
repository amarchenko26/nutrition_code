// ============================================================================
// 26_build_predicted_share_iv.do -- the PREDICTED-SHARE instrument on the
// clean sample (Anya's advisor, planned 2026-09-17, built 2026-09-29).
//
// The production shock d_iv = D.iv_income_fips/10 uses the household's
// REALIZED cell at t, so 88% of its variance is cell switching that happens
// at t (job loss, retirement, a child leaving). build/iv/build_iv_predicted.py
// replaces the realized year-t cell with the out-of-fold transition
// probabilities of a 96-group table (origin occupation bin x origin size bin
// x cell-defining head's age bin, all at t-1; education held at t-1):
//   d_iv_pred      = sum_c P(c | t-1) z_c(t) - z[own cell](t-1)
//                  = d_iv_frozen + d_iv_expswitch
//   d_iv_frozen    = z[t-1 cell](t) - z[t-1 cell](t-1)     within-cell shift
//   d_iv_expswitch = sum_c P(c | t-1) z_c(t) - z[t-1 cell](t)   expected switch
// All in $10k here (the builder writes $1,000s, like iv_income_fips).
//
// Sample and spec exactly as in 12_: non-movers, clean = employed cell at t-1
// (t-1 only), [pw = projection_factor], absorb(year_state hhid kids
// avg_age_hh_head hh_comp) for nutrition, absorb(year ...) for labor supply,
// county-clustered SE, outcomes z-scored, forward change t -> t+1.
//
// Outputs (tabs/labor_supply/):
//   predicted_shares_descriptives.tex  SDs / correlations of the shocks and
//                                      the variance split of d_iv_pred
//   first_stage_predicted.tex          D.real_income on each shock
//   hi_predicted.tex                   nutrition: current vs predicted shock
//   hi_predicted_variants.tex          predicted shock: + origin-group FE;
//                                      heads under 62 at t-1; each piece alone
//   pretrend_predicted.tex             pre-trend placebo, predicted shock
//   labor_supply_predicted.tex         hours / employment, predicted shock
// (predicted_shares_fit.tex, the out-of-fold fit of the transition table, is
//  written by the Python builder.)
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/final/labor_supply_reg_data.dta", clear

ren hhid household_code
ren year panel_year
merge 1:1 household_code panel_year using "$data/interim/panel_dataset/iv_predicted.dta", ///
	keepusing(d_iv_pred d_iv_frozen d_iv_expswitch iv_own_lag ///
	          p_stay p_switch p_notemp grp grp_n_train defined_mass) ///
	keep(master match)
drop _merge
ren household_code hhid
ren panel_year year

xtset hhid year
keep if movers_f == 0

// ------------------------------------------------------------ clean-sample flag
// exactly as in 12_ (labor_supply.tex "Measurement of key variables" / "Sample")
gen occ_src = male_head_occupation
replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
gen byte emp_cell = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)

// ------------------------------------------------------------ shocks in $10k
foreach v in d_iv_pred d_iv_frozen d_iv_expswitch {
	replace `v' = `v' / 10
}
label var d_iv           "Current shock (realized cell at \(t\))"
label var d_iv_pred      "Predicted-share shock"
label var d_iv_frozen    "Within-cell piece"
label var d_iv_expswitch "Expected-switch piece"

// validation: the builder's t-1 own-cell income is the production IV lagged,
// and the two pieces add up to the predicted shock
gen _chk1 = abs(iv_own_lag - L.iv_income_fips)
gen _chk2 = abs(d_iv_pred - d_iv_frozen - d_iv_expswitch)
quietly summarize _chk1
assert r(max) < 1e-6
quietly summarize _chk2
assert r(max) < 1e-6
drop _chk1 _chk2

// origin group (occupation x size x head-age bin at t-1) as share controls
egen grp_id = group(grp)

// ------------------------------------------------- z-scored nutrition outcomes
local outcomes hi hi_usda hei_usda
foreach v of local outcomes {
	quietly summarize `v' [aw = projection_factor]
	gen z_`v'  = (`v' - r(mean)) / r(sd)
	gen fz_`v' = F.z_`v' - z_`v'
}

cap drop f_inc
gen f_inc     = F.real_income - real_income
gen f_iv_pred = F.d_iv_pred          // the predicted shock at t+1 (t information)

egen year_state = group(year fips_state_code)

// ============================================================================
// Table 1: descriptives of the shocks on the clean sample (weighted)
// ============================================================================

local shocks d_iv d_iv_pred d_iv_frozen d_iv_expswitch
quietly correlate `shocks' [aw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred, D.real_income)
matrix C = r(C)
local N = r(N)
foreach v of local shocks {
	quietly summarize `v' [aw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred, D.real_income)
	local sd_`v' : di %5.3f r(sd)
	local var_`v' = r(Var)
}
quietly correlate d_iv_frozen d_iv_expswitch [aw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred, D.real_income), cov
local cov_fe = r(C)[2,1]
local sh_frozen : di %4.1f 100 * `var_d_iv_frozen'    / `var_d_iv_pred'
local sh_expsw  : di %4.1f 100 * `var_d_iv_expswitch' / `var_d_iv_pred'
local sh_cov    : di %4.1f 100 * 2 * `cov_fe'         / `var_d_iv_pred'
quietly summarize p_switch [aw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred, D.real_income)
local p_switch : di %4.1f 100 * r(mean)
quietly summarize p_notemp [aw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred, D.real_income)
local p_notemp : di %4.1f 100 * r(mean)

local lab_d_iv           "Current shock (realized cell at \(t\))"
local lab_d_iv_pred      "Predicted-share shock"
local lab_d_iv_frozen    "\quad Within-cell piece"
local lab_d_iv_expswitch "\quad Expected-switch piece"

file open fh using "$overleaf/tabs/labor_supply/predicted_shares_descriptives.tex", write replace text
file write fh "\begin{tabular}{lccc}" _n
file write fh "\toprule" _n
file write fh " & SD (\(\$\)10k) & Corr. with current & Corr. with predicted \\" _n
file write fh "\midrule" _n
local i = 0
foreach v of local shocks {
	local ++i
	local c1 : di %5.3f C[`i', 1]
	local c2 : di %5.3f C[`i', 2]
	file write fh "`lab_`v'' & `sd_`v'' & `c1' & `c2' \\" _n
}
file write fh "\midrule" _n
file write fh "\multicolumn{4}{l}{Variance of the predicted-share shock: within-cell `sh_frozen'\%, expected-switch `sh_expsw'\%, covariance `sh_cov'\%} \\" _n
file write fh "\multicolumn{4}{l}{Mean predicted probability of switching cell `p_switch'\%; of moving to the not-employed cell `p_notemp'\%} \\" _n
file write fh "\multicolumn{4}{l}{Household-years: " %9.0fc (`N') "} \\" _n
file write fh "\bottomrule" _n
file write fh "\end{tabular}" _n
file close fh

// ============================================================================
// Table 2: first stages -- D.real_income on each shock
// ============================================================================

eststo clear
local j = 0
foreach v of local shocks {
	local ++j
	eststo fs`j': reghdfe D.real_income `v' [pw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred), ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = (_b[`v'] / _se[`v'])^2
}
eststo fs5: reghdfe D.real_income d_iv_frozen d_iv_expswitch [pw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred), ///
	absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
test d_iv_frozen d_iv_expswitch
estadd scalar fs_F = r(F)
eststo fs6: reghdfe D.real_income d_iv_pred [pw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred), ///
	absorb(year_state hhid kids avg_age_hh_head hh_comp grp_id) cluster(fips)
estadd scalar fs_F = (_b[d_iv_pred] / _se[d_iv_pred])^2

esttab fs1 fs2 fs3 fs4 fs5 fs6 ///
	using "$overleaf/tabs/labor_supply/first_stage_predicted.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(`shocks') order(`shocks') ///
	varlabels(d_iv "`lab_d_iv'" d_iv_pred "`lab_d_iv_pred'" ///
	          d_iv_frozen "Within-cell piece" d_iv_expswitch "Expected-switch piece") ///
	mtitles("Current" "Predicted" "Within-cell" "Exp. switch" "Both pieces" "Predicted") ///
	mgroups("" "+ origin-group FE", pattern(1 0 0 0 0 1) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.1fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab fs1 fs2 fs3 fs4 fs5 fs6, keep(`shocks') b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Current" "Pred" "Frozen" "ExpSw" "Both" "Pred+gFE") stats(fs_F N, fmt(%9.1fc %9.0fc))

// ============================================================================
// Table 3: nutrition, current vs predicted shock, clean sample
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo c_`v': ivreghdfe fz_`v' (D.real_income = d_iv) ///
		[pw = projection_factor] if clean == 1 & !missing(d_iv_pred), ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
	eststo p_`v': ivreghdfe fz_`v' (D.real_income = d_iv_pred) ///
		[pw = projection_factor] if clean == 1 & !missing(d_iv), ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab c_hi p_hi c_hi_usda p_hi_usda c_hei_usda p_hei_usda ///
	using "$overleaf/tabs/labor_supply/hi_predicted.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\\$10k)") ///
	mtitles("Current" "Predicted" "Current" "Predicted" "Current" "Predicted") ///
	mgroups("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)", pattern(1 0 1 0 1 0) ///
	        prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.1fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab c_hi p_hi c_hi_usda p_hi_usda c_hei_usda p_hei_usda, ///
	keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syn cur" "Syn pred" "USDA cur" "USDA pred" "HEI cur" "HEI pred") ///
	stats(fs_F N, fmt(%9.1fc %9.0fc))

// ============================================================================
// Table 4: variants of the predicted shock (fragment/append blocks, as in 22_)
//   g   + origin-group FE (occupation x size x head-age bin at t-1)
//   u   heads under 62 at t-1 (average head age; the retirement path off)
//   f   within-cell piece alone as the instrument
//   e   expected-switch piece alone as the instrument
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo g_`v': ivreghdfe fz_`v' (D.real_income = d_iv_pred) ///
		[pw = projection_factor] if clean == 1 & !missing(d_iv), ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp grp_id) cluster(fips)
	estadd scalar fs_F = e(widstat)
	eststo u_`v': ivreghdfe fz_`v' (D.real_income = d_iv_pred) ///
		[pw = projection_factor] if clean == 1 & !missing(d_iv) & L.avg_age_hh_head < 62, ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
	eststo f_`v': ivreghdfe fz_`v' (D.real_income = d_iv_frozen) ///
		[pw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred), ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
	eststo e_`v': ivreghdfe fz_`v' (D.real_income = d_iv_expswitch) ///
		[pw = projection_factor] if clean == 1 & !missing(d_iv, d_iv_pred), ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

local blocks g u f e
local lab_g "Predicted shock, + origin-group FE"
local lab_u "Predicted shock, heads under 62 at \(t-1\)"
local lab_f "Within-cell piece only"
local lab_e "Expected-switch piece only"

foreach k of local blocks {
	esttab `k'_hi `k'_hi_usda `k'_hei_usda, keep(D.real_income) b(4) se(4) ///
		star(* 0.10 ** 0.05 *** 0.01) mtitles("Syndigo" "USDA" "HEI") title("`lab_`k''") ///
		stats(fs_F N, fmt(%9.1fc %9.0fc))
}

local file "$overleaf/tabs/labor_supply/hi_predicted_variants.tex"
local i = 0
foreach k of local blocks {
	local ++i
	if `i' == 1 {
		esttab `k'_hi `k'_hi_usda `k'_hei_usda using "`file'", ///
			replace fragment booktabs ///
			b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
			keep(D.real_income) varlabels(D.real_income "`lab_`k''") ///
			mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
			collabels(none) nonotes ///
			stats(fs_F N, fmt(%9.1fc %9.0fc) labels("First-stage F" "N")) ///
			prehead("\begin{tabular}{l*{3}{c}}" "\toprule") posthead("\midrule") ///
			prefoot("%") postfoot("\midrule")
	}
	else if `i' < 4 {
		esttab `k'_hi `k'_hi_usda `k'_hei_usda using "`file'", ///
			append fragment booktabs ///
			b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
			keep(D.real_income) varlabels(D.real_income "`lab_`k''") ///
			nomtitles nonumbers collabels(none) nonotes ///
			stats(fs_F N, fmt(%9.1fc %9.0fc) labels("First-stage F" "N")) ///
			posthead("%") prefoot("%") postfoot("\midrule")
	}
	else {
		esttab `k'_hi `k'_hi_usda `k'_hei_usda using "`file'", ///
			append fragment booktabs ///
			b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
			keep(D.real_income) varlabels(D.real_income "`lab_`k''") ///
			nomtitles nonumbers collabels(none) nonotes ///
			stats(fs_F N, fmt(%9.1fc %9.0fc) labels("First-stage F" "N")) ///
			posthead("%") prefoot("%") postfoot("\bottomrule" "\end{tabular}")
	}
}

// ============================================================================
// Table 5: pre-trend placebo with the predicted shock (as in 12_ Table 2)
// ============================================================================

eststo clear
foreach v of local outcomes {
	eststo pt_`v': ivreghdfe D.z_`v' (f_inc = f_iv_pred) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year_state hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
}

esttab pt_hi pt_hi_usda pt_hei_usda ///
	using "$overleaf/tabs/labor_supply/pretrend_predicted.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(f_inc) varlabels(f_inc "Future $\Delta$ Income (\\$10k)") ///
	mtitles("Simple (Syndigo)" "Simple (USDA)" "Full HEI (USDA)") ///
	collabels(none) ///
	stats(fs_F N, fmt(%9.1fc %9.0fc) labels("First-stage F" "N")) ///
	nonotes

esttab pt_hi pt_hi_usda pt_hei_usda, keep(f_inc) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Syndigo" "USDA" "HEI") stats(fs_F N, fmt(%9.1fc %9.0fc))

// ============================================================================
// Table 6: labor supply with the predicted shock (as in 12_ Table 3)
// ============================================================================

eststo clear
local lsvars hh_avg_workhours hh_avg_workhours_emp hh_employed
local lslvls hh_avg_workhours hh_avg_workhours_if_employed hh_employed
forvalues j = 1/3 {
	local y : word `j' of `lsvars'
	local lv : word `j' of `lslvls'
	eststo ls_`j': ivreghdfe f_`y' (D.real_income = d_iv_pred) ///
		[pw = projection_factor] if clean == 1, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	estadd scalar fs_F = e(widstat)
	quietly summarize `lv' [aw = projection_factor] if e(sample)
	estadd scalar mean_y = r(mean)
}

esttab ls_1 ls_2 ls_3 ///
	using "$overleaf/tabs/labor_supply/labor_supply_predicted.tex", ///
	replace booktabs ///
	b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
	keep(D.real_income) varlabels(D.real_income "$\Delta$ Income (\\$10k)") ///
	mtitles("Avg weekly hours" "Hours $|$ employed" "Share heads employed") ///
	collabels(none) ///
	stats(mean_y fs_F N, fmt(%9.2fc %9.1fc %9.0fc) labels("Mean outcome" "First-stage F" "N")) ///
	nonotes

esttab ls_1 ls_2 ls_3, keep(D.real_income) b(4) se(4) star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("Hours" "Hrs|emp" "Employed") stats(mean_y fs_F N, fmt(%9.2fc %9.1fc %9.0fc))

di "26_build_predicted_share_iv.do complete"
