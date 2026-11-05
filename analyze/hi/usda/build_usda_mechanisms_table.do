// ============================================================================
// build_usda_mechanisms_table.do -- USDA twin of tabs/expenditure.tex (the
// Mechanisms slide table). Mirrors the diet/spend loops in build_hi.do exactly:
// forward-diff IV, non-movers, HH/year/kids/age/composition FE, county-clustered.
//
// Column mapping vs the Syndigo original:
//   Total spend, Produce share  -- classification-free (Nielsen), re-run as-is
//   High sugar                  -- reclassified with USDA labels
//                                  (expenditure_hh_year_usda.dta, built by
//                                   build_expenditure_panel.py usda)
//   Calories, Sugar density     -- total_cals_usda, sugar_per_1000cal_usda
//
// Formatting (redone 2026-09-04, per David):
//   - table is written by hand (file write) so each column gets its own format:
//     produce share carries 2 significant digits, calories no decimals
//   - "Mean outcome" is the LEVEL of the outcome in the estimation sample
//     (weighted), in each column's own units -- NOT the mean forward
//     difference, which is what the old table showed (hence the old negative /
//     zero entries)
//   - adds "Effect, % of mean" = 100*b/mean, a unit-free row comparable
//     across outcomes
//   - produce share rescaled to percent of spend (x100)
//
// Output: tabs/expenditure_usda.tex
// ============================================================================
clear all

global TABS "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/tabs"
global PD   "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset"

// USDA-classified high-sugar spend, keyed to match final_reg_data's renames
use household_code panel_year spend_high_sugar using "$PD/expenditure_hh_year_usda.dta", replace
rename household_code hhid
rename panel_year year
rename spend_high_sugar spend_high_sugar_usda
tempfile usda_spend
save `usda_spend'

use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/final/final_reg_data.dta", replace
merge 1:1 hhid year using `usda_spend', keep(master match) nogen
xtset hhid year

// sodium in mg per 1,000 kcal; produce share in percent of spend
gen sodium_mg_1000cal_usda = 1000 * sodium_per_1000cal_usda
gen produce_pct = 100 * spend_share_produce

local vars spend_total spend_high_sugar_usda produce_pct total_cals_usda sugar_per_1000cal_usda sodium_mg_1000cal_usda
foreach var of local vars {
	cap drop f_`var'
	gen f_`var' = F.`var' - `var'
}

// ---- run the six IV regressions, collect b / se / p / N / level mean ----
local i = 0
foreach var of local vars {
	local ++i
	quietly ivreghdfe f_`var' (D.real_income=D.iv_income_fips) ///
		[pw = projection_factor] if movers_f == 0, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
	local b`i'  = _b[D.real_income]
	local se`i' = _se[D.real_income]
	local p`i'  = 2 * normal(-abs(`b`i'' / `se`i''))
	local n`i'  = e(N)
	quietly summarize `var' [aw = projection_factor] if e(sample)
	local m`i'   = r(mean)
	local pct`i' = 100 * `b`i'' / `m`i''
	di "`var': b=`b`i'' se=`se`i'' p=`p`i'' mean=`m`i'' pct=`pct`i'' N=`n`i''"
}

// ---- per-column number formats ----
// b/se formats: spend cols 1 decimal, produce 2 significant digits (dynamic),
// calories 0 decimals w/ commas, sugar 2 decimals, sodium 1 decimal
local decb  = max(0, 1 - floor(log10(abs(`b3'))))
local decse = max(0, 1 - floor(log10(abs(`se3'))))
local dec3  = max(`decb', `decse')
local fmt1 "%12.1f"
local fmt2 "%12.1f"
local fmt3 "%12.`dec3'f"
local fmt4 "%12.0fc"
local fmt5 "%12.2f"
local fmt6 "%12.1f"
// level-mean formats
local mfmt1 "%12.0fc"
local mfmt2 "%12.0fc"
local mfmt3 "%12.1f"
local mfmt4 "%12.0fc"
local mfmt5 "%12.1f"
local mfmt6 "%12.0fc"

forvalues i = 1/6 {
	local bs`i'  : di `fmt`i'' `b`i''
	local bs`i'  = strtrim("`bs`i''")
	local ses`i' : di `fmt`i'' `se`i''
	local ses`i' = strtrim("`ses`i''")
	local ms`i'  : di `mfmt`i'' `m`i''
	local ms`i'  = strtrim("`ms`i''")
	local ns`i'  : di %12.0fc `n`i''
	local ns`i'  = strtrim("`ns`i''")
	local ps`i'  : di %12.2f `pct`i''
	local ps`i'  = strtrim("`ps`i''")
	local st`i'  = cond(`p`i'' < .01, "\sym{***}", ///
	               cond(`p`i'' < .05, "\sym{**}",  ///
	               cond(`p`i'' < .10, "\sym{*}", "")))
}

// ---- write the LaTeX fragment ----
file open fh using "$TABS/expenditure_usda.tex", write replace text
file write fh "{" _n
file write fh "\def\sym#1{\ifmmode^{#1}\else\(^{#1}\)\fi}" _n
file write fh "\begin{tabular}{l*{6}{c}}" _n
file write fh "\toprule" _n
file write fh "            &\multicolumn{3}{c}{Expenditure}&\multicolumn{3}{c}{Nutrition components}\\\cmidrule(lr){2-4}\cmidrule(lr){5-7}" _n
file write fh "            &\multicolumn{1}{c}{(1)}&\multicolumn{1}{c}{(2)}&\multicolumn{1}{c}{(3)}&\multicolumn{1}{c}{(4)}&\multicolumn{1}{c}{(5)}&\multicolumn{1}{c}{(6)}\\" _n
file write fh "            &Total&High sugar&Produce share&Calories&Sugar density&Sodium density\\" _n
file write fh "            &(\\$/year)&(\\$/year)&(\% of spend)&(kcal/year)&(g/1{,}000 kcal)&(mg/1{,}000 kcal)\\" _n
file write fh "\midrule" _n
file write fh "\(m_t\)&`bs1'`st1'&`bs2'`st2'&`bs3'`st3'&`bs4'`st4'&`bs5'`st5'&`bs6'`st6'\\" _n
file write fh "            &(`ses1')&(`ses2')&(`ses3')&(`ses4')&(`ses5')&(`ses6')\\" _n
file write fh "\midrule" _n
file write fh "Effect, \% of mean&`ps1'\%&`ps2'\%&`ps3'\%&`ps4'\%&`ps5'\%&`ps6'\%\\" _n
file write fh "Mean outcome&`ms1'&`ms2'&`ms3'&`ms4'&`ms5'&`ms6'\\" _n
file write fh "N&`ns1'&`ns2'&`ns3'&`ns4'&`ns5'&`ns6'\\" _n
file write fh "\bottomrule" _n
file write fh "\end{tabular}" _n
file write fh "}" _n
file close fh

di "build_usda_mechanisms_table.do complete"
