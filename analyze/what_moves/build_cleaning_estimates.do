// ============================================================================
// build_cleaning_estimates.do -- the main income estimates on the nutrition measures before and after the
// cleaning of 2026-11-05, for the Data cleaning section of 0_main.tex (2026-11-06).
//
// Same regression, same sample rules and same income and instrument variables on two versions of the outcomes:
//   before: z_backup_2026-11-05/final/labor_supply_reg_data.dta (the panels as of 2026-09-16, with the 2009 step in HEI)
//   after : final/labor_supply_reg_data.dta
// and, for HEI only, the panel of 2026-11-04 (fatty-acid fix done, the rest not yet) merged onto the before file.
// Rows follow analyze/labor_supply 12_, 13_, 18_: main change after; test on purchases t-3 to t-2; without the instrument;
// households not employed at t-1 under 67; level with household fixed effects.
// Outcomes in standard deviations of each version.
// Output: interim/what_moves/cleaning_estimates.csv
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
global OUTCSV "$idir/what_moves/cleaning_estimates.csv"
cap file close fh
file open fh using "$OUTCSV", write replace
file write fh "version,outcome,row,b,se,f,n,sd" _n
file close fh
cap program drop wr
program define wr
	args version outcome row b se f n sd
	file open fh using "$OUTCSV", write append
	file write fh "`version',`outcome',`row',`b',`se',`f',`n',`sd'" _n
	file close fh
end
tempfile nov4
pq use household_code panel_year hei_usda using "$data/z_backup_2026-11-05/interim_panel_dataset/hei_usda_panel.parquet", clear
ren (household_code panel_year hei_usda) (hhid year hei_nov4)
save `nov4'

foreach version in before after {
	if "`version'" == "before" {
		use "$data/z_backup_2026-11-05/final/labor_supply_reg_data.dta", clear
		merge 1:1 hhid year using `nov4', keep(master match) nogen
		local outs hi hi_usda hei_usda hei_nov4
	}
	else {
		use "$gdir/labor_supply_reg_data.dta", clear
		local outs hi hi_usda hei_usda
	}
	xtset hhid year
	keep if movers_f == 0
	gen occ_src = male_head_occupation
	replace occ_src = female_head_occupation if missing(occ_src) | inlist(occ_src, 0, 7)
	gen byte emp_cell = inlist(occ_src, 1, 2, 3, 4, 5, 6, 8, 9, 11) if !missing(occ_src) & !inlist(occ_src, 0, 7)
	gen byte not_emp_cell = inlist(occ_src, 10, 12) if !missing(occ_src) & !inlist(occ_src, 0, 7)
	gen byte clean = (L.emp_cell == 1) if !missing(L.emp_cell)
	gen byte unemp = (L.not_emp_cell == 1 & L.avg_age_hh_head < 67) if !missing(L.not_emp_cell) & !missing(L.avg_age_hh_head)
	egen year_state = group(year fips_state_code)
	foreach v of local outs {
		quietly summarize `v' [aw = projection_factor]
		local sd = r(sd)
		gen double z = (`v' - r(mean)) / r(sd)
		gen double fz = F.z - z
		gen double bz = L2.z - L3.z
		gen double lz = F.z
		local FE "year_state hhid kids avg_age_hh_head hh_comp"
		quietly ivreghdfe fz (D.real_income = D.iv_income_fips) [pw = projection_factor] if clean == 1, absorb(`FE') cluster(fips)
		wr `version' `v' main `=_b[D.real_income]' `=_se[D.real_income]' `=e(widstat)' `=e(N)' `sd'
		di "`version' `v' main: " %7.4f _b[D.real_income] " (" %6.4f _se[D.real_income] ") F " %5.0f e(widstat)
		gen byte smp = e(sample)
		quietly reghdfe fz D.real_income [pw = projection_factor] if smp == 1, absorb(`FE') cluster(fips)
		wr `version' `v' ols `=_b[D.real_income]' `=_se[D.real_income]' . `=e(N)' `sd'
		di "`version' `v' without instrument: " %7.4f _b[D.real_income] " (" %6.4f _se[D.real_income] ")"
		quietly ivreghdfe bz (D.real_income = D.iv_income_fips) [pw = projection_factor] if clean == 1, absorb(`FE') cluster(fips)
		wr `version' `v' before `=_b[D.real_income]' `=_se[D.real_income]' `=e(widstat)' `=e(N)' `sd'
		di "`version' `v' before: " %7.4f _b[D.real_income] " (" %6.4f _se[D.real_income] ")"
		quietly ivreghdfe fz (D.real_income = D.iv_income_fips) [pw = projection_factor] if unemp == 1, absorb(`FE') cluster(fips)
		wr `version' `v' unemp `=_b[D.real_income]' `=_se[D.real_income]' `=e(widstat)' `=e(N)' `sd'
		di "`version' `v' not employed: " %7.4f _b[D.real_income] " (" %6.4f _se[D.real_income] ")"
		quietly ivreghdfe lz (real_income = iv_income_fips) [pw = projection_factor] if clean == 1, absorb(`FE') cluster(fips)
		wr `version' `v' levels `=_b[real_income]' `=_se[real_income]' `=e(widstat)' `=e(N)' `sd'
		di "`version' `v' levels: " %7.4f _b[real_income] " (" %6.4f _se[real_income] ")"
		drop z fz bz lz smp
	}
}
di "build_cleaning_estimates.do complete"
