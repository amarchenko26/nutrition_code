// ============================================================================
// build_variety_demand_regs.do -- Jaravel (2019) by healthiness, long differences
// across counties (Anya 2026-11-05): when a county's earnings grow, do products
// and prices in that county's food modules respond, and more for the modules rich
// people buy, for ultra-processed, little-preparation or unhealthy modules?
//
// Unit: county x module, change from 2009-2011 to 2018-2020 (variety_longdiff.dta,
// build_variety_long_diff.py); weights = W0 spending; SE clustered two ways on
// county and module, because shocks are county-level and groups module-level.
//
// County shocks: wb = log change in the private wage bill (QCEW, administrative),
// bk = shift-share prediction of wb from the county's 2004-2006 industry mix, pay =
// log change in pay per worker, hat and inc = the pipeline's Homescan series
// (predicted leave-one-out and realized county income, log change).
// Specifications (y = change in the outcome):
//   S0  y = module FE + b shock                 county's change vs other counties, same module
//   S1  y = county FE + module FE + b shock x rich_z        the Jaravel object: modules rich people buy (rich_z capped at +/-4)
//   S1g the same, separately by module group
//   S2  y = county FE + module FE + sum_g b_g shock x 1[g]  (base group dropped): gap between module groups in the same county
//   S4  y = county FE + module FE + b shock x eta_m          the existing pipeline's elasticity (eta_iv)
//   S4g the same by group
//   IV1 S1 with the wb interaction instrumented by the bk interaction
// Groups: hi_group 1 unhealthy (claude_hi 0-3) 2 middle (4-5) 3 healthy (6-10), base 3; upf_group 1 mostly not
// ultra-processed, 2 mostly (module NOVA-4 share of UPCs >= 0.818), base 1; prep_group 1 open-and-eat (score 1) 2 little
// preparation (2-3) 3 cooking (4-9), base 3.
// Output: interim/what_moves/variety_demand_coefs.csv
// ============================================================================
// Run in parts so they can go in parallel: `do build_variety_demand_regs.do A` ... D; each writes variety_demand_coefs_<part>.csv
clear all
set more off
local part "`1'"
if "`part'" == "" local part ALL
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

global Y dlog_spend dlog_real dlog_nupcs d_new_county d_ssnp dlog_ces dlog_va
use product_module_code fips_n total_spending0 hi_group upf_group prep_group eta_iv rich_z ///
	dlog_spend dlog_real dlog_nupcs d_new_county d_ssnp dlog_ces dlog_va ///
	dlog_wagebill bartik_wagebill dlog_pay dlog_income_hs dlog_income_hat_hs using "$idir/what_moves/variety_longdiff.dta", clear
compress
replace rich_z = max(min(rich_z, 4), -4) if !missing(rich_z)
ren dlog_wagebill s_wb
ren bartik_wagebill s_bk
ren dlog_pay s_pay
ren dlog_income_hat_hs s_hat
ren dlog_income_hs s_inc
global FE2 absorb(fips_n product_module_code) cluster(fips_n product_module_code)
global FE1 absorb(product_module_code) cluster(fips_n product_module_code)
local out "$idir/what_moves/variety_demand_coefs_`part'.csv"
cap file close fh
file open fh using "`out'", write replace text
file write fh "spec,shock,grouping,term,outcome,b,se,N,F" _n
file close fh

cap program drop writecoef
program define writecoef
	args out spec shock grouping term var y F
	if "`F'" == "" local F .
	file open fh using "`out'", write append text
	file write fh "`spec',`shock',`grouping',`term',`y',`=_b[`var']',`=_se[`var']',`=e(N)',`F'" _n
	file close fh
end

cap program drop run_spec
program define run_spec
	args out spec shock outcomes
	foreach y of local outcomes {
		if "`spec'" == "S0" {
			quietly reghdfe `y' s_`shock' [aw = total_spending0], $FE1
			writecoef "`out\'" `spec' `shock' all all s_`shock' `y'
		}
		if inlist("`spec'", "S1", "S4") {
			local m = cond("`spec'" == "S1", "rich_z", "eta_iv")
			quietly gen float x = s_`shock' * `m'
			quietly reghdfe `y' x [aw = total_spending0], $FE2
			writecoef "`out\'" `spec' `shock' all all x `y'
			drop x
		}
	}
end

cap program drop run_group
program define run_group
	args out spec shock g base outcomes
	quietly levelsof `g', local(levs)
	local xs
	foreach k of local levs {
		if "`spec'" == "S1g" quietly gen float x`k' = s_`shock' * rich_z * (`g' == `k') if !missing(`g')
		if "`spec'" == "S4g" quietly gen float x`k' = s_`shock' * eta_iv * (`g' == `k') if !missing(`g')
		if "`spec'" == "S2" quietly gen float x`k' = s_`shock' * (`g' == `k') if !missing(`g')
		if "`spec'" != "S2" | `k' != `base' local xs `xs' x`k'
	}
	foreach y of local outcomes {
		quietly reghdfe `y' `xs' [aw = total_spending0] if !missing(`g'), $FE2
		foreach x of local xs {
			local k = substr("`x'", 2, .)
			writecoef "`out\'" `spec' `shock' `g' `k' `x' `y'
		}
	}
	foreach k of local levs {
		drop x`k'
	}
end

// ---------------------------------------------------------------- A: pooled
if inlist("`part'", "A", "ALL") {
	foreach s in wb bk hat inc pay {
		run_spec "`out'" S0 `s' "$Y"
		di "S0 `s' done"
	}
	foreach s in wb bk hat inc pay {
		run_spec "`out'" S1 `s' "$Y"
		di "S1 `s' done"
	}
	foreach s in wb hat {
		run_spec "`out'" S4 `s' "$Y"
	}
}
// ---------------------------------------------------------------- B: gaps between module groups in the same county
if inlist("`part'", "B", "ALL") {
	foreach s in wb bk hat {
		run_group "`out'" S2 `s' hi_group 3 "$Y"
		run_group "`out'" S2 `s' upf_group 1 "$Y"
		run_group "`out'" S2 `s' prep_group 3 "$Y"
		di "S2 `s' done"
	}
}
// ---------------------------------------------------------------- C: rich and elasticity interactions by group
if inlist("`part'", "C", "ALL") {
	foreach s in wb bk {
		run_group "`out'" S1g `s' hi_group 3 "$Y"
		run_group "`out'" S1g `s' upf_group 1 "$Y"
		run_group "`out'" S1g `s' prep_group 3 "$Y"
		di "S1g `s' done"
	}
	foreach s in hat {
		run_group "`out'" S4g `s' hi_group 3 "$Y"
		run_group "`out'" S4g `s' upf_group 1 "$Y"
		run_group "`out'" S4g `s' prep_group 3 "$Y"
		di "S4g `s' done"
	}
}
// ---------------------------------------------------------------- D: IV
if inlist("`part'", "D", "ALL") {
	quietly gen float xw = s_wb * rich_z
	quietly gen float xb = s_bk * rich_z
	foreach y of global Y {
		quietly ivreghdfe `y' (xw = xb) [aw = total_spending0], $FE2
		writecoef "`out'" IV1 wb all all xw `y' `=e(widstat)'
	}
	drop xw xb
}
di "build_variety_demand_regs.do part `part' complete"
