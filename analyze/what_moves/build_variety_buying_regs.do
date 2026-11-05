// ============================================================================
// build_variety_buying_regs.do -- does more variety go with more buying? (2026-11-05)
// County x module change 2009-2011 to 2018-2020 (variety_longdiff.dta): log change in spending on the log change in UPCs
// sold in the county, and on the log change in the variety-adjusted price; county and module fixed effects, so a
// module in a county is compared with the same module elsewhere and with other modules in that county; W0-spending weights;
// SE clustered on county and module. OLS is an association: more spending also brings more UPCs onto the shelf. The demand
// shock is NOT used as an instrument, because it raises spending directly. The one supply-side attempt: launch_z = the
// county's W0 stocking breadth in the module (its UPCs / the module's national UPCs) x the module's national log change in
// UPCs, controlling for breadth: national launches reach counties that already carry more of the range. First-stage F = 4 pooled
// and below 1 within module groups (run 2026-11-05), so the instrument is too weak to use; only the pooled estimate is kept.
// Output: interim/what_moves/variety_buying_coefs.csv
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
use product_module_code fips_n total_spending0 hi_group upf_group prep_group dlog_spend dlog_nupcs dlog_va breadth0 launch_z ///
	using "$idir/what_moves/variety_longdiff.dta", clear
compress
local out "$idir/what_moves/variety_buying_coefs.csv"
cap file close fh
file open fh using "`out'", write replace text
file write fh "spec,regressor,grouping,term,b,se,N,F" _n
file close fh
global FE absorb(fips_n product_module_code) cluster(fips_n product_module_code)

// two-way, county and module SEs for the same regression; exhibits report the largest (see build_variety_demand_se.do)
cap program drop threese
program define threese
	args out regressor grouping xs labs ifc
	local n : word count `xs'
	quietly reghdfe dlog_spend `xs' [aw = total_spending0] `ifc', absorb(fips_n product_module_code) cluster(fips_n)
	forvalues i = 1/`n' {
		local x : word `i' of `xs'
		local sc`i' = _se[`x']
	}
	quietly reghdfe dlog_spend `xs' [aw = total_spending0] `ifc', absorb(fips_n product_module_code) cluster(product_module_code)
	forvalues i = 1/`n' {
		local x : word `i' of `xs'
		local sm`i' = _se[`x']
	}
	quietly reghdfe dlog_spend `xs' [aw = total_spending0] `ifc', absorb(fips_n product_module_code) cluster(fips_n product_module_code)
	file open fh using "`out'", write append text
	forvalues i = 1/`n' {
		local x : word `i' of `xs'
		local l : word `i' of `labs'
		local se = max(_se[`x'], `sc`i'', `sm`i'')
		file write fh "OLS,`regressor',`grouping',`l',`=_b[`x']',`se',`=e(N)'," _n
	}
	file close fh
end

foreach x in dlog_nupcs dlog_va {
	threese "`out'" `x' all "`x'" "all"
	foreach g in hi_group upf_group prep_group {
		quietly levelsof `g', local(levs)
		local xs
		local labs
		foreach k of local levs {
			quietly gen float i`k' = `x' * (`g' == `k') if !missing(`g')
			local xs `xs' i`k'
			local labs `labs' `k'
		}
		threese "`out'" `x' `g' "`xs'" "`labs'" "if !missing(`g')"
		drop i*
	}
}
quietly ivreghdfe dlog_spend breadth0 (dlog_nupcs = launch_z) [aw = total_spending0], $FE
file open fh using "`out'", write append text
file write fh "IV_launch,dlog_nupcs,all,all,`=_b[dlog_nupcs]',`=_se[dlog_nupcs]',`=e(N)',`=e(widstat)'" _n
file close fh
di "build_variety_buying_regs.do complete"
