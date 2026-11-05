// ============================================================================
// build_variety_demand_se.do -- one-way clustered SEs for the specifications reported from build_variety_demand_regs.do.
// Each observation is one county x module, so the two-way formula is V(county) + V(module) - V(robust). That sum can be
// negative (reghdfe then shows SE = 0) or too small. Exhibits report the largest of the two-way, county and module SEs
// (Cameron, Gelbach and Miller 2011). `do build_variety_demand_se.do 1` .. 3 run in parallel; output
// interim/what_moves/variety_demand_se_<part>.csv
// ============================================================================
clear all
set more off
local part "`1'"
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
use product_module_code fips_n total_spending0 hi_group upf_group prep_group eta_iv rich_z ///
	dlog_spend dlog_real dlog_nupcs d_new_county d_ssnp dlog_ces dlog_va ///
	dlog_wagebill bartik_wagebill dlog_income_hs dlog_income_hat_hs using "$idir/what_moves/variety_longdiff.dta", clear
compress
replace rich_z = max(min(rich_z, 4), -4) if !missing(rich_z)
ren dlog_wagebill s_wb
ren bartik_wagebill s_bk
ren dlog_income_hat_hs s_hat
ren dlog_income_hs s_inc
global Y dlog_spend dlog_real dlog_nupcs d_new_county d_ssnp dlog_ces dlog_va
local out "$idir/what_moves/variety_demand_se_`part'.csv"
cap file close fh
file open fh using "`out'", write replace text
file write fh "spec,shock,grouping,term,outcome,se_county,se_module" _n
file close fh

// run one regression twice, write both SEs for each listed term
cap program drop two
program define two
	args out spec shock grouping y fe xs labs ifc
	local n : word count `xs'
	quietly reghdfe `y' `xs' [aw = total_spending0] `ifc', absorb(`fe') cluster(fips_n)
	forvalues i = 1/`n' {
		local x : word `i' of `xs'
		local sc`i' = _se[`x']
	}
	quietly reghdfe `y' `xs' [aw = total_spending0] `ifc', absorb(`fe') cluster(product_module_code)
	file open fh using "`out'", write append text
	forvalues i = 1/`n' {
		local x : word `i' of `xs'
		local l : word `i' of `labs'
		file write fh "`spec',`shock',`grouping',`l',`y',`sc`i'',`=_se[`x']'" _n
	}
	file close fh
end

if "`part'" == "1" {
	foreach s in wb bk hat inc {
		foreach y of global Y {
			two "`out'" S0 `s' all `y' "product_module_code" "s_`s'" "all"
		}
		di "S0 `s' done"
	}
	foreach s in wb bk hat inc {
		gen float x = s_`s' * rich_z
		foreach y of global Y {
			two "`out'" S1 `s' all `y' "fips_n product_module_code" "x" "all"
		}
		drop x
		di "S1 `s' done"
	}
	foreach s in wb hat {
		gen float x = s_`s' * eta_iv
		foreach y of global Y {
			two "`out'" S4 `s' all `y' "fips_n product_module_code" "x" "all"
		}
		drop x
		di "S4 `s' done"
	}
}
if "`part'" == "2" {
	foreach s in wb bk {
		foreach pair in "hi_group 3" "upf_group 1" "prep_group 3" {
			local g : word 1 of `pair'
			local base : word 2 of `pair'
			quietly levelsof `g', local(levs)
			local xs
			local labs
			foreach k of local levs {
				gen float x`k' = s_`s' * (`g' == `k') if !missing(`g')
				if `k' != `base' {
					local xs `xs' x`k'
					local labs `labs' `k'
				}
			}
			foreach y of global Y {
				two "`out'" S2 `s' `g' `y' "fips_n product_module_code" "`xs'" "`labs'" "if !missing(`g')"
			}
			foreach k of local levs {
				drop x`k'
			}
		}
		di "S2 `s' done"
	}
}
if "`part'" == "3" {
	foreach pair in "hi_group" "upf_group" "prep_group" {
		local g `pair'
		quietly levelsof `g', local(levs)
		local zs
		local labs
		foreach k of local levs {
			gen float z`k' = s_wb * rich_z * (`g' == `k') if !missing(`g')
			local zs `zs' z`k'
			local labs `labs' `k'
		}
		foreach y of global Y {
			two "`out'" S1g wb `g' `y' "fips_n product_module_code" "`zs'" "`labs'" "if !missing(`g')"
		}
		drop z*
		di "S1g `g' done"
	}
}
di "build_variety_demand_se.do part `part' complete"
