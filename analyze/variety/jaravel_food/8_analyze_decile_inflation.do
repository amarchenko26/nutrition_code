do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
* Inflation and new-product share by price decile, healthy vs unhealthy modules (cell level, module fixed effects,
* weights sqrt of mean cell spending, clustered by module).
* Usage: do 8_analyze_decile_inflation.do <END>
* Reads  $idir/variety/jaravel_food/cells_2006_<END>/analysis_main.dta.
* Writes $idir/variety/jaravel_food/cells_2006_<END>/descriptive_regs.dta (outcome, term, decile, b, se, p).
args END
version 18
set more off
local d "$idir/variety/jaravel_food/cells_2006_`END'"
use "`d'/analysis_main.dta", clear
gen double y_ces = 100 * RMS_lces_price_index_win
gen double y_full = 100 * RMS_lfullces_pi_win_s57
gen double y_new = 100 * RMS_spend_new / RMS_spend_all
gen double w = sqrt(RMS_avg_spending)

tempname P
postfile `P' str8 outcome str16 term int decile double(b se p) using "`d'/descriptive_regs.dta", replace
foreach y in ces full new {
    reghdfe y_`y' ib1.quality_rank##i.unhealthy [aw=w], absorb(product_module_code) cluster(product_module_code)
    local df = e(df_r)
    forvalues k = 2/10 {
        post `P' ("`y'") ("healthy") (`k') (_b[`k'.quality_rank]) (_se[`k'.quality_rank]) ///
            (2*ttail(`df', abs(_b[`k'.quality_rank]/_se[`k'.quality_rank])))
        post `P' ("`y'") ("difference") (`k') (_b[`k'.quality_rank#1.unhealthy]) (_se[`k'.quality_rank#1.unhealthy]) ///
            (2*ttail(`df', abs(_b[`k'.quality_rank#1.unhealthy]/_se[`k'.quality_rank#1.unhealthy])))
        lincom `k'.quality_rank + `k'.quality_rank#1.unhealthy
        local lb = r(estimate)
        local ls = r(se)
        post `P' ("`y'") ("unhealthy") (`k') (`lb') (`ls') (2*ttail(`df', abs(`lb'/`ls')))
    }
    testparm i(2/10).quality_rank#1.unhealthy
    post `P' ("`y'") ("joint_diff") (.) (.) (.) (r(p))
    reghdfe y_`y' c.quality_rank##i.unhealthy [aw=w], absorb(product_module_code) cluster(product_module_code)
    local df = e(df_r)
    post `P' ("`y'") ("slope_healthy") (.) (_b[quality_rank]) (_se[quality_rank]) (2*ttail(`df', abs(_b[quality_rank]/_se[quality_rank])))
    lincom quality_rank + 1.unhealthy#c.quality_rank
    local lb = r(estimate)
    local ls = r(se)
    post `P' ("`y'") ("slope_unhealthy") (.) (`lb') (`ls') (2*ttail(`df', abs(`lb'/`ls')))
    post `P' ("`y'") ("slope_diff") (.) (_b[1.unhealthy#c.quality_rank]) (_se[1.unhealthy#c.quality_rank]) ///
        (2*ttail(`df', abs(_b[1.unhealthy#c.quality_rank]/_se[1.unhealthy#c.quality_rank])))
    post `P' ("`y'") ("N") (.) (e(N)) (e(N_clust)) (.)
}
postclose `P'
