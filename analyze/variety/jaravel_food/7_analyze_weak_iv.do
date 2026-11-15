do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
* Anderson-Rubin tests (valid with a weak first stage, clustered by module) of a zero slope in the linear-age IV, all cells and
* each healthiness group's cells alone, both windows, sigma 5.7.
* Reads  $idir/variety/jaravel_food/cells_2006_<END>/analysis_main.dta, END = 2015 and 2020.
* Writes $idir/variety/jaravel_food/ar_checks.dta (window, outcome, sample, b, se, archi2p, F_kp, N); read by 9_make_table5_tables.py.
version 18
set more off
local root "$idir/variety/jaravel_food"
tempname P
postfile `P' str6 window str8 outcome str10 sample double(b se archi2p F_kp N) using "`root'/ar_checks.dta", replace
foreach E in 2015 2020 {
    use "`root'/cells_2006_`E'/analysis_main.dta", clear
    gen double x = RMS_avg_Dlog_realspending_s57
    gen double w = sqrt(RMS_avg_spending)
    foreach y in RMS_lces_price_index_win RMS_lfullces_pi_win_s57 {
        local o = cond("`y'" == "RMS_lces_price_index_win", "ces", "full")
        foreach s in all healthy unhealthy g1 g2 g3 {
            local cond ""
            if "`s'" == "healthy" local cond "if unhealthy == 0"
            if "`s'" == "unhealthy" local cond "if unhealthy == 1"
            if "`s'" == "g1" local cond "if hi3 == 1"
            if "`s'" == "g2" local cond "if hi3 == 2"
            if "`s'" == "g3" local cond "if hi3 == 3"
            ivreg2 `y' (x = inst_linageres) [aw=w] `cond', ffirst cluster(product_module_code)
            post `P' ("`E'") ("`o'") ("`s'") (_b[x]) (_se[x]) (e(archi2p)) (e(rkf)) (e(N))
        }
    }
}
postclose `P'
use "`root'/ar_checks.dta", clear
list, clean noobs
