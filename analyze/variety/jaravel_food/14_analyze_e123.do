do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
* E1-E3 regressions, module fixed effects, clustered by module.
* Reads  $idir/variety/jaravel_food/e123/e1_hms_cells.dta, e1_rms_cells.dta, e2_upc.dta, e3_groups.dta (13_build_e123_data.py).
* Writes $idir/variety/jaravel_food/e123/e123_results.dta (block, src, window, outcome, weight, term, b, se, p, N, modules);
*        read by 15_make_e123_exhibits.py.
* E1: growth on price-decile dummies (and the decile number, with and without module fixed effects), weights log mean
*     cell spending.  E2: barcode measure on decile dummies, each barcode once or weighted by spending.  E3: growth of
*     module x tercile on tercile dummies, weights log mean spending.
version 18
set more off
local d "$idir/variety/jaravel_food/e123"
tempname P
postfile `P' str8 block str4 src str4 window str16 outcome str6 weight str10 term double(b se p N nmod) using "`d'/e123_results.dta", replace

* ---------------- E1 Homescan ----------------
use "`d'/e1_hms_cells.dta", clear
foreach k in his ext {
    preserve
    keep if bal_`k' == 1 & !missing(growth_`k')
    reghdfe growth_`k' ib1.quality_rank [aw=w_`k'], absorb(product_module_code) cluster(product_module_code)
    forvalues q = 2/10 {
        post `P' ("E1") ("hms") ("`k'") ("growth_nom") ("logsp") ("d`q'") (_b[`q'.quality_rank]) (_se[`q'.quality_rank]) ///
            (2*ttail(e(df_r), abs(_b[`q'.quality_rank]/_se[`q'.quality_rank]))) (e(N)) (e(N_clust))
    }
    reghdfe growth_`k' quality_rank [aw=w_`k'], absorb(product_module_code) cluster(product_module_code)
    post `P' ("E1") ("hms") ("`k'") ("growth_nom") ("logsp") ("slope_fe") (_b[quality_rank]) (_se[quality_rank]) ///
        (2*ttail(e(df_r), abs(_b[quality_rank]/_se[quality_rank]))) (e(N)) (e(N_clust))
    reg growth_`k' quality_rank [aw=w_`k'], cluster(product_module_code)
    post `P' ("E1") ("hms") ("`k'") ("growth_nom") ("logsp") ("slope_nofe") (_b[quality_rank]) (_se[quality_rank]) ///
        (2*ttail(e(df_r), abs(_b[quality_rank]/_se[quality_rank]))) (e(N)) (e(N_clust))
    restore
}
* ---------------- E1 RMS ----------------
use "`d'/e1_rms_cells.dta", clear
foreach k in his ext {
    foreach y in growth_nom growth_real {
        preserve
        keep if window == "`k'"
        reghdfe `y' ib1.quality_rank [aw=w], absorb(product_module_code) cluster(product_module_code)
        forvalues q = 2/10 {
            post `P' ("E1") ("rms") ("`k'") ("`y'") ("logsp") ("d`q'") (_b[`q'.quality_rank]) (_se[`q'.quality_rank]) ///
                (2*ttail(e(df_r), abs(_b[`q'.quality_rank]/_se[`q'.quality_rank]))) (e(N)) (e(N_clust))
        }
        reghdfe `y' quality_rank [aw=w], absorb(product_module_code) cluster(product_module_code)
        post `P' ("E1") ("rms") ("`k'") ("`y'") ("logsp") ("slope_fe") (_b[quality_rank]) (_se[quality_rank]) ///
            (2*ttail(e(df_r), abs(_b[quality_rank]/_se[quality_rank]))) (e(N)) (e(N_clust))
        reg `y' quality_rank [aw=w], cluster(product_module_code)
        post `P' ("E1") ("rms") ("`k'") ("`y'") ("logsp") ("slope_nofe") (_b[quality_rank]) (_se[quality_rank]) ///
            (2*ttail(e(df_r), abs(_b[quality_rank]/_se[quality_rank]))) (e(N)) (e(N_clust))
        restore
    }
}
* ---------------- E2 ----------------
use "`d'/e2_upc.dta", clear
foreach y in addsugar_1000_w sugar_1000_w hi_1000_w organic_pct {
    foreach wt in none spend {
        local W ""
        if "`wt'" == "spend" local W "[aw=spend]"
        reghdfe `y' ib1.quality_rank `W', absorb(product_module_code) cluster(product_module_code)
        forvalues q = 2/10 {
            post `P' ("E2") ("hms") ("") ("`y'") ("`wt'") ("d`q'") (_b[`q'.quality_rank]) (_se[`q'.quality_rank]) ///
                (2*ttail(e(df_r), abs(_b[`q'.quality_rank]/_se[`q'.quality_rank]))) (e(N)) (e(N_clust))
        }
        reghdfe `y' quality_rank `W', absorb(product_module_code) cluster(product_module_code)
        post `P' ("E2") ("hms") ("") ("`y'") ("`wt'") ("slope_fe") (_b[quality_rank]) (_se[quality_rank]) ///
            (2*ttail(e(df_r), abs(_b[quality_rank]/_se[quality_rank]))) (e(N)) (e(N_clust))
        summarize `y' `W' if e(sample)
        post `P' ("E2") ("hms") ("") ("`y'") ("`wt'") ("mean") (r(mean)) (r(sd)) (.) (r(N)) (.)
    }
}
* ---------------- E3 ----------------
use "`d'/e3_groups.dta", clear
foreach s in hms rms {
    foreach m in addsugar_1000 hi_1000 {
        foreach k in his ext {
            preserve
            keep if src == "`s'" & measure == "`m'" & !missing(growth_`k')
            reghdfe growth_`k' ib1.tercile [aw=w_`k'], absorb(product_module_code) cluster(product_module_code)
            foreach q in 2 3 {
                post `P' ("E3") ("`s'") ("`k'") ("`m'") ("logsp") ("t`q'") (_b[`q'.tercile]) (_se[`q'.tercile]) ///
                    (2*ttail(e(df_r), abs(_b[`q'.tercile]/_se[`q'.tercile]))) (e(N)) (e(N_clust))
            }
            restore
        }
    }
}
postclose `P'
