do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
* Check of E2 (prints only): the barcode measures on the price decile with module x year fixed effects (year = the
* barcode's last year bought) in place of module fixed effects, each barcode once or weighted by spending.
* Reads $idir/variety/jaravel_food/e123/e2_upc.dta.
version 18
set more off
use "$idir/variety/jaravel_food/e123/e2_upc.dta", clear
foreach y in addsugar_1000_w sugar_1000_w hi_1000_w organic_pct {
    foreach wt in none spend {
        local W ""
        if "`wt'" == "spend" local W "[aw=spend]"
        quietly reghdfe `y' quality_rank `W', absorb(product_module_code#year) cluster(product_module_code)
        display "E2 module x year FE: `y' `wt' slope " %7.3f _b[quality_rank] " (" %6.3f _se[quality_rank] ")  N " e(N)
        quietly reghdfe `y' ib1.quality_rank `W', absorb(product_module_code#year) cluster(product_module_code)
        display "      decile 10 - 1: " %7.2f _b[10.quality_rank] " (" %6.2f _se[10.quality_rank] ")"
    }
}
