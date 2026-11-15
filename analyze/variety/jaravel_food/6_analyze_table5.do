do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
* Jaravel (2019) Table 5 on food module x price-decile cells, and the healthiness split (6_ of analyze/variety/jaravel_food).
* Usage: do 6_analyze_table5.do <END> <SIG>     (END 2015 or 2020; SIG s57, s4, s115, s5 = sigma 5.7, 4, 11.5, 5)
* Reads  $idir/variety/jaravel_food/cells_2006_<END>/analysis_main.dta (5_export_cells_dta.py).
* Writes $idir/variety/jaravel_food/cells_2006_<END>/results_<SIG>.dta: one row per estimate (block, outcome, instrument,
*        fixed effects, sample, term, b, se, p, Kleibergen-Paap F, Cragg-Donald F, N, modules).
* Specification as his 4h_Table5.do: ivreg2 y (x = z) [aw=sqrt(RMS_avg_spending)], first cluster(product_module_code).
* Healthiness: second endogenous regressor unhealthy x x, instrumented by unhealthy x z, with an unhealthy dummy; also each
* group's cells alone, and three groups (score 0-3 / 4-5 / 6-10).
args END SIG
version 18
set more off
local d "$idir/variety/jaravel_food/cells_2006_`END'"
use "`d'/analysis_main.dta", clear

gen double x = RMS_avg_Dlog_realspending_`SIG'
gen double y_ces = RMS_lces_price_index_win
gen double y_full = RMS_lfullces_pi_win_`SIG'
gen double w = sqrt(RMS_avg_spending)
gen byte G2 = hi3 == 2
gen byte G3 = hi3 == 3
foreach z in inst_linageres inst_allres {
    gen double `z'_U = `z' * unhealthy
    gen double `z'_2 = `z' * G2
    gen double `z'_3 = `z' * G3
}
gen double xU = x * unhealthy
gen double x2 = x * G2
gen double x3 = x * G3

tempname P
postfile `P' str24 block str8 outcome str10 inst str4 fe str14 sample str14 term ///
    double(b se p F_kp F_cd F_sw N nmod) using "`d'/results_`SIG'.dta", replace

capture program drop pz
program define pz, rclass
    * p-value with the normal distribution (ivreg2 reports z statistics)
    args b se
    return scalar p = 2 * normal(-abs(`b' / `se'))
end

* ---------------- Table 5 ----------------
* (1) first stage
reg x inst_linageres [aw=w], cluster(product_module_code)
post `P' ("table5") ("x") ("linageres") ("no") ("all") ("first_stage") (_b[inst_linageres]) (_se[inst_linageres]) ///
    (2*ttail(e(df_r), abs(_b[inst_linageres]/_se[inst_linageres]))) (.) (.) (.) (e(N)) (e(N_clust))
reg x inst_allres [aw=w], cluster(product_module_code)
post `P' ("table5") ("x") ("allres") ("no") ("all") ("first_stage") (_b[inst_allres]) (_se[inst_allres]) ///
    (2*ttail(e(df_r), abs(_b[inst_allres]/_se[inst_allres]))) (.) (.) (.) (e(N)) (e(N_clust))

foreach y in ces full {
    * reduced form
    reg y_`y' inst_linageres [aw=w], cluster(product_module_code)
    post `P' ("table5") ("`y'") ("linageres") ("no") ("all") ("reduced_form") (_b[inst_linageres]) (_se[inst_linageres]) ///
        (2*ttail(e(df_r), abs(_b[inst_linageres]/_se[inst_linageres]))) (.) (.) (.) (e(N)) (e(N_clust))
    * IV, linear age
    ivreg2 y_`y' (x = inst_linageres) [aw=w], first cluster(product_module_code)
    pz _b[x] _se[x]
    post `P' ("table5") ("`y'") ("linageres") ("no") ("all") ("x") (_b[x]) (_se[x]) (r(p)) (e(rkf)) (e(cdf)) (.) (e(N)) (e(N_clust))
    * IV, all household-type dummies
    ivreg2 y_`y' (x = inst_allres) [aw=w], first cluster(product_module_code)
    pz _b[x] _se[x]
    post `P' ("table5") ("`y'") ("allres") ("no") ("all") ("x") (_b[x]) (_se[x]) (r(p)) (e(rkf)) (e(cdf)) (.) (e(N)) (e(N_clust))
    * IV, all household-type dummies, product-group and price-decile fixed effects partialled out
    ivreg2 y_`y' (x = inst_allres) i.product_group_code i.quality_rank [aw=w], ///
        partial(i.product_group_code i.quality_rank) first cluster(product_module_code)
    pz _b[x] _se[x]
    post `P' ("table5") ("`y'") ("allres") ("yes") ("all") ("x") (_b[x]) (_se[x]) (r(p)) (e(rkf)) (e(cdf)) (.) (e(N)) (e(N_clust))
    * OLS, for reference
    reg y_`y' x [aw=w], cluster(product_module_code)
    post `P' ("table5") ("`y'") ("ols") ("no") ("all") ("x") (_b[x]) (_se[x]) ///
        (2*ttail(e(df_r), abs(_b[x]/_se[x]))) (.) (.) (.) (e(N)) (e(N_clust))
}

* ---------------- healthiness: one interaction (unhealthy = claude_hi 0-3) ----------------
foreach y in ces full {
    foreach spec in "linageres no" "allres no" "allres yes" {
        local z  : word 1 of `spec'
        local fe : word 2 of `spec'
        local fev ""
        if "`fe'" == "yes" local fev "i.product_group_code i.quality_rank"
        local part ""
        if "`fe'" == "yes" local part "partial(`fev')"
        ivreg2 y_`y' (x xU = inst_`z' inst_`z'_U) unhealthy `fev' [aw=w], `part' ffirst cluster(product_module_code)
        matrix F = e(first)
        local swx = F[rownumb(F, "SWF"), colnumb(F, "x")]
        local swu = F[rownumb(F, "SWF"), colnumb(F, "xU")]
        local kp = e(rkf)
        local N = e(N)
        local G = e(N_clust)
        pz _b[x] _se[x]
        post `P' ("health2") ("`y'") ("`z'") ("`fe'") ("all") ("healthy") (_b[x]) (_se[x]) (r(p)) (`kp') (e(cdf)) (`swx') (`N') (`G')
        lincom x + xU
        local lb = r(estimate)
        local ls = r(se)
        pz `lb' `ls'
        post `P' ("health2") ("`y'") ("`z'") ("`fe'") ("all") ("unhealthy") (`lb') (`ls') (r(p)) (`kp') (.) (`swu') (`N') (`G')
        pz _b[xU] _se[xU]
        post `P' ("health2") ("`y'") ("`z'") ("`fe'") ("all") ("difference") (_b[xU]) (_se[xU]) (r(p)) (`kp') (.) (`swu') (`N') (`G')
        * separate samples
        foreach u in 0 1 {
            local lab = cond(`u' == 1, "unhealthy", "healthy")
            ivreg2 y_`y' (x = inst_`z') `fev' [aw=w] if unhealthy == `u', `part' first cluster(product_module_code)
            pz _b[x] _se[x]
            post `P' ("health_split") ("`y'") ("`z'") ("`fe'") ("`lab'") ("x") (_b[x]) (_se[x]) (r(p)) (e(rkf)) (e(cdf)) (.) (e(N)) (e(N_clust))
        }
        * three groups: 0-3, 4-5, 6-10
        ivreg2 y_`y' (x x2 x3 = inst_`z' inst_`z'_2 inst_`z'_3) G2 G3 `fev' [aw=w], `part' ffirst cluster(product_module_code)
        matrix F = e(first)
        local sw1 = F[rownumb(F, "SWF"), colnumb(F, "x")]
        local sw2 = F[rownumb(F, "SWF"), colnumb(F, "x2")]
        local sw3 = F[rownumb(F, "SWF"), colnumb(F, "x3")]
        local kp = e(rkf)
        local N = e(N)
        local G = e(N_clust)
        pz _b[x] _se[x]
        post `P' ("health3") ("`y'") ("`z'") ("`fe'") ("all") ("hi_0_3") (_b[x]) (_se[x]) (r(p)) (`kp') (e(cdf)) (`sw1') (`N') (`G')
        lincom x + x2
        local lb = r(estimate)
        local ls = r(se)
        pz `lb' `ls'
        post `P' ("health3") ("`y'") ("`z'") ("`fe'") ("all") ("hi_4_5") (`lb') (`ls') (r(p)) (`kp') (.) (`sw2') (`N') (`G')
        lincom x + x3
        local lb = r(estimate)
        local ls = r(se)
        pz `lb' `ls'
        post `P' ("health3") ("`y'") ("`z'") ("`fe'") ("all") ("hi_6_10") (`lb') (`ls') (r(p)) (`kp') (.) (`sw3') (`N') (`G')
        lincom x3 - x2
        local lb = r(estimate)
        local ls = r(se)
        pz `lb' `ls'
        post `P' ("health3") ("`y'") ("`z'") ("`fe'") ("all") ("d_6_10_vs_4_5") (`lb') (`ls') (r(p)) (`kp') (.) (.) (`N') (`G')
        pz _b[x2] _se[x2]
        post `P' ("health3") ("`y'") ("`z'") ("`fe'") ("all") ("d_4_5_vs_0_3") (_b[x2]) (_se[x2]) (r(p)) (`kp') (.) (.) (`N') (`G')
        pz _b[x3] _se[x3]
        post `P' ("health3") ("`y'") ("`z'") ("`fe'") ("all") ("d_6_10_vs_0_3") (_b[x3]) (_se[x3]) (r(p)) (`kp') (.) (.) (`N') (`G')
        foreach h in 1 2 3 {
            ivreg2 y_`y' (x = inst_`z') `fev' [aw=w] if hi3 == `h', `part' first cluster(product_module_code)
            pz _b[x] _se[x]
            post `P' ("health3_split") ("`y'") ("`z'") ("`fe'") ("hi`h'") ("x") (_b[x]) (_se[x]) (r(p)) (e(rkf)) (e(cdf)) (.) (e(N)) (e(N_clust))
        }
        ivreg2 y_`y' (x x2 x3 = inst_`z' inst_`z'_2 inst_`z'_3) G2 G3 `fev' [aw=w], `part' ffirst cluster(product_module_code)
        test x2 x3
        post `P' ("health3") ("`y'") ("`z'") ("`fe'") ("all") ("joint_equal") (.) (.) (r(p)) (`kp') (.) (.) (`N') (`G')
    }
}
postclose `P'
use "`d'/results_`SIG'.dta", clear
list if block == "table5", clean noobs
