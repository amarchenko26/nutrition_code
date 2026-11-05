
// build_monthly_cycle_regs.do
//
// Within-HH DiD regressions for income timing effects on food quality.
// No external control group — identification is entirely within-HH.
//
// (A) SNAP within-month cycle:
//     Compare week 1 (days 1-7, flush after benefit receipt) vs.
//     week 4 (days 22-31, depleted). Within HH x year-month cell.
//     Y_{h,w,ym} = β late + HH FE + year-month FE + ε
//     Sample: SNAP-eligible HHs only (per-capita income <= 130% FPL).
//
// (B) EITC annual cycle:
//     Compare March vs. January within EITC-eligible HHs.
//     Y_{h,m,y} = β march + HH FE + year FE + ε
//     Sample: EITC-eligible HHs only (children + income <= $55k).
//     January is baseline — both are low-spending winter months,
//     so this isolates the refund shock rather than seasonal trends.

clear all
set graphics on

local base "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
local fig  "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs"
local tab  "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/tabs"

// ============================================================================
// Load daily panel
// ============================================================================

pq use using "`base'/interim/panel_dataset/monthly_cycle_hh_day.parquet", clear
ren household_code hhid

// Numeric IDs for FE
// encode household_code, gen(hhid)
gen ym = year * 100 + month
// encode ym, gen(ym_fe)

// ============================================================================
// ANALYSIS A: SNAP within-month cycle
// Week 1 (days 1-7) vs week 4 (days 22-31), within HH x year-month
// ============================================================================

preserve
keep if snap_eligible == 1
keep if week_of_month == 1 | week_of_month == 4

// early = 1 for week 1 (flush)
gen early = (week_of_month == 1)

eststo clear

foreach var in spend_total spend_healthy spend_unhealthy healthy_share unhealthy_share {
    quietly sum `var' if early == 0
    local mean_`var' = r(mean)

    eststo snap_`var': reghdfe `var' early [pw = projection_factor], ///
        absorb(hhid ym) cluster(hhid)

    estadd scalar mean_y = `mean_`var'': snap_`var'
}

// Print to screen
esttab snap_spend_total snap_spend_healthy snap_spend_unhealthy snap_healthy_share snap_unhealthy_share, ///
    keep(early) b(4) se(4) ///
    star(* 0.10 ** 0.05 *** 0.01)  ///
    title("SNAP: Week 1 (flush) vs. Week 4 (depleted), within-HH") ///
    mtitles("Total $" "Healthy $" "Unhealthy $" "Health share" "Unhealth share") ///
    stats(mean_y N r2, fmt(%9.4f %9.0fc %9.4f) labels("Mean (week 1)" "N" "R-sq"))

// // Export to LaTeX
// esttab snap_spend_total snap_healthy_share snap_produce_share snap_unhealthy_share ///
//     using "`tab'/snap_cycle.tex", replace ///
//     keep(late) b(4) se(4) ///
//     star(* 0.10 ** 0.05 *** 0.01) booktabs ///
//     title("SNAP within-month cycle: depleted (week 4) vs.\ flush (week 1)") ///
//     mtitles("Total spend" "Healthy share" "Produce share" "Unhealthy share") ///
//     varlabels(late "Week 4 (days 22--31)") ///
//     stats(mean_y N r2, fmt(%9.4f %9.0fc %9.4f) labels("Mean at week 1" "N" "R-sq")) ///
//     nonotes addnotes("HH and year-month FE. SE clustered by HH." ///
//         "Sample: SNAP-eligible HHs (per-capita income $\leq$ 130\% FPL)." ///
//         "Week 1 = days 1--7 (flush after benefit receipt). Week 4 = days 22--31 (depleted)." ///
//         "Real spending in 2013 dollars.")

restore

// // Event study by week (week 1 is baseline)
// preserve
// keep if snap_eligible == 1
// gen wk2 = (week_of_month == 2)
// gen wk3 = (week_of_month == 3)
// gen wk4 = (week_of_month == 4)
//
// eststo snap_es: reghdfe healthy_share wk2 wk3 wk4 [pw = projection_factor], ///
//     absorb(hhid ym) cluster(hhid)
//
// coefplot snap_es, keep(wk2 wk3 wk4) ///
//     rename(wk2 = "Week 2" wk3 = "Week 3" wk4 = "Week 4") ///
//     vertical yline(0, lcolor(gray) lpattern(dash)) ///
//     xtitle("Week of month (baseline = week 1, days 1-7)") ///
//     ytitle("Healthy share relative to week 1") ///
//     title("SNAP cycle: healthy food share by week", size(medlarge)) ///
//     note("HH + year-month FE. Sample: SNAP-eligible HHs.") ///
//     ciopts(recast(rcap) lwidth(medthick))
//
// // graph export "`fig'/snap_cycle_event_study.png", replace
//
// restore

// ============================================================================
// ANALYSIS B: EITC annual cycle — March vs. January within-HH
// ============================================================================

preserve
keep if eitc_eligible == 1
keep if month == 1 | month == 3   // January (baseline) vs. March (EITC refund)

gen eitc_month = (month == 3 | month ==2)


eststo clear
foreach var in spend_total spend_healthy spend_unhealthy healthy_share unhealthy_share {
    quietly sum `var' if eitc_month == 0
    local mean_`var' = r(mean)

    eststo eitc_`var': reghdfe `var' eitc_month [pw = projection_factor] if inlist(month, 1, 2, 3), ///
        absorb(hhid year) cluster(hhid)

    estadd scalar mean_y = `mean_`var'': eitc_`var'
}

// Print to screen
esttab eitc_spend_total eitc_spend_healthy eitc_spend_unhealthy eitc_healthy_share eitc_unhealthy_share, ///
    keep(eitc_month) b(4) se(4) ///
    star(* 0.10 ** 0.05 *** 0.01) compress ///
    title("EITC: March + February vs. January, within-HH") ///
    mtitles("Total $" "Healthy $" "Unhealthy $" "Health share" "Unhealth share") ///
    stats(mean_y N r2, fmt(%9.4f %9.0fc %9.4f) labels("Mean (January)" "N" "R-sq"))

restore
//
// // Event study: coefficients for each month relative to January
// preserve
// keep if eitc_eligible == 1
//
// forvalues m = 2/12 {
//     gen mo`m' = (month == `m')
// }
//
// eststo eitc_es: reghdfe healthy_share ///
//     mo2 mo3 mo4 mo5 mo6 mo7 mo8 mo9 mo10 mo11 mo12 ///
//     [pw = projection_factor], ///
//     absorb(hhid year) cluster(hhid)
//
// coefplot eitc_es, keep(mo*) ///
//     rename(mo2="Feb" mo3="Mar" mo4="Apr" mo5="May" mo6="Jun" ///
//            mo7="Jul" mo8="Aug" mo9="Sep" mo10="Oct" mo11="Nov" mo12="Dec") ///
//     vertical yline(0, lcolor(gray) lpattern(dash)) ///
//     xline(2, lcolor(gold) lpattern(shortdash) lwidth(medthick)) ///
//     xline(3, lcolor(gold) lpattern(shortdash) lwidth(medthick)) ///
//     xtitle("Month (baseline = January)") ///
//     ytitle("Healthy share relative to January") ///
//     title("EITC cycle: healthy food share by month", size(medlarge)) ///
//     note("Gold lines = EITC window (Feb-Mar). HH + year FE. EITC-eligible HHs.") ///
//     ciopts(recast(rcap) lwidth(medthick))
//
// graph export "`fig'/eitc_cycle_event_study.png", replace
//
// restore
