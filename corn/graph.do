*import delimited "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/NASS_2017-2022/qs.census2017.txt"

// br gov_all_amt_real gov_cons_amt_real gov_noncons_amt_calc gov_all_n gov_noncons_pf_calc
// br if strpos(short_desc, "CONSERVATION") > 0 & agg_level_desc == "COUNTY" & domain_desc =="TOTAL" 

*********************************** MERGE SOIL SUITABILITY 
import delimited "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/census_merged_deflated.tsv", clear 
drop if inlist(statefip, 2, 15)

** merge the census data to the soil suitability data
merge m:1 fips using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/gaez/gaez_by_county.dta", keepusing(si_* corn_share_*)

keep if level ==1

*********************************** MAKE VARS

sum total_corn_harvested_acres
destring harvested_acres corn_for_grain_bu, replace force
sum harvested_acres

cap drop treat
g treat = 0
sum corn_share_8500, d 
replace treat = 1 if corn_share_8500 > r(p50)
tab treat 


*********************************** CHECK PRE-TRENDS

preserve 
collapse (mean) total_corn_harvested_acres, by(year treat)

twoway ///
    (line total_corn_harvested_acres year if treat == 1, lcolor(ebblue) lwidth(medthick) ///
        lpattern(solid) ///
        legend(label(1 "Above median share of VS"))) ///
    (line total_corn_harvested_acres year if treat == 0, lcolor(orange_red) lwidth(medthick) ///
        lpattern(dash) ///
        legend(label(2 "Below median share of VS"))) ///
    , ///
    legend(order(1 2) position(6) col(1)) ///
	title("Total corn acres harvested, avg by county") ///
    ytitle("Mean corn acres") ///
	xline(1981 1985 1990 1996 2002 2008 2014 2018 2025)
    xtitle("Year") ///
    scheme(s1color)
restore


preserve 
collapse (mean) share_corn_harvested_acres, by(year treat)

twoway ///
    (line share_corn_harvested_acres year if treat == 1, lcolor(ebblue) lwidth(medthick) ///
        lpattern(solid) ///
        legend(label(1 "Above median share of VS"))) ///
    (line share_corn_harvested_acres year if treat == 0, lcolor(orange_red) lwidth(medthick) ///
        lpattern(dash) ///
        legend(label(2 "Below median share of VS"))) ///
    , ///
    legend(order(1 2) position(6) col(1)) ///
	title("Share of acres harvested that're corn, avg by county") ///
    xline(1981 1985 1990 1996 2002 2008 2014 2018 2025) ///
    xtitle("Year") ///
    ytitle("Mean corn acres") ///
    scheme(s1color)
restore



*********************************** CHECK FIRST STAGE


reghdfe gov_all_amt_real si_* , absorb(counfip year) vce(cluster counfip)


** reg total corn acres harvested on suitability for corn at the county level
reghdfe total_corn_harvested_acres si_* harvested_acres, absorb(counfip year) vce(cluster counfip)

reghdfe total_corn_harvested_acres si_* harvested_acres, absorb(counfip#year) vce(cluster counfip)

reghdfe total_corn_harvested_acres treat harvested_acres, absorb(counfip#year) vce(cluster counfip)

reghdfe total_corn_harvested_acres corn_share_8500 harvested_acres, absorb(counfip#year) vce(cluster counfip)

reghdfe corn_for_grain_bu corn_share_8500 harvested_acres, absorb(counfip#year) vce(cluster counfip)


* what about if the suitability index is very high?
reg total_corn_harvested_acres si_* i.statefip i.year if level == 1 & si_corn > 8500, cluster(statefip)

* soil suitability predictive of gov't payments?  
reg gov_noncons_amt_calc si_* i.statefip i.year if level == 1 , cluster(statefip)

* gov't payments predictive of corn harvested?  
reg total_corn_harvested_acres gov_noncons_amt_calc si_* i.counfip i.year if level == 1, cluster(statefip)


* is share more suitable for corn predictive of acres of corn
reghdfe total_corn_harvested_acres corn_share_8500, absorb(counfip year) vce(cluster statefip)

reghdfe total_corn_harvested_acres corn_share_5500 if level==1, absorb(counfip year) vce(cluster statefip)







preserve 
collapse (mean) gov_noncons_pf_calc, by(year treat)

twoway ///
    (line gov_noncons_pf_calc year if treat == 1, lcolor(ebblue) lwidth(medthick) ///
        lpattern(solid) ///
        legend(label(1 "Above median share of VS"))) ///
    (line gov_noncons_pf_calc year if treat == 0, lcolor(orange_red) lwidth(medthick) ///
        lpattern(dash) ///
        legend(label(2 "Below median share of VS"))) ///
    , ///
    legend(order(1 2) position(6) col(1)) ///
    ytitle("Mean corn acres") ///
	title("Trends in non-cons govt payments per farm avg by county (treat is median)") ///
    xline(1981 1985 1990 1996 2002 2008 2014 2018 2025) ///
    xtitle("Year") ///
    title("Share of acres harvested that are corn, by corn suitability") ///
    scheme(s1color)
restore


// corn harvested by SI
preserve
xtile si_decile = si_corn, nq(10)
collapse (sum) acres = total_corn_harvested_acres, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of CORN suitability") ///
    ytitle("Total acres of corn") xtitle("Corn suitability index decile") ///
	xlabel(1(1)10)
restore


preserve
xtile si_decile = si_wheat, nq(10)
collapse (sum) acres = total_corn_harvested_acres, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of WHEAT suitability") ///
    ytitle("Total acres of corn") xtitle("wheat suitability index decile") ///
	xlabel(1(1)10)
restore


preserve
xtile si_decile = si_peanut, nq(10)
collapse (sum) acres = total_corn_harvested_acres, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of PEANUT suitability") ///
    ytitle("Total acres of corn") xtitle("peanut suitability index decile") ///
	xlabel(1(1)10)
restore

preserve
xtile si_decile = si_rice, nq(10)
collapse (sum) acres = total_corn_harvested_acres, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of RICE suitability") ///
    ytitle("Total acres of corn") xtitle("rice suitability index decile") ///
	xlabel(1(1)10)
restore


// gov payments 
preserve
xtile si_decile = si_corn, nq(10)
collapse (mean) acres = share_corn_harvested_acres, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of CORN suitability") ///
    ytitle("Share of acres that are corn") xtitle("Corn suitability index decile") ///
	xlabel(1(1)10)
restore


preserve
xtile si_decile = si_soybean, nq(10)
collapse (mean) acres = share_corn_harvested_acres, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of SOY suitability") ///
    ytitle("Share of acres that are corn") xtitle("Corn suitability index decile") ///
	xlabel(1(1)10)
restore


preserve
xtile si_decile = si_wheat, nq(10)
collapse (mean) acres = share_corn_harvested_acres, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of WHEAT suitability") ///
    ytitle("Share acres of corn") xtitle("wheat suitability index decile") ///
	xlabel(1(1)10)
restore


preserve
xtile si_decile = si_peanut, nq(10)
collapse (mean) acres = share_corn_harvested_acres, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of PEANUT suitability") ///
    ytitle("Share acres of corn") xtitle("peanut suitability index decile") ///
	xlabel(1(1)10)
restore

preserve
xtile si_decile = si_rice, nq(10)
collapse (mean) acres = share_corn_harvested_acres, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of RICE suitability") ///
    ytitle("Share acres of corn") xtitle("rice suitability index decile") ///
	xlabel(1(1)10)
restore


// gov payments
preserve
xtile si_decile = si_corn, nq(10)
collapse (mean) acres = gov_noncons_amt_calc, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Govt payments by decile of CORN suitability") ///
    ytitle("Share of acres that are corn") xtitle("Corn suitability index decile") ///
	xlabel(1(1)10)
restore


preserve
xtile si_decile = si_wheat, nq(10)
collapse (mean) acres = gov_noncons_amt_calc, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of WHEAT suitability") ///
    ytitle("Share acres of corn") xtitle("wheat suitability index decile") ///
	xlabel(1(1)10)
restore


preserve
xtile si_decile = si_peanut, nq(10)
collapse (mean) acres = gov_noncons_amt_calc, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of PEANUT suitability") ///
    ytitle("Share acres of corn") xtitle("peanut suitability index decile") ///
	xlabel(1(1)10)
restore

preserve
xtile si_decile = si_rice, nq(10)
collapse (mean) acres = gov_noncons_amt_calc, by(si_decile)

twoway bar acres si_decile, barwidth(0.8) ///
    title("Corn harvested by decile of RICE suitability") ///
    ytitle("Share acres of corn") xtitle("rice suitability index decile") ///
	xlabel(1(1)10)
restore

twoway ///
    (histogram si_corn, width(500) color(navy%40) lcolor(navy) ///
        legend(label(1 "Corn suitability"))) ///
    (histogram si_soybean, width(500) color(orange%40) lcolor(orange) ///
        legend(label(2 "Soybean suitability"))), ///
    legend(order(1 2) pos(6) ring(0)) ///
    title("Distribution of suitability indices") ///
    xtitle("Suitability index value") ///
    ytitle("Frequency") ///
    note("Transparent fill allows overlap to be visible")

