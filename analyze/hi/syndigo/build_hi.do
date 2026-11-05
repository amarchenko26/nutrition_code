

clear all
set graphics on 

// Construct an IV at HH level 
// IV is the projection-factor-weighted average income of all other households in the same demographic cell (hh_size × education × occupation), nationwide, EXCLUDING households in the same zip code


// Panelist cols: 
//   Columns: ['hhid', 'panel_year', 'projection_factor', 'projection_factor_magnet', 'household_income', 'hh_size', 'type_of_residence', 'hh_comp', 'kids', 'male_head_age', 'female_head_age', 'male_head_employment', 'female_head_employment', 'male_head_education', 'female_head_education', 'male_head_occupation', 'female_head_occupation', 'male_head_birth', 'female_head_birth', 'marital_status', 'race', 'hisp', 'panelist_zip_code', 'fips_state_code', 'fips_county_code', 'region_code', 'wic_indicator_current', 'wic_indicator_ever_not_current', 'household_income_label', 'household_income_midpoint', 'hh_comp_label', 'kids_label', 'male_head_age_label', 'female_head_age_label', 'male_head_employment_label', 'female_head_employment_label', 'male_head_education_label', 'female_head_education_label', 'marital_status_label', 'race_label', 'hisp_label']

//hh_employed: 0/1 indicator, averaged across heads (so a two-earner household = 1.0, one-earner = 0.5, no earner = 0.0)
// hh_avg_workhours_if_employed 
* Under 30 hours --> 20 
* 30-34 hours --> 32
* 35+ hrs --> 40
* Not Employed --> 0 (excluded from hh_avg_workhours_if_employed, included in hh_avg_workhours)

// expenditure cols
// ['hhid', 'spend_total', 'spend_produce', 'spend_bread', 'spend_whole_bread', 'spend_high_sugar', 'spend_magnet_data', 'spend_dairy_milk_refrigerated', 'spend_reference_card_meat', 'spend_soft_drinks___carbonated', 'spend_cereal___ready_to_eat', 'spend_soft_drinks___low_calorie', 'spend_bakery___bread___fresh', 'spend_cookies', 'spend_yogurt_refrigerated', 'spend_candy_chocolate', 'spend_reference_card_fruits', 'spend_soup_canned', 'spend_reference_card_prepared_foods', 'spend_ice_cream___bulk', 'spend_fresh_fruit_remaining', 'spend_snacks___potato_chips', 'spend_pizza_frozen', 'spend_cheese___shredded', 'spend_reference_card_poultry', 'spend_eggs_fresh', 'panel_year', 'spend_share_produce', 'spend_share_whole_bread', 'spend_share_high_sugar']


// hh_avg_yrsofschool: education in years of schooling (6/10/12/14/16/18), averaged across both heads; single head used if only one present
// hh_avg_workhours: weekly hours worked (24/32/40 for employed, 0 for not employed), averaged across heads
// hh_employed: 0/1 indicator, averaged across heads (so a two-earner household = 1.0, one-earner = 0.5, no earner = 0.0)

// ============================================================================
// PATHS
// ============================================================================

// pq use using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panelists/panelists_all_years.parquet", clear
// //
// //
// use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/ssiv_zip_year.dta", clear


// ============================================================================
// Load HH panel
// ============================================================================

pq use using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/panel_hh_year.parquet", clear

tostring zip_code, replace format(%05.0f)

egen zip_by_year = group(zip_code panel_year)

// N = 289k
merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panelists/panelists_all_years.dta", ///
	keepusing(hh_avg_yrsofschool hh_avg_workhours hh_avg_workhours_if_employed hh_employed ///
	          male_head_occupation fips_county_code fips_state_code age_and_presence_of_children ///
	          household_composition hispanic_origin race obesity n_dietary_conditions hypertension ///
	          heart_disease diabetes_type1 diabetes_type2 cholesterol any_metabolic_disease ///
	          male_head_status female_head_status male_head_employed female_head_employed ///
	          n_heads n_head_earners hh_total_workhours ///
	          has_recovered_partner recovered_partner_sex n_recovered_partner_earners n_earners_total) ///
	keep(master match)

drop _merge


merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/iv_income.dta", ///
	keepusing(iv_income_zip iv_income_fips iv_cell_n_lo_fips iv_cell_n_lo_zip cell_zip_share) ///
	keep(master match)

drop _merge


merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/expenditure_hh_year.dta", ///
	keepusing(spend_total spend_produce spend_high_sugar  spend_soft_drinks___carbonated spend_cookies spend_ice_cream___bulk spend_share_produce) ///
	keep(master match)
	
drop _merge 


merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/trips_hh_year.dta", ///
	keepusing(n_trips avg_spend_trip) ///
	keep(master match)
	
drop _merge

merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/panel_prep_time.dta", ///
	keepusing(prep_time_raw prep_time) keep(master match)
	
drop _merge


// USDA-based Health Index + component outcomes (built by build_hi_usda_panel.py;
// hi_usda standardized identically to hi; components: added sugar is USDA-only,
// addsugar_cal_share_usda = share of matched calories carrying an added-sugar value)
merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/hi_usda_panel.dta", ///
	keepusing(hi_usda hi_usda_allcott rHI_usda total_cals_usda ///
	sugar_per_1000cal_usda sodium_per_1000cal_usda addsugar_per_1000cal_usda ///
	addsugar_cal_share_usda produce_usda whole_usda) keep(master match)
drop _merge

// Full HEI-2020 (USDA scoring, 0-100; built by build_hei_panel.py).
// hei_usda_90 excludes the fatty-acid component (module-median imputed) and rescales.
cap merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/hei_usda_panel.dta", ///
	keepusing(hei_usda hei_usda_90) keep(master match)
cap drop _merge

rename *, lower

tostring fips_state_code, replace format(%02.0f)
tostring fips_county_code, replace format(%03.0f)
g fips = fips_state_code + fips_county_code

ren age_and_presence_of_children kids
ren household_code hhid
ren household_composition hh_comp
ren household_size hh_size
ren hispanic_origin hisp
ren spend_soft_drinks___carbonated spend_soda
ren spend_ice_cream___bulk spend_icecream
ren panel_year year 

// ============================================================================
// Winsorize hi

winsor2 hi, replace cuts(1 99)

cap confirm variable hi_usda
if _rc == 0 winsor2 hi_usda, replace cuts(1 99)


// ============================================================================
// Winsorize number of grocery trips

winsor2 n_trips, replace cuts(1 99)


// ============================================================================
// Make income 10k rather than 1k 
replace real_income = real_income / 10


// ============================================================================
// Define Movers

bysort hhid zip_code (year): gen byte uniq_zip = (_n == 1)

* Count how many distinct zip codes each household has
bysort hhid: egen n_unique_zip = total(uniq_zip)
gen byte movers = (n_unique_zip > 1)

label define movers_lbl 0 "non-mover" 1 "mover"
label values movers movers_lbl

// Define movers by change in FIPS
bysort hhid fips (year): gen byte uniq_fips = (_n == 1)

bysort hhid: egen n_unique_fips = total(uniq_fips)
gen byte movers_f = (n_unique_fips > 1)
la var movers_f "FIPS movers"

label define movers_lblf 0 "non-mover" 1 "mover"
label values movers_f movers_lblf

xtset hhid year


// ============================================================================
// define forward diff
cap drop f_hi
gen f_hi = F.hi - hi

cap drop f_hi_usda
cap gen f_hi_usda = F.hi_usda - hi_usda

cap drop f_inc
gen f_inc = F.real_income - real_income

cap drop f_iv
gen f_iv = F.iv_income_fips - iv_income_fips

cap drop f_n_trips
gen f_n_trips = F.n_trips - n_trips

// Labor supply variable construction (head hours, FT/PT/NE indicators, forward
// diffs, head-specific shocks) moved to analyze/labor_supply/1_build_labor_supply_data.do

// ============================================================================
// Save final dataset
save "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/final/final_reg_data.dta", replace


use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/final/final_reg_data.dta", replace

// ============================================================================

eststo clear

preserve 
keep if movers_f == 0 // No FIPS FE needed!!

// (1) OLS - no controls
// OLS beta, no HH fixed effects = .0028, t = 33
eststo m1: reghdfe hi real_income [pw = projection_factor], ///
	 cluster(fips)

// (2) OLS - with HH fixed effects
// OLS beta = .00015, t = 1.67
eststo m2: reghdfe hi real_income [pw = projection_factor], ///
	absorb(year hhid kids hh_comp avg_age_hh_head) cluster(fips)

// (3) First stage
// first stage (beta = .327, t = 51)
eststo m3: reghdfejl real_income iv_income_fips [pw = projection_factor], ///
	absorb(year hhid kids hh_comp avg_age_hh_head) vce(cluster zip_code)

// (4) 2SLS FIPS
// 2SLS FIPS (beta = .0019***)
eststo m4: reghdfejl hi (real_income=iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (5) Small-share HHs IV
// Small-share HHs: restrict to HHs whose cell is <25% of their zip's weight.
// These HHs' national wage shift can't mechanically drive their local area income.
// beta = .0024***
eststo m5: ivreghdfe hi (real_income=iv_income_zip) [pw = projection_factor] ///
	if cell_zip_share < 0.25, ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (6) FD
// FD .001 t = 1.02
eststo m6: reghdfejl D.hi (D.real_income=D.iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (7) Forward diff
// Forward diff (t+1-t) on (t - t-1)
// beta = .0026, t = 1.88**
eststo m7: reghdfejl f_hi (D.real_income=D.iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (8) Pre-trend
// placebo test -- regress future change in income on past change in nutrition
// -.00003, p = .848
eststo m8: ivreghdfe D.hi (f_inc = f_iv) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// ============================================================================
// USDA-based HI: re-run the key forward-diff and pre-trend on hi_usda
// (identical non-mover sample and spec; only the HI nutrition source differs)
// ============================================================================
eststo m7_usda: reghdfejl f_hi_usda (D.real_income=D.iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

eststo m8_usda: ivreghdfe D.hi_usda (f_inc = f_iv) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// Side-by-side: Syndigo vs USDA HI (forward diff = main IV result; pre-trend = placebo)
esttab m7 m7_usda m8 m8_usda, ///
	keep(D.real_income f_inc) b(4) se(4) ///
	star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("FwdDiff Syndigo" "FwdDiff USDA" "Pre-trend Syndigo" "Pre-trend USDA") ///
	stats(N, fmt(%9.0fc) labels("N"))

// Export to LaTeX
esttab m1 m2 m6 m7 m8 using "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/tabs/results.tex", ///
	replace booktabs label ///
	b(3) se(3) ///
	star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("OLS" "OLS+HHFE" "First Diff" "Fwd Diff" "Pre-trends") ///
	keep(real_income D.real_income f_inc) ///
	varlabels(real_income "Income (t)" D.real_income "Income (t-1)" f_inc "Income (t+1)") ///
	stats(N r2, fmt(%9.0fc %9.4f) labels("N" "R-squared")) ///
	nonotes
	
	
esttab m4 m5 using "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/tabs/results_robust.tex", ///
	replace booktabs label ///
	b(3) se(3) ///
	star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("2SLS" "2SLS Small Share") ///
	keep(real_income) ///
	varlabels(real_income "Income (t)" D.real_income "Income (t-1)" f_inc "Income (t+1)") ///
	stats(N r2, fmt(%9.0fc %9.4f) labels("N" "R-squared")) ///
	nonotes
	
restore 



*------------------------------------------------------------
* Forward difference regression by quartile
*------------------------------------------------------------
cap drop inc_q
estimates clear 

// Sort HHs by baseline income
bysort hhid (year): gen baseline_income = real_income[1]
xtile inc_q = baseline_income [aw=projection_factor], nquantiles(4)

// 2SLS by quintile
forvalues q = 1/4 {
	reghdfejl hi (real_income=iv_income_fips) [pw = projection_factor] if movers==0 & inc_q == `q', ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
    
	estimates store iv_q`q'
}
set graphics on
coefplot iv_q*, keep(real_income) ///
    rename(real_income = " ") ///
    xtitle("Income quartile", size(medlarge)) ytitle("IV effect on Nutrition (std. dev. per $10k)", size(medlarge)) ///
    title("Effect of Income on Nutrition by Baseline Income") ///
    vertical yline(0, lcolor(gray) lpattern(solid)) ciopts(recast(rcap) lwidth(medthick)) ///
    yline(0.02, lcolor(blue) lpattern(shortdash) lwidth(medthick)) legend(off) ///
    text(.025 .57 "Overall", color(blue) size(medlarge)) ///
    xlabel(.7 "Q1 (Lowest)" .9 "Q2" 1.1 "Q3" 1.3 "Q4 (Highest)", labsize(medlarge)) ///
    ylabel(, labsize(medlarge))

graph export "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/baseline_inc.png", replace 

// Export one blank one 
coefplot iv_q*, keep(real_income) ///
    rename(real_income = " ") ///
    xtitle("Income quartile", size(medlarge)) ytitle("IV effect on Nutrition (std. dev. per $10k)", size(medlarge)) ///
    title("Effect of Income on Nutrition by Baseline Income") ///
    vertical yline(0, lcolor(gray) lpattern(solid)) ///
    yline(0.02, lcolor(blue) lpattern(shortdash) lwidth(medthick)) legend(off) ///
    text(.025 .57 "Overall", color(blue) size(medlarge)) ///
    xlabel(.7 "Q1 (Lowest)" .9 "Q2" 1.1 "Q3" 1.3 "Q4 (Highest)", labsize(medlarge)) ///
    ylabel(, labsize(medlarge)) ///
    mcolor(white) mlcolor(white) ciopts(recast(rcap) lwidth(medthick) lcolor(white))
	
graph export "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/baseline_inc_blank.png", replace 

	
// 2SLS by quintile
forvalues q = 1/4 {
	reghdfejl real_income iv_income_fips [pw = projection_factor] if movers==0 & inc_q == `q', ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
    
	estimates store iv_q`q'
}


*------------------------------------------------------------
* Binned scatter plots using fitted (instrumented) income
*------------------------------------------------------------

* Flag households where occupation never changes from baseline
bysort hhid (year): gen baseline_occ = male_head_occupation[1]
gen occ_match = (male_head_occupation == baseline_occ)
bysort hhid: egen occupation_stable = min(occ_match)

reghdfejl hi (real_income = iv_income_fips) ///
    [pw = projection_factor] if movers_f==0 & occupation_stable==1, ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)


*------------------------------------------------------------
* Binned scatter plots using fitted (instrumented) income
*------------------------------------------------------------

* --- 1. First stage: get fitted values of income -----------

cap drop Dhat_income fhat_income r_fhi r_Dhat r_dhi r_fhat

* Forward diff first stage
reghdfe D.real_income D.iv_income_fips [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
predict Dhat_income, xb        // fitted instrumented income

* Pre-trend first stage  
reghdfe f_inc f_iv [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
predict fhat_income, xb        // fitted instrumented income


* --- 2. Residualize Y and fitted X on FEs ------------------
* binscatter can't absorb HDFEs, so partial them out manually

* -- Forward diff --
reghdfe f_hi [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_fhi, resid

reghdfe Dhat_income [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_Dhat, resid

* -- Pre-trend --
reghdfe D.hi [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_dhi, resid

reghdfe fhat_income [pw = projection_factor], ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips) resid
predict r_fhat, resid


* --- 3. Binned scatter plots --------------------------------

binscatter r_fhi r_Dhat [aw = projection_factor], ///
    nquantiles(15) ///
    xtitle("{&Delta} Instrumented Income Year Prior", size(medlarge)) ///
    ytitle("{&Delta} Nutrition Year After", size(medlarge)) ///
    title("Forward Difference", size(large)) ///
    xlabel(, labsize(medlarge)) ///
    ylabel(, labsize(medlarge)) ///
    lcolor(navy) mcolor(navy) ///
    name(g1, replace)

* Pre-trend placebo (expect flat/zero)
binscatter r_dhi r_fhat [aw = projection_factor], ///
    nquantiles(15) ///
    xtitle("{&Delta} Instrumented Income Year After", size(medlarge)) ///
    ytitle("{&Delta} Nutrition Year Prior", size(medlarge)) ///
    title("Pre-trend", size(large)) ///
    xlabel(, labsize(medlarge)) ///
    ylabel(, labsize(medlarge)) ///
    lcolor(maroon) mcolor(maroon) ///
    name(g2, replace)

* --- 4. Combine ---------------------------------------------
graph combine g1 g2, ///
    cols(2) ///
    ycommon xsize(7) ysize(3.5)

graph export "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/iv_binscatter.png", replace width(2400)



*------------------------------------------------------------
* Other outcomes
*------------------------------------------------------------

local diet_vars  total_cals whole produce sugar_per_1000cal
local spend_vars spend_total spend_produce spend_share_produce spend_high_sugar spend_soda 

foreach var in `diet_vars' `spend_vars' {
    cap gen f_`var' = F.`var' - `var'
}

************* Diet vars
foreach var in `diet_vars' {
	quietly eststo f_`var': ivreghdfe f_`var' (D.real_income=D.iv_income_fips) ///
        [pw = projection_factor] if movers_f == 0, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
    quietly summarize f_`var' if e(sample)
    local mean_`var' = r(mean)
    estadd scalar mean_y = `mean_`var'': f_`var'
}

************* Spend vars
foreach var in `spend_vars' {
	
	quietly eststo f_`var': ivreghdfe f_`var' (D.real_income=D.iv_income_fips) ///
        [pw = projection_factor] if movers_f == 0, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
    quietly summarize f_`var' if e(sample)
    local mean_`var' = r(mean)
    estadd scalar mean_y = `mean_`var'': f_`var'
}


esttab f_spend_total f_spend_high_sugar f_spend_share_produce f_total_cals f_sugar_per_1000cal  ///
    using "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/tabs/expenditure.tex", replace ///
    keep(D.real_income) varlabels(D1.real_income "$m_t$") ///
    b(3) se(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
	mlabels("Total" "High sugar" "Produce share" "Calories" "Sugar density") ///
    mgroups("Expenditure (\\$s per year)" "Nutrition Components", pattern(1 0 0 1 0) ///
        prefix(\multicolumn{@span}{c}{) suffix(}) ///
        span erepeat(\cmidrule(lr){@span})) ///
    collabels(none) ///
    booktabs ///
    stats(mean_y N, fmt(%9.2fc %9.0fc) labels("Mean outcome" "N")) ///
    nonotes

	
************* Health vars
local health_vars any_diabetes obesity any_metabolic_disease cholesterol diabetes_type2 heart_disease hypertension
foreach var in `health_vars' {
    eststo `var': ivreghdfe F3.`var' (D.real_income=D.iv_income_fips) [pw = projection_factor] if movers_f==0, ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

    quietly summarize `var' if e(sample)
    local mean_`var' = r(mean)
    estadd scalar mean_y = `mean_`var'': `var'
}

esttab any_metabolic_disease hypertension obesity cholesterol  ///
    using "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/tabs/health.tex", replace ///
    keep(D.real_income) varlabels(D.real_income "$m_t$") ///
    b(3) se(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
	mlabels("Any Metabolic Disease" "Hypertension" "Obesity" "High Cholesterol") ///
    collabels(none) ///
    booktabs ///
    stats(mean_y N, fmt(%9.3fc %9.0fc) labels("Mean outcome" "N")) ///
    nonotes
	
	
************* Num trips 
ivreghdfe n_trips (real_income=iv_income_fips) [pw = projection_factor] if movers_f==0, ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)


ivreghdfe f_n_trips (D.real_income=D.iv_income_fips) [pw = projection_factor] if movers_f==0, ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

	
************* Prep time
cap gen f_prep_time = F.prep_time - prep_time
ivreghdfe f_prep_time (D.real_income=D.iv_income_fips) [pw = projection_factor] if movers_f==0, absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)


************* Labor supply (hours worked, employment, by sex of head)
* Moved to analyze/labor_supply/4_build_labor_supply_regs.do

*------------------------------------------------------------
* Compliers
*------------------------------------------------------------

	
reghdfejl male_head_occupation  (real_income = iv_income_fips) [pw = projection_factor] if movers_f==0, absorb(year hhid kids hh_comp) cluster(fips)
	
	
	
	
	
************* Forward Differences Setup
* Generate first differences for all outcomes
local diet_vars whole produce sugar_per_1000cal total_cals
local spend_vars spend_total spend_produce spend_high_sugar spend_soda spend_cookies spend_icecream spend_share_produce avg_spend_trip
local health_vars any_diabetes any_metabolic_disease cholesterol diabetes_type1 diabetes_type2 heart_disease hypertension obesity

foreach var in `diet_vars' `spend_vars' `health_vars' {
    cap gen f_`var' = F.`var' - `var'
}

************* Diet vars
local diet_vars whole produce sugar_per_1000cal total_cals
foreach var in `diet_vars' {
    eststo f_`var': ivreghdfe f_`var' (D.real_income=D.iv_income_fips) ///
        [pw = projection_factor] if movers_f == 0, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
}
esttab f_whole f_produce f_sugar_per_1000cal f_total_cals, ///
    keep(D.real_income) compress ///
    b(3) p(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    title("FD-IV Results: Effect of Income on Diet") ///
    mtitles("whole" "produce" "sugar_per_1000cal" "total_cals")

************* Spend vars
local spend_vars spend_total spend_produce spend_high_sugar spend_soda spend_cookies spend_icecream spend_share_produce
foreach var in `spend_vars' {
    eststo f_`var': ivreghdfe f_`var' (D.real_income=D.iv_income_fips) ///
        [pw = projection_factor] if movers_f == 0, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
}
esttab f_spend_total f_spend_produce f_spend_high_sugar f_spend_soda f_spend_cookies f_spend_icecream f_spend_share_produce, ///
    keep(D.real_income) compress ///
    b(3) p(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    title("FD-IV Results: Effect of Income on Spending") ///
    mtitles("spend_total" "spend_produce" "spend_high_sugar" "spend_soda" "spend_cookies" "spend_ice_cream" "spend_share_produce")

************* Health vars
local health_vars any_diabetes any_metabolic_disease cholesterol diabetes_type1 diabetes_type2 heart_disease hypertension obesity
foreach var in `health_vars' {
    eststo `var': ivreghdfe `var' (real_income=iv_income_fips) ///
        [pw = projection_factor] if movers_f == 0, ///
		absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
}
esttab any_diabetes any_metabolic_disease cholesterol diabetes_type1 diabetes_type2 heart_disease hypertension obesity, ///
    keep(real_income) compress ///
    b(3) p(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    title("FD-IV Results: Effect of Income on Health") ///
    mtitles("diabetes" "met_disease" "cholesterol" "diabetes_type1" "diabetes_type2" "heart_disease" "hypertension" "obesity")



		

	
// ============================================================================

// Construct an SSIV at zip-code level 

preserve 
collapse (mean) hi real_income [aw=projection_factor], by(zip_code year)

merge 1:1 zip_code year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/ssiv_zip_year.dta", ///
	keepusing(ssiv_income) ///
	keep(master match)


ivreghdfe hi (real_income=ssiv_income), ///
	absorb(year zip_code) robust

restore 

	
	
// ============================================================================
// Check HI is correlated with disease

preserve 

collapse obesity n_dietary_conditions hypertension heart_disease diabetes_type1 diabetes_type2 cholesterol any_metabolic_disease hi real_income avg_age_hh_head hh_comp [pw=projection_factor], by(hhid)

#delimit ; 
binscatter diabetes_type2 hi, 
    n(50)
    msymbol(O)
    linetype(lfit)
    xtitle("Nutrition")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/corr_diab_hi.png")
    replace
;

binscatter obesity hi, 
    n(50)
    msymbol(O)
    linetype(lfit)
    xtitle("Nutrition")
    ytitle("Obesity (any HH member)")
    savegraph("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/corr_ob_hi.png")
    replace;
	
binscatter cholesterol hi, 
    n(50)
    msymbol(O)
    linetype(lfit)
    xtitle("Nutrition")
    ytitle("Cholesterol (any HH member)")
    savegraph("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/corr_chol_hi.png")
    replace;

binscatter any_metabolic_disease hi, 
    n(50)
    msymbol(O)
    linetype(lfit)
    xtitle("Nutrition")
    ytitle("Any Metabolic Disease (any HH member)")
    savegraph("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/corr_any_hi.png")
    replace;
	
	
binscatter obesity real_income, 
    n(40)
    msymbol(O)
    linetype(qfit)
    xtitle("HH income $1000s")
    ytitle("Obesity (any HH member)")
    savegraph("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/corr_ob_inc.png")
    replace;
	
	
binscatter diabetes_type2 real_income, 
    n(40)
    msymbol(O)
	controls(avg_age_hh_head hh_comp)
    linetype(qfit)
    xtitle("HH income $1000s")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/corr_diab_inc.png")
    replace;
	
	
binscatter diabetes_type2 real_income, 
    n(40)
	controls(hi)
    msymbol(O)
    linetype(qfit)
    xtitle("HH income $1000s (CONTROLLING FOR HI)")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/corr_diab_inc2.png")
    replace;
	
binscatter diabetes_type2 hi, 
    n(40)
	controls(real_income)
    msymbol(O)
    linetype(qfit)
    xtitle("HH Nutrition (controlling for income)")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/corr_diab_hi2.png")
    replace;

	
binscatter hi real_income, 
    n(50)
    msymbol(O)
    linetype(qfit)
    xtitle("HH income $1000s")
    ytitle("HH Nutrition")
    savegraph("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/corr_inc_hi.png")
    replace;

#delimit cr

restore


	
	
	
	
	
	
	
	
	
	
	
	
	