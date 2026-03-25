

clear all
set graphics on 

// Construct an IV at HH level 
// IV is the projection-factor-weighted average income of all other households in the same demographic cell (hh_size × education × occupation), nationwide, EXCLUDING households in the same zip code


// Panelist cols: 
//   Columns: ['hhid', 'panel_year', 'projection_factor', 'projection_factor_magnet', 'household_income', 'hh_size', 'type_of_residence', 'hh_comp', 'kids', 'male_head_age', 'female_head_age', 'male_head_employment', 'female_head_employment', 'male_head_education', 'female_head_education', 'male_head_occupation', 'female_head_occupation', 'male_head_birth', 'female_head_birth', 'marital_status', 'race', 'hisp', 'panelist_zip_code', 'fips_state_code', 'fips_county_code', 'region_code', 'wic_indicator_current', 'wic_indicator_ever_not_current', 'household_income_label', 'household_income_midpoint', 'hh_comp_label', 'kids_label', 'male_head_age_label', 'female_head_age_label', 'male_head_employment_label', 'female_head_employment_label', 'male_head_education_label', 'female_head_education_label', 'marital_status_label', 'race_label', 'hisp_label']

//hh_employed: 0/1 indicator, averaged across heads (so a two-earner household = 1.0, one-earner = 0.5, no earner = 0.0)

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
	keepusing(hh_avg_yrsofschool hh_avg_workhours hh_employed male_head_occupation fips_county_code fips_state_code hh_avg_workhours age_and_presence_of_children household_composition hispanic_origin race obesity n_dietary_conditions hypertension heart_disease diabetes_type1 diabetes_type2 cholesterol any_metabolic_disease) ///
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

cap drop f_inc
gen f_inc = F.real_income - real_income

cap drop f_iv
gen f_iv = F.iv_income_fips - iv_income_fips


// ============================================================================
// Save final dataset
save "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/final_reg_data.dta", replace



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
// beta = .0026, t = 1.91**
eststo m7: reghdfejl f_hi (D.real_income=D.iv_income_fips) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// (8) Pre-trend
// placebo test -- regress future change in income on past change in nutrition
// -.00003, p = .848
eststo m8: ivreghdfe D.hi (f_inc = f_iv) [pw = projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// Export to LaTeX
esttab m1 m2 m6 m7 m8 using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/tabs/results.tex", ///
	replace booktabs label ///
	b(3) se(3) ///
	star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("OLS" "OLS+HHFE" "First Diff" "Fwd Diff" "Pre-trends") ///
	keep(real_income D.real_income f_inc) ///
	varlabels(real_income "Income (t)" D.real_income "Income (t-1)" f_inc "Income (t+1)") ///
	stats(N r2, fmt(%9.0fc %9.4f) labels("N" "R-squared")) ///
	nonotes addnotes("* p$<$0.10, ** p$<$0.05, *** p$<$0.01. Outcome variable is differences of nutrition. Income is in 10,000s.")
	
	
esttab m4 m5 using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/tabs/results_robust.tex", ///
	replace booktabs label ///
	b(3) se(3) ///
	star(* 0.10 ** 0.05 *** 0.01) ///
	mtitles("2SLS" "2SLS Small Share") ///
	keep(real_income) ///
	varlabels(real_income "Income (t)" D.real_income "Income (t-1)" f_inc "Income (t+1)") ///
	stats(N r2, fmt(%9.0fc %9.4f) labels("N" "R-squared")) ///
	nonotes addnotes("* p$<$0.10, ** p$<$0.05, *** p$<$0.01. Outcome variable is nutrition. Income is in 10,000s.")
	
restore 







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
    nquantiles(20) ///
    xtitle("{&Delta} Instrumented Income Year Prior", size(medlarge)) ///
    ytitle("{&Delta} Nutrition Year After", size(medlarge)) ///
    title("Forward Difference", size(large)) ///
    xlabel(, labsize(medlarge)) ///
    ylabel(, labsize(medlarge)) ///
    lcolor(navy) mcolor(navy) ///
    name(g1, replace)

* Pre-trend placebo (expect flat/zero)
binscatter r_dhi r_fhat [aw = projection_factor], ///
    nquantiles(20) ///
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

graph export "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/iv_binscatter.png", replace width(2400)


************* Diet vars
local diet_vars whole produce sugar_per_1000cal total_cals 
foreach var in `diet_vars' {
    eststo `var': ivreghdfe `var' (real_income=iv_income_fips) [pw = projection_factor] if movers_f==0, ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
}

esttab `diet_vars', ///
    keep(real_income) compress ///
    b(3) p(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    title("IV Regression Results: Effect of Income") ///
    mtitles(`diet_vars')

*************
************* Spend vars
local spend_vars spend_total spend_produce spend_high_sugar spend_soda spend_cookies spend_icecream spend_share_produce
foreach var in `spend_vars' {
    eststo `var': ivreghdfe `var' (real_income=iv_income_fips) [pw = projection_factor] if movers_f==0, ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
}

* Print table 
esttab spend_total spend_produce spend_high_sugar spend_soda, ///
    keep(real_income) compress ///
    b(3) p(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    title("IV Regression Results: Effect of Income") ///
    mlabels("total" "produce" "high_sugar" "soda" "cookies" "icecream" "share_produce")

	
************* Health vars
local health_vars any_diabetes obesity any_metabolic_disease cholesterol diabetes_type2 heart_disease hypertension obesity
foreach var in `health_vars' {
    eststo `var': ivreghdfe `var' (real_income=iv_income_fips) [pw = projection_factor] if movers_f==0, ///
    absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)
}

* Print table 
esttab `health_vars', ///
    keep(real_income) compress ///
    b(3) p(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    title("IV Regression Results: Effect of Income") ///
    mtitles(`health_vars')


	
************* First Differences Setup
* Generate first differences for all outcomes
local diet_vars whole produce sugar_per_1000cal total_cals
local spend_vars spend_total spend_produce spend_high_sugar spend_soda spend_cookies spend_ice_cream___bulk spend_share_produce
local health_vars any_diabetes any_metabolic_disease cholesterol diabetes_type1 diabetes_type2 heart_disease hypertension obesity

foreach var in `diet_vars' `spend_vars' `health_vars' {
    cap gen d_`var' = D.`var'
}

************* Diet vars
local diet_vars whole produce sugar_per_1000cal total_cals
foreach var in `diet_vars' {
    eststo d_`var': ivreghdfe d_`var' (d_real_income = d_iv_income_fips) ///
        [pw = projection_factor] if movers_f == 0, ///
        absorb(year d_kids d_hh_comp d_avg_age_hh_head) cluster(fips)
}
esttab d_whole d_produce d_sugar_per_1000cal d_total_cals, ///
    keep(d_real_income) compress ///
    b(3) p(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    title("FD-IV Results: Effect of Income on Diet") ///
    mtitles("whole" "produce" "sugar_per_1000cal" "total_cals")

************* Spend vars
local spend_vars spend_total spend_produce spend_high_sugar spend_soda spend_cookies spend_ice_cream___bulk spend_share_produce
foreach var in `spend_vars' {
    eststo d_`var': ivreghdfe d_`var' (d_real_income = d_iv_income_fips) ///
        [pw = projection_factor] if movers_f == 0, ///
        absorb(year d_kids d_hh_comp d_avg_age_hh_head) cluster(fips)
}
esttab d_spend_total d_spend_produce d_spend_high_sugar d_spend_soda d_spend_cookies d_spend_ice_cream___bulk d_spend_share_produce, ///
    keep(d_real_income) compress ///
    b(3) p(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    title("FD-IV Results: Effect of Income on Spending") ///
    mtitles("spend_total" "spend_produce" "spend_high_sugar" "spend_soda" "spend_cookies" "spend_ice_cream" "spend_share_produce")

************* Health vars
local health_vars any_diabetes any_metabolic_disease cholesterol diabetes_type1 diabetes_type2 heart_disease hypertension obesity
foreach var in `health_vars' {
    eststo d_`var': ivreghdfe d_`var' (d_real_income = d_iv_income_fips) ///
        [pw = projection_factor] if movers_f == 0, ///
        absorb(year d_kids d_hh_comp d_avg_age_hh_head) cluster(fips)
}
esttab d_any_diabetes d_any_metabolic_disease d_cholesterol d_diabetes_type1 d_diabetes_type2 d_heart_disease d_hypertension d_obesity, ///
    keep(d_real_income) compress ///
    b(3) p(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    title("FD-IV Results: Effect of Income on Health") ///
    mtitles("any_diabetes" "any_metabolic_disease" "cholesterol" "diabetes_type1" "diabetes_type2" "heart_disease" "hypertension" "obesity")



		
cap drop inc_q
estimates clear 
xtile inc_q = real_income [aw=projection_factor], nq(5)

// IV by quintile
forvalues q = 1/5 {
    ivreghdfe d_hi (d_real_income=d_iv_income_fips) [pw = projection_factor] if movers_f==0 & inc_q == `q', ///
	absorb(year d_kids d_hh_comp d_avg_age_hh_head) cluster(fips)

    estimates store iv_q`q'
}
set graphics on
coefplot iv_q*, keep(d_real_income) ///
    rename(d_real_income = " ") ///
    xtitle("Income quintile") ytitle("IV effect on HI (std. dev. per $1k)") ///
    title("Effect of Income on Healthfulness by Income Group") ///
    vertical yline(0, lcolor(gray) lpattern(dash)) ciopts(recast(rcap))
//     xlabel(1 "Q1 (Lowest)" 2 "Q2" 3 "Q3" 4 "Q4" 5 "Q5 (Highest)") ///


	
	
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
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_diab_hi.png")
    replace
;

binscatter obesity hi, 
    n(50)
    msymbol(O)
    linetype(lfit)
    xtitle("Nutrition")
    ytitle("Obesity (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_ob_hi.png")
    replace;
	
binscatter cholesterol hi, 
    n(50)
    msymbol(O)
    linetype(lfit)
    xtitle("Nutrition")
    ytitle("Cholesterol (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_chol_hi.png")
    replace;

binscatter any_metabolic_disease hi, 
    n(50)
    msymbol(O)
    linetype(lfit)
    xtitle("Nutrition")
    ytitle("Any Metabolic Disease (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_any_hi.png")
    replace;
	
	
binscatter obesity real_income, 
    n(40)
    msymbol(O)
    linetype(qfit)
    xtitle("HH income $1000s")
    ytitle("Obesity (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_ob_inc.png")
    replace;
	
	
binscatter diabetes_type2 real_income, 
    n(40)
    msymbol(O)
	controls(avg_age_hh_head hh_comp)
    linetype(qfit)
    xtitle("HH income $1000s")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_diab_inc.png")
    replace;
	
	
binscatter diabetes_type2 real_income, 
    n(40)
	controls(hi)
    msymbol(O)
    linetype(qfit)
    xtitle("HH income $1000s (CONTROLLING FOR HI)")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_diab_inc2.png")
    replace;
	
binscatter diabetes_type2 hi, 
    n(40)
	controls(real_income)
    msymbol(O)
    linetype(qfit)
    xtitle("HH Nutrition (controlling for income)")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_diab_hi2.png")
    replace;

	
binscatter hi real_income, 
    n(50)
    msymbol(O)
    linetype(qfit)
    xtitle("HH income $1000s")
    ytitle("HH Nutrition")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_inc_hi.png")
    replace;

#delimit cr

restore


	
	
	
	
	
	
	
	
	
	
	
	
	