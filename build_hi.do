

clear all
set graphics on 


// Panelist cols: 
//   Columns: ['household_code', 'panel_year', 'projection_factor', 'projection_factor_magnet', 'household_income', 'household_size', 'type_of_residence', 'household_composition', 'age_and_presence_of_children', 'male_head_age', 'female_head_age', 'male_head_employment', 'female_head_employment', 'male_head_education', 'female_head_education', 'male_head_occupation', 'female_head_occupation', 'male_head_birth', 'female_head_birth', 'marital_status', 'race', 'hispanic_origin', 'panelist_zip_code', 'fips_state_code', 'fips_county_code', 'region_code', 'wic_indicator_current', 'wic_indicator_ever_not_current', 'household_income_label', 'household_income_midpoint', 'household_composition_label', 'age_and_presence_of_children_label', 'male_head_age_label', 'female_head_age_label', 'male_head_employment_label', 'female_head_employment_label', 'male_head_education_label', 'female_head_education_label', 'marital_status_label', 'race_label', 'hispanic_origin_label']

//hh_employed: 0/1 indicator, averaged across heads (so a two-earner household = 1.0, one-earner = 0.5, no earner = 0.0)


// ============================================================================
// PATHS
// ============================================================================

pq use using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panelists/panelists_all_years.parquet", clear
//
//
use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/ssiv_zip_year.dta", clear


// ============================================================================
// Load HH panel
// ============================================================================

// hh_avg_yrsofschool: education in years of schooling (6/10/12/14/16/18), averaged across both heads; single head used if only one present
// hh_avg_workhours: weekly hours worked (24/32/40 for employed, 0 for not employed), averaged across heads
// hh_employed: 0/1 indicator, averaged across heads (so a two-earner household = 1.0, one-earner = 0.5, no earner = 0.0)

// N = 289k 
pq use using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/panel_hh_year.parquet", clear

tostring zip_code, replace format(%05.0f)

// N = 289k
merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panelists/panelists_all_years.dta", ///
	keepusing(hh_avg_yrsofschool hh_avg_workhours hh_employed fips_county_code hh_avg_workhours age_and_presence_of_children age_and_presence_of_children_lab household_composition hispanic_origin hispanic_origin_label race race_label obesity n_dietary_conditions hypertension heart_disease diabetes_type1 diabetes_type2 cholesterol any_metabolic_disease) ///
	keep(master match)

drop _merge

merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/iv_income.dta", ///
	keepusing(iv_income iv_cell_n_lo) ///
	keep(master match)

drop _merge

rename *, lower

// ============================================================================
// Check HI is correlated with disease

collapse obesity n_dietary_conditions hypertension heart_disease diabetes_type1 diabetes_type2 cholesterol any_metabolic_disease hi real_income [pw=projection_factor], by(household_code)

#delimit ; 
binscatter diabetes_type2 hi, 
    n(50)
    msymbol(O)
    linetype(qfit)
    rd(0)
    xtitle("Household HI")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_diab_hi.png")
    replace
;

binscatter obesity hi, 
    n(50)
    msymbol(O)
    linetype(qfit)
    rd(0)
    xtitle("Household HI")
    ytitle("Obesity (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_ob_hi.png")
    replace;
	
binscatter cholesterol hi, 
    n(50)
    msymbol(O)
    linetype(qfit)
    rd(0)
    xtitle("Household HI")
    ytitle("Cholesterol (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_chol_hi.png")
    replace;

binscatter any_metabolic_disease hi, 
    n(50)
    msymbol(O)
    linetype(qfit)
    rd(0)
    xtitle("Household HI")
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
    rd(0)
    xtitle("Household HI (CONTROLLING FOR INCOME)")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_diab_hi2.png")
    replace;

	
binscatter hi real_income, 
    n(50)
    msymbol(O)
    linetype(qfit)
    xtitle("HH income $1000s")
    ytitle("Household HI")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_inc_hi.png")
    replace;

#delimit cr





// ============================================================================

// Construct an IV at HH level 
// IV is the projection-factor-weighted average income of all other households in the same demographic cell (household_size × education × occupation), nationwide, EXCLUDING households in the same zip code

// OLS 
reghdfe hi real_income [pw = projection_factor], ///
	absorb(panel_year zip_code) vce(cluster zip_code)

// first stage 
reghdfe real_income iv_income [pw = projection_factor], ///
	absorb(panel_year) vce(cluster zip_code)

// 2SLS 
// size of IV estimates is 50% larger than OLS.
ivreghdfe hi (real_income=iv_income) [pw = projection_factor], ///
	absorb(panel_year zip_code) cluster(zip_code)

// add some HH demos - kids, hispanic, race  	
// 2SLS 
ivreghdfe hi (real_income=iv_income) [pw = projection_factor], ///
	absorb(panel_year zip_code hispanic_origin race age_and_presence_of_children) cluster(zip_code)


// MERGE IN TOTAL EXPENDITURE ON FOOD 
// MERGE IN PRICES PAID???? 

local outcome_vars whole produce sugar_per_1000cal total_cals
foreach var in `outcome_vars' {
	
	ivreghdfe `var' (real_income=iv_income) [pw = projection_factor], absorb(panel_year zip_code hispanic_origin race age_and_presence_of_children) cluster(zip_code)

}

	
	
	
		
cap drop inc_q
estimates clear 
xtile inc_q = hhavincome [aw=projection_factor], nq(5)
// IV by quintile
forvalues q = 1/5 {
    ivreghdfe hi_allcott (real_income = iv_income) [pw=projection_factor] if inc_q == `q', ///
        absorb(panel_year zip_code hispanic_origin race age_and_presence_of_children) ///
        cluster(zip_code)
    estimates store iv_q`q'
}
set graphics on
coefplot iv_q*, keep(real_income) ///
    rename(real_income = " ") ///
    xtitle("Income quintile") ytitle("IV effect on HI (std. dev. per $1k)") ///
    title("Effect of Income on Healthfulness by Income Group") ///
    vertical yline(0, lcolor(gray) lpattern(dash)) ciopts(recast(rcap))
//     xlabel(1 "Q1 (Lowest)" 2 "Q2" 3 "Q3" 4 "Q4" 5 "Q5 (Highest)") ///


	
	
// ============================================================================

// Construct an SSIV at zip-code level 

preserve 
collapse (mean) hi real_income [aw=projection_factor], by(zip_code panel_year)

merge 1:1 zip_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/ssiv_zip_year.dta", ///
	keepusing(ssiv_income) ///
	keep(master match)


ivreghdfe hi (real_income=ssiv_income), ///
	absorb(panel_year zip_code) robust



restore 








	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	



// // ============================================================================
// // STEP 3: Drop reference card products
// // CollapseTransactions.do:26-27
// // ============================================================================
//
// drop if product_module_code >= 445 & product_module_code <= 468
//
// // Drop department_code 99 (magnet/random-weight transactions)
// capture drop if department_desc == "MAGNET DATA"
//
// drop if product_module_code == .
//
//
// // replace outliers 
// replace sodium_per_100g = . if sodium_per_100g > 5    // max plausible ~5g/100g (soy sauce)
// replace chol_per_100g = . if chol_per_100g > 2         // max plausible ~2g/100g (organ meats)
//
// // ============================================================================
// // STEP 5: Flag fruit and veg
// // UPCDataPrep.do:26-33
// // ============================================================================
//
// // freshfruit: UPCDataPrep.do:26
// gen freshfruit = cond(inlist(product_module_code, ///
//     453, 3560, 3563, 4010, 4085, 4180, 4225, 4230, 4355, 4470, 6049, 6050), 1, 0)
//
// replace freshfruit = 1 if inlist(product_group, "FRUIT")
//
// // fruit (includes canned/dried/frozen): UPCDataPrep.do:27-28
// // NOTE: product_group_code not available — omitting group 504, 1010 check
// //   (misses some canned/frozen fruit only identifiable by group code)
// gen fruit = cond(freshfruit == 1 ///
//     | inlist(product_module_code, 6, 42, 2664) == 1, 1, 0)
//
// replace fruit = 1 if inlist(product_group, "FRUIT - CANNED", "FRUIT - DRIED")
//	
// // ============================================================================
//
//	
// // freshveg: UPCDataPrep.do:30
// gen byte freshveg = cond(inlist(product_module_code, ///
//     460, 3544, 4015, 4020, 4023, 4050, 4055, 4060, ///
//     4140, 4275, 4280, 4350, 4400, 4415, 4460, 4475, 6064, 6070) == 1, 1, 0)
//
// replace freshveg = 1 if inlist(product_group, "FRESH PRODUCE")
// replace freshveg = 1 if inlist(product_module_normalized, "FRESH VEGETABLES REMAINING")
//
// // veg (includes canned/frozen): UPCDataPrep.do:31-33
// // EXCLUDES: cream corn (1071), frozen veg in pastry (2618),
// //   breaded frozen veg (2635), breaded mushrooms (2637),
// //   breaded onions (2638), veg in sauce (2639)
// // NOTE: product_group_code not available — omitting group 514, 2010 check
// //   (misses some canned/frozen veg only identifiable by group code)
// gen byte veg = cond( ///
//     (freshveg == 1 ///
//     | inlist(product_module_code, 24, 96, 1316, 3565) == 1) ///
//     & inlist(product_module_code, 1071, 2618, 2635, 2637, 2638, 2639) == 0, 1, 0)
//
// replace veg = 1 if inlist(product_group, "VEGETABLES", "VEGETABLES - CANNED", "VEGETABLES-FROZEN")
//
// replace veg = 1 if inlist(product_module_normalized, "VEGETABLES REMAINING FROZEN")
//
//
// label var freshfruit "1(Fresh fruit)"
// label var fruit      "1(fruit)"
// label var freshveg   "1(Fresh vegetable)"
// label var veg        "1(vegetable)"
//
// count if fruit == 1
// count if veg == 1
//
//
//
//
// // ============================================================================
// // STEP 6: Compute Health Index per 100g
// // GetHealthIndex.do:8-13
// // ============================================================================
//
// di _n "=== STEP 6: COMPUTING HEALTH INDEX ==="
//
// // Fixed HI for fruit/veg (GetHealthIndex.do:8)
// gen double hi_per_100g = fruit * 100/320 + veg * 100/390
//
// // Standard formula for non-produce (GetHealthIndex.do:11-13)
// replace hi_per_100g = fiber_per_100g/29.5 ///
//     - sugar_per_100g/32.8 ///
//     - satfat_per_100g/17.2 ///
//     - sodium_per_100g/2.3 ///
//     - chol_per_100g/0.3 ///
//     if fruit == 0 & veg == 0
//
// label var hi_per_100g "Health Index per 100g"
//
//
// // ============================================================================
// // STEP 7: Compute HI per 1000 calories
// // GetHealthIndex.do:33
// // ============================================================================
//
// // cals_per_upc = cal_per_100g * g_total / 100
// gen double cals_per_upc = cal_per_100g * g_total / 100
//
// // HI per 1000 cal, only if cals_per_upc > 1
// // (GetHealthIndex.do:33: "if cals_per1 > 1")
// gen double hi_per_1000cal = hi_per_100g / cal_per_100g * 1000 if cals_per_upc > 1
//
//
// gen double cals_per_row = cals_per_upc * quantity
//
//
// // ============================================================================
// // STEP 9: Calorie-weighted collapse to household-year
// // CollapseTransactions.do:68-69
// // collapse (rawsum) Calories=cals_perRow
// //          (mean) $Attributes_cals [pw=cals_perRow]
// // ============================================================================
//
// drop if cals_per_row <= 0 | cals_per_row == .
// drop if hi_per_1000cal == .
//
//
// // //do a simple collapse anya can understand across hh
// // collapse (rawsum) total_calories=cals_per_row ///
// //          total_spending=total_price_paid ///
// //     (mean) hi_household=hi_per_1000cal ///
// //          fruit veg ///
// //          household_income_midpoint ///
// //          projection_factor ///
// //     [pw=cals_per_row], ///
// //     by(household_code)
// //
// // corr hi_household household_income_midpoint [aw=projection_factor]
// //
// // preserve
// // set graphics on
// // collapse (mean) hi_household [pw=projection_factor], ///
// //     by(household_income_midpoint)
// //
// // twoway scatter hi_household household_income_midpoint, ///
// //     ytitle("Weighted mean of hi_household") ///
// //     xtitle("Household income midpoint") ///
// //     lwidth(medthick)
// //
// // restore
//
// // Calorie-weighted collapse
// collapse (rawsum) total_calories=cals_per_row ///
//          total_spending=total_price_paid ///
//     (mean) hi_household=hi_per_1000cal ///
//          fruit veg ///
//          household_income_midpoint ///
//          projection_factor ///
//     [pw=cals_per_row], ///
//     by(household_code panel_year) fast
//
//
// // ============================================================================
// // STEP 10: Normalize HI to mean=0, sd=1
// // InsertHealthMeasures.do:125-136
// //
// // Mean: weighted by projection_factor, pooled across all years
// // SD:   weighted SD of year-demeaned residuals
// // ============================================================================
//
// di _n "=== STEP 10: NORMALIZING HEALTH INDEX ==="
//
// // Weighted pooled mean (InsertHealthMeasures.do:126)
// sum hi_household [aw=projection_factor]
// local hi_mean = r(mean)
//
// // Year-demeaned SD (InsertHealthMeasures.do:129-133)
// // Regress on year FE, get residuals, take weighted SD
// reg hi_household i.panel_year [aw=projection_factor]
// predict year_dummies
// gen hi_residual = hi_household - year_dummies
// sum hi_residual [aw=projection_factor]
// local hi_sd = r(sd)
//
// drop year_dummies hi_residual
//
// // Normalize: (raw - mean) / sd
// gen double hi_hh_normalized = (hi_household - `hi_mean') / `hi_sd'
// label var hi_hh_normalized "Health Index (normalized, mean=0 sd=1)"


