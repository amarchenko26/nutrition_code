

clear all
set graphics on 


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

use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/fracking/BLS_IRS_fossil_working.dta", clear

tostring fips, replace format(%05.0f)
ren year panel_year
keep if industry == "1 Total" // keep all industries affected by fracking

save "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/fracking/BLS_IRS_fossil_working_anya.dta", replace

// fips — 5-digit county FIPS (already padded)
// year — 2004–2012 in sample
// newvalue_capita — new fracking production value per capita ($billions) — this is exactly what Figure 2 maps (cumulative 2004–2012 max = 2.83, matches the legend)
// newvalue_capita_ins — Feyrer's own instrument for newvalue_capita (national oil/gas price shocks × baseline production potential)
// pop0 — base population
// industry 
// employment




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


// merge in fracking info 
merge m:1 fips panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/fracking/BLS_IRS_fossil_working_anya.dta", ///
	keepusing(name newvalue_capita newvalue_capita_ins newoilvalue_ins newgasvalue_ins) ///
	keep(master match)

// there will be no matches for every year after 2014 (when fracking panel ends)
drop _merge
 
ren newvalue_capita frack_value_capita
	
	
ren age_and_presence_of_children kids
ren household_code hhid
ren household_composition hh_comp
ren household_size hh_size
ren hispanic_origin hisp


// ============================================================================
// Define Movers as any change in zip code

bysort hhid zip_code (panel_year): gen byte uniq_zip = (_n == 1)

* 2) Count how many distinct zip codes each household has
bysort hhid: egen n_unique_zip = total(uniq_zip)

* 3) Movers = 1 if household has more than one distinct zip across panel years
gen byte movers = (n_unique_zip > 1)

label define movers_lbl 0 "non-mover" 1 "mover"
label values movers movers_lbl

* 2. Construct first differences
*------------------------------------------------------------*

xtset hhid panel_year

gen d_hi          = D.hi
gen d_real_income = D.real_income
gen d_iv_income_zip   = D.iv_income_zip
gen d_iv_income_fips  = D.iv_income_fips





// ============================================================================

// Construct an IV at HH level 
// IV is the projection-factor-weighted average income of all other households in the same demographic cell (hh_size × education × occupation), nationwide, EXCLUDING households in the same zip code



// OLS beta = .0004***
reghdfe hi real_income [pw = projection_factor], ///
	absorb(panel_year hhid kids hh_comp avg_age_hh_head fips) cluster(fips)


// first stage 
reghdfejl real_income iv_income_fips [pw = projection_factor], ///
	absorb(panel_year fips hhid) vce(cluster zip_code)

// 2SLS Zip (beta = .0023***)
reghdfejl hi (real_income=iv_income_zip) [pw = projection_factor], ///
	absorb(panel_year hhid hisp race kids avg_age_hh_head hh_comp) cluster(zip_code)
	
reghdfejl hi (real_income=iv_income_fips) [pw = projection_factor], ///
	absorb(panel_year hhid hisp race kids avg_age_hh_head hh_comp) cluster(fips)

	
// first diff  = when a HH becomes richer last year, does their nutrition improve this year? 
// beta = -.0003 and insignificant
ivreghdfe d_hi (d_real_income=d_iv_income_fips) [pw = projection_factor] if movers==0, ///
	absorb(panel_year kids hh_comp avg_age_hh_head) cluster(fips)


// Use zip by year instead (beta = .00367***)
ivreghdfe hi (real_income=iv_income) [pw = projection_factor], ///
	absorb(zip_by_year hisp race kids) cluster(zip_code)


// Exclude movers (beta = .0035***)
ivreghdfe hi (real_income=iv_income) [pw = projection_factor] if movers == 0, ///
	absorb(panel_year zip_code hisp race kids) cluster(zip_code)

// Small-share HHs: restrict to HHs whose cell is <10% of their zip's weight.
// These HHs' national wage shift can't mechanically drive their local area income.
// beta = .0024***
ivreghdfe hi (real_income=iv_income_zip) [pw = projection_factor] if cell_zip_share < 0.25, ///
	absorb(panel_year zip_code hhid hisp race kids avg_age_hh_head hh_comp) cluster(zip_code)



	
// MERGE IN TOTAL EXPENDITURE ON FOOD 
// MERGE IN PRICES PAID???? 

local outcome_vars whole produce sugar_per_1000cal total_cals 
foreach var in `outcome_vars' {
	
	ivreghdfe `var' (real_income=iv_income_zip) [pw = projection_factor], absorb(panel_year zip_code hhid hisp race kids avg_age_hh_head hh_comp) cluster(zip_code)

}

local spend_vars spend_total 

// spend_produce spend_high_sugar spend_soft_drinks___carbonated spend_cookies spend_ice_cream___bulk spend_share_produce

foreach var in `spend_vars' {
	
	ivreghdfe `var' (real_income=iv_income) [pw = projection_factor], absorb(panel_year zip_code hhid hisp race kids avg_age_hh_head hh_comp) cluster(zip_code)

}
	
	
		
cap drop inc_q
estimates clear 
xtile inc_q = hh_real_income_avg [aw=projection_factor], nq(5)
// IV by quintile
forvalues q = 1/5 {
    ivreghdfe hi (real_income = iv_income) [pw=projection_factor] if inc_q == `q', ///
        absorb(panel_year zip_code hisp race kids) ///
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

// ============================================================================
// Do a fracking analysis 

// .0127 (t = .42)
reghdfejl hi (real_income =newoilvalue_ins newgasvalue_ins) if panel_year < 2015 [aw=projection_factor], ///
	absorb(panel_year hhid hisp race kids avg_age_hh_head hh_comp) cluster(fips)


cap drop frack_county
g frack_county = (frack_value_capita > 0) & frack_value_capita !=.

preserve 
collapse (mean) real_income hi frack_value_capita [aw=projection_factor], by(fips panel_year)

// first stage 
reghdfejl real_income frack_value_capita if panel_year < 2015 & frack_value_capita>0, ///
	absorb(panel_year fips) cluster(fips)


reghdfejl hi (real_income = frack_value_capita) if panel_year < 2015, ///
	absorb(panel_year fips) cluster(fips)

// reghdfejl hi (real_income=iv_income_fips) [pw = projection_factor], ///
// 	absorb(panel_year fips hhid hisp race kids avg_age_hh_head hh_comp) cluster(fips)

restore 


	
	
	
// ============================================================================
// Check HI is correlated with disease

preserve 

collapse obesity n_dietary_conditions hypertension heart_disease diabetes_type1 diabetes_type2 cholesterol any_metabolic_disease hi real_income [pw=projection_factor], by(hhid)

#delimit ; 
binscatter diabetes_type2 hi, 
    n(50)
    msymbol(O)
    linetype(qfit)
    xtitle("HH Nutrition")
    ytitle("Type 2 Diabetes (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_diab_hi.png")
    replace
;

binscatter obesity hi, 
    n(50)
    msymbol(O)
    linetype(qfit)
    xtitle("HH Nutrition")
    ytitle("Obesity (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_ob_hi.png")
    replace;
	
binscatter cholesterol hi, 
    n(50)
    msymbol(O)
    linetype(qfit)
    xtitle("HH Nutrition")
    ytitle("Cholesterol (any HH member)")
    savegraph("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs/corr_chol_hi.png")
    replace;

binscatter any_metabolic_disease hi, 
    n(50)
    msymbol(O)
    linetype(qfit)
    xtitle("HH Nutrition")
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


	
	
	
	
	
	
	
	
	
	
	
	
	