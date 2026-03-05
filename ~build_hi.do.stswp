

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

egen zip_by_year = group(zip_code panel_year)

// N = 289k
merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panelists/panelists_all_years.dta", ///
	keepusing(hh_avg_yrsofschool hh_avg_workhours hh_employed fips_county_code hh_avg_workhours age_and_presence_of_children age_and_presence_of_children_lab household_composition hispanic_origin hispanic_origin_label race race_label obesity n_dietary_conditions hypertension heart_disease diabetes_type1 diabetes_type2 cholesterol any_metabolic_disease) ///
	keep(master match)

drop _merge

merge 1:1 household_code panel_year using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/iv_income.dta", ///
	keepusing(iv_income iv_cell_n_lo cell_zip_share) ///
	keep(master match)

drop _merge

rename *, lower



// ============================================================================
// Define Movers as any change in zip code

bysort household_code zip_code (panel_year): gen byte uniq_zip = (_n == 1)

* 2) Count how many distinct zip codes each household has
bysort household_code: egen n_unique_zip = total(uniq_zip)

* 3) Movers = 1 if household has more than one distinct zip across panel years
gen byte movers = (n_unique_zip > 1)

label define movers_lbl 0 "non-mover" 1 "mover"
label values movers movers_lbl






// ============================================================================
// Check HI is correlated with disease

preserve 

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

restore



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
// 2SLS (beta = .0038***)
ivreghdfe hi (real_income=iv_income) [pw = projection_factor], ///
	absorb(panel_year zip_code hispanic_origin race age_and_presence_of_children) cluster(zip_code)
	
// Use zip by year instead (beta = .00367***)
ivreghdfe hi (real_income=iv_income) [pw = projection_factor], ///
	absorb(zip_by_year hispanic_origin race age_and_presence_of_children) cluster(zip_code)


// Exclude movers (beta = .0035***)
ivreghdfe hi (real_income=iv_income) [pw = projection_factor] if movers == 0, ///
	absorb(panel_year zip_code hispanic_origin race age_and_presence_of_children) cluster(zip_code)

// Small-share HHs: restrict to HHs whose cell is <10% of their zip's weight.
// These HHs' national wage shift can't mechanically drive their local area income.
// beta = .0031***
ivreghdfe hi (real_income=iv_income) [pw = projection_factor] if cell_zip_share < 0.10, ///
	absorb(panel_year zip_code hispanic_origin race age_and_presence_of_children) cluster(zip_code)

	
	
// MERGE IN TOTAL EXPENDITURE ON FOOD 
// MERGE IN PRICES PAID???? 

local outcome_vars whole produce sugar_per_1000cal total_cals
foreach var in `outcome_vars' {
	
	ivreghdfe `var' (real_income=iv_income) [pw = projection_factor], absorb(panel_year zip_code hispanic_origin race age_and_presence_of_children) cluster(zip_code)

}

	
	
	
		
cap drop inc_q
estimates clear 
xtile inc_q = hh_real_income_avg [aw=projection_factor], nq(5)
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








	
	
	
	
	
	
	
	
	
	
	
	
	
	
	