// ============================================================================
// Descriptives on household head structure in the Nielsen Consumer Panel.
//
// Nielsen has exactly two head slots (male_head_* / female_head_*), so every
// two-head household is one man + one woman BY CONSTRUCTION. Whether a HH
// reports 1 or 2 heads is self-reported at signup. Non-heads appear as
// Member_1..7 with birth year, relationship-sex code, and BINARY employment.
//
// Outputs (Overleaf):
//   tabs/labor_supply/head_structure.tex        counts by head structure,
//                                               one-head HHs split by presence
//                                               of other adults
//   figs/labor_supply/head_structure_over_time.png
//   figs/labor_supply/head_employment_by_sex.png
// ============================================================================

clear all

global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global overleaf "/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf"

use "$data/interim/panelists/panelists_all_years.dta", clear

// ============================================================================
// Adults among non-head members (member birth year -> age at panel year)
// ============================================================================

gen n_adult_members = 0
forvalues i = 1/7 {
	replace n_adult_members = n_adult_members + 1 ///
		if !missing(member_`i'_birth) & (panel_year - member_`i'_birth) >= 18
}
label var n_adult_members "Non-head members aged 18+"

// ============================================================================
// Head structure categories
// ============================================================================

gen byte head_structure = .
replace head_structure = 1 if n_heads == 2
replace head_structure = 2 if n_heads == 1 & male_head_status   != "" & n_adult_members == 0
replace head_structure = 3 if n_heads == 1 & male_head_status   != "" & n_adult_members >  0
replace head_structure = 4 if n_heads == 1 & female_head_status != "" & n_adult_members == 0
replace head_structure = 5 if n_heads == 1 & female_head_status != "" & n_adult_members >  0

label define hs_lbl 1 "Two heads (one man, one woman)" ///
                    2 "One male head, no other adults" ///
                    3 "One male head, other adult(s) present" ///
                    4 "One female head, no other adults" ///
                    5 "One female head, other adult(s) present"
label values head_structure hs_lbl
label var head_structure "Household head structure"

// ============================================================================
// Table: head structure, HH-years and unique households (at first observation)
// ============================================================================

bysort household_code (panel_year): gen byte first_obs = (_n == 1)

eststo clear
eststo hhyears:    estpost tabulate head_structure
local hs_labels `e(labels)'
eststo households: estpost tabulate head_structure if first_obs

esttab hhyears households using "$overleaf/tabs/labor_supply/head_structure.tex", ///
	replace booktabs ///
	cells("b(fmt(%12.0fc)) pct(fmt(1))") ///
	collabels("N" "\%") ///
	varlabels(`hs_labels', blist(Total "\midrule ")) ///
	mtitles("HH-years" "Households") ///
	nonumbers noobs nonotes

// Console check
esttab hhyears households, cells("b(fmt(%12.0fc)) pct(fmt(1))") mtitles("HH-years" "HHs")

// ============================================================================
// Figure: head structure shares over time
// ============================================================================

preserve
gen byte two_heads       = (head_structure == 1)          if !missing(head_structure)
gen byte one_male_head   = inlist(head_structure, 2, 3)   if !missing(head_structure)
gen byte one_female_head = inlist(head_structure, 4, 5)   if !missing(head_structure)

collapse (mean) two_heads one_male_head one_female_head, by(panel_year)
foreach v in two_heads one_male_head one_female_head {
	replace `v' = 100 * `v'
}

twoway (line two_heads       panel_year, lwidth(thick)) ///
       (line one_female_head panel_year, lwidth(thick)) ///
       (line one_male_head   panel_year, lwidth(thick)), ///
	ytitle("Share of households (%)", size(medlarge)) xtitle("") ///
	legend(order(1 "Two heads" 2 "One female head" 3 "One male head") ///
	       rows(1) position(6) size(medlarge)) ///
	xlabel(2004(4)2024, labsize(medlarge)) ylabel(0(20)80, labsize(medlarge))

graph export "$overleaf/figs/labor_supply/head_structure_over_time.png", replace width(2400)
restore

// ============================================================================
// Figure: employment status (hour bins) by head sex, conditional on head present
// ============================================================================

preserve
keep household_code panel_year male_head_status female_head_status
rename (male_head_status female_head_status) (status1 status2)
gen long obs = _n
reshape long status, i(obs) j(sexn)
drop if status == ""
gen sex = cond(sexn == 1, "Male heads", "Female heads")

gen byte ft = 100 * (status == "FT")
gen byte pt = 100 * (status == "PT")
gen byte ne = 100 * (status == "NE")

graph bar (mean) ft pt ne, over(sex) ///
	bargap(10) ///
	legend(order(1 "Full-time (35+ hrs)" 2 "Part-time (<35 hrs)" 3 "Not employed") ///
	       rows(1) position(6) size(medlarge)) ///
	ytitle("Share of heads (%)", size(medlarge)) ///
	ylabel(0(20)60, labsize(medlarge)) ///
	blabel(bar, format(%4.0f) size(medium))

graph export "$overleaf/figs/labor_supply/head_employment_by_sex.png", replace width(2400)
restore
