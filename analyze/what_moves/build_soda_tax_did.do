// ============================================================================
// build_soda_tax_did.do -- soda taxes as a relative-price shock (2026-11-05,
// "what moves nutrition" review). Event-study difference-in-differences.
//
// Treated: households living in a taxed jurisdiction in EVERY observed year
//   Philadelphia (county 42101, 1.5c/oz from Jan 2017, diet drinks included);
//   pooled city taxes: Berkeley (zips 94701-94720, Jan 2015, 1c), Albany (94706,
//   Apr 2017), Oakland (94601-94621, 94649, 94659-94662, Jul 2017), Boulder
//   (80301-80310, Jul 2017, 2c), San Francisco (94102-94188, Jan 2018), Seattle
//   (98101-98199, Jan 2018, 1.75c; zip range approximates the city);
//   Cook County IL (17031, 1c Aug-Nov 2017 then repealed) as a check.
// Control: households in the same state never in a taxed zip/county and never
//   in a county adjacent to one (Philadelphia: Bucks 42017, Montgomery 42091,
//   Delaware 42045; Cook: DuPage 17043, Lake 17097, Will 17197, Kane 17089,
//   McHenry 17111; Bay Area: the rest of Alameda 06001, San Mateo 06081, Marin
//   06041, Contra Costa 06013; Seattle: the rest of King 53033, Snohomish 53061,
//   Pierce 53053; Boulder: the rest of Boulder 08013, Jefferson 08059,
//   Broomfield 08014, Larimer 08069, Weld 08123). Adjacent-county households
//   are a separate group (a placebo: Homescan records purchases wherever bought).
// y_it = a_i + tau_{state,t} + sum_r beta_r 1[treated, r] + sum_r gamma_r 1[adjacent, r]
//        + x_it'b + e_it ; r = year - first taxed year, r = -1 omitted, binned at
// -4 and +3; x = kids, composition, 5-year age bin; pw; cluster on county.
// Outcomes: sugary-soda ounces per household-year (2010-2020), share of those ounces bought in the own zip3,
// soda share of grocery spending (%), soda spending (2013 $), sugar g
// per 1,000 kcal, HEI, ultra-processed share of labelled calories (%).
// Soda = module SOFT DRINKS - CARBONATED (diet included).
// Outputs: interim/what_moves/soda_tax_coefs.csv, soda_tax_summary.csv
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use hhid year fips zip_code fips_state_code spend_soda spend_total sugar_per_1000cal_usda hei_usda ///
	projection_factor kids hh_comp avg_age_hh_head using "$gdir/labor_supply_reg_data.dta", clear
destring fips zip_code fips_state_code, replace force
rename (fips_state_code zip_code) (state zip)
merge 1:1 hhid year using "$idir/what_moves/upf_share_hh_year.dta", keep(master match) nogen
// sugary-soda OUNCES (module 1484 SOFT DRINKS - CARBONATED, diet is module 1553) and the share of those ounces bought at
// stores in the household's own zip3 (cross-border shopping), from the purchase files for the five tax states, 2010-2020
merge 1:1 hhid year using "$idir/what_moves/soda_quantity_hh_year.dta", keep(master match) nogen

gen soda_share = 100 * spend_soda / spend_total
gen cpi = .
foreach p in "2004 188.9" "2005 195.3" "2006 201.6" "2007 207.3" "2008 215.3" "2009 214.5" "2010 218.1" "2011 224.9" "2012 229.6" ///
	"2013 233.0" "2014 236.7" "2015 237.0" "2016 240.0" "2017 245.1" "2018 251.1" "2019 255.7" "2020 258.8" {
	replace cpi = real(word("`p'", 2)) if year == real(word("`p'", 1))
}
gen soda_real = spend_soda * 233.0 / cpi
gen age5 = min(85, 5 * floor(avg_age_hh_head / 5))
egen year_state = group(year state)

// ------------------------------------------------------------ jurisdictions
gen byte z_phil = fips == 42101
gen byte z_cook = fips == 17031
gen byte z_berk = inrange(zip, 94701, 94720)
gen byte z_alb  = zip == 94706
gen byte z_oak  = inrange(zip, 94601, 94621) | zip == 94649 | inrange(zip, 94659, 94662)
gen byte z_boul = inrange(zip, 80301, 80310)
gen byte z_sf   = inrange(zip, 94102, 94188)
gen byte z_sea  = inrange(zip, 98101, 98199)
gen byte z_city = z_berk | z_alb | z_oak | z_boul | z_sf | z_sea
gen start_city = cond(z_berk, 2015, cond(z_alb | z_oak | z_boul, 2017, cond(z_sf | z_sea, 2018, .)))
gen byte adj_phil = inlist(fips, 42017, 42091, 42045)
gen byte adj_cook = inlist(fips, 17043, 17097, 17197, 17089, 17111)
gen byte adj_city = (inlist(fips, 6001, 6081, 6041, 6013) & !z_city) | (inlist(fips, 53033, 53061, 53053) & !z_city) ///
	| (inlist(fips, 8013, 8059, 8014, 8069, 8123) & !z_city)

cap file close fh
file open fh using "$idir/what_moves/soda_tax_coefs.csv", write replace
file write fh "design,group,outcome,r,b,se,N,n_treated" _n
file close fh
file open fh using "$idir/what_moves/soda_tax_summary.csv", write replace
file write fh "design,group,outcome,n_treated,n_adjacent,n_control,post,post_se,pre,pre_se,p_pre,N" _n
file close fh

// run_design "name" zone adjacent "state condition" start(expr)
cap program drop run_design
program define run_design
	args name zone adj statecond
	preserve
	keep if `statecond'
	// treated = in the zone in every observed year; adjacent = in an adjacent county in every year; control = never in either
	bys hhid: egen zmin = min(`zone')
	bys hhid: egen zmax = max(`zone')
	bys hhid: egen amin = min(`adj')
	bys hhid: egen amax = max(`adj')
	gen byte treated = zmin == 1
	gen byte adjacent = amin == 1 & zmax == 0
	gen byte control = zmax == 0 & amax == 0
	keep if treated | adjacent | control
	// event time: treated households relative to their own start year; adjacent households get the start of the
	// nearest zone (same start for Philadelphia / Cook; for the cities use the earliest city start in their state)
	gen start = .
	if "`name'" == "philadelphia" replace start = 2017
	if "`name'" == "cook" replace start = 2017
	if "`name'" == "cities" {
		bys hhid: egen s_hh = min(start_city)
		replace start = s_hh if treated
		replace start = cond(state == 6, 2017, cond(state == 53, 2018, 2017)) if adjacent   // CA pooled 2017 (Oakland/Albany; Berkeley's 8 HHs aside), WA 2018, CO 2017
	}
	gen r = year - start
	gen rb = max(-4, min(3, r))
	local tlist
	local alist
	forvalues k = -4/3 {
		if `k' == -1 continue
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		gen byte t_`s' = treated & rb == `k'
		gen byte a_`s' = adjacent & rb == `k'
		local tlist `tlist' t_`s'
		local alist `alist' a_`s'
	}
	bys hhid: gen byte _f = _n == 1
	quietly count if _f & treated
	local nt = r(N)
	quietly count if _f & control
	local nc = r(N)
	quietly count if _f & adjacent
	local nadj = r(N)
	di "`name': treated households `nt', adjacent `nadj', control `nc'"
	foreach y in soda_oz home_share soda_share soda_real sugar_per_1000cal_usda hei_usda upf_share {
		quietly reghdfe `y' `tlist' `alist' [pw = projection_factor], absorb(hhid year_state kids hh_comp age5) cluster(fips)
		local N = e(N)
		foreach g in t a {
			local gname = cond("`g'" == "t", "treated", "adjacent")
			file open fh using "$idir/what_moves/soda_tax_coefs.csv", write append
			forvalues k = -4/3 {
				if `k' == -1 {
					file write fh "`name',`gname',`y',-1,0,0,`N',`nt'" _n
					continue
				}
				local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
				file write fh "`name',`gname',`y',`k',`=_b[`g'_`s']',`=_se[`g'_`s']',`N',`nt'" _n
			}
			file close fh
			quietly lincom (`g'_p0 + `g'_p1 + `g'_p2) / 3
			local post = r(estimate)
			local post_se = r(se)
			quietly lincom (`g'_m4 + `g'_m3 + `g'_m2) / 3
			local pre = r(estimate)
			local pre_se = r(se)
			quietly test `g'_m4 `g'_m3 `g'_m2
			local pp = r(p)
			file open fh using "$idir/what_moves/soda_tax_summary.csv", write append
			file write fh "`name',`gname',`y',`nt',`nadj',`nc',`post',`post_se',`pre',`pre_se',`pp',`N'" _n
			file close fh
			di "`name' / `gname' / `y': post(0..2) = " %7.3f `post' " (" %6.3f `post_se' ")  pre(-4..-2) = " %7.3f `pre' " (" %6.3f `pre_se' ") p = " %5.3f `pp' "  N = `N'"
		}
	}
	restore
end

run_design philadelphia z_phil adj_phil "state == 42"
run_design cities z_city adj_city "inlist(state, 6, 8, 53)"
run_design cook z_cook adj_cook "state == 17"
di "build_soda_tax_did.do complete"
