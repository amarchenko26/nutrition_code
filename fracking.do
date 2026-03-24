// fips — 5-digit county FIPS (already padded)
// year — 2004–2012 in sample
// newvalue_capita — new fracking production value per capita ($billions) — this is exactly what Figure 2 maps (cumulative 2004–2012 max = 2.83, matches the legend)
// newvalue_capita_ins — Feyrer's own instrument for newvalue_capita (national oil/gas price shocks × baseline production potential)
// pop0 — base population
// industry 
// employment




// ============================================================================
// Do a fracking analysis 
// The dependent variable, Δ Yit, is the one year change in annual income (or the one year change in employment) divided by the one year lag of total employment.
// We control for geography fixed effects (county, commuting zone, or state where appropriate), αi, and year fixed effects, ωt. In order to deal with the potential for dynamic effects, we also include the one-year lag of new production as an additional control.
// Our solution to this problem is to scale production and our outcome variables by one year lagged employment.


// Start with my data, save the mean of hi by fips/year
use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/final_reg_data.dta", clear

tempfile hi_means
collapse (mean) hi [pw = projection_factor], by(fips year)
save `hi_means'

// Load fracking dataset + merge HI
use "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/fracking/BLS_IRS_fossil_working.dta", clear
tostring fips, replace format(%05.0f)

merge m:1 fips year using `hi_means', nogenerate keep(master match)
global cluster_county "fips year"




// Let's try running their main regression of how fracking affects hh income
// This is their Table 1 IV Panel regression 
// I can recreate it exactly 
// Outcome = one year change in annual income divided by the one year lag of total employment.
xtivreg2 d_irsagi_capita (L(0/1).newvalue_capita = L(0/1).newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fe partial(yeardum*)


// Now let's take their outcome var "style"
// i.e., everything is per capita at the county level 
cap drop hi_capita hi_capita_scale
g hi_capita = hi / L.allemployment
g hi_capita = hi / pop0

g hi_capita_scale = hi_capita / L.employment
replace hi_capita_scale = hi_capita_scale * 100

// REDUCED FORM 
// Let's just replace their regression outcome w/ our outcome 
// beta = .002, p = .074 -- good!! same coef. as 2SLS at HH-level 
// This is great but not quite the regression we want...This says fracking reserves increases HI
xtivreg2 hi_capita (L(0/1).newvalue_capita = L(0/1).newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fe partial(yeardum*)

// Remove the lags
// beta = .002, p = .039 -- great!!
xtivreg2 hi_capita (newvalue_capita = newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fe partial(yeardum*)


// 2SLS
// instrument income w/ fracking IV on RHS
// Our regression was "Income increases by $1000, how much does HI increase?"
// So rescale to 1000s of dollars
cap drop d_irsagi_capita_k d_wages_capita_k d_irswages_capita_k

g d_wages_capita_k = d_wages_capita / 1000
g d_irswages_capita_k = d_irswages_capita / 1000
g d_irsagi_capita_k = d_irsagi_capita / 1000  // per $1000
g d_irsotherinc_capita_k = d_irsotherinc_capita / 1000


CHECK HI_EMP
gen hi_emp = hi / L.employment


***************** CONTEMPORANEOUS WAGE AND INCOME OUTCOMES 
// beta = .028, p = .041
// a 1000$ increase in per capita wages 
xtivreg2 hi_capita (d_wages_capita_k = newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fe partial(yeardum*)

// beta = .063, p = 0.048
xtivreg2 hi_capita (d_irswages_capita_k = newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fe partial(yeardum*)

// beta = .014, p = .103
xtivreg2 hi_capita (d_irsagi_capita_k = newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fe partial(yeardum*)

// beta = .0179, p = .131
xtivreg2 hi_capita (d_irsotherinc_capita_k = newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fe partial(yeardum*)


***************** FIRST DIFFERENCES 
// beta = .028, p = .041
// a 1000$ increase in per capita wages 
xtivreg2 D.hi_capita (d_wages_capita_k = D.newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fd partial(yeardum*)

reghdfe D.hi_capita (D.d_wages_capita_k = D.newvalue_capita_ins) if id==1 & sample, ///
	cluster($cluster_county) absorb(yeardum*)



***************** LEADS of outcome 
* One period lead (HI tomorrow predicted by income today?)

// beta = .03, p = .041, half as small one year out 
xtivreg2 F.hi (d_wages_capita_k = newvalue_capita_ins) yeardum* ///
    if id==1 & sample, cluster($cluster_county) fe partial(yeardum*) 

// beta = .016, p = .257, smaller and insig 2 years out 
xtivreg2 F2.hi (d_wages_capita_k = newvalue_capita_ins) yeardum* ///
    if id==1 & sample, cluster($cluster_county) fe partial(yeardum*) 


// Check first stage -- is IV stronger for wages than AGI? 
// F = 7, p-value > F is .02
xtivreg2 hi (d_wages_capita_k = newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fe partial(yeardum*) first

// F = 18, p-value > F is .004
xtivreg2 hi (d_irsagi_capita_k = newvalue_capita_ins) yeardum* if id==1 & sample, cluster($cluster_county) fe partial(yeardum*) first


	
***************** CHECK PRETRENDS 
* Reduced form: does the instrument predict lagged HI?
// beta = 3.39 p = .180
xtivreg2 L.hi newvalue_capita_ins yeardum* ///
    if id==1 & sample, partial(yeardum*) cluster($cluster_county) fe
    
// beta = 2.11 p = .106	
xtivreg2 L2.hi newvalue_capita_ins yeardum* ///
    if id==1 & sample, partial(yeardum*) cluster($cluster_county) fe

// IV doesn't predict lagged HI!



// Let's rewrite as the equivalent ivreghdfe expression 
// They get beta = 176,600, SE = 22,828
// I get beta    = 176,600, SE = 24,662
// I call this a success even though reghdfe probably estimates errors slightly differently. 
ivreghdfe d_irsagi_capita ///
    (newvalue_capita L.newvalue_capita = newvalue_capita_ins L.newvalue_capita_ins) ///
    if id==1 & sample, ///
    absorb(fips yeardum*) ///
    cluster($cluster_county)
	
// So what is their first stage here? 
// beta = 1.30424, t = 2.63 
// Not super strong 
ivreghdfe newvalue_capita newvalue_capita_ins ///
    if id==1 & sample, ///
    absorb(fips yeardum*) ///
    cluster($cluster_county)

// let's add in those lagged variables they have 
ivreghdfe newvalue_capita newvalue_capita_ins (L.newvalue_capita = L.newvalue_capita_ins) ///
    if id==1 & sample, ///
    absorb(fips yeardum*) ///
    cluster($cluster_county)






// .0128 (t = .43)
reghdfejl hi (real_income =newoilvalue_ins newgasvalue_ins) if year < 2015 [aw=projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

// .0128 (t = .43)
reghdfejl hi (real_income =newoilvalue_ins newgasvalue_ins) if year < 2015 [aw=projection_factor], ///
	absorb(year fips kids avg_age_hh_head hh_comp) cluster(fips)

	
reghdfejl real_income newoilvalue_ins newgasvalue_ins if year < 2015 [aw=projection_factor], ///
	absorb(year hhid kids avg_age_hh_head hh_comp) cluster(fips)

	
	cap drop frack_county
g frack_county = (frack_value_capita > 0) & frack_value_capita !=.

preserve 
collapse (mean) real_income hi frack_value_capita [aw=projection_factor], by(fips year)

// first stage 
reghdfejl real_income frack_value_capita if year < 2015, ///
	absorb(year fips) cluster(fips)


reghdfejl hi (real_income = frack_value_capita) if year < 2015, ///
	absorb(year fips) cluster(fips)

// reghdfejl hi (real_income=iv_income_fips) [pw = projection_factor], ///
// 	absorb(year fips hhid hisp race kids avg_age_hh_head hh_comp) cluster(fips)

restore 


	