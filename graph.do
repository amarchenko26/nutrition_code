import delimited "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/census_merged_1992_2022_deflated.tsv", clear 

drop if inlist(statefip, 2, 15)

** merge the census data to the soil suitability data
merge m:1 fips using "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/gaez/gaez_by_county.dta", keepusing(si_* corn_share_*)


***********************************

*import delimited "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/NASS_2017-2022/qs.census2017.txt"

br gov_all_amt_real gov_cons_amt_real gov_noncons_amt_calc gov_all_n gov_noncons_pf_calc
br if strpos(short_desc, "CONSERVATION") > 0 & agg_level_desc == "COUNTY" & domain_desc =="TOTAL" 




** reg total corn acres harvested on suitability for corn at the county level

reg total_corn_harvested_acres si_* i.statefip i.year if level == 1, cluster(statefip)

reg share_corn_harvested_acres si_* i.statefip i.year if level == 1, cluster(statefip)


* what about if the suitability index is very high?
reg total_corn_harvested_acres si_* i.statefip i.year if level == 1 & si_corn > 8500, cluster(statefip)

* soil suitability predictive of gov't payments?  
reg gov_noncons_amt_calc si_* i.statefip i.year if level == 1 & si_corn > 8500, cluster(statefip)

* gov't payments predictive of corn harvested?  
reg total_corn_harvested_acres gov_noncons_amt_calc  i.statefip i.year if level == 1, cluster(statefip)


* is share more suitable for corn predictive of acres of corn
reghdfe total_corn_harvested_acres corn_share_8500 if level==1, absorb(counfip year) vce(cluster statefip)

reghdfe total_corn_harvested_acres corn_share_5500 if level==1, absorb(counfip year) vce(cluster statefip)
