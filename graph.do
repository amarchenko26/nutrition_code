import delimited "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/census_merged_1992_2022_deflated.tsv", clear 

*import delimited "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/NASS_2017-2022/qs.census2017.txt"


import delimited "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/census_merged_1992_2022_deflated.tsv", clear 
br gov_all_amt_real gov_cons_amt_real gov_noncons_amt_calc gov_all_n gov_noncons_pf_calc

br if strpos(short_desc, "CONSERVATION") > 0 & agg_level_desc == "COUNTY" & domain_desc =="TOTAL" 

preserve
