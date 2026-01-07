#!/usr/bin/env python3
"""
Agricultural Census Data Collection Script

This script collects census data files from different years, filters for the corresponding variables, and merges them into one data file into an interim folder.

Census years and corresponding folder mappings:
- DS0042: 1992
- DS0043: 1997  
- DS0044: 2002
- DS0045: 2007
- DS0047: 2012

Census years and corresponding variable names:
- 1992: 
        1 stateicp ICPSR state code
        2 counicp ICPSR county code
        3 name Name of area
        4 fips State/county FIPS code
        5 statefip State FIPS code
        6 counfip County FIPS code
        7 level County=1 state=2 USA=3

        9 item010001 Farms (number), 1992
        11 item010002 Land in farms (acres), 1992
        13 item010003 Average size of farm (acres), 1992
        35 item010014 Total crop land (acres), 1992
        39 item010016 Harvested crop land (acres), 1992

        45 item010019 Market value of agricultural products sold ($1,000)
        47 item010020 Market value of agricultural products sold, average per farm (dollars), 1992
        49 item010021 Market value of agricultural products sold-Crops, including nursery & greenhouse crops ($1,000), 1992

        75 item010034 Net cash return from agricultural sales for the farm unit (see text), average per farm (dollars), 1992

        121 item010057 Corn for grain or seed (farms), 1992
        123 item010058 Corn for grain or seed (acres), 1992
        125 item010059 Corn for grain or seed (bu.), 1992
        127 item010060 Corn for silage or green chop (farms), 1992
        129 item010061 Corn for silage or green chop (acres), 1992
        131 item010062 Corn for silage/green chop (tons, green), 1992

        526 item040010 Gov payments-Total received, (farms), 1992
        527 item040011 Gov payments-Total received, ($1,000), 1992
        529 item040012 Gov payments-Total received average per farm (dollars), 1992
        531 item040013 Gov payments-CRP & WRP, (farms), 1992
        533 item040014 Gov payments-CRP & WRP, ($1,000), 1992
        535 item040015 Gov payments-CRP & WRP avg/farm (dollars), 1992

        565 item040030 CCC Loan-Total (farms), 1992
        567 item040031 CCC Loan-Total ($1,000), 1992
        569 item040032 CCC Loan-Corn, (farms), 1992
        571 item040033 CCC Loan-Corn, ($1,000), 1992
        573 item040034 CCC Loan-Wheat, (farms), 1992
        575 item040035 CCC Loan-Wheat, ($1,000), 1992

        837 item060074 Land under Federal acreage reduction programs--Diverted under annual commodity programs, (farms), 1992
        839 item060075 Land under Federal acreage reduction programs--Diverted under annual commodity programs, (acres), 1992
        841 item060076 Land under Federal acreage reduction programs-- Conservation Reserve or Wetlands Reserve Programs (farms), 1992
        843 item060077 Land under Federal acreage reduction programs-- Conservation Reserve or Wetlands Reserve Programs, (acres), 1992

- 1997: 
        1 state State ICPSR code
        2 county County ICPSR code
        3 name Name of state/county
        4 level County=1 state=2 USA=3
        5 statefip State FIPS code
        6 counfip County FIPS code
        7 fips State/county FIPS 

        8 item01001 Farms (number), 1997
        21 item01014 Total cropland (farms), 1997
        22 item01015 Total cropland (acres), 1997
        23 item01016 Total cropland, harvested cropland (farms), 1997
        24 item01017 Total cropland, harvested cropland (acres), 1997

        28 item01021 Market value of agricultural products sold, average per farm ($), 1997 
        29 item01022 Market value of ag prod sold-crops, incl nursery & greenhouse crops ($1,000), 1997
        42 item01035 Net cash return from ag sales for fm unit (see text) , average per farm ($), 1997
        260 item04003 Net cash return from ag sales for farm unit (see text) ,aver per farm ($), 1997 

        65 item01058 Corn for grain or seed (farms), 1997
        66 item01059 Corn for grain or seed (acres), 1997
        67 item01060 Corn for grain or seed (bushels), 1997
        68 item01061 Corn for silage or green chop (farms), 1997
        69 item01062 Corn for silage or green chop (acres), 1997
        70 item01063 Corn for silage or green chop (tons, green), 1997

        123 item02001 Market value of agricultural products sold, total sales (see text) (farms), 1997
        124 item02002 Market value of agricultural products sold, total sales (see text) ($1,000), 1997
        125 item02003 Market value of agricultural products sold, total sales, average per farm ($), 1997
        154 item02032 Sales by commodity/commodity group: Crops, incl nursery/greenhouse crops, grains, corn for grain (farms), 1997
        155 item02033 Sales by commodity/commodity group: Crops, incl nursery/greenhouse crops, grains, corn for grain ($1,000), 1997
        
        267 item04010 Government payments, total received (farms), 1997
        268 item04011 Government payments, total received ($1,000), 1997
        269 item04012 Government payments, total received, average per farm ($), 1997
        270 item04013 Govt pay, amount from Conservation Reserve/Wetlands Reserve Programs (farms), 1997
        271 item04014 Govt pay, amount from Conservation Reserve/Wetlands Reserve Program ($1,000), 1997
        272 item04015 Gov pay, amount from Conservation Res/Wetlands Res Program, average per farm ($), 1997

        287 item04030 Commodity Credit Corporation Loans - Total (farms), 1997
        288 item04031 Commodity Credit Corporation Loans - Total ($1,000), 1997
        289 item04032 Commodity Credit Corporation Loans - Corn (farms), 1997
        290 item04033 Commodity Credit Corporation Loans - Corn ($1,000), 1997
        291 item04034 Commodity Credit Corporation Loans - Wheat (farms), 1997
        292 item04035 Commodity Credit Corporation Loans - Wheat ($1,000), 1997

            750 item12101 Government payments-Total received (farms), 1997
            751 item12102 Government payments-Total received ($1,000), 1997
            752 item12103 Government payments-Total received, average per farm ($), 1997
            753 item12104 Government payments-Amount from Conservation Reserve/Wetlands Reserve Programs (farms), 1997
            754 item12105 Government payments-Amount from Conservation Reserve/Wetlands Reserve Programs ($1,000), 1997
            755 item12106 Government payments-Amount from Conservation Reserve/Wetlands Reserve Program, average per farm ($), 1997

            874 item12225 Corn for grain or seed (farms), 1997
            875 item12226 Corn for grain or seed (acres), 1997
            876 item12227 Corn for grain or seed (bushels), 1997
            877 item12228 Corn for silage or green chop (farms), 1997
            878 item12229 Corn for silage or green chop (acres), 1997
            879 item12230 Corn for silage or green chop (tons, green), 1997

- 2002: 
        1 state ICPSR state code
        2 county ICPSR county code
        3 level 1=county 2=state 3=USA
        4 fips State\county FIPS code
        5 statefip State FIPS code
        6 counfip County FIPS code
        7 name Name of area

        8 item01001 Farms (number, 2002)
        21 item01014 Total crop land (farms, 2002)
        22 item01015 Total crop land (acres, 2002)
        23 item01016 Total crop land, Harvested crop land (farms, 2002)
        24 item01017 Total crop land, Harvested crop land (acres, 2002)
        27 item01020 Market value of agricultural products sold (see text) ($1,000, 2002)
        28 item01021 Market value of agricultural products sold (see text), Average per farm (dollars, 2002)
    29 item01022 Market value of agricultural products sold (see text), Crops ($1,000, 2002)

        69 item01062 Selected crops harvested, Corn for grain (farms, 2002)
        70 item01063 Selected crops harvested, Corn for grain (acres, 2002)
        71 item01064 Selected crops harvested, Corn for grain (bushels)
        72 item01065 Selected crops harvested, Corn for silage or greenchop (farms, 2002)
        73 item01066 Selected crops harvested, Corn for silage or greenchop (acres, 2002)
        77 item01067 Selected crops harvested, Corn for silage or greenchop (tons, 2002)

        384 item05001 Government payments, Total received (farms, 2002)
        386 item05003 Government payments, Total received ($1,000, 2002)
        388 item05005 Government payments, Total received, Average per farm (dollars, 2002)
        390 item05007 Government payments, Total received, Amount from Conservation Reserve & Wetlands Reserve Programs (farms, 2002)
        392 item05009 Government payments, Total received, Amount from Conservation Reserve & Wetlands Reserve Programs ($1,000, 2002)
        394 item05011 Government payments, Total received, Amount from Conservation Reserve & Wetlands Reserve Programs, Average per farm (dollars, 2002)
        396 item05013 Government payments, Total received, Amount from other federal farm programs (farms, 2002)
        398 item05015 Government payments, Total received, Amount from other federal farm programs ($1,000, 2002)
        400 item05017 Government payments, Total received, Amount from other federal farm programs, Average per farm (dollars, 2002)
        402 item05019 Commodity Credit Corporation loans, Total (farms, 2002)
        404 item05021 Commodity Credit Corporation loans, Total ($1,000, 2002)

        #-------2002 doesn't break CCC loans further by crop 

- 2007: 
        1 state State ICPSR code
        2 statefip State FIPS code
        3 county County ICPSR code
        4 countyfip County FIPS code
        5 name Name of state/county
        6 fips FIPS code
        7 level County=1 state=2 USA=3
        8 data1_1 Farms (number)

        21 data1_14 Total cropland (farms)
        22 data1_15 Total cropland (acres)
        23 data1_16 Total cropland\Harvested cropland (farms)
        24 data1_17 Total cropland\Harvested cropland (acres)

        27 data1_20 Market value of agricultural products sold 2007 ($1,000)
        28 data1_21 Market value of agricultural products sold 2007\Average per farm
        (dollars)
        29 data1_22 Market value of agricultural products sold 2007\Crops, including
        nursery & greenhouse crops ($1,000)

        69 data1_62 Selected crops harvested\Corn for grain (farms)
        70 data1_63 Selected crops harvested\Corn for grain (acres)
        71 data1_64 Selected crops harvested\Corn for grain (bushels)
        72 data1_65 Selected crops harvested\Corn for silage or greenchop (farms)
        73 data1_66 Selected crops harvested\Corn for silage or greenchop (acres)
        74 data1_67 Selected crops harvested\Corn for silage or greenchop (tons)

        422 data5_1 Government payments\Total received (farms, 2007)
        424 data5_3 Government payments\Total received ($1,000, 2007)
        426 data5_5 Government payments\Total received\Average per farm (dollars, 2007)
        428 data5_7 Government payments\Total received\Amount from Conservation Reserve, Wetlands Reserve, Farmable Wetlands, & Conservation Reserve Enhancement Programs 2002 (farms, 2007)
        430 data5_9 Government payments\Total received\Amount from conservation reserve, wetlands reserve, farmable wetlands, & conservation reserve enhancement programs 2002 ($1,000, 2007)
        432 data5_11 Government payments\Total received\Amount from conservation reserve, wetlands reserve, farmable wetlands, & conservation reserve enhancement programs 2002\Average per farm (dollars, 2007)
        434 data5_13 Government payments\Total received\Amount from other federal farm programs (farms, 2007)
        436 data5_15 Government payments\Total received\Amount from other federal farm programs ($1,000, 2007)
        438 data5_17 Government payments\Total received\Amount from other federal farm programs\Average per farm (dollars, 2007)
        440 data5_19 Commodity credit corporation loans\Total (farms, 2007)
        442 data5_21 Commodity credit corporation loans\Total ($1,000, 2007)

- 2012: 
        1 stateicp State ICPSR code
        2 counicp County ICPSR code
        3 name Name of geographic area
        4 level County=1 state=2 USA=3
        5 fips State/county FIPS code
        6 statefip State FIPS code
        7 cofips County FIPS code
        8 data1_1 Farms (number)

        22 data1_15 Total cropland (farms)
        23 data1_16 Total cropland (acres)
        24 data1_17 Total cropland\Harvested cropland (farms)
        25 data1_18 Total cropland\Harvested cropland (acres)

        28 data1_21 Market value of agricultural products sold ($1,000)
        29 data1_22 Market value of agricultural products sold\average per farm ($)
        30 data1_23 Market value of agricultural products sold\Crops, including nursery & greenhouse
        crops ($1,000)

        70 data1_63 Selected crops harvested\Corn for grain (farms)
        71 data1_64 Selected crops harvested\Corn for grain (acres)
        72 data1_65 Selected crops harvested\Corn for grain (bushels)
        73 data1_66 Selected crops harvested\Corn for silage or greenchop (farms)
        74 data1_67 Selected crops harvested\Corn for silage or greenchop (acres)
        75 data1_68 Selected crops harvested\Corn for silage or greenchop (tons)

        201 data2_55 Total sales\Crops,(farms, 2012)
        203 data2_57 Total sales\Crops,($1,000, 2012)
        209 data2_63 Total sales\Crops,Corn (farms, 2012)
        211 data2_65 Total sales\Crops,Corn ($1,000, 2012)

        439 data5_1 Government payments\Total received (farms, 2012)
        441 data5_3 Government payments\Total received ($1,000, 2012)
        443 data5_5 Government payments\Total received\average per farm ($, 2012)
        445 data5_7 Government payments\Total received\Amount from conservation reserve, wetlands reserve & conservation reserve enhancement programs 2007 (farms, 2012)
        447 data5_9 Government payments\Total received\Amount from conservation reserve, wetlands reserve & conservation reserve enhancement programs 2007 ($1,000, 2012)
        449 data5_11 Government payments\Total received\Amount from conservation reserve, wetlands reserve & conservation reserve enhancement programs 2007\Average per farm ($, 2012)
        451 data5_13 Government payments\Total received\Amount from other federal farm programs (farms, 2012)
        453 data5_15 Government payments\Total received\Amount from other federal farm programs ($1,000, 2012)
        455 data5_17 Government payments\Total received\Amount from other federal farm programs\Average per farm ($, 2012)
        457 data5_19 Commodity credit corporation loans\Total (farms, 2012)
        459 data5_21 Commodity credit corporation loans\Total ($1,000, 2012)
        463 data5_25 Commodity credit corporation loans\Total\Amount spent to repay CCC loans ($1,000, 2012)

- 2017:
    Variables stored under short_desc
    domain_desc == "TOTAL" 
    agg_level_desc == "COUNTY" | "NATIONAL" | "STATE", we don't use AMERICAN INDIAN RESERVATION, WATERSHED, or ZIP CODE

        'GOVT PROGRAMS, FEDERAL - RECEIPTS, MEASURED IN $ / OPERATION', 
        'GOVT PROGRAMS, FEDERAL - OPERATIONS WITH RECEIPTS'
        'GOVT PROGRAMS, FEDERAL - RECEIPTS, MEASURED IN $'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - ACRES'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - NUMBER OF OPERATIONS'
        'INCOME, FARM-RELATED, GOVT PROGRAMS, STATE & LOCAL - OPERATIONS WITH RECEIPTS'
        'INCOME, FARM-RELATED, GOVT PROGRAMS, STATE & LOCAL - RECEIPTS, MEASURED IN $'
        'INCOME, FARM-RELATED, GOVT PROGRAMS, STATE & LOCAL - RECEIPTS, MEASURED IN $ / OPERATION'
        'GOVT PROGRAMS, FEDERAL, (EXCL CONSERVATION & WETLANDS) - RECEIPTS, MEASURED IN $'
        'GOVT PROGRAMS, FEDERAL, (EXCL CONSERVATION & WETLANDS) - RECEIPTS, MEASURED IN $ / OPERATION'
        'GOVT PROGRAMS, FEDERAL, (EXCL CONSERVATION & WETLANDS) - OPERATIONS WITH RECEIPTS'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - RECEIPTS, MEASURED IN $'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - RECEIPTS, MEASURED IN $ / OPERATION'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - OPERATIONS WITH RECEIPTS'
        'COMMODITY TOTALS, INCL GOVT PROGRAMS - RECEIPTS, MEASURED IN $'
        'COMMODITY TOTALS, INCL GOVT PROGRAMS - RECEIPTS, MEASURED IN $ / OPERATION'
        'COMMODITY TOTALS, INCL GOVT PROGRAMS - OPERATIONS WITH RECEIPTS'
        CCC LOANS - RECEIPTS, MEASURED IN $
        CCC LOANS - OPERATIONS WITH RECEIPTS
        CCC LOANS - REPAYMENTS, MEASURED IN $
        CCC LOANS - OPERATIONS WITH REPAYMENTS

    $------- Previous years dont track repayments

- 2022: 
        'COMMODITY TOTALS, INCL GOVT PROGRAMS - OPERATIONS WITH RECEIPTS'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - NUMBER OF OPERATIONS'
        'INCOME, FARM-RELATED, GOVT PROGRAMS, STATE & LOCAL - OPERATIONS WITH RECEIPTS'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - ACRES'
        'COMMODITY TOTALS, INCL GOVT PROGRAMS - RECEIPTS, MEASURED IN $ / OPERATION'
        'GOVT PROGRAMS, FEDERAL - RECEIPTS, MEASURED IN $'
        'GOVT PROGRAMS, FEDERAL, (EXCL CONSERVATION & WETLANDS) - RECEIPTS, MEASURED IN $'
        'GOVT PROGRAMS, FEDERAL - OPERATIONS WITH RECEIPTS'
        'COMMODITY TOTALS, INCL GOVT PROGRAMS - RECEIPTS, MEASURED IN $'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - OPERATIONS WITH RECEIPTS'
        'INCOME, FARM-RELATED, GOVT PROGRAMS, STATE & LOCAL - RECEIPTS, MEASURED IN $'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - RECEIPTS, MEASURED IN $'
        'GOVT PROGRAMS, FEDERAL - RECEIPTS, MEASURED IN $ / OPERATION'
        'GOVT PROGRAMS, FEDERAL, (EXCL CONSERVATION & WETLANDS) - RECEIPTS, MEASURED IN $ / OPERATION'
        'GOVT PROGRAMS, FEDERAL, (EXCL CONSERVATION & WETLANDS) - OPERATIONS WITH RECEIPTS'
        'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - RECEIPTS, MEASURED IN $ / OPERATION'
        'INCOME, FARM-RELATED, GOVT PROGRAMS, STATE & LOCAL - RECEIPTS, MEASURED IN $ / OPERATION'
        CCC LOANS - RECEIPTS, MEASURED IN $
        CCC LOANS - RECEIPTS, MEASURED IN $ / OPERATION
        CCC LOANS - OPERATIONS WITH RECEIPTS
        CCC LOANS - REPAYMENTS, MEASURED IN $
        CCC LOANS - OPERATIONS WITH REPAYMENTS
"""

import os
import shutil
import pandas as pd
from pathlib import Path
import logging
import numpy as np
import re 

# -----------------------------------
# Logging
# -----------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------------
# Config
# -----------------------------------
# NASS files (2017–2022)
NASS_2017_FILE = "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/NASS_2017-2022/qs.census2017.txt"
NASS_2022_FILE = "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/NASS_2017-2022/qs.census2022.txt"

VARIABLE_MAPPING = {
    'farms_n': {  
        'deflate': False,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: 'item01001',
            1987: 'item01001',
            1992: 'item010001',
            1997: 'item01001',
            2002: 'item01001',
            2007: 'data1_1',
            2012: 'data1_1',
        },
        'nass_short_desc': 'FARM OPERATIONS - NUMBER OF OPERATIONS'
    },
    'crop_acres': {   #total cropland in acres
        'deflate': False,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: 'item01014',
            1987: 'item01014',
            1992: 'item010014',
            1997: 'item01015',
            2002: 'item01015',
            2007: 'data1_15',
            2012: 'data1_16',
        },
        'nass_short_desc': 'AG LAND, CROPLAND - ACRES'
    },
    'harvested_acres': {  #harvested cropland in acres
        'deflate': False,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: 'item01016',
            1987: 'item01016',
            1992: 'item010016',
            1997: 'item01017',
            2002: 'item01017',
            2007: 'data1_17',
            2012: 'data1_18',
        },
        'nass_short_desc': 'AG LAND, CROPLAND, HARVESTED - ACRES'
    },
    'corn_for_grain_acres': { # corn for grain in acres, should be harvested
        'deflate': False,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: 'item01056',
            1987: 'item01056',
            1992: 'item010058',
            1997: 'item01059',
            2002: 'item01063',
            2007: 'data1_63',
            2012: 'data1_64',
        },
        'nass_short_desc': 'CORN, GRAIN - ACRES HARVESTED'
    },
    'corn_for_grain_bu': { #corn for grain in bushels, harvested 
        'deflate': False,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: 'item01057',
            1987: 'item01057',
            1992: 'item010059',
            1997: 'item01060',
            2002: 'item01064',
            2007: 'data1_64',
            2012: 'data1_65',
        },
        'nass_short_desc': 'CORN, GRAIN - PRODUCTION, MEASURED IN BU'
    },
    'corn_for_silage_acres': { #no bushels version of this, acres harvested 
        'deflate': False,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: 'item01059',
            1987: 'item01059',
            1992: 'item010061',
            1997: 'item01062',
            2002: 'item01066',
            2007: 'data1_66',
            2012: 'data1_67',
        },
        'nass_short_desc': 'CORN, SILAGE - ACRES HARVESTED'
    },
    'gov_all_pf': { # nominal dollars per farm 
        'deflate': True,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: 'item04012',
            1987: 'item04012',
            1992: 'item040012',
            1997: 'item04012',
            2002: 'item05005',
            2007: 'data5_5',
            2012: 'data5_5',
        },
        'nass_short_desc': 'GOVT PROGRAMS, FEDERAL - RECEIPTS, MEASURED IN $ / OPERATION'
    },
    'gov_all_n': { # farms with receipts
        'deflate': False,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: 'item04010',
            1987: 'item04010',
            1992: 'item040010',
            1997: 'item04010',
            2002: 'item05001',
            2007: 'data5_1',
            2012: 'data5_1',
        },
        'nass_short_desc': 'GOVT PROGRAMS, FEDERAL - OPERATIONS WITH RECEIPTS'
    },
    'gov_all_amt': { # total nominal dollars 
        'deflate': True,
        'icpsr_in_thousands': True,
        'icpsr_columns': {
            1982: 'item04011',
            1987: 'item04011',
            1992: 'item040011',
            1997: 'item04011',
            2002: 'item05003',
            2007: 'data5_3',
            2012: 'data5_3',
        },
        'nass_short_desc': 'GOVT PROGRAMS, FEDERAL - RECEIPTS, MEASURED IN $'
    },
    'gov_cons_pf': { # nominal dollars per farm in conservation payments 
        'deflate': True,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: '', # CRP not established until 1985
            1987: '', # Conservation payments not tracked
            1992: 'item040015',
            1997: 'item04015',
            2002: 'item05011',
            2007: 'data5_11',
            2012: 'data5_11',
        },
        'nass_short_desc': 'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - RECEIPTS, MEASURED IN $ / OPERATION'
    },
    'gov_cons_amt': { # total nominal dollars 
        'deflate': True,
        'icpsr_in_thousands': True,
        'icpsr_columns': {
            1982: '', # CRP not established until 1985
            1987: '', # Conservation payments not tracked
            1992: 'item040014',
            1997: 'item04014',
            2002: 'item05009',
            2007: 'data5_9',
            2012: 'data5_9',
        },
        'nass_short_desc': 'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - RECEIPTS, MEASURED IN $'
    },
    'gov_cons_n': { # farms with receipts
        'deflate': False,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: '', # CRP not established until 1985
            1987: '', # Conservation payments not tracked 
            1992: 'item040013',
            1997: 'item04013',
            2002: 'item05007',
            2007: 'data5_7',
            2012: 'data5_7',
        },
        'nass_short_desc': 'GOVT PROGRAMS, FEDERAL, CONSERVATION & WETLANDS - OPERATIONS WITH RECEIPTS'
    },
    'gov_noncons_pf': { # nominal dollars per farm
        'deflate': True,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: '',
            1987: '',
            1992: '',
            1997: '',
            2002: '',
            2007: '',
            2012: '',
        },
        'nass_short_desc': 'GOVT PROGRAMS, FEDERAL, (EXCL CONSERVATION & WETLANDS) - RECEIPTS, MEASURED IN $ / OPERATION'
    },
    'gov_other_pay_pf': { # nominal dollars per farm 
        'deflate': True,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: '',
            1987: '',
            1992: '',
            1997: '',
            2002: '',
            2007: 'data5_17',
            2012: 'data5_17',
        },
        'nass_short_desc': ''
    }
    ,
    'ccc_loan_amt': { # $1,000 in loans  
        'deflate': True,
        'icpsr_in_thousands': True,
        'icpsr_columns': {
            1982: 'item04029',
            1987: 'item04029',
            1992: 'item040031',
            1997: 'item04031',
            2002: 'item05021',
            2007: 'data5_21',
            2012: 'data5_21',
        },
        'nass_short_desc': 'CCC LOANS - RECEIPTS, MEASURED IN $'
    },
    'ccc_loan_n': { # total farms receiving loans 
        'deflate': False,
        'icpsr_in_thousands': False,
        'icpsr_columns': {
            1982: 'item04028',
            1987: 'item04028',
            1992: 'item040030',
            1997: 'item04030',
            2002: 'item05019',
            2007: 'data5_19',
            2012: 'data5_19',
        },
        'nass_short_desc': 'CCC LOANS - OPERATIONS WITH RECEIPTS'
    }
}

# Manual calculated columns to create AFTER deflation.
# Supported ops: 'add' (sum all inputs), 'sub' (first minus the rest).
# If you want NaNs to be treated as 0 (e.g., add across sparse years), set na_zero=True per spec.

MANUAL_CALCS = [
    {
        "name": "gov_noncons_amt_calc",
        "op": "sub",
        "inputs": ["gov_all_amt_real", "gov_cons_amt_real"],
        "na_zero": True
    },
    {
        "name": "gov_noncons_pf_calc_real",
        "op": "div",
        "inputs": ["gov_noncons_amt_calc", "gov_all_n"],  # numerator / denominator
        "na_zero": False
    },
    {
        "name": "ccc_loan_pf_real",
        "op": "div",
        "inputs": ["ccc_loan_amt", "ccc_loan_n"],  # numerator / denominator
        "na_zero": False
    },
    {
        "name": "total_corn_harvested_acres",
        "op": "add",
        "inputs": ["corn_for_grain_acres", "corn_for_silage_acres"], 
        "na_zero": False
    },
    {
        "name": "share_corn_harvested_acres",
        "op": "div",
        "inputs": ["total_corn_harvested_acres", "harvested_acres"], 
        "na_zero": False
    }
]

YEARS = [1982, 1987, 1992, 1997, 2002, 2007, 2012, 2017, 2022]

STATE_NAMES = [
    "ALABAMA","ALASKA","ARIZONA","ARKANSAS","CALIFORNIA","COLORADO","CONNECTICUT","DELAWARE",
    "DISTRICT OF COLUMBIA","FLORIDA","GEORGIA","HAWAII","IDAHO","ILLINOIS","INDIANA","IOWA",
    "KANSAS","KENTUCKY","LOUISIANA","MAINE","MARYLAND","MASSACHUSETTS","MICHIGAN","MINNESOTA",
    "MISSISSIPPI","MISSOURI","MONTANA","NEBRASKA","NEVADA","NEW HAMPSHIRE","NEW JERSEY",
    "NEW MEXICO","NEW YORK","NORTH CAROLINA","NORTH DAKOTA","OHIO","OKLAHOMA","OREGON",
    "PENNSYLVANIA","RHODE ISLAND","SOUTH CAROLINA","SOUTH DAKOTA","TENNESSEE","TEXAS","UTAH",
    "VERMONT","VIRGINIA","WASHINGTON","WEST VIRGINIA","WISCONSIN","WYOMING"
]
# -----------------------------------
# Helpers
# -----------------------------------

def setup_directories():
    """Create interim directory structure if it doesn't exist."""
    interim_dir = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim")
    interim_dir.mkdir(exist_ok=True)
    for year in YEARS:
        (interim_dir / str(year)).mkdir(exist_ok=True)
    return interim_dir

def _as_number(series: pd.Series) -> pd.Series:
    """Coerce strings like '$1,234' or '1,234.5' to float; keep NaNs."""
    return (
        series.astype(str)
              .str.replace(r'[^\d\.\-]', '', regex=True)
              .replace('', np.nan)
              .astype(float)
    )


def _strip_state_prefix(raw: str) -> str:
    """
    For county-level rows, turn things like:
      'Alabama\\Jefferson'  -> 'JEFFERSON'
      'Alabama/Jefferson'   -> 'JEFFERSON'
      'AlabamaJefferson'    -> 'JEFFERSON'
      'JEFFERSON'           -> 'JEFFERSON'
    Always returns UPPERCASE, trimmed.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return pd.NA
    s = str(raw).strip()

    # Normalize slashes and split if present
    s_norm = s.replace("\\", "/")
    if "/" in s_norm:
        tail = s_norm.split("/")[-1].strip()
        return re.sub(r"^\W+|\W+$", "", tail).upper()

    up = s_norm.upper().strip()

    # Remove leading state name if string starts with it (no separator case)
    for st in STATE_NAMES:
        if up.startswith(st):
            tail = up[len(st):].strip()
            # Drop a leftover slash, backslash, or hyphen if present
            tail = re.sub(r"^[\\/\-\s]+", "", tail)
            return re.sub(r"^\W+|\W+$", "", tail).upper()

    # Already just a county name
    return re.sub(r"^\W+|\W+$", "", up).upper()

def standardize_geo_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    - level==1 (COUNTY): name -> county name ONLY (uppercase)
    - level==2 (STATE):  uppercase state name
    - level==3 (US):     'UNITED STATES'
    Operates in place and returns df.
    """
    if 'level' not in df.columns or 'name' not in df.columns:
        return df

    # Ensure numeric levels
    df['level'] = pd.to_numeric(df['level'], errors='coerce').astype('Int64')

    # COUNTY rows → county name only
    m_county = df['level'] == 1
    df.loc[m_county, 'name'] = df.loc[m_county, 'name'].apply(_strip_state_prefix)

    # STATE rows → uppercase
    m_state = df['level'] == 2
    df.loc[m_state, 'name'] = df.loc[m_state, 'name'].astype(str).str.upper().str.strip()

    # NATIONAL rows → fixed label
    m_us = df['level'] == 3
    df.loc[m_us, 'name'] = 'UNITED STATES'

    return df

def load_nass_census_data(file_path, year):
    """Load NASS census data TSV."""
    try:
        df = pd.read_csv(file_path, sep='\t', low_memory=False)
        return df
    except Exception as e:
        logger.error(f"Error loading NASS {year} data: {e}")
        return None

def make_fips_from_parts(statefip, counfip, level):
    """
    Build FIPS per rules:
      - COUNTY: concat state (2-digit, zero-padded) + county (3-digit, zero-padded),
                then convert to int to drop any leading zero -> 4 or 5 digits.
      - STATE:  state FIPS as int (no leading zero).
      - US:     99000.
    Returns pandas nullable Int64.
    """
    if level == 1:  # COUNTY
        s = (pd.Series(statefip, dtype="string").str.strip()
                 .str.replace(r'\.0$', '', regex=True).fillna("")).str.zfill(2)
        c = (pd.Series(counfip, dtype="string").str.strip()
                 .str.replace(r'\.0$', '', regex=True).fillna("")).str.zfill(3)
        combo = (s + c).where((s != "") & (c != ""), None)
        # int() to drop any leading zero; cast back to nullable Int64
        return pd.to_numeric(combo, errors="coerce").astype("Int64")
    elif level == 2:  # STATE
        s = pd.Series(statefip, dtype="string").str.strip().str.replace(r'\.0$', '', regex=True)
        return pd.to_numeric(s, errors="coerce").astype("Int64")
    elif level == 3:  # US
        return pd.Series([99000] * (len(statefip) if hasattr(statefip, "__len__") else 1), dtype="Int64")
    else:
        return pd.Series([pd.NA] * (len(statefip) if hasattr(statefip, "__len__") else 1), dtype="Int64")


def normalize_fips_after_merge(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize FIPS across all years:
      - NATIONAL (level==3): fips = 99000
      - STATE    (level==2): fips = numeric(statefip)  [2-digit, no leading zero kept]
      - COUNTY   (level==1): if both parts present, fips = int(SS.zfill(2)+CCC.zfill(3)); else NA
    Ensures no 0-valued FIPS from missing parts; uses pandas nullable Int64.
    """
    df = df.copy()
    # Coerce level to numeric
    df['level'] = pd.to_numeric(df.get('level'), errors='coerce').astype('Int64')

    # Clean the component codes to strings; don't pad yet
    def _clean_part(s):
        s = pd.Series(s, dtype="string")
        s = s.str.strip().str.replace(r'\.0$', '', regex=True)   # drop accidental float tails
        s = s.replace({"": pd.NA, ".": pd.NA, "nan": pd.NA, "None": pd.NA})
        return s

    if 'statefip' in df.columns:
        df['statefip'] = _clean_part(df['statefip'])
    if 'counfip' in df.columns:
        df['counfip'] = _clean_part(df['counfip'])

    # NATIONAL → 99000
    m_nat = df['level'] == 3
    df.loc[m_nat, 'fips'] = 99000

    # STATE → numeric(statefip)
    m_state = df['level'] == 2
    df.loc[m_state, 'fips'] = pd.to_numeric(df.loc[m_state, 'statefip'], errors='coerce').astype('Int64')

    # COUNTY → concat padded parts, but only if BOTH present
    m_county = df['level'] == 1
    s_ok = df.loc[m_county, 'statefip'].notna()
    c_ok = df.loc[m_county, 'counfip'].notna()
    ok = m_county.copy()
    ok.loc[ok] = s_ok & c_ok

    # Build only for rows with both parts; leave others NA
    ss = df.loc[ok, 'statefip'].astype(str).str.zfill(2)
    cc = df.loc[ok, 'counfip'].astype(str).str.zfill(3)
    df.loc[ok, 'fips'] = pd.to_numeric(ss + cc, errors='coerce').astype('Int64')

    # Any residual 0s (which imply bad/missing parts) → NA except national
    m_zero_bad = df['fips'].fillna(-1).eq(0) & ~m_nat
    df.loc[m_zero_bad, 'fips'] = pd.NA

    # Keep nullable Int64
    df['fips'] = df['fips'].astype('Int64')
    return df


def process_nass_census_data(df, year):
    """
    Minimal processor for NASS 2017/2022:
      - DOMAIN_DESC == 'TOTAL' only
      - exact SHORT_DESC match using VARIABLE_MAPPING[var]['nass_short_desc'] (case/space-normalized)
      - cleans VALUE: (D)/(H)/(NA)/(Z) -> NaN; remove $ and commas
      - maps by level to columns named by VARIABLE_MAPPING keys
      - constructs FIPS per user rules (county=5-digit combo, state=2-digit, US=99000; no leading zeros retained)
    """
    logger.info(f"Processing NASS {year} data (minimal TOTAL-domain pipeline)")

    # --- helpers ---
    def norm(s):
        return s.astype(str).str.strip().str.upper()

    def clean_value(s):
        s = s.astype(str).str.strip()
        s = s.where(~s.isin(['(D)', '(H)', '(NA)', '(Z)']))
        s = s.str.replace(r'[\$,]', '', regex=True)
        return pd.to_numeric(s, errors='coerce')

    # normalize needed text cols (if present)
    for col in ['AGG_LEVEL_DESC','SHORT_DESC','DOMAIN_DESC','STATE_NAME','COUNTY_NAME','COUNTRY_NAME','UNIT_DESC']:
        if col in df.columns:
            df[col] = norm(df[col])

    # zero-pad FIPS source fields for keying; we'll drop leading zeros when making integers
    if 'STATE_FIPS_CODE' in df.columns:
        df['STATE_FIPS_CODE'] = df['STATE_FIPS_CODE'].astype(str).str.zfill(2)
    if 'COUNTY_CODE' in df.columns:
        df['COUNTY_CODE'] = df['COUNTY_CODE'].astype(str).str.zfill(3)

    # 1) TOTAL domain only
    if 'DOMAIN_DESC' in df.columns:
        df = df[df['DOMAIN_DESC'] == 'TOTAL'].copy()

    # 2) keep only supported levels
    level_map = {'COUNTY': 1, 'STATE': 2, 'NATIONAL': 3}
    df = df[df['AGG_LEVEL_DESC'].isin(level_map.keys())].copy()
    df['level'] = df['AGG_LEVEL_DESC'].map(level_map)

    out_frames = []

    for lvl_name, lvl_num in level_map.items():
        sub = df[df['AGG_LEVEL_DESC'] == lvl_name]
        if sub.empty:
            continue

        # base geography
        if lvl_name == 'COUNTY':
            base = (sub[['YEAR','COUNTY_NAME','STATE_FIPS_CODE','COUNTY_CODE','level']]
                    .drop_duplicates()
                    .rename(columns={'YEAR':'year','COUNTY_NAME':'name',
                                     'STATE_FIPS_CODE':'statefip','COUNTY_CODE':'counfip'}))
            # FIPS (county): concat then convert to integer (drops leading zeros)
            base['fips'] = make_fips_from_parts(base['statefip'], base['counfip'], level=1)
            make_bkey = lambda b: b['name'] + '_' + b['statefip'] + '_' + b['counfip']
            make_key  = lambda d: d['COUNTY_NAME'] + '_' + d['STATE_FIPS_CODE'] + '_' + d['COUNTY_CODE']

        elif lvl_name == 'STATE':
            base = (sub[['YEAR','STATE_NAME','STATE_FIPS_CODE','level']]
                    .drop_duplicates()
                    .rename(columns={'YEAR':'year','STATE_NAME':'name','STATE_FIPS_CODE':'statefip'}))
            base['counfip'] = np.nan
            # FIPS (state): numeric 2-digit
            base['fips'] = make_fips_from_parts(base['statefip'], None, level=2)
            make_bkey = lambda b: b['name'] + '_' + b['statefip']
            make_key  = lambda d: d['STATE_NAME'] + '_' + d['STATE_FIPS_CODE']

        else:  # NATIONAL
            base = (sub[['YEAR','COUNTRY_NAME','level']]
                    .drop_duplicates()
                    .rename(columns={'YEAR':'year','COUNTRY_NAME':'name'}))
            base['statefip'] = np.nan
            base['counfip'] = np.nan
            # FIPS (US): 99000
            base['fips'] = make_fips_from_parts(None, None, level=3)
            make_bkey = lambda b: b['name']
            make_key  = lambda d: d['COUNTRY_NAME']

        # for each requested variable, map by exact (normalized) SHORT_DESC
        for var_name, cfg in VARIABLE_MAPPING.items():
            short = cfg.get('nass_short_desc')
            if not short:
                continue
            target_short = str(short).strip().upper()

            rows = sub[sub['SHORT_DESC'] == target_short].copy()
            if rows.empty or 'VALUE' not in rows.columns:
                base[var_name] = np.nan
                continue

            rows['VALNUM'] = clean_value(rows['VALUE'])
            kmap = dict(zip(make_key(rows), rows['VALNUM']))
            base[var_name] = make_bkey(base).map(kmap)

        # drop rows where all requested vars are NaN
        wanted_cols = list(VARIABLE_MAPPING.keys())
        base = base.dropna(subset=[c for c in wanted_cols if c in base.columns], how='all')

        if not base.empty:
            out_frames.append(base)

    result = pd.concat(out_frames, ignore_index=True) if out_frames else pd.DataFrame()
    if result.empty:
        logger.warning(f"No usable rows found for NASS {year} with DOMAIN=='TOTAL' after simple mapping.")
    else:
        keep_cols = ['year','name','level','fips','statefip','counfip'] + [c for c in VARIABLE_MAPPING.keys() if c in result.columns]
        logger.info(f"NASS {year} processed rows: {len(result)} | cols: {len(keep_cols)}")
        logger.info(result[keep_cols].head().to_string())
        result = result[keep_cols]
    return result


def load_deflator_data():
    """Load the BEA price deflator (A191RG) and return year + price_deflator."""
    deflator_path = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/deflator/price_index_A191RG_BEA.csv")
    if not deflator_path.exists():
        logger.error(f"Deflator file not found: {deflator_path}")
        return None
    try:
        deflator_df = pd.read_csv(deflator_path)
        logger.info(f"Loaded deflator data: {len(deflator_df)} rows")
        deflator_df['year'] = pd.to_datetime(deflator_df['observation_date']).dt.year
        deflator_df = deflator_df.rename(columns={'A191RG3A086NBEA': 'price_deflator'})
        deflator_df = deflator_df[['year', 'price_deflator']].copy()
        logger.info(f"Deflator data years: {deflator_df['year'].min()} to {deflator_df['year'].max()}")
        return deflator_df
    except Exception as e:
        logger.error(f"Error loading deflator data: {e}")
        return None


def deflate_columns(df, deflator_df, variable_mapping: dict):
    """Deflate columns flagged in `variable_mapping` using the price deflator (2017=100)."""
    logger.info("Deflating monetary columns to 2017 dollars...")
    df_with_deflator = df.merge(deflator_df, on='year', how='left')

    # warn if missing deflator
    missing_deflator = df_with_deflator['price_deflator'].isna().sum()
    if missing_deflator > 0:
        logger.warning(f"Missing deflator data for {missing_deflator} records")
        missing_years = df_with_deflator[df_with_deflator['price_deflator'].isna()]['year'].unique()
        logger.info(f"Missing years: {missing_years}")

    deflatable_cols = [
        name for name, cfg in (variable_mapping or {}).items()
        if isinstance(cfg, dict) and cfg.get('deflate')
    ]
    logger.info(f"Deflatable columns (from VARIABLE_MAPPING): {deflatable_cols}")

    # compute real columns
    for col in deflatable_cols:
        if col in df_with_deflator.columns:
            df_with_deflator[col] = (
                df_with_deflator[col]
                .astype(str)
                .str.replace(r'[^\d\.\-]', '', regex=True)
                .replace('', np.nan)
                .astype(float)
            )
            real_col = f"{col}_real"
            df_with_deflator[real_col] = df_with_deflator[col] * (100 / df_with_deflator['price_deflator'])
            logger.info(f"Created deflated column: {real_col}")
        else:
            logger.warning(f"Column {col} not found in data, skipping deflation")

    return df_with_deflator

def get_icpsr_variable_mapping(year):
    """Get column mapping for ICPSR files for a given year."""
    base_mapping = {
        'name': 'name',
        'level': 'level',
        'fips': 'fips',
        'statefip': 'statefip',
        'counfip': 'counfip'
    }
    for var_name, var_config in VARIABLE_MAPPING.items():
        col = var_config['icpsr_columns'].get(year, None)
        if isinstance(col, str) and col.strip():
            base_mapping[var_name] = col

    if year == 2007:
        base_mapping['counfip'] = 'countyfip'
    elif year == 2012:
        base_mapping['counfip'] = 'cofips'
    return base_mapping

def filter_and_process_data(file_path, year, variable_mapping, full_variable_specs=VARIABLE_MAPPING):
    """Select variables by mapping and attach year column (ICPSR files).
       Also: scale ICPSR 'in thousands' fields to dollars immediately.
    """
    try:
        df = pd.read_csv(file_path, sep='\t', low_memory=False)
        logger.info(f"Loaded {len(df)} rows from {file_path}")

        # map of lowercase -> actual column name in file
        df_columns_lower = {col.lower(): col for col in df.columns}

        # which raw vars do we need for this year?
        required_vars = [v for v in variable_mapping.values()
                         if isinstance(v, str) and v.strip()]

        available_vars, missing_vars = [], []
        for var in required_vars:
            var_lower = var.lower()
            if var_lower in df_columns_lower:
                available_vars.append(df_columns_lower[var_lower])
            else:
                missing_vars.append(var)

        if missing_vars:
            logger.warning(f"Missing variables in {year}: {missing_vars}")
            logger.info("Examples of columns with 'item'/'data': " +
                        str([c for c in df.columns if 'item' in c.lower() or 'data' in c.lower()][:10]))

        if not available_vars:
            logger.error(f"No required variables found in {year}")
            return None

        # keep only what we need, then rename to standardized names
        df_filtered = df[available_vars].copy()

        actual_to_standard = {}
        for standard_name, original_var in variable_mapping.items():
            if not isinstance(original_var, str) or not original_var.strip():
                continue
            original_var_lower = original_var.lower()
            if original_var_lower in df_columns_lower:
                actual_to_standard[df_columns_lower[original_var_lower]] = standard_name

        df_filtered = df_filtered.rename(columns=actual_to_standard)

        # Attach year
        df_filtered['year'] = year
        cols = ['year'] + [c for c in df_filtered.columns if c != 'year']
        df_filtered = df_filtered[cols]

        # ---- NEW: scale ICPSR fields that are "in thousands" to dollars ----
        # We only touch variables that (a) are present in this df and (b) are flagged icpsr_in_thousands=True
        # This happens early so all downstream logic sees standardized $ units.
        for var_name, spec in (full_variable_specs or {}).items():
            if not isinstance(spec, dict):
                continue
            if spec.get('icpsr_in_thousands') and (var_name in df_filtered.columns):
                before_nonnull = df_filtered[var_name].notna().sum()
                df_filtered[var_name] = _as_number(df_filtered[var_name]) * 1000.0
                logger.info(f"[{year}] Scaled '{var_name}' from thousands to dollars "
                            f"({before_nonnull} non-missing values).")
            # If not in_thousands but numeric-like, we leave as-is to avoid surprising changes.

        return df_filtered

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return None


def collect_census_files():
    """Process ICPSR (1992–2012) files and write merged outputs for those years."""
    base_path = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/ICPSR_1850-2012")
    folder_year_mapping = {"DS0040": 1982, "DS0041": 1987, "DS0042": 1992, "DS0043": 1997, "DS0044": 2002, "DS0045": 2007, "DS0047": 2012}
    folder_file_mapping = {"DS0040": "0040", "DS0041": "0041", "DS0042": "0042", "DS0043": "0043", "DS0044": "0044", "DS0045": "0045", "DS0047": "0047"}

    # deflator
    deflator_df = load_deflator_data()
    if deflator_df is None:
        logger.error("Failed to load deflator data. Exiting.")
        return [], [{'error': 'Failed to load deflator data'}]

    interim_dir = setup_directories()

    collected_files, missing_files, processed_dataframes = [], [], []

    for folder, year in folder_year_mapping.items():
        logger.info(f"Processing {folder} (Year: {year})")
        file_number = folder_file_mapping[folder]
        source_file = base_path / folder / f"35206-{file_number}-Data.tsv"

        if not source_file.exists():
            logger.warning(f"✗ File not found: {source_file}")
            missing_files.append({'folder': folder, 'year': year, 'error': 'File not found'})
            continue

        try:
            variable_mapping = get_icpsr_variable_mapping(year)
            df_filtered = filter_and_process_data(source_file, year, variable_mapping, full_variable_specs=VARIABLE_MAPPING)
            if df_filtered is None:
                missing_files.append({'folder': folder, 'year': year, 'error': 'Failed to process data'})
                continue

            year_file = interim_dir / str(year) / f"census_{year}_filtered.tsv"
            df_filtered.to_csv(year_file, sep='\t', index=False)
            processed_dataframes.append(df_filtered)

            collected_files.append({
                'year': year,
                'folder': folder,
                'source': str(source_file),
                'destination': str(year_file),
                'rows': len(df_filtered),
                'columns': len(df_filtered.columns)
            })
            logger.info(f"✓ Processed {source_file.name} → {year_file.name} ({len(df_filtered)} rows, {len(df_filtered.columns)} cols)")
        except Exception as e:
            logger.error(f"✗ Failed to process {source_file}: {e}")
            missing_files.append({'folder': folder, 'year': year, 'error': str(e)})

    return collected_files, missing_files

def print_summary(collected_files, missing_files):
    """Print a summary of the collection process."""
    print("\n" + "="*80)
    print("AGRICULTURAL CENSUS DATA PROCESSING SUMMARY")
    print("="*80)

    if collected_files:
        print(f"\n✓ Successfully processed {len([f for f in collected_files if f['year'] != 'merged'])} year files:")
        for file_info in collected_files:
            if file_info['year'] != 'merged':
                print(f"  {file_info['year']}: {file_info['rows']} rows, {file_info['columns']} columns")
                print(f"    → {file_info['destination']}")

        merged_files = [f for f in collected_files if f['year'] in ['merged_deflated', 'merged_full']]
        if merged_files:
            print(f"\n✓ Created merged datasets:")
            for mf in merged_files:
                file_type = "deflated (real dollars)" if mf['year'] == 'merged_deflated' else "full (nominal + real + deflator)"
                print(f"  {file_type}: {mf['rows']} total rows, {mf['columns']} columns")

    if missing_files:
        print(f"\n✗ {len(missing_files)} files could not be processed:")
        for file_info in missing_files:
            print(f"  {file_info['year']} ({file_info['folder']}): {file_info['error']}")

    print(f"\nOutput directory: /Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim")
    print("="*80)

def process_nass_data():
    """Process NASS 2017 & 2022 and convert to ICPSR-like format."""
    processed = []

    df_2017 = load_nass_census_data(NASS_2017_FILE, 2017)
    if df_2017 is not None:
        p2017 = process_nass_census_data(df_2017, 2017)
        if p2017 is not None:
            processed.append(p2017)

    df_2022 = load_nass_census_data(NASS_2022_FILE, 2022)
    if df_2022 is not None:
        p2022 = process_nass_census_data(df_2022, 2022)
        if p2022 is not None:
            processed.append(p2022)

    return processed


def apply_manual_calculations(df: pd.DataFrame, calcs: list) -> pd.DataFrame:
    """
    Apply manual column calculations on df based on 'calcs' specs.
    Supported ops: 'add', 'sub', 'div'.
    Each spec:
      {
        "name": str,
        "op": "add"|"sub"|"div",
        "inputs": [str, ...],
        "na_zero": bool (optional)
      }
    Notes:
      - 'sub': first input minus sum of the rest
      - 'div': first input divided by second (ignores others if >2)
      - if 'na_zero' True: NaNs in inputs are treated as 0
    """
    out = df.copy()

    for spec in calcs or []:
        name   = spec.get("name")
        op     = (spec.get("op") or "").strip().lower()
        inputs = spec.get("inputs") or []
        na_zero = bool(spec.get("na_zero", True))

        if not name or op not in {"add", "sub", "div"} or len(inputs) == 0:
            logger.warning(f"Skipping manual calc with invalid spec: {spec}")
            continue

        # collect inputs (fall back to NaN if missing)
        series_list = []
        for col in inputs:
            if col in out.columns:
                s = pd.to_numeric(out[col], errors="coerce")
            else:
                logger.warning(f"Manual calc '{name}': missing input column '{col}'. Filling with NaN.")
                s = pd.Series(np.nan, index=out.index, dtype="float64")
            series_list.append(s)

        if na_zero:
            series_list = [s.fillna(0.0) for s in series_list]

        # compute
        if op == "add":
            res = sum(series_list)
        elif op == "sub":
            res = series_list[0].copy()
            for s in series_list[1:]:
                res = res - s
        elif op == "div":
            num = series_list[0]
            denom = series_list[1] if len(series_list) > 1 else np.nan
            res = num / denom.replace({0: np.nan})  # avoid div by zero
        else:
            logger.warning(f"Unsupported op: {op}")
            continue

        out[name] = res

    return out


def main():
    """Main orchestrator."""
    logger.info("Starting agricultural census data collection...")

    interim_dir = setup_directories()

    # Load deflator once
    deflator_df = load_deflator_data()
    if deflator_df is None:
        logger.error("Failed to load deflator data. Exiting.")
        return

    # Step 1: ICPSR 1992–2012
    logger.info("Step 1: Processing ICPSR census data...")
    icpsr_files, icpsr_missing = collect_census_files()

    # Step 2: NASS 2017/2022
    logger.info("Step 2: Processing NASS census data (2017, 2022)...")
    processed_nass_data = process_nass_data()

    # Step 3: Merge all years (if we have any NASS data)
    if processed_nass_data:
        logger.info("Step 3: Creating merged dataset...")

        # Load already-saved ICPSR per-year files
        icpsr_data = []
        for y in YEARS:
            f = interim_dir / str(y) / f"census_{y}_filtered.tsv"
            if f.exists():
                icpsr_data.append(pd.read_csv(f, sep='\t', low_memory=False))

        # Combine ICPSR + NASS
        all_data = icpsr_data + processed_nass_data
        merged_df = pd.concat(all_data, ignore_index=True)

        merged_df = normalize_fips_after_merge(merged_df) 
        merged_df = standardize_geo_names(merged_df)

        # Deflate
        logger.info("Step 4: Applying deflation...")
        merged_df_deflated = deflate_columns(merged_df, deflator_df, VARIABLE_MAPPING)

        # Build any manual calculated columns (post-deflation)
        merged_df_deflated = apply_manual_calculations(merged_df_deflated, MANUAL_CALCS)

        # Build outputs
        essential_columns = ['year', 'name', 'level', 'fips', 'statefip', 'counfip']
        deflatable_vars = {
            name for name, cfg in VARIABLE_MAPPING.items()
            if isinstance(cfg, dict) and cfg.get('deflate')
        }
        real_columns = [c for c in merged_df_deflated.columns if c.endswith('_real')]
        # Exclude price_deflator from deflated dataset
        other_columns = [col for col in merged_df_deflated.columns
                         if col not in essential_columns
                         and not col.endswith('_real')
                         and col not in deflatable_vars
                         and col != 'price_deflator']

        # Deflated dataset (NO deflator)
        final_columns = [c for c in (essential_columns + other_columns + real_columns)
                         if c in merged_df_deflated.columns]
        final_df = merged_df_deflated[final_columns].copy()

        # drop ALASKA and HAWAII rows
        merged_df_deflated = merged_df_deflated[~merged_df_deflated['statefip'].isin([2, 15])].copy()

        # Full dataset (WITH deflator)
        full_df = merged_df_deflated.copy()

        # Save
        final_file = interim_dir / "census_merged_deflated.tsv"
        final_df.to_csv(final_file, sep='\t', index=False)

        full_file = interim_dir / "census_merged_full.tsv"
        full_df.to_csv(full_file, sep='\t', index=False)

        logger.info(f"✓ Created deflated merged dataset: {final_file} ({len(final_df)} rows, {len(final_df.columns)} cols)")
        logger.info(f"✓ Created full dataset (nominal + real + deflator): {full_file} ({len(full_df)} rows, {len(full_df.columns)} cols)")

        # Extend summaries from earlier step
        collected_files = icpsr_files + [
            {
                'year': 2017,
                'folder': 'NASS',
                'source': 'NASS_2017',
                'destination': str(interim_dir / "2017" / "census_2017_filtered.tsv"),
                'rows': len(processed_nass_data[0]) if processed_nass_data else 0,
                'columns': len(processed_nass_data[0].columns) if processed_nass_data else 0
            },
            {
                'year': 2022,
                'folder': 'NASS',
                'source': 'NASS_2022',
                'destination': str(interim_dir / "2022" / "census_2022_filtered.tsv"),
                'rows': len(processed_nass_data[1]) if len(processed_nass_data) > 1 else 0,
                'columns': len(processed_nass_data[1].columns) if len(processed_nass_data) > 1 else 0
            },
            {
                'year': 'merged_deflated',
                'folder': 'all',
                'source': 'multiple',
                'destination': str(final_file),
                'rows': len(final_df),
                'columns': len(final_df.columns)
            },
            {
                'year': 'merged_full',
                'folder': 'all',
                'source': 'multiple',
                'destination': str(full_file),
                'rows': len(full_df),
                'columns': len(full_df.columns)
            }
        ]
        print_summary(collected_files, icpsr_missing)
    else:
        # No NASS data processed; still print ICPSR-only summary
        print_summary(icpsr_files, icpsr_missing)

if __name__ == "__main__":
    main()
