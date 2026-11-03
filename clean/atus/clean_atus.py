"""Clean the American Time Use Survey (ATUS) multi-year microdata to one row per respondent.

Input (downloaded by hand from https://www.bls.gov/tus/data/datafiles-0324.htm, which refuses
scripted downloads; place the zips in raw/atus/):
  raw/atus/atussum-0324.zip   Activity Summary file: one row per respondent with minutes per
                              day in each 6-digit activity code (columns t010101 ...), the
                              multi-year weight TUFNWGTP (TU20FWGT for 2020), TUYEAR, TEAGE,
                              TESEX, PEEDUCA, TELFS, TUDIARYDAY, TRCHILDNUM, TRERNWA
  raw/atus/atuscps-0324.zip   ATUS-CPS file: the CPS record of every household member;
                              the respondent is TULINENO == 1; family income bracket
                              HEFAMINC (2010+) or HUFAMINC (2003-2009), codes 1-16
Any other year range in the file names is accepted (the first atussum-*.zip / atuscps-*.zip).

Output interim/atus/atus_respondents.parquet, one row per respondent, ages 18+:
  tucaseid, year, weight (TUFNWGTP, or TU20FWGT for 2020), age, female, educ_years (PEEDUCA
  mapped to years: <HS 10, HS 12, some college 13, associate 14, BA 16, MA 18, prof/PhD 20),
  educ3 (12 or less / 13-15 / 16+), employed (TELFS 1-2), diary_weekend, n_children,
  income_bracket (1-16; missing if not reported), income_mid (bracket midpoint, nominal $),
  minutes: cook (020201 food and drink preparation + 020202 food presentation + 020203
  kitchen and food clean-up + 020299), cook_prep (020201 only), eat (1101xx + 1102xx +
  119999 eating and drinking), grocery (070101 grocery shopping), travel_eat (181101).
Average minutes per day for a group = weighted mean of the minutes with the weight (the
weights sum to person-days, so the mean over respondents is the mean over days; ATUS User's
Guide, chapter 7).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import zipfile
import numpy as np
import pandas as pd

RAW = rdir / 'atus'
OUT = idir / 'atus' / 'atus_respondents.parquet'
EDUC_YEARS = {31: 10, 32: 10, 33: 10, 34: 10, 35: 10, 36: 10, 37: 10, 38: 10, 39: 12, 40: 13, 41: 14, 42: 14, 43: 16, 44: 18, 45: 20, 46: 20}
INCOME_MID = {1: 2500, 2: 6250, 3: 8750, 4: 11250, 5: 13750, 6: 17500, 7: 22500, 8: 27500, 9: 32500, 10: 37500,
              11: 45000, 12: 55000, 13: 67500, 14: 87500, 15: 125000, 16: 200000}
ACT = {'cook': ['t020201', 't020202', 't020203', 't020299'], 'cook_prep': ['t020201'],
       'eat': ['t110101', 't110199', 't110201', 't110289', 't110299', 't119999'], 'grocery': ['t070101'], 'travel_eat': ['t181101']}


def read_zip_csv(pattern, usecols=None):
    files = sorted(RAW.glob(pattern))
    assert files, f'no {pattern} in {RAW}; download it from https://www.bls.gov/tus/data/datafiles-0324.htm'
    with zipfile.ZipFile(files[0]) as z:
        name = [n for n in z.namelist() if n.lower().endswith('.dat') or n.lower().endswith('.csv')][0]
        with z.open(name) as f:
            head = pd.read_csv(f, nrows=0)
        cols = None if usecols is None else [c for c in head.columns if c in usecols or c.upper() in usecols]
        with z.open(name) as f:
            df = pd.read_csv(f, usecols=cols)
    df.columns = [c.upper() if not c.lower().startswith('t0') and not c.lower().startswith('t1') else c.lower() for c in df.columns]
    return df


def main():
    summ = read_zip_csv('atussum-*.zip')
    summ.columns = [c.lower() if c.lower().startswith('t') and c[1:].isdigit() else c.upper() for c in summ.columns]
    print(f'activity summary: {len(summ):,} respondents, years {summ.TUYEAR.min()}-{summ.TUYEAR.max()}')
    w = summ['TUFNWGTP'].astype(float)
    if 'TU20FWGT' in summ.columns:
        w = w.where(summ['TUYEAR'] != 2020, summ['TU20FWGT'].astype(float))
    d = pd.DataFrame({'tucaseid': summ['TUCASEID'], 'year': summ['TUYEAR'], 'weight': w, 'age': summ['TEAGE'],
                      'female': (summ['TESEX'] == 2).astype(int), 'educ_years': summ['PEEDUCA'].map(EDUC_YEARS),
                      'employed': summ['TELFS'].isin([1, 2]).astype(int), 'diary_weekend': summ['TUDIARYDAY'].isin([1, 7]).astype(int),
                      'n_children': summ['TRCHILDNUM']})
    d['educ3'] = pd.cut(d['educ_years'], [0, 12, 15.5, 30], labels=['12 years or less', '13-15 years', '16+ years'])
    for k, cols in ACT.items():
        have = [c for c in cols if c in summ.columns]
        d[k] = summ[have].sum(axis=1) if have else np.nan
        print(f'  {k}: columns {have}')
    cps = read_zip_csv('atuscps-*.zip', usecols={'TUCASEID', 'TULINENO', 'HEFAMINC', 'HUFAMINC'})
    cps = cps[cps['TULINENO'] == 1]
    inc = cps['HEFAMINC'] if 'HEFAMINC' in cps.columns else pd.Series(np.nan, index=cps.index)
    if 'HUFAMINC' in cps.columns:
        inc = inc.where(inc > 0, cps['HUFAMINC'])
    cps['income_bracket'] = inc.where(inc > 0)
    d = d.merge(cps[['TUCASEID', 'income_bracket']].rename(columns={'TUCASEID': 'tucaseid'}), on='tucaseid', how='left')
    d['income_mid'] = d['income_bracket'].map(INCOME_MID)
    d = d[d['age'] >= 18]
    print(f'adults 18+: {len(d):,}; income reported {d.income_bracket.notna().mean():.3f}')
    print(d.groupby('year').apply(lambda g: pd.Series({'n': len(g), 'cook_min': np.average(g['cook'], weights=g['weight']),
                                                       'eat_min': np.average(g['eat'], weights=g['weight'])})).round(1).to_string())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    d.to_parquet(OUT, index=False)
    print(f'saved {OUT}')


if __name__ == '__main__':
    main()
