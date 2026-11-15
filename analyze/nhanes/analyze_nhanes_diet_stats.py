"""Survey-weighted diet facts from NHANES/WWEIA 2017-18 (for talk + paper intro).

Computes, for adults 20+ (survey-weighted):
  1) share reporting ZERO vegetables (FNDDS food group 7) on the day-1 recall
     and across both recall days, with and without white potatoes
  2) top WWEIA food categories by share of calories and of gram weight (day 1)
  3) vegetable share of total calories

Raw files live in nielsen_data/raw/nhanes/ and are re-downloaded from
CDC/ARS if missing. Mixed dishes are single WWEIA foods, so "zero vegetables"
means no vegetable eaten as a distinct food or side (sauce on pizza, lettuce
on a burger don't count) -- conservative in the other direction, fries count.

Numbers used on the SSSS_August2026 deck (Measuring nutrition + top-10 table).
"""
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
D = rdir / 'nhanes'

CDC = 'https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles/'
SOURCES = {
    'DEMO_J.xpt': CDC + 'DEMO_J.xpt',
    'DR1TOT_J.xpt': CDC + 'DR1TOT_J.xpt',
    'DR2TOT_J.xpt': CDC + 'DR2TOT_J.xpt',
    'DR1IFF_J.xpt': CDC + 'DR1IFF_J.xpt',
    'DR2IFF_J.xpt': CDC + 'DR2IFF_J.xpt',
    'wweia_foodcat.xlsx':
        'https://www.ars.usda.gov/ARSUserFiles/80400530/apps/WWEIA1718_foodcat_FNDDS.xlsx',
}
for fname, url in SOURCES.items():
    if not (D / fname).exists():
        print(f'downloading {fname}...')
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        (D / fname).write_bytes(urllib.request.urlopen(req).read())

demo = pd.read_sas(D / 'DEMO_J.xpt', format='xport')[['SEQN', 'RIDAGEYR']]
tot1 = pd.read_sas(D / 'DR1TOT_J.xpt', format='xport')[['SEQN', 'DR1DRSTZ', 'WTDRD1']]
tot2 = pd.read_sas(D / 'DR2TOT_J.xpt', format='xport')[['SEQN', 'DR2DRSTZ', 'WTDR2D']]
iff1 = pd.read_sas(D / 'DR1IFF_J.xpt', format='xport')[['SEQN', 'DR1IFDCD', 'DR1IGRMS', 'DR1IKCAL']]
iff2 = pd.read_sas(D / 'DR2IFF_J.xpt', format='xport')[['SEQN', 'DR2IFDCD', 'DR2IGRMS', 'DR2IKCAL']]

cat = pd.read_excel(D / 'wweia_foodcat.xlsx')
cat = cat[['food_code', 'category_description']].rename(
    columns={'food_code': 'fdcd', 'category_description': 'wweia'})
cat['fdcd'] = cat['fdcd'].astype(float)

# ---------- sample frames ----------
p = demo.merge(tot1, on='SEQN').merge(tot2, on='SEQN', how='left')
adult1 = p[(p.DR1DRSTZ == 1) & (p.WTDRD1 > 0) & (p.RIDAGEYR >= 20)].copy()
adult2 = p[(p.DR1DRSTZ == 1) & (p.DR2DRSTZ == 1) & (p.WTDR2D > 0) & (p.RIDAGEYR >= 20)].copy()
print(f'day-1 adult sample: {len(adult1)}, two-day adult sample: {len(adult2)}')

# ---------- zero vegetables ----------
def veg_flags(iff, codevar):
    f = iff.copy()
    f['veg_any'] = (f[codevar] // 1e7).astype(int) == 7            # incl. all white potatoes
    f['veg_nopot'] = f.veg_any & ((f[codevar] // 1e6).astype(int) != 71)  # excl. white potatoes
    return f.groupby('SEQN')[['veg_any', 'veg_nopot']].any()

v1 = veg_flags(iff1, 'DR1IFDCD')
v2 = veg_flags(iff2, 'DR2IFDCD')

a1 = adult1.merge(v1, on='SEQN', how='left').fillna({'veg_any': False, 'veg_nopot': False})
for var, lab in [('veg_any', 'any veg incl. potatoes/fries'), ('veg_nopot', 'veg excl. white potatoes')]:
    print(f'DAY 1, adults 20+: zero [{lab}]: {np.average(~a1[var], weights=a1.WTDRD1):.1%}')

both = adult2.merge(v1.add_suffix('_d1'), on='SEQN', how='left') \
             .merge(v2.add_suffix('_d2'), on='SEQN', how='left').fillna(False)
for var, lab in [('veg_any', 'any veg incl. potatoes/fries'), ('veg_nopot', 'veg excl. white potatoes')]:
    zero = ~(both[f'{var}_d1'] | both[f'{var}_d2'])
    print(f'BOTH DAYS, adults 20+: zero [{lab}]: {np.average(zero, weights=both.WTDR2D):.1%}')

# ---------- top WWEIA categories, day 1, adults ----------
f1 = iff1.merge(adult1[['SEQN', 'WTDRD1']], on='SEQN')
f1 = f1.merge(cat, left_on='DR1IFDCD', right_on='fdcd', how='left')
f1['wweia'] = f1.wweia.fillna('UNMATCHED')
f1['wkcal'] = f1.DR1IKCAL * f1.WTDRD1
f1['wgrms'] = f1.DR1IGRMS * f1.WTDRD1

for col, lab in [('wkcal', 'CALORIES'), ('wgrms', 'GRAM WEIGHT')]:
    tab = f1.groupby('wweia')[col].sum().sort_values(ascending=False)
    shares = tab / tab.sum()
    print(f'\nTOP 15 WWEIA CATEGORIES BY {lab} (share of total), adults day 1:')
    for name, s in shares.head(15).items():
        print(f'  {s:6.1%}  {name}')

veg_kcal = f1[(f1.DR1IFDCD // 1e7).astype(int) == 7].wkcal.sum() / f1.wkcal.sum()
nopot = f1[((f1.DR1IFDCD // 1e7).astype(int) == 7)
           & ((f1.DR1IFDCD // 1e6).astype(int) != 71)].wkcal.sum() / f1.wkcal.sum()
print(f'\nVegetables incl. potatoes = {veg_kcal:.1%} of adult calories; '
      f'excl. white potatoes = {nopot:.1%}')
