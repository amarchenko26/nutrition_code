"""
build_acs_cell_iv.py  --  run LOCALLY

ACS income of the household's group: national mean household income by year x household size x schooling x occupation,
the groups of the Nielsen instrument (build_iv.py) built from ACS microdata. Rebuilt 2026-11-13 (Anya) so that the ACS groups
follow the Nielsen rules:
  - heads: the householder and a spouse or unmarried partner (Nielsen's two heads are whoever the household names)
  - occupation: the male head's, unless there is no male head or he is in the armed forces, then the female head's
    (build_iv.bin_occupation); a household whose occupation person does not work in the survey week (EMPSTAT 2 or 3) is in
    no working group. Occupations go to the bin of the Nielsen code that lists the job (Consumer Panel Dataset Manual 2024,
    pp. 48-49), see occ2010_to_bin
  - schooling: mean years of the heads, Nielsen's years per level (clean_panelist.EDUC_YEARS), EDUC_BINS of build_iv.py;
    size: NUMPREC, SIZE_BINS of build_iv.py
  - income: HHINCOME x ADJUST (the Census factor that puts income for the 12 months before the interview in survey-year
    dollars), in 2013 dollars with the CPI of build_iv.py, $1,000s
  - group mean weighted by HHWT, groups with MIN_CELL_N or more ACS households
Until 2026-11-13: extract usa_00004 (ACS 2005-2020), head and spouse only, no employment status, the male's occupation
whether or not he worked, current dollars, and the household's group in each year (Table 9 row 'Group income from the ACS').

Input: raw/acs/usa_00005.csv.gz (IPUMS extract 5, annual ACS 2004-2020, build/iv/download_ipums_acs.py)
Output: interim/panel_dataset/iv_income_acs.parquet / .dta, one row per Nielsen household-year:
  d_iv_acs_t1    the household keeps its group of t-1: ACS income of that group at t minus at t-1 (2013 $1,000s); for
                 households in a working group at t-1 (white, blue, service), i.e. the clean sample. Table 9 row 'acs'
                 (analyze/what_moves/build_iv_robustness.do), clustered on the group
  acs_grp_t1     that group, 1-48 (occupation x schooling x size)
  iv_income_acs  ACS income of the household's group in the same year (2013 $1,000s; working groups only; read by
                 analyze/labor_supply/20_, which the labor-supply note does not use)
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd

from build_iv import bin_occupation, SIZE_BINS, SIZE_LABELS, EDUC_BINS, EDUC_LABELS, CPI, CPI_BASE, PANELISTS

ACS_CSV = rdir / 'acs' / 'usa_00005.csv.gz'
OUT = idir / 'panel_dataset' / 'iv_income_acs.parquet'
MIN_CELL_N = 100
GQ_HH = [1, 2, 5]                                   # households (3, 4 = group quarters)
OCC_ORDER = ['white', 'blue', 'service']
CELL = ['occ_bin', 'educ_bin', 'size_bin']
USECOLS = ['YEAR', 'SERIAL', 'PERNUM', 'HHWT', 'GQ', 'RELATE', 'RELATED', 'SEX', 'NUMPREC', 'OCC2010', 'EDUCD',
           'HHINCOME', 'EMPSTAT', 'EMPSTATD', 'ADJUST']


def occ2010_to_bin(occ):
    """OCC2010 -> the bin of the Nielsen code that lists the job: white = 1-4 (professional, manager or owner, clerical,
    sales), blue = 5, 6, 11 (craftsman or foreman, operative or driver, laborer), service = 8, 9 (service worker, farmer).
    Military (9800-9830) has no bin."""
    o = pd.to_numeric(occ, errors='coerce')
    out = pd.Series(np.nan, index=o.index, dtype=object)
    out[o.between(10, 3540) | o.between(4700, 5940)] = 'white'      # management, business, science, arts, health practitioners; sales, office
    out[o.between(3600, 4650) | o.between(6005, 6050)] = 'service'  # health support, protective, food, cleaning, personal care; farming
    out[o.between(6100, 6130) | o.between(6200, 7630) | o.between(7700, 9750)] = 'blue'   # fishing, forestry; construction, repair; production, transport
    out[o.isin([205, 3310, 3500, 9720])] = 'service'   # farmers and farm managers (9); dental hygienists, practical nurses (8); garbage collectors (8, 'sanitation')
    out[o.isin([4210, 4240, 4250, 5610, 1550, 1560, 2900]) | o.between(1900, 1965)] = 'blue'   # landscaping supervisors (5, 'foreman'), pest
    # control (5, 'exterminator'), grounds workers (11, 'gardener'), shipping and receiving clerks (11), technicians except medical (5)
    out[o.isin([9030, 9360])] = 'white'                # pilots (1), gas station attendants (3)
    return out


def educd_to_years(educd):
    """EDUCD -> Nielsen's years per level: grade school 6, some high school 10, diploma 12, some college 14, bachelor's 16,
    graduate 18."""
    e = pd.to_numeric(educd, errors='coerce')
    yrs = pd.Series(np.nan, index=e.index)
    yrs[e.between(2, 26)] = 6        # no schooling through grade 8
    yrs[e.between(30, 61)] = 10      # grade 9 through 12th grade, no diploma
    yrs[e.between(62, 64)] = 12      # diploma or GED
    yrs[e.between(65, 100)] = 14     # some college, associate's
    yrs[e == 101] = 16               # bachelor's
    yrs[e.between(110, 116)] = 18    # graduate degree
    return yrs


def acs_households():
    """One row per ACS household with its working group (or none) and income in 2013 $1,000s."""
    parts = []
    for c in pd.read_csv(ACS_CSV, usecols=USECOLS, chunksize=4_000_000):
        parts.append(c[c['GQ'].isin(GQ_HH) & ((c['RELATE'] <= 2) | (c['RELATED'] == 1114))])
    h = pd.concat(parts, ignore_index=True)
    key = ['YEAR', 'SERIAL']
    print(f"  heads: {len(h):,} householders, spouses and unmarried partners; partners {(h['RELATED'] == 1114).sum():,}")

    # each head's bin: the occupation if working, not_emp if not working, none if in the armed forces
    mil = (h['EMPSTAT'] == 1) & (h['OCC2010'].between(9800, 9830) | h['EMPSTATD'].between(13, 15))
    h['pbin'] = pd.Series(np.where(h['EMPSTAT'].isin([2, 3]), 'not_emp', None), index=h.index, dtype=object)
    work = (h['EMPSTAT'] == 1) & ~mil
    h.loc[work, 'pbin'] = occ2010_to_bin(h.loc[work, 'OCC2010'])
    h['educ_yrs'] = educd_to_years(h['EDUCD'])

    # the male head (the householder if male, else the first male spouse or partner); the female head likewise
    h['nonhh'] = (h['RELATE'] != 1).astype(int)
    h = h.sort_values(key + ['nonhh', 'PERNUM'])
    hh = h[h['RELATE'] == 1].drop_duplicates(key)[key + ['HHWT', 'NUMPREC', 'HHINCOME', 'ADJUST']].set_index(key)
    male = h[h['SEX'] == 1].drop_duplicates(key).set_index(key)['pbin']
    female = h[h['SEX'] == 2].drop_duplicates(key).set_index(key)['pbin']
    hh['occ_bin'] = male.reindex(hh.index)
    hh['occ_bin'] = hh['occ_bin'].where(hh['occ_bin'].notna(), female.reindex(hh.index))   # build_iv.bin_occupation
    hh['educ_mean'] = h.groupby(key)['educ_yrs'].mean().reindex(hh.index)
    hh = hh.reset_index()
    print('  ACS households by year:', hh.groupby('YEAR').size().to_dict())
    print('  ADJUST by year (min, max):', hh.groupby('YEAR')['ADJUST'].agg(['min', 'max']).round(4).apply(tuple, axis=1).to_dict())
    print('  occupation group shares (weighted):',
          (hh.groupby(hh['occ_bin'].fillna('none'))['HHWT'].sum() / hh['HHWT'].sum()).round(3).to_dict())

    hh = hh[hh['HHINCOME'].between(-20000, 9999998)]   # 9999999 = N/A
    hh['inc'] = hh['HHINCOME'] * hh['ADJUST'] * CPI_BASE / hh['YEAR'].map(CPI) / 1000
    hh['size_bin'] = pd.cut(hh['NUMPREC'], bins=SIZE_BINS, labels=SIZE_LABELS).astype(str)
    hh['educ_bin'] = pd.cut(hh['educ_mean'], bins=EDUC_BINS, labels=EDUC_LABELS).astype(str)
    return hh[hh['occ_bin'].isin(OCC_ORDER) & hh['educ_bin'].isin(EDUC_LABELS) & hh['size_bin'].isin(SIZE_LABELS)
              & hh['inc'].notna() & (hh['HHWT'] > 0)]


def acs_groups(hh):
    hh = hh.assign(wi=hh['HHWT'] * hh['inc'])
    g = (hh.groupby(['YEAR'] + CELL).agg(wi=('wi', 'sum'), w=('HHWT', 'sum'), n=('HHWT', 'size')).reset_index())
    g['acs_income'] = g['wi'] / g['w']
    print(f"  group-years: {len(g):,}; ACS households per group-year: median {g['n'].median():,.0f}, "
          f"10th percentile {g['n'].quantile(0.1):,.0f}, smallest {g['n'].min():,}; below {MIN_CELL_N}: {(g['n'] < MIN_CELL_N).sum()}")
    print('  smallest group by year:', g.groupby('YEAR')['n'].min().to_dict())
    g = g[g['n'] >= MIN_CELL_N].copy()
    # year-to-year change of each group, net of the national change
    g = g.sort_values(CELL + ['YEAR'])
    g['d'] = g.groupby(CELL)['acs_income'].diff().where(g.groupby(CELL)['YEAR'].diff() == 1)
    g['d_net'] = g['d'] - g.groupby('YEAR')['d'].transform('mean')
    g['d_net_next'] = g.groupby(CELL)['d_net'].shift(-1)
    print(f"  group change net of the national change ($1,000s 2013): SD {g['d_net'].std():.2f}; "
          f"correlation with the next year's {g[['d_net', 'd_net_next']].corr().iloc[0, 1]:.2f}")
    g['acs_grp'] = (g['occ_bin'].map({o: i for i, o in enumerate(OCC_ORDER)}) * 16
                    + g['educ_bin'].map({e: i for i, e in enumerate(EDUC_LABELS)}) * 4
                    + g['size_bin'].map({s: i for i, s in enumerate(SIZE_LABELS)}) + 1).astype(int)
    return g[['YEAR'] + CELL + ['acs_grp', 'acs_income', 'n']]


def main():
    print('ACS households and groups...')
    g = acs_groups(acs_households())

    print('Nielsen households...')
    pan = pd.read_parquet(PANELISTS, columns=['household_code', 'panel_year', 'household_size', 'hh_avg_yrsofschool',
                                              'male_head_occupation', 'female_head_occupation'])
    pan['occ_bin'] = bin_occupation(pan)
    pan = pan[pan['occ_bin'].isin(OCC_ORDER)].copy()
    pan['size_bin'] = pd.cut(pd.to_numeric(pan['household_size'], errors='coerce'), bins=SIZE_BINS, labels=SIZE_LABELS).astype(str)
    pan['educ_bin'] = pd.cut(pan['hh_avg_yrsofschool'], bins=EDUC_BINS, labels=EDUC_LABELS).astype(str)
    pan = pan[['household_code', 'panel_year'] + CELL]

    gy = g.rename(columns={'YEAR': 'panel_year'})
    lev = (pan.merge(gy[['panel_year'] + CELL + ['acs_income']], on=['panel_year'] + CELL, how='inner')
              .rename(columns={'acs_income': 'iv_income_acs'})[['household_code', 'panel_year', 'iv_income_acs']])
    # the group of t-1 at t-1 and at t
    lag = pan.merge(gy[['panel_year'] + CELL + ['acs_grp', 'acs_income']], on=['panel_year'] + CELL, how='inner')
    nxt = gy.assign(panel_year=gy['panel_year'] - 1)[['panel_year'] + CELL + ['acs_income']].rename(columns={'acs_income': 'inc_next'})
    lag = lag.merge(nxt, on=['panel_year'] + CELL, how='inner')
    lag['d_iv_acs_t1'] = lag['inc_next'] - lag['acs_income']
    lag['panel_year'] = lag['panel_year'] + 1
    lag = lag.rename(columns={'acs_grp': 'acs_grp_t1'})[['household_code', 'panel_year', 'd_iv_acs_t1', 'acs_grp_t1']]

    out = lev.merge(lag, on=['household_code', 'panel_year'], how='outer').sort_values(['household_code', 'panel_year'])
    out['acs_grp_t1'] = out['acs_grp_t1'].astype('Int64')
    assert not out.duplicated(['household_code', 'panel_year']).any()
    print(f"  household-years: {len(out):,}; with iv_income_acs {out['iv_income_acs'].notna().sum():,}, "
          f"with d_iv_acs_t1 {out['d_iv_acs_t1'].notna().sum():,} (years {int(lag['panel_year'].min())}-{int(lag['panel_year'].max())})")
    print(f"  d_iv_acs_t1 ($1,000s 2013): mean {out['d_iv_acs_t1'].mean():.2f}, SD {out['d_iv_acs_t1'].std():.2f}")
    # against the Nielsen change of the same t-1 group (build_iv_predicted.py, leave-one-county-out)
    pr = idir / 'panel_dataset' / 'iv_predicted.parquet'
    if pr.exists():
        c = out.merge(pd.read_parquet(pr, columns=['household_code', 'panel_year', 'd_iv_frozen']), on=['household_code', 'panel_year'])
        print(f"  correlation with the Nielsen change of the t-1 group (d_iv_frozen): {c[['d_iv_acs_t1', 'd_iv_frozen']].corr().iloc[0, 1]:.3f} "
              f"on {c[['d_iv_acs_t1', 'd_iv_frozen']].dropna().shape[0]:,} household-years")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    out.astype({'acs_grp_t1': 'float64'}).to_stata(str(OUT.with_suffix('.dta')), write_index=False)
    print(f'Saved: {OUT} and .dta')


if __name__ == '__main__':
    main()
