"""
figure_nhanes_calories.py -- daily energy intake of adults from the NHANES day-1 dietary recall,
in total and from foods obtained at a store, for notes/obesity.tex. No Nielsen data.

  figs/health/diet/calories_nhanes.png  (two panels)
      left  : mean daily energy intake (kcal, DR1TKCAL; DRXTKCAL in 1999-2002) of adults 20+ with a
              reliable day-1 recall, by survey cycle 1999-2000 through Aug 2021-Aug 2023, plotted at
              the cycle midpoint; the 2017-March 2020 pre-pandemic file is its own point at 2018.5
              (hollow marker) and overlaps 2017-18, which is also drawn
      right : the same, counting only the calories of items whose reported source (DR1FS in the
              item file DR1IFF) is a store: code 1 "Store - grocery/supermarket" plus, from 2011-12
              on, 27 "Store - convenience type" and 28 "Store - no additional info" (items with the
              source missing count as not from a store). Store kcal are summed per respondent, zero
              for respondents with no store item. The 1999-2000 and 2001-2002 item files (DRXIFF)
              have no source-of-food variable (DRD040 is the place the food was eaten, DRD040Z is
              "eaten at home?"), so the right panel starts at 2003-04.
      One line for all adults and one per family income-to-poverty ratio group (INDFMPIR: below
      1.30; 1.30 to 3.49; 3.50 and above; adults with INDFMPIR missing are in the all-adults line only).

Definitions:
  * reliable recall = DR1DRSTZ == 1 (DRDDRSTZ in 2001-02, DRDDRSTS in 1999-2000); weights WTDRD1
    (WTDRD1PP in the pre-pandemic file), weight > 0; no age adjustment
  * also printed: store kcal per "adult equivalent" for ALL ages = each person's store kcal divided by
    (USDA calorie need for the person's age and sex / 2000), the same calorie_need table as
    build/health/build_nova_calorie_panel.py, for comparison with the Nielsen series
Raw .xpt files live in nielsen_data/raw/nhanes/ and are downloaded from CDC if missing.
Prints every series with the unweighted n per cell and the distinct DR1FS codes in each cycle.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import urllib.request
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

RAW = rdir / 'nhanes'
FIG = fdir / 'health' / 'diet'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#999999'
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10.5,
                     'axes.spines.top': False, 'axes.spines.right': False})

# (label, midpoint, download year, DEMO, totals file, items file, recall-status var, day-1 weight, total kcal,
#  item kcal, item source var or None)
CYCLES = [
    ('1999-2000',           1999.5, 1999, 'DEMO',   'DRXTOT',   'DRXIFF',   'DRDDRSTS', 'WTDRD1',   'DRXTKCAL', 'DRXIKCAL', None),
    ('2001-2002',           2001.5, 2001, 'DEMO_B', 'DRXTOT_B', 'DRXIFF_B', 'DRDDRSTZ', 'WTDRD1',   'DRXTKCAL', 'DRXIKCAL', None),
    ('2003-2004',           2003.5, 2003, 'DEMO_C', 'DR1TOT_C', 'DR1IFF_C', 'DR1DRSTZ', 'WTDRD1',   'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
    ('2005-2006',           2005.5, 2005, 'DEMO_D', 'DR1TOT_D', 'DR1IFF_D', 'DR1DRSTZ', 'WTDRD1',   'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
    ('2007-2008',           2007.5, 2007, 'DEMO_E', 'DR1TOT_E', 'DR1IFF_E', 'DR1DRSTZ', 'WTDRD1',   'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
    ('2009-2010',           2009.5, 2009, 'DEMO_F', 'DR1TOT_F', 'DR1IFF_F', 'DR1DRSTZ', 'WTDRD1',   'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
    ('2011-2012',           2011.5, 2011, 'DEMO_G', 'DR1TOT_G', 'DR1IFF_G', 'DR1DRSTZ', 'WTDRD1',   'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
    ('2013-2014',           2013.5, 2013, 'DEMO_H', 'DR1TOT_H', 'DR1IFF_H', 'DR1DRSTZ', 'WTDRD1',   'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
    ('2015-2016',           2015.5, 2015, 'DEMO_I', 'DR1TOT_I', 'DR1IFF_I', 'DR1DRSTZ', 'WTDRD1',   'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
    ('2017-2018',           2017.5, 2017, 'DEMO_J', 'DR1TOT_J', 'DR1IFF_J', 'DR1DRSTZ', 'WTDRD1',   'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
    ('2017-Mar 2020 (pre-pandemic)', 2018.5, 2017, 'P_DEMO', 'P_DR1TOT', 'P_DR1IFF', 'DR1DRSTZ', 'WTDRD1PP', 'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
    ('Aug 2021-Aug 2023',   2022.5, 2021, 'DEMO_L', 'DR1TOT_L', 'DR1IFF_L', 'DR1DRSTZ', 'WTDRD1',   'DR1TKCAL', 'DR1IKCAL', 'DR1FS'),
]
PREPANDEMIC_MID = 2018.5
STORE_CODES = [1, 27, 28]   # Store - grocery/supermarket; Store - convenience type; Store - no additional info
GROUPS = [('All adults', None), ('Below 1.30', (0, 1.30)), ('1.30 to 3.49', (1.30, 3.50)), ('3.50 and above', (3.50, np.inf))]


def calorie_need(age, male):
    """USDA DGA estimated daily calorie need, moderately active (midpoints); copied from build_nova_calorie_panel.py."""
    bins = [4, 9, 14, 19, 31, 51, np.inf]
    male_need = [1200, 1500, 2000, 2600, 2700, 2500, 2300]
    female_need = [1200, 1400, 1800, 2000, 2100, 2000, 1800]
    idx = np.searchsorted(bins, age, side='right')
    return np.where(male, np.take(male_need, idx), np.take(female_need, idx))


def fetch(name, year):
    """Return the local path of NHANES file <name>.xpt, downloading it from CDC if missing."""
    path = RAW / f'{name}.xpt'
    if not path.exists():
        url = f'https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/{year}/DataFiles/{name}.xpt'
        print(f'downloading {url}')
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        path.write_bytes(urllib.request.urlopen(req).read())
    return path


def wmean(x, w):
    return float(np.average(x, weights=w)) if len(x) else np.nan


def load_cycle(label, year, demo_f, tot_f, iff_f, statvar, wvar, kcalvar, ikcalvar, srcvar):
    demo = pd.read_sas(fetch(demo_f, year), format='xport')[['SEQN', 'RIDAGEYR', 'RIAGENDR', 'INDFMPIR']]
    tot = pd.read_sas(fetch(tot_f, year), format='xport')[['SEQN', statvar, wvar, kcalvar]]
    tot = tot.rename(columns={statvar: 'status', wvar: 'w', kcalvar: 'kcal'})
    d = demo.merge(tot, on='SEQN', how='inner')
    d = d[(d.status == 1) & (d.w > 0) & d.kcal.notna()].copy()
    d['cycle'] = label
    d['male'] = d.RIAGENDR == 1
    d['ae'] = calorie_need(d.RIDAGEYR.values, d.male.values) / 2000.0
    if srcvar is None:
        d['store_kcal'] = np.nan
        d['kcal_src_missing'] = np.nan
        codes = []
    else:
        iff = pd.read_sas(fetch(iff_f, year), format='xport')[['SEQN', srcvar, ikcalvar]]
        iff = iff[iff.SEQN.isin(d.SEQN)]
        codes = sorted(iff[srcvar].dropna().unique().astype(int).tolist())
        store = iff[iff[srcvar].isin(STORE_CODES)].groupby('SEQN')[ikcalvar].sum().rename('store_kcal')
        miss = iff[iff[srcvar].isna()].groupby('SEQN')[ikcalvar].sum().rename('kcal_src_missing')
        d = d.merge(store, on='SEQN', how='left').merge(miss, on='SEQN', how='left')
        d['store_kcal'] = d.store_kcal.fillna(0.0)
        d['kcal_src_missing'] = d.kcal_src_missing.fillna(0.0)
    return d, codes


rows, allages = [], []
for label, mid, year, demo_f, tot_f, iff_f, statvar, wvar, kcalvar, ikcalvar, srcvar in CYCLES:
    d, codes = load_cycle(label, year, demo_f, tot_f, iff_f, statvar, wvar, kcalvar, ikcalvar, srcvar)
    print(f'{label}: {len(d):,} reliable day-1 recalls (all ages); {srcvar or "no source variable"}; codes present: {codes}')
    a = d[d.RIDAGEYR >= 20]
    for gname, rng in GROUPS:
        s = a if rng is None else a[(a.INDFMPIR >= rng[0]) & (a.INDFMPIR < rng[1])]
        rows.append({'cycle': label, 'mid': mid, 'group': gname, 'n': len(s),
                     'kcal': wmean(s.kcal, s.w),
                     'store_kcal': wmean(s.store_kcal, s.w) if srcvar else np.nan,
                     'store_share': wmean(s.store_kcal, s.w) / wmean(s.kcal, s.w) if srcvar else np.nan,
                     'src_missing_share': wmean(s.kcal_src_missing, s.w) / wmean(s.kcal, s.w) if srcvar else np.nan})
    allages.append({'cycle': label, 'n_all_ages': len(d),
                    'kcal_per_ae': wmean(d.kcal / d.ae, d.w),
                    'store_kcal_per_ae': wmean(d.store_kcal / d.ae, d.w) if srcvar else np.nan,
                    'store_kcal_per_ae_ratio_of_means': wmean(d.store_kcal, d.w) / wmean(d.ae, d.w) if srcvar else np.nan})
res = pd.DataFrame(rows)
order = [c[0] for c in CYCLES]

pd.set_option('display.width', 200)
print('\nNHANES day-1 recall, adults 20+ with a reliable recall, by family income-to-poverty ratio (INDFMPIR)')
print('mean total energy intake, kcal/day:')
print(res.pivot(index='cycle', columns='group', values='kcal').loc[order].round(0).to_string())
print('\nmean kcal/day from foods obtained at a store (DR1FS in 1, 27, 28):')
print(res.pivot(index='cycle', columns='group', values='store_kcal').loc[order].round(0).to_string())
print('\nstore share of calories:')
print(res.pivot(index='cycle', columns='group', values='store_share').loc[order].round(3).to_string())
print('\nshare of calories from items with the source missing (counted as not from a store):')
print(res.pivot(index='cycle', columns='group', values='src_missing_share').loc[order].round(3).to_string())
print('\nunweighted n per cell (all adults column includes those with INDFMPIR missing):')
print(res.pivot(index='cycle', columns='group', values='n').loc[order].astype(int).to_string())
print('\nALL AGES with a reliable recall: kcal per adult equivalent (calorie need / 2000), weighted mean of the person ratio;')
print('the last column is weighted mean store kcal / weighted mean adult equivalents:')
print(pd.DataFrame(allages).set_index('cycle').round(0).to_string())

# ---------- figure ----------
STYLE = {'All adults': (GREY, 'D', '--'), 'Below 1.30': (RED, 'o', '-'), '1.30 to 3.49': (ORANGE, 's', '-'),
         '3.50 and above': (BLUE, '^', '-')}
LABELS = {'All adults': 'All adults', 'Below 1.30': 'Income below 1.30 x poverty line',
          '1.30 to 3.49': 'Income 1.30 to 3.49 x poverty line', '3.50 and above': 'Income 3.50 x poverty line and above'}
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
for ax, var, title in [(axes[0], 'kcal', 'Total energy intake, adults 20+'),
                       (axes[1], 'store_kcal', 'Calories from foods obtained at a store, adults 20+')]:
    for gname, _ in GROUPS:
        s = res[(res.group == gname) & res[var].notna()].sort_values('mid')
        color, marker, ls = STYLE[gname]
        ax.plot(s.mid, s[var], color=color, lw=2.5, ls=ls, marker=marker, ms=7, mfc=color, label=LABELS[gname], zorder=3)
        pre = s[s.mid == PREPANDEMIC_MID]
        ax.plot(pre.mid, pre[var], color=color, marker=marker, ms=7, mfc='white', mew=2, ls='none', zorder=4)
    ax.set_title(title)
    ax.set_ylim(0, None)
    ax.set_xlim(1998.5, 2023.5)
    ax.set_xticks(range(2000, 2024, 2))
    ax.set_ylabel('kcal per person per day (day-1 recall)')
    ax.set_xlabel('Survey cycle midpoint')
axes[0].legend(loc='lower left', frameon=False)
axes[0].text(0.02, 0.30, 'Hollow marker: 2017–March 2020 pre-pandemic file', transform=axes[0].transAxes, fontsize=10.5, color='#555555')
axes[1].text(0.02, 0.07, 'Hollow marker: 2017–March 2020 pre-pandemic file', transform=axes[1].transAxes, fontsize=10.5, color='#555555')
axes[1].text(0.02, 0.02, 'No source-of-food variable in the 1999-2002 item files', transform=axes[1].transAxes, fontsize=10.5, color='#555555')
fig.tight_layout()
out = FIG / 'calories_nhanes.png'
fig.savefig(out, bbox_inches='tight', dpi=150)
plt.close(fig)
print(f'\nsaved {out}')
