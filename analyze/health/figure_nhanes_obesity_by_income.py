"""
figure_nhanes_obesity_by_income.py -- measured adult obesity by family income, from NHANES microdata,
for notes/obesity.tex. No Nielsen data.

  figs/health/obesity/nhanes_obesity_by_income.png
      Percent of adults aged 20+ with measured BMI >= 30 (BMXBMI from the body-measures file BMX),
      by survey cycle 1999-2000 through Aug 2021-Aug 2023, plotted at the cycle midpoint; the
      2017-March 2020 pre-pandemic file (P_DEMO, P_BMX) is its own point at 2018.5 (hollow marker)
      and overlaps 2017-18, which is also drawn. One line for all adults and one per family
      income-to-poverty ratio group (INDFMPIR: below 1.30; 1.30 to 3.49; 3.50 and above; adults
      with INDFMPIR missing are in the all-adults line only).

Definitions (NCHS Data Brief 360 conventions):
  * sample = examined adults 20+ with a BMI value, pregnant women (RIDEXPRG == 1) excluded,
    MEC exam weight > 0; weights WTMEC2YR (WTMECPRP for the pre-pandemic file)
  * age-adjusted by direct standardization to the 2000 US standard population with the three NCHS
    age groups 20-39, 40-59, 60+ and weights 0.3965, 0.3718, 0.2317
Raw .xpt files live in nielsen_data/raw/nhanes/ and are downloaded from CDC if missing.
Prints the age-adjusted and crude percentages and the unweighted n per cycle x group.
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
FIG = fdir / 'health' / 'obesity'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#999999'
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10.5,
                     'axes.spines.top': False, 'axes.spines.right': False})

# (label, cycle midpoint, start year of the download folder, DEMO file, BMX file, MEC weight)
CYCLES = [
    ('1999-2000',           1999.5, 1999, 'DEMO',   'BMX',   'WTMEC2YR'),
    ('2001-2002',           2001.5, 2001, 'DEMO_B', 'BMX_B', 'WTMEC2YR'),
    ('2003-2004',           2003.5, 2003, 'DEMO_C', 'BMX_C', 'WTMEC2YR'),
    ('2005-2006',           2005.5, 2005, 'DEMO_D', 'BMX_D', 'WTMEC2YR'),
    ('2007-2008',           2007.5, 2007, 'DEMO_E', 'BMX_E', 'WTMEC2YR'),
    ('2009-2010',           2009.5, 2009, 'DEMO_F', 'BMX_F', 'WTMEC2YR'),
    ('2011-2012',           2011.5, 2011, 'DEMO_G', 'BMX_G', 'WTMEC2YR'),
    ('2013-2014',           2013.5, 2013, 'DEMO_H', 'BMX_H', 'WTMEC2YR'),
    ('2015-2016',           2015.5, 2015, 'DEMO_I', 'BMX_I', 'WTMEC2YR'),
    ('2017-2018',           2017.5, 2017, 'DEMO_J', 'BMX_J', 'WTMEC2YR'),
    ('2017-Mar 2020 (pre-pandemic)', 2018.5, 2017, 'P_DEMO', 'P_BMX', 'WTMECPRP'),
    ('Aug 2021-Aug 2023',   2022.5, 2021, 'DEMO_L', 'BMX_L', 'WTMEC2YR'),
]
PREPANDEMIC_MID = 2018.5
# 2000 US standard population, NCHS age groups 20-39 / 40-59 / 60+ (Data Brief 360)
AGE_BINS = [20, 40, 60, np.inf]
STD_W = np.array([0.3965, 0.3718, 0.2317])
GROUPS = [('All adults', None), ('Below 1.30', (0, 1.30)), ('1.30 to 3.49', (1.30, 3.50)), ('3.50 and above', (3.50, np.inf))]


def fetch(name, year):
    """Return the local path of NHANES file <name>.xpt, downloading it from CDC if missing."""
    path = RAW / f'{name}.xpt'
    if not path.exists():
        url = f'https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/{year}/DataFiles/{name}.xpt'
        print(f'downloading {url}')
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        path.write_bytes(urllib.request.urlopen(req).read())
    return path


def load_cycle(label, year, demo_file, bmx_file, wvar):
    demo = pd.read_sas(fetch(demo_file, year), format='xport')[['SEQN', 'RIDAGEYR', 'RIDEXPRG', 'INDFMPIR', wvar]]
    bmx = pd.read_sas(fetch(bmx_file, year), format='xport')[['SEQN', 'BMXBMI']]
    d = demo.merge(bmx, on='SEQN', how='inner').rename(columns={wvar: 'w'})
    d = d[(d.RIDAGEYR >= 20) & d.BMXBMI.notna() & (d.RIDEXPRG != 1) & (d.w > 0)].copy()
    d['obese'] = (d.BMXBMI >= 30).astype(float)
    d['agegrp'] = pd.cut(d.RIDAGEYR, AGE_BINS, right=False, labels=False)
    d['cycle'] = label
    return d


def age_adjusted(d):
    """Direct standardization: weighted obesity share within each age group, combined with STD_W."""
    rates = []
    for a in range(3):
        s = d[d.agegrp == a]
        rates.append(np.average(s.obese, weights=s.w) if len(s) else np.nan)
    return 100 * float(np.dot(STD_W, rates))


def crude(d):
    return 100 * float(np.average(d.obese, weights=d.w))


rows = []
for label, mid, year, demo_file, bmx_file, wvar in CYCLES:
    d = load_cycle(label, year, demo_file, bmx_file, wvar)
    for gname, rng in GROUPS:
        s = d if rng is None else d[(d.INDFMPIR >= rng[0]) & (d.INDFMPIR < rng[1])]
        rows.append({'cycle': label, 'mid': mid, 'group': gname, 'adj': age_adjusted(s), 'crude': crude(s), 'n': len(s),
                     'n_pir_missing': int(d.INDFMPIR.isna().sum()) if rng is None else np.nan})
res = pd.DataFrame(rows)

pd.set_option('display.width', 200)
print('\nNHANES adults 20+, measured BMI >= 30, by family income-to-poverty ratio (INDFMPIR)')
print('age-adjusted percent (2000 standard population, 20-39/40-59/60+):')
print(res.pivot(index='cycle', columns='group', values='adj').loc[[c[0] for c in CYCLES]].round(1).to_string())
print('\ncrude weighted percent:')
print(res.pivot(index='cycle', columns='group', values='crude').loc[[c[0] for c in CYCLES]].round(1).to_string())
print('\nunweighted n per cell (all adults column includes those with INDFMPIR missing):')
print(res.pivot(index='cycle', columns='group', values='n').loc[[c[0] for c in CYCLES]].astype(int).to_string())
print('\nadults with INDFMPIR missing, by cycle:')
print(res[res.group == 'All adults'].set_index('cycle')['n_pir_missing'].astype(int).to_string())

# ---------- figure ----------
STYLE = {'All adults': (GREY, 'D', '--'), 'Below 1.30': (RED, 'o', '-'), '1.30 to 3.49': (ORANGE, 's', '-'),
         '3.50 and above': (BLUE, '^', '-')}
LABELS = {'All adults': 'All adults', 'Below 1.30': 'Income below 1.30 x poverty line',
          '1.30 to 3.49': 'Income 1.30 to 3.49 x poverty line', '3.50 and above': 'Income 3.50 x poverty line and above'}
fig, ax = plt.subplots(figsize=(10, 6))
for gname, _ in GROUPS:
    s = res[res.group == gname].sort_values('mid')
    color, marker, ls = STYLE[gname]
    ax.plot(s.mid, s.adj, color=color, lw=2.5, ls=ls, marker=marker, ms=7, mfc=color, label=LABELS[gname], zorder=3)
    pre = s[s.mid == PREPANDEMIC_MID]
    ax.plot(pre.mid, pre.adj, color=color, marker=marker, ms=7, mfc='white', mew=2, ls='none', zorder=4)
ax.set_ylabel('Percent of adults 20+ with BMI ≥ 30 (age-adjusted)')
ax.set_xlabel('Survey cycle midpoint')
ax.text(0.02, 0.03, 'Hollow marker: 2017–March 2020 pre-pandemic file', transform=ax.transAxes, fontsize=10.5, color='#555555')
ax.set_ylim(10, 50)
ax.set_xticks(range(2000, 2024, 2))
ax.set_title('NHANES: measured obesity among adults 20+, by family income relative to the poverty line')
ax.legend(loc='upper left', frameon=False)
out = FIG / 'nhanes_obesity_by_income.png'
fig.savefig(out, bbox_inches='tight', dpi=150)
plt.close(fig)
print(f'\nsaved {out}')
