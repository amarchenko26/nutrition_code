"""NHANES dietary trend figures for the talk (motivation + eating-out appendix slides).

Rebuilds (reproducibly) the four NHANES trend figures that were previously made by a
one-off script, restyled and extended through the 2021-2023 cycle:

  figs/nhanes/pizza_chips_trend.png                (motivation slide, left)
  figs/nhanes/fruits_vegetables_trend.png          (motivation slide, right)
  figs/nhanes/soda_trend.png                       (follow-up slide, left)
  figs/nhanes/frozen_meals_trend.png               (follow-up slide, right)
  figs/nhanes/pizza_burgers_sandwiches_trend.png   (broad composite, kept for reference)
  figs/nhanes/convenience_mains_trend.png          (pizza+burgers+pasta, kept for reference)
  figs/nhanes/eating_out_trend.png                 (appendix: calories away from home)
  figs/nhanes/eating_out_by_income_trend.png       (appendix: eating out by income quartile)

Series (adults 20+, reliable day-1 recalls, survey-weighted, share of total calories):
  1) "Pizza, burgers & sandwiches": FNDDS food codes 27500000-27599999 (burgers and
     meat/poultry/fish sandwiches) + 58100000-58106999 (Mexican mixed dishes: burritos,
     tacos, nachos + pizza). Matches the old series (12.9% in 2017-18).
     CAVEAT: FNDDS moved many sandwiches from ingredient-level coding to single codes
     in 2011-12, which mechanically raises this series at the 2009-10 -> 2011-12 seam.
  1b) "Convenience mains": pizza + burgers + pasta mixed dishes -- no Mexican dishes
     and no deli/frankfurter sandwiches (the categories hit hardest by the 2011
     recoding). Code sets are taken from the 2017-18 WWEIA category mapping at the
     5-digit-prefix level and applied to every cycle. Burgers and pasta dishes still
     pick up some later single-coding (FNDDS 2015-16 "(single code)" wave; pasta again
     in 2021-23), flagged in the figure.
     Also written to the CSV: each component + soft drinks, cookies & brownies, and
     salty snacks/chips, so alternative baskets can be assembled without rerunning.
  1c) "Pizza & chips" (the motivation-slide series): pizza + potato/tortilla/other
     chips, pretzels, popcorn. Both families are single-coded in every FNDDS version,
     so this series has NO coding seams.
  1d) "Soda": the carbonated soft-drink code family (5-digit prefixes 92400/92410/
     92411) -- REGULAR AND DIET combined; the two WWEIA categories share these
     prefixes.
  1e) "Frozen & microwavable meals": food codes whose FNDDS description contains
     "frozen meal", "frozen dinner", "pot pie", or "microwav", restricted to mixed-
     dish/mains groups (first digit 2, 4, or 5). Matched per cycle against that
     cycle's own food-code description file (DRXFCD_*, available 2003-04 onward, so
     the series starts in 2003). Frozen pizza cannot be separated from other pizza
     and is NOT included.
  2) "Fruits & vegetables": FNDDS groups 6 (fruit) + 7 (vegetables incl. potatoes) +
     411xxxxx (cooked dry beans/peas, counted as veg by the HEI), EXCLUDING juices
     (3-digit prefixes 612 citrus juice, 641 fruit juice, 642 nectars, 672 juice
     blends, 743 tomato juice; a handful of tiny juice codes under fruit prefixes
     remain, <0.1% of calories). Juice's own share is kept as a separate column.
  3) "Eaten out": day-1 foods with source = restaurant with waiter, fast food/pizza,
     bar/tavern, or restaurant NS (DR1FS in {2,3,4,5}); available 2003+ only.
     Overall and by family income-to-poverty (INDFMPIR) quartile.

Cycles: 1999-2000 ... 2017-2018 plus August 2021-August 2023 ("2021-2023"). There is
no released 2019-2020 cycle (COVID interrupted fieldwork), so the last segment of each
line is drawn dashed. Raw .xpt files download to nielsen_data/raw/nhanes/ if missing.
Tidy series are written to nielsen_data/interim/nhanes/nhanes_diet_trends.csv.

FNDDS CODING BREAK (probed 2026-09-04): single food codes for burritos/tacos, deli
sandwiches, and frankfurter sandwiches were introduced in FNDDS 2011-12 (previously
ingredient-coded), so the pizza/burgers/sandwiches share jumps mechanically at the
2009-10 -> 2011-12 seam (Mexican dishes alone: 1.8% -> 4.4%). Pizza itself is
single-coded throughout and shows NO seam. The figure marks the break with a vertical
line, and the headline growth number is splice-adjusted: growth(1999-2009) x
growth(2011-2023). The same recoding pulls ingredient veg out of the fruit/veg series
(biasing it DOWN at the seam), so "flat fruit & veg" is conservative.
"""
import urllib.request
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

RAW = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/nhanes')
OUT = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/nhanes')
FIGS = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/nhanes')
OUT.mkdir(parents=True, exist_ok=True)
FIGS.mkdir(parents=True, exist_ok=True)

CDC = 'https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/{yr}/DataFiles/{f}'

# (label, midyear for plotting, url year, DEMO file, day-1 totals file,
#  day-1 foods file, food-code description file or None)
CYCLES = [
    ('1999-2000', 1999.5, 1999, 'DEMO.xpt',    'DRXTOT.xpt',   'DRXIFF.xpt',   None),
    ('2001-2002', 2001.5, 2001, 'DEMO_B.xpt',  'DRXTOT_B.xpt', 'DRXIFF_B.xpt', None),
    ('2003-2004', 2003.5, 2003, 'DEMO_C.xpt',  'DR1TOT_C.xpt', 'DR1IFF_C.xpt', 'DRXFCD_C.xpt'),
    ('2005-2006', 2005.5, 2005, 'DEMO_D.xpt',  'DR1TOT_D.xpt', 'DR1IFF_D.xpt', 'DRXFCD_D.xpt'),
    ('2007-2008', 2007.5, 2007, 'DEMO_E.xpt',  'DR1TOT_E.xpt', 'DR1IFF_E.xpt', 'DRXFCD_E.xpt'),
    ('2009-2010', 2009.5, 2009, 'DEMO_F.xpt',  'DR1TOT_F.xpt', 'DR1IFF_F.xpt', 'DRXFCD_F.xpt'),
    ('2011-2012', 2011.5, 2011, 'DEMO_G.xpt',  'DR1TOT_G.xpt', 'DR1IFF_G.xpt', 'DRXFCD_G.xpt'),
    ('2013-2014', 2013.5, 2013, 'DEMO_H.xpt',  'DR1TOT_H.xpt', 'DR1IFF_H.xpt', 'DRXFCD_H.xpt'),
    ('2015-2016', 2015.5, 2015, 'DEMO_I.xpt',  'DR1TOT_I.xpt', 'DR1IFF_I.xpt', 'DRXFCD_I.xpt'),
    ('2017-2018', 2017.5, 2017, 'DEMO_J.xpt',  'DR1TOT_J.xpt', 'DR1IFF_J.xpt', 'DRXFCD_J.xpt'),
    ('2021-2023', 2022.5, 2021, 'DEMO_L.xpt',  'DR1TOT_L.xpt', 'DR1IFF_L.xpt', 'DRXFCD_L.xpt'),
]

FROZEN_RE = 'frozen meal|frozen dinner|pot pie|microwav'

RESTAURANT_SOURCES = {2, 3, 4, 5}  # waiter, fast food/pizza, bar/tavern, restaurant NS

# juices excluded from the fruit/veg series (3-digit FNDDS prefixes)
JUICE_P3 = {612, 641, 642, 672, 743}

# WWEIA categories defining the convenience-mains components and the extra
# basket columns; resolved to 5-digit code prefixes from the 2017-18 mapping
WWEIA_GROUPS = {
    'burgers': ['Burgers (single code)'],
    'pasta_dishes': ['Pasta mixed dishes, excludes macaroni and cheese',
                     'Macaroni and cheese', 'Pasta mixed dishes'],
    'soft_drinks': ['Soft drinks'],
    'cookies_brownies': ['Cookies and brownies'],
    'chips': ['Potato chips', 'Tortilla, corn, and other chips',
              'Pretzels/snack mix', 'Popcorn'],
}


def wweia_p5_sets():
    path = RAW / 'wweia_foodcat.xlsx'
    if not path.exists():
        url = ('https://www.ars.usda.gov/ARSUserFiles/80400530/apps/'
               'WWEIA1718_foodcat_FNDDS.xlsx')
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        path.write_bytes(urllib.request.urlopen(req).read())
    w = pd.read_excel(path)
    w.columns = [c.lower() for c in w.columns]
    w['p5'] = w.food_code.astype('int64') // 1000
    return {k: set(w.loc[w.category_description.isin(cats), 'p5'])
            for k, cats in WWEIA_GROUPS.items()}


P5 = None  # filled in main; module-level so cycle_stats can see it


def fetch(yr, fname):
    path = RAW / fname
    if not path.exists():
        print(f'downloading {fname}...')
        req = urllib.request.Request(CDC.format(yr=yr, f=fname),
                                     headers={'User-Agent': 'Mozilla/5.0'})
        path.write_bytes(urllib.request.urlopen(req).read())
    return path


def first_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def frozen_meal_codes(yr, fcd_f):
    """Codes whose FNDDS description marks a frozen/microwavable main, this cycle."""
    fcd = pd.read_sas(fetch(yr, fcd_f), format='xport')
    codevar = first_col(fcd, ['DRXFDCD'])
    descvar = first_col(fcd, ['DRXFCLD', 'DRXFCSD'])
    desc = fcd[descvar]
    if desc.dtype == object:  # SAS bytes -> str
        desc = desc.str.decode('latin-1', errors='ignore')
    codes = fcd.loc[desc.str.lower().str.contains(FROZEN_RE, regex=True, na=False),
                    codevar].astype('int64')
    return set(codes[codes // 10000000 == 2]) | set(codes[codes // 10000000 == 4]) \
        | set(codes[codes // 10000000 == 5])


def cycle_stats(label, midyear, yr, demo_f, tot_f, iff_f, fcd_f):
    demo = pd.read_sas(fetch(yr, demo_f), format='xport')
    tot = pd.read_sas(fetch(yr, tot_f), format='xport')
    iff = pd.read_sas(fetch(yr, iff_f), format='xport')

    status = first_col(tot, ['DR1DRSTZ', 'DRDDRSTZ', 'DRDDRSTS'])
    weight = first_col(tot, ['WTDRD1', 'WTDR4YR'])
    code = first_col(iff, ['DR1IFDCD', 'DRXIFDCD', 'DRDIFDCD'])
    kcal = first_col(iff, ['DR1IKCAL', 'DRXIKCAL'])
    src = first_col(iff, ['DR1FS', 'DRDIFDFS', 'DRXIFDFS'])
    assert status and weight and code and kcal, f'{label}: missing vars in {tot_f}/{iff_f}'

    p = demo[['SEQN', 'RIDAGEYR', 'INDFMPIR']].merge(tot[['SEQN', status, weight]], on='SEQN')
    adult = p[(p[status] == 1) & (p[weight] > 0) & (p.RIDAGEYR >= 20)]

    f = iff[['SEQN', code, kcal] + ([src] if src else [])].merge(
        adult[['SEQN', weight, 'INDFMPIR']], on='SEQN')
    f = f.dropna(subset=[kcal])
    f['wk'] = f[kcal] * f[weight]
    fc = f[code].astype('int64')

    total = f.wk.sum()
    p3, p5, g1 = fc // 100000, fc // 1000, fc // 10000000
    pizza = f.loc[(p3 == 275) | p5.between(58100, 58106), 'wk'].sum()
    is_juice = p3.isin(JUICE_P3)
    fruitveg = f.loc[(g1.isin([6, 7]) | (p3 == 411)) & ~is_juice, 'wk'].sum()
    juice = f.loc[g1.isin([6, 7]) & is_juice, 'wk'].sum()

    row = {'cycle': label, 'midyear': midyear, 'n_adults': len(adult),
           'pizza_burgers_sandwiches_pct': 100 * pizza / total,
           'fruits_vegetables_pct': 100 * fruitveg / total,
           'juice_pct': 100 * juice / total,
           'pizza_pct': 100 * f.loc[p5 == 58106, 'wk'].sum() / total,
           'eating_out_pct': np.nan}
    for k in WWEIA_GROUPS:
        row[f'{k}_pct'] = 100 * f.loc[p5.isin(P5[k]), 'wk'].sum() / total
    row['convenience_mains_pct'] = (row['pizza_pct'] + row['burgers_pct']
                                    + row['pasta_dishes_pct'])
    row['pizza_chips_pct'] = row['pizza_pct'] + row['chips_pct']
    row['frozen_meals_pct'] = np.nan
    if fcd_f is not None:
        fmc = frozen_meal_codes(yr, fcd_f)
        row['frozen_meals_pct'] = 100 * f.loc[fc.isin(fmc), 'wk'].sum() / total
    for q in range(1, 5):
        row[f'eating_out_q{q}_pct'] = np.nan

    # eating out (food-source codes only comparable from 2003 on)
    if src and yr >= 2003:
        out = f[src].isin(RESTAURANT_SOURCES)
        row['eating_out_pct'] = 100 * f.loc[out, 'wk'].sum() / total
        fq = f.dropna(subset=['INDFMPIR']).copy()
        # weighted PIR quartile cutpoints among adult day-1 respondents
        pir = adult.dropna(subset=['INDFMPIR'])
        cuts = weighted_quantiles(pir['INDFMPIR'].values, pir[weight].values, [.25, .5, .75])
        fq['q'] = 1 + np.searchsorted(cuts, fq['INDFMPIR'].values, side='right')
        for q in range(1, 5):
            sub = fq[fq.q == q]
            row[f'eating_out_q{q}_pct'] = \
                100 * sub.loc[sub[src].isin(RESTAURANT_SOURCES), 'wk'].sum() / sub.wk.sum()
    print(f"{label}: pizza/sandw {row['pizza_burgers_sandwiches_pct']:.2f}  "
          f"fruit/veg {row['fruits_vegetables_pct']:.2f}  "
          f"eat out {row['eating_out_pct']:.2f}  (n={len(adult)})")
    return row


def weighted_quantiles(x, w, qs):
    order = np.argsort(x)
    x, w = x[order], w[order]
    cw = np.cumsum(w) - 0.5 * w
    cw /= np.sum(w)
    return np.interp(qs, cw, x)


# ---------------------------------------------------------------- build series
P5 = wweia_p5_sets()
rows = [cycle_stats(*c) for c in CYCLES]
trends = pd.DataFrame(rows)
trends.to_csv(OUT / 'nhanes_diet_trends.csv', index=False)
print(f'\nwrote {OUT / "nhanes_diet_trends.csv"}')

for col in ['pizza_burgers_sandwiches_pct', 'convenience_mains_pct', 'pizza_pct',
            'burgers_pct', 'pasta_dishes_pct', 'soft_drinks_pct',
            'cookies_brownies_pct', 'chips_pct', 'pizza_chips_pct',
            'frozen_meals_pct', 'fruits_vegetables_pct', 'juice_pct']:
    d = trends[col].dropna()
    first, last = d.iloc[0], d.iloc[-1]
    print(f'{col}: {first:.2f} -> {last:.2f}  ({100 * (last / first - 1):+.0f}%)')

# splice-adjusted growth for the broad series (across the 2011 FNDDS coding break)
s = trends.set_index('cycle').pizza_burgers_sandwiches_pct
splice = (s['2009-2010'] / s['1999-2000']) * (s['2021-2023'] / s['2011-2012'])
print(f'pizza/burgers/sandwiches splice-adjusted growth 1999->2023: {100 * (splice - 1):+.0f}%')

# convenience mains: burgers + pasta dishes got single codes in FNDDS 2015-16,
# so splice across the 2013-14 -> 2015-16 seam
c = trends.set_index('cycle').convenience_mains_pct
splice_c = (c['2013-2014'] / c['1999-2000']) * (c['2021-2023'] / c['2015-2016'])
print(f'convenience mains splice-adjusted growth 1999->2023: {100 * (splice_c - 1):+.0f}%')


# ---------------------------------------------------------------- figure style
plt.rcParams.update({
    'figure.facecolor': 'white', 'axes.facecolor': 'white',
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.linewidth': 1.4, 'font.size': 18,
    'axes.labelsize': 22, 'axes.titlesize': 26, 'axes.titleweight': 'bold',
    'xtick.labelsize': 19, 'ytick.labelsize': 19,
})

RED = '#C83232'    # deck priceRed
GREEN = '#00A050'  # deck prefGreen
BLUE = '#0066CC'   # deck incomeBlue


def trend_plot(df, col, color, title, fname, label_pts=True, coding_breaks=()):
    """Line + fill-to-axis-floor trend with non-colliding point labels.

    The 2017-18 -> 2021-23 segment is dashed (no 2019-20 cycle released, COVID).
    coding_breaks: x positions of FNDDS single-coding waves to mark.
    """
    d = df.dropna(subset=[col])
    x, y = d.midyear.values, d[col].values
    fig, ax = plt.subplots(figsize=(10, 6.8))

    solid = x <= 2017.5
    ax.plot(x[solid], y[solid], '-', color=color, lw=4, zorder=3,
            marker='o', ms=11, mec='white', mew=1.5)
    if (~solid).any():
        seg = np.concatenate([[x[solid][-1]], x[~solid]]), \
              np.concatenate([[y[solid][-1]], y[~solid]])
        ax.plot(*seg, '--', color=color, lw=4, zorder=3, dashes=(4, 2))
        ax.plot(x[~solid], y[~solid], 'o', color=color, ms=11,
                mec='white', mew=1.5, zorder=4)

    lo = min(y.min() - 0.12 * (y.max() - y.min()), y.min() - 0.5)
    hi = y.max() + 0.22 * (y.max() - y.min() + 1)
    ax.set_ylim(lo, hi)
    ax.fill_between(x, y, lo, color=color, alpha=0.13, zorder=1)

    if label_pts:
        # drop a label below its point when the previous label is at nearly the
        # same height (consecutive near-equal values otherwise collide)
        prev_y, prev_above = None, True
        for xi, yi in zip(x, y):
            above = not (prev_above and prev_y is not None
                         and abs(yi - prev_y) < 0.09 * (hi - lo))
            ax.annotate(f'{yi:.1f}%', (xi, yi), textcoords='offset points',
                        xytext=(0, 13 if above else -30), ha='center', fontsize=16,
                        fontweight='bold', color='#222222', zorder=5)
            prev_y, prev_above = yi, above

    for i, xb in enumerate(coding_breaks):
        ax.axvline(xb, color='#999999', lw=1.6, ls=':')
        if i == 0:
            ax.annotate('food-coding\nchange', (xb, lo + 0.04 * (hi - lo)),
                        textcoords='offset points', xytext=(6, 0), fontsize=13,
                        color='#777777', ha='left', va='bottom')

    ax.set_xlim(x.min() - 1.1, x.max() + 1.1)
    ax.set_xticks([2000, 2004, 2008, 2012, 2016, 2020])
    ax.set_ylabel('Share of calories (%)')
    ax.set_title(title, pad=14)
    ax.grid(axis='y', color='#DDDDDD', lw=0.8, alpha=0.6)
    fig.tight_layout()
    fig.savefig(FIGS / fname, dpi=200)
    plt.close(fig)
    print(f'wrote {FIGS / fname}')


trend_plot(trends, 'pizza_chips_pct', RED,
           'Pizza & chips', 'pizza_chips_trend.png')
trend_plot(trends, 'soft_drinks_pct', RED,
           'Soda (regular & diet)', 'soda_trend.png')
trend_plot(trends, 'frozen_meals_pct', '#E8A33D',
           'Frozen & microwavable meals', 'frozen_meals_trend.png')
trend_plot(trends, 'pizza_burgers_sandwiches_pct', RED,
           'Pizza, burgers & sandwiches', 'pizza_burgers_sandwiches_trend.png',
           coding_breaks=(2010.5,))
trend_plot(trends, 'convenience_mains_pct', RED,
           'Pizza, burgers & pasta dishes', 'convenience_mains_trend.png',
           coding_breaks=(2014.5,))
trend_plot(trends, 'fruits_vegetables_pct', GREEN,
           'Fruits & vegetables (excl. juice)', 'fruits_vegetables_trend.png')
trend_plot(trends, 'eating_out_pct', BLUE,
           'Calories eaten away from home', 'eating_out_trend.png')

# ------------------------------------------------- eating out by income quartile
d = trends.dropna(subset=['eating_out_q1_pct'])
fig, ax = plt.subplots(figsize=(11, 6.8))
qcolors = {1: RED, 2: '#E8A33D', 3: GREEN, 4: BLUE}
qlabels = {1: 'Q1 (lowest income)', 2: 'Q2', 3: 'Q3', 4: 'Q4 (highest income)'}
ends = {}
for q in range(1, 5):
    x, y = d.midyear.values, d[f'eating_out_q{q}_pct'].values
    solid = x <= 2017.5
    ax.plot(x[solid], y[solid], '-', color=qcolors[q], lw=3.5, zorder=3,
            marker='o', ms=8, mec='white', mew=1.2)
    if (~solid).any():
        ax.plot(np.concatenate([[x[solid][-1]], x[~solid]]),
                np.concatenate([[y[solid][-1]], y[~solid]]),
                '--', color=qcolors[q], lw=3.5, zorder=3, dashes=(4, 2))
        ax.plot(x[~solid], y[~solid], 'o', color=qcolors[q], ms=8,
                mec='white', mew=1.2, zorder=4)
    ends[q] = (x[-1], y[-1])

# vertically dodge the direct end-of-line labels so converging series stay legible
ylo = min(v[1] for v in ends.values())
yspan = max(v[1] for v in ends.values()) - ylo
gap = max(0.09 * (yspan + 3), 0.55)
order = sorted(ends, key=lambda q: ends[q][1])
label_y = {}
for i, q in enumerate(order):
    yq = ends[q][1]
    label_y[q] = yq if i == 0 else max(yq, label_y[order[i - 1]] + gap)
for q in range(1, 5):
    ax.annotate(qlabels[q], (ends[q][0], label_y[q]), textcoords='offset points',
                xytext=(12, -5), fontsize=17, fontweight='bold', color=qcolors[q])

ax.set_xlim(d.midyear.min() - 0.8, d.midyear.max() + 6.5)
ax.set_xticks([2004, 2008, 2012, 2016, 2020])
ax.set_ylabel('Share of calories eaten out (%)')
ax.set_title('Calories eaten away from home, by income quartile', pad=14)
ax.grid(axis='y', color='#DDDDDD', lw=0.8, alpha=0.6)
fig.tight_layout()
fig.savefig(FIGS / 'eating_out_by_income_trend.png', dpi=200)
plt.close(fig)
print(f'wrote {FIGS / "eating_out_by_income_trend.png"}')
