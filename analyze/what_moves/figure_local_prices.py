"""Figures and table for the local relative-price analysis (build_county_price_index.py,
build_local_price_index.py, build_local_price_regs.do). Outputs (Overleaf clone):
  figs/what_moves/prices/local_price_dispersion.png      county relative price of healthy vs unhealthy food
                                                           (RMS index, log points since 2007) across counties in
                                                           2010 and 2020; SD across counties by year for the three
                                                           pairs; the zip3 unit-value check in grey
  figs/what_moves/prices/local_price_long_difference.png  county change in HEI and in the ultra-processed share,
                                                           2008-10 to 2018-20, against the change in the relative
                                                           price (twenty bins, weighted by households)
  tabs/what_moves/local_price_regs.tex                    coefficients per 10 log points by outcome and design
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
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#7a7a7a'
FIG = fdir / 'what_moves' / 'prices'
TAB = tdir / 'what_moves'
FIG.mkdir(parents=True, exist_ok=True); TAB.mkdir(parents=True, exist_ok=True)
plt.rcParams.update({'font.size': 14, 'axes.labelsize': 15, 'axes.titlesize': 14, 'xtick.labelsize': 13,
                     'ytick.labelsize': 13, 'legend.fontsize': 12, 'axes.spines.top': False, 'axes.spines.right': False})
WM = idir / 'what_moves'

cty = pd.read_parquet(WM / 'county_price_index.parquet').dropna(subset=['rel_hi_ces'])
zip3 = pd.read_parquet(WM / 'local_price_index.parquet') if (WM / 'local_price_index.parquet').exists() else None
if zip3 is not None:
    zip3 = zip3[zip3['n_households'] >= 30]

# ---------------------------------------------------------------- dispersion
fig, axes = plt.subplots(1, 2, figsize=(15, 5.8))
ax = axes[0]
for y, color in [(2010, BLUE), (2020, RED)]:
    d = cty.loc[cty['year'] == y, 'rel_hi_ces'] * 100
    ax.hist(d, bins=np.arange(-20, 20.5, 1), alpha=0.5, color=color, label=f'{y}: {len(d):,} counties, SD {d.std():.1f}')
ax.axvline(0, color=GREY, lw=1, ls='--')
ax.set_xlabel('Healthy minus unhealthy price level since 2007, log points')
ax.set_ylabel('Counties')
ax.set_title('Relative price of healthy food across counties (RMS stores)')
ax.legend(frameon=False)
ax = axes[1]
for col, color, lab in [('rel_hi_ces', BLUE, 'healthy minus unhealthy'), ('rel_upf_ces', RED, 'mostly ultra-processed minus not'),
                        ('rel_prep_ces', ORANGE, 'little preparation minus cooking')]:
    sd = cty.groupby('year')[col].std() * 100
    ax.plot(sd.index, sd.values, color=color, lw=2.5, marker='o', ms=5, label=lab)
if zip3 is not None:
    sd = zip3.groupby('year')['rel_hi'].std() * 100
    ax.plot(sd.index, sd.values, color=GREY, lw=2, ls='--', marker='s', ms=4, label='healthy minus unhealthy, panelists\' unit values by 3-digit zip')
ax.set_xlabel('Year'); ax.set_ylabel('SD across markets, log points')
ax.set_title('How much markets differ, by year')
ax.legend(frameon=False, fontsize=10)
fig.tight_layout()
fig.savefig(FIG / 'local_price_dispersion.png', dpi=150, bbox_inches='tight'); plt.close(fig)
print('saved', FIG / 'local_price_dispersion.png')

# ---------------------------------------------------------------- long difference binscatter (county)
ld = pd.read_stata(WM / 'local_price_longdiff_county.dta')
fig, axes = plt.subplots(1, 2, figsize=(15, 5.8))
for ax, (y, x, ylab, xlab) in zip(axes, [('d_hei_usda', 'd_rel_hi_ces', 'Change in HEI (points)', 'Change in healthy-minus-unhealthy price, log points'),
                                           ('d_upf_share', 'd_rel_upf_ces', 'Change in ultra-processed share (pct. points)', 'Change in ultra-processed-minus-not price, log points')]):
    d = ld[[y, x, 'w']].dropna().copy()
    d[x] = d[x] * 100
    d['bin'] = pd.qcut(d[x], 20, labels=False, duplicates='drop')
    b = d.groupby('bin').apply(lambda g: pd.Series({'x': np.average(g[x], weights=g.w), 'y': np.average(g[y], weights=g.w)}))
    ax.scatter(b['x'], b['y'], color=BLUE, s=60, zorder=3)
    X = np.vstack([np.ones(len(d)), d[x]]).T; W = np.sqrt(d.w.values)
    beta = np.linalg.lstsq(X * W[:, None], d[y].values * W, rcond=None)[0]
    xs = np.linspace(d[x].min(), d[x].max(), 50)
    ax.plot(xs, beta[0] + beta[1] * xs, color=RED, lw=2, label=f'slope {beta[1] * 10:+.3f} per 10 log points')
    ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(0, color=GREY, lw=1, ls=':')
    ax.set_xlabel(xlab); ax.set_ylabel(ylab)
    ax.set_title(f'{len(d):,} counties, 2008-10 to 2018-20, twenty bins')
    ax.legend(frameon=False)
fig.tight_layout()
fig.savefig(FIG / 'local_price_long_difference.png', dpi=150, bbox_inches='tight'); plt.close(fig)
print('saved', FIG / 'local_price_long_difference.png')

# ---------------------------------------------------------------- table
r = pd.read_csv(WM / 'local_price_regs.csv')
labels = {'hei_usda': 'Full HEI (points)', 'hi_usda': 'Simple index (SD)', 'produce_share': 'Produce share of calories (pct. pts)',
          'sugar_per_1000cal_usda': 'Sugar, g per 1,000 kcal', 'cals_ae_day': 'Calories per adult eq. per day',
          'upf_share': 'Ultra-processed share of calories (pct. pts)', 'prep_time': 'Basket preparation-time score'}
prices = {'hi': 'healthy $-$ unhealthy', 'upf': 'ultra-processed $-$ not', 'prep': 'little prep $-$ cooking'}
cols = [('county', 'hh_year'), ('county', 'hh_stateyear'), ('county', 'iv'), ('county', 'longdiff'), ('county', 'longdiff_iv'), ('zip3', 'hh_year'), ('zip3', 'longdiff')]
def cell(y, idx, spec):
    s = r[(r['outcome'] == y) & (r['index'] == idx) & (r['spec'] == spec) & (~r['price'].str.endswith('_va'))]
    if s.empty:
        return ''
    s = s.iloc[0]
    txt = f'{s.b / 10:.3f} ({s.se / 10:.3f})'
    if spec in ('iv', 'longdiff_iv') and pd.notna(s.fs_F):
        txt += f' [F {s.fs_F:.0f}]'
    return txt
rows = ['\\begin{tabular}{llccccccc}', '\\toprule',
        ' & & \\multicolumn{5}{c}{County, RMS store price index (2008-2020)} & \\multicolumn{2}{c}{3-digit zip, panelists\' unit values (2004-2020)} \\\\',
        '\\cmidrule(lr){3-7} \\cmidrule(lr){8-9}',
        'Outcome & Relative price & HH + year FE & HH + state-year FE & IV, shift-share & Long diff. & Long diff. IV & HH + year FE & Long diff. \\\\', '\\midrule']
for y in labels:
    p = r.loc[r['outcome'] == y, 'price']
    if p.empty:
        continue
    pair = p.iloc[0].replace('rel_', '').replace('_ces', '')
    rows.append(f'{labels[y]} & {prices.get(pair, pair)} & ' + ' & '.join(cell(y, i, s) for i, s in cols) + ' \\\\')
n_c = r[(r['index'] == 'county') & (r['spec'] == 'hh_year')]['N'].max(); n_z = r[(r['index'] == 'zip3') & (r['spec'] == 'hh_year')]['N'].max() if (r['index'] == 'zip3').any() else np.nan
rows += ['\\midrule', f'\\multicolumn{{9}}{{l}}{{Household-years: county design {n_c:,.0f}; zip3 design {n_z:,.0f}. Coefficients per 10 log points (about 10 percent) of relative price, SE in parentheses, F = first-stage F.}} \\\\',
         '\\bottomrule', '\\end{tabular}']
(TAB / 'local_price_regs.tex').write_text('\n'.join(rows) + '\n')
print('saved', TAB / 'local_price_regs.tex')
print(r.assign(b10=r.b / 10, se10=r.se / 10).round(4).to_string(index=False))
