"""Figures and tables for the local relative-price analysis (build_county_price_index.py,
build_local_price_index.py, build_local_price_regs.do). Outputs (Overleaf clone):
  figs/what_moves/prices/local_price_dispersion.png       change since 2007 in the relative price of healthy vs
                                                            unhealthy food across counties (RMS), 2010 and 2020;
                                                            SD across markets by year
  figs/what_moves/prices/local_price_long_difference.png  county change in HEI and in the ultra-processed share,
                                                            2008-10 to 2018-20, against the change in the relative
                                                            price (twenty bins, weighted by projected households)
  tabs/what_moves/local_price_regs.tex                    county RMS price index: household designs and long differences
  tabs/what_moves/local_price_regs_zip3.tex               3-digit-zip unit-value check
Every table cell is the change in the outcome when the relative price FALLS by 10 log points (about 10 percent).
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
plt.rcParams.update({'font.size': 15, 'axes.labelsize': 16, 'axes.titlesize': 15, 'xtick.labelsize': 14,
                     'ytick.labelsize': 14, 'legend.fontsize': 12.5, 'axes.spines.top': False, 'axes.spines.right': False})
WM = idir / 'what_moves'
ROWS = [('hei_usda', 'hi', 'Full HEI (points)'), ('hei_usda', 'upf', 'Full HEI (points)'), ('hei_usda', 'prep', 'Full HEI (points)'),
        ('upf_share', 'upf', 'Ultra-processed share of calories (pct. pts)'), ('prep_time_raw', 'prep', 'Basket preparation score'),
        ('hi_usda', 'hi', 'Simple index (SD)'), ('produce_share', 'hi', 'Produce share of calories (pct. pts)'),
        ('sugar_per_1000cal_usda', 'hi', 'Sugar, g per 1,000 kcal'), ('cals_ae_day', 'hi', 'Calories per adult eq. per day')]
CHEAPER = {'hi': 'healthy', 'upf': 'ultra-processed', 'prep': 'little preparation'}


def dispersion(cty, zip3):
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.8))
    ax = axes[0]
    for y, color in [(2010, BLUE), (2020, RED)]:
        d = cty.loc[cty['year'] == y, 'rel_hi_ces'].dropna() * 100
        ax.hist(d, bins=np.arange(-20, 15.5, 1), alpha=0.5, color=color, label=f'{y}: {len(d):,} counties, SD {d.std():.1f}')
    ax.axvline(0, color=GREY, lw=1, ls='--')
    ax.set_xlabel('Change since 2007 in healthy minus unhealthy price, log points')
    ax.set_ylabel('Counties')
    ax.set_title('Relative price of healthy food, RMS stores')
    ax.legend(frameon=False)
    ax = axes[1]
    for col, color, lab in [('rel_hi_ces', BLUE, 'healthy minus unhealthy'), ('rel_upf_ces', RED, 'ultra-processed minus not'),
                            ('rel_prep_ces', ORANGE, 'little preparation minus cooking')]:
        sd = cty.groupby('year')[col].std() * 100
        ax.plot(sd.index, sd.values, color=color, lw=2.5, marker='o', ms=5, label=f'{lab}, counties (change since 2007)')
    if zip3 is not None:
        sd = zip3.groupby('year')['rel_hi'].std() * 100
        ax.plot(sd.index, sd.values, color=GREY, lw=2, ls='--', marker='s', ms=4, label='healthy minus unhealthy, 3-digit zips (price level)')
    ax.set_xlabel('Year'); ax.set_ylabel('SD across markets, log points')
    ax.set_title('How much markets differ, by year')
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, fontsize=11, loc='lower right')
    fig.tight_layout()
    fig.savefig(FIG / 'local_price_dispersion.png', dpi=150, bbox_inches='tight'); plt.close(fig)
    print('saved', FIG / 'local_price_dispersion.png')


def long_difference():
    ld = pd.read_stata(WM / 'local_price_longdiff_county.dta')
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.8))
    for ax, (y, x, ylab, xlab) in zip(axes, [
            ('d_hei_usda', 'd_rel_hi_ces', 'Change in HEI (points)', 'Change in healthy minus unhealthy price, log points'),
            ('d_upf_share', 'd_rel_upf_ces', 'Change in ultra-processed share (pct. points)', 'Change in ultra-processed minus not price, log points')]):
        d = ld[[y, x, 'w']].dropna().copy()
        d[x] = d[x] * 100
        d['bin'] = pd.qcut(d[x], 20, labels=False, duplicates='drop')
        b = d.groupby('bin').apply(lambda g: pd.Series({'x': np.average(g[x], weights=g.w), 'y': np.average(g[y], weights=g.w)}))
        ax.scatter(b['x'], b['y'], color=BLUE, s=70, zorder=3)
        X = np.vstack([np.ones(len(d)), d[x]]).T; W = np.sqrt(d.w.values)
        beta = np.linalg.lstsq(X * W[:, None], d[y].values * W, rcond=None)[0]
        xs = np.linspace(b['x'].min(), b['x'].max(), 50)
        ax.plot(xs, beta[0] + beta[1] * xs, color=RED, lw=2.5, label=f'county-level slope: {beta[1] * 10:+.3f} per 10 log points')
        ax.axvline(0, color=GREY, lw=1, ls=':')
        ax.set_xlabel(xlab); ax.set_ylabel(ylab)
        ax.set_title(f'{len(d):,} counties, 2008-10 to 2018-20, twenty bins')
        ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(FIG / 'local_price_long_difference.png', dpi=150, bbox_inches='tight'); plt.close(fig)
    print('saved', FIG / 'local_price_long_difference.png')


def cell(r, y, p, spec, star_f=False, zprice=False):
    price = f'z_rel_{p}' if zprice else f'rel_{p}_ces'
    s = r[(r['outcome'] == y) & (r['price'] == price) & (r['spec'] == spec)]
    if s.empty or pd.isna(s.iloc[0]['b']):
        return ''
    s = s.iloc[0]
    return f'{-s.b / 10:.3f} ({s.se / 10:.3f})'


def fmt_int(x):
    return f'{x:,.0f}' if pd.notna(x) else ''


def table_county(r):
    head = ['\\begin{tabular}{llcccc}', '\\toprule']
    A = [('hh_year', 'Same household,'), ('hh_stateyear', 'Same household,'), ('iv', 'Shift-share IV'), ('iv_base', 'Shift-share IV,')]
    head += ['\\multicolumn{6}{l}{\\textit{Panel A. Household panel, 2008--2020}} \\\\',
             ' & Food that & Same household, & Same household, & Shift-share & Shift-share IV, \\\\',
             'Outcome & gets cheaper & year FE & state-by-year FE & IV & baseline $\\times$ year \\\\', '\\midrule']
    for y, p, lab in ROWS:
        head.append(f'{lab} & {CHEAPER[p]} & ' + ' & '.join(cell(r, y, p, s) for s, _ in A) + ' \\\\')
    head += ['\\midrule']
    for p in ['hi', 'upf', 'prep']:
        s = r[(r['outcome'] == 'hei_usda') & (r['price'] == f'rel_{p}_ces') & (r['spec'] == 'iv')]
        if not s.empty:
            s = s.iloc[0]
            head.append(f'First stage, {CHEAPER[p]} & & & & {s.fs_b:.3f} ({s.fs_se:.3f}) & \\\\')
            head.append(f'\\quad first-stage F & & & & {s.fs_F:.1f} & \\\\')
    n = r[(r['outcome'] == 'hei_usda') & (r['price'] == 'rel_hi_ces') & (r['spec'] == 'hh_year')]
    if not n.empty:
        head.append(f'Household-years; counties & & {fmt_int(n.iloc[0].N)}; {fmt_int(n.iloc[0].n_units)} & & & \\\\')
    head += ['\\midrule', '\\multicolumn{6}{l}{\\textit{Panel B. Long differences, 2008--10 to 2018--20}} \\\\',
             ' & Food that & Counties & Same households & Counties, & Placebo: diet change \\\\',
             'Outcome & gets cheaper & & & IV & 2004--05 to 2006--07 \\\\', '\\midrule']
    for y, p, lab in ROWS:
        head.append(f'{lab} & {CHEAPER[p]} & ' + ' & '.join(cell(r, y, p, s) for s in ['longdiff', 'longdiff_hh', 'longdiff_iv', 'placebo_pre_x']) + ' \\\\')
    n = r[(r['outcome'] == 'hei_usda') & (r['price'] == 'rel_hi_ces')]
    nc = n[n['spec'] == 'longdiff']; nh = n[n['spec'] == 'longdiff_hh']
    if not nc.empty and not nh.empty:
        head += ['\\midrule', f'Counties; households & & {fmt_int(nc.iloc[0].N)} & {fmt_int(nh.iloc[0].N)} & & \\\\']
    head += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'local_price_regs.tex').write_text('\n'.join(head) + '\n')
    print('saved', TAB / 'local_price_regs.tex')


def table_zip3(r):
    head = ['\\begin{tabular}{llcccccc}', '\\toprule',
            ' & Food that & Across & Across markets, & Same household, & Same household, & Long diff., & Long diff., \\\\',
            'Outcome & gets cheaper & markets & income, schooling & year FE & state-by-year FE & markets & same households \\\\', '\\midrule']
    for y, p, lab in ROWS:
        head.append(f'{lab} & {CHEAPER[p]} & ' + ' & '.join(cell(r, y, p, s) for s in ['cross', 'cross_x', 'hh_year', 'hh_stateyear', 'longdiff', 'longdiff_hh']) + ' \\\\')
    n = r[(r['outcome'] == 'hei_usda') & (r['price'] == 'rel_hi_ces')]
    nc, nl, nh = n[n['spec'] == 'hh_year'], n[n['spec'] == 'longdiff'], n[n['spec'] == 'longdiff_hh']
    if not nc.empty:
        head += ['\\midrule', f'Household-years; markets & & & & {fmt_int(nc.iloc[0].N)}; {fmt_int(nc.iloc[0].n_units)} & & ' + (f'{fmt_int(nl.iloc[0].N)}' if not nl.empty else '') + ' & ' + (f'{fmt_int(nh.iloc[0].N)}' if not nh.empty else '') + ' \\\\']
    head += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'local_price_regs_zip3.tex').write_text('\n'.join(head) + '\n')
    print('saved', TAB / 'local_price_regs_zip3.tex')


def main():
    cty = pd.read_parquet(WM / 'county_price_index.parquet')
    zpath = WM / 'local_price_index.parquet'
    zip3 = pd.read_parquet(zpath) if zpath.exists() else None
    if zip3 is not None:
        zip3 = zip3[zip3['n_households'] >= 30]
    dispersion(cty, zip3)
    long_difference()
    rc = pd.read_csv(WM / 'local_price_regs_county.csv', na_values='.')
    table_county(rc)
    print(rc.assign(fall10=-rc.b / 10, se10=rc.se / 10).drop(columns=['index', 'b', 'se']).round(4).to_string(index=False))
    zp = WM / 'local_price_regs_zip3.csv'
    if zp.exists():
        rz = pd.read_csv(zp, na_values='.')
        table_zip3(rz)
        print(rz.assign(fall10=-rz.b / 10, se10=rz.se / 10).drop(columns=['index', 'b', 'se', 'fs_F', 'fs_b', 'fs_se']).round(4).to_string(index=False))


if __name__ == '__main__':
    main()
