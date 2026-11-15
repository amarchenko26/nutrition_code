"""New food products that stores offer, by the income of the ZIP codes their shoppers live in (Anya 2026-11-15: the options a
store gives its shoppers, not spending, as in figure_store_access.py; the Jaravel (2019) questions by place: do stores in poorer
areas offer more new unhealthy than healthy products, are those products quicker to prepare, and is their variety-adjusted price
lower).

Reads interim/what_moves/store_new_options_cells.parquet (per RMS store, year t = 2009-2020 and module cell, from
build_store_new_options_rms.py on Oscar after build_store_upc_year_rms.py and build_upc_first_year_all_rms.py; all RMS stores),
store_shopper_income.parquet and store_choice_sets.parquet (store type). Stores, bins and quartiles as figure_store_access.py:
50+ panel trips from 10+ households, Allcott et al.'s $10,000 bins of shoppers' ZIP median income, quartiles of stores, store-years
counted equally.
Per store-year and group (unhealthy = module score 0-3, healthy = 4-10):
  newsh     % of the store's barcodes in the group that are new nationally (first sold at any RMS store that year); 20+ barcodes
  new       number of such barcodes
  open_new  % of the store's new unhealthy barcodes, among modules with a preparation score, in open-and-eat modules (5+ new);
  open_all  the same over all its unhealthy barcodes (20+); open_tilt = open_new - open_all
  cont      equal-weighted mean log change of the store's unit value of barcodes it sold in t-1 and t, clipped at the module's 1st and
            99th percentiles (log points); var = -(1/(sigma-1)) x change in log number of barcodes offered, sigma = 5; va = cont + var.
            Price measures need 20+ barcodes in the group in both years, 10+ continuing barcodes, and a change in the store's total
            number of barcodes within +-0.5 log points (drops part-year reporting).
Differences: bottom minus top quartile store-years, year fixed effects (and store-type fixed effects in the last column), clustered
by store. Outputs: figs/what_moves/variety/store_new_options_by_income.png, tabs/what_moves/store_new_options.tex; prints the
grocery-only and without-2018 versions and the yearly differences. Built from the scratch analysis of 2026-11-15 (all stores; the
barcode x county variety files keep only the stores of the 2006 store list).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.stars import star_num

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

RED, GREY = '#a63d40', '#999999'
plt.rcParams.update({'font.size': 24, 'axes.labelsize': 24, 'axes.titlesize': 26, 'xtick.labelsize': 21, 'ytick.labelsize': 22,
                     'axes.spines.top': False, 'axes.spines.right': False})
MIN_TRIPS, MIN_HH = 50, 10
EDGES = [0, 30, 40, 50, 60, 70, 80, np.inf]
MIDS = [25, 35, 45, 55, 65, 75, 100]
TICKS = ['<30', '30-40', '40-50', '50-60', '60-70', '70-80', '80+']
XLAB = "Shoppers' ZIP median income ($000s)"
SIGMA = 5
MIN_N, MIN_CONT, MIN_NEW, MAX_DLOGN = 20, 10, 5, 0.5
PREP = {1: 'open', 2: 'little', 3: 'cook'}
CNT = ['n_t', 'n_new', 'n_tm1', 'n_cont', 'sum_dlp']
TABLE = [(r"A. New products, \% of the store's products in the group",
          [('newsh_U', 'Unhealthy (score 0--3)', 2), ('newsh_H', 'Healthy (score 4--10)', 2), ('gap_newsh', 'Unhealthy minus healthy', 2)]),
         ('B. New products per store', [('new_U', 'Unhealthy', 0), ('new_H', 'Healthy', 0)]),
         (r"C. Open and eat, \% of the store's unhealthy products",
          [('open_new_U', 'New products', 1), ('open_all_U', 'All products', 1), ('open_tilt_U', 'New minus all', 2)]),
         ('D. Price change, unhealthy minus healthy (log points a year)',
          [('gap_va', 'Variety-adjusted', 2), ('gap_cont', 'Continuing products', 2), ('gap_var', 'Variety term', 2)])]


def side(x, tag, stable):
    g = x.groupby(['store', 'year'])[CNT].sum()
    r = pd.DataFrame(index=g.index)
    big = g['n_t'] >= MIN_N
    r[f'n_{tag}'] = g['n_t']
    r[f'new_{tag}'] = g['n_new']
    r[f'newsh_{tag}'] = (100 * g['n_new'] / g['n_t']).where(big)
    ok = big & (g['n_tm1'] >= MIN_N) & (g['n_cont'] >= MIN_CONT) & stable.reindex(g.index).fillna(False).values
    var = -(np.log(g['n_t'].where(g['n_t'] > 0)) - np.log(g['n_tm1'].where(g['n_tm1'] > 0))) / (SIGMA - 1)
    r[f'cont_{tag}'] = (100 * g['sum_dlp'] / g['n_cont']).where(ok)
    r[f'var_{tag}'] = (100 * var).where(ok)
    r[f'va_{tag}'] = r[f'cont_{tag}'] + r[f'var_{tag}']
    return r


def store_measures():
    c = pd.read_parquet(idir / 'what_moves' / 'store_new_options_cells.parquet', columns=['store', 'year', 'cell'] + CNT)
    for k in CNT:
        c[k] = c[k].fillna(0)
    c['unh'] = c['cell'] // 100
    c['prep'] = (c['cell'] // 10) % 10
    tot = c.groupby(['store', 'year'])[['n_t', 'n_tm1']].sum()
    stable = (np.log(tot['n_t'].where(tot['n_t'] > 0)) - np.log(tot['n_tm1'].where(tot['n_tm1'] > 0))).abs() <= MAX_DLOGN
    parts = [side(c[c.unh == 1], 'U', stable), side(c[c.unh == 0], 'H', stable)]
    for p, pl in PREP.items():
        parts.append(side(c[(c.unh == 1) & (c.prep == p)], f'U{pl}', stable))
    m = pd.concat(parts, axis=1)
    m['n_all_t'] = tot['n_t']
    m = m.reset_index()
    for a in ['newsh', 'new', 'va', 'cont', 'var']:
        m[f'gap_{a}'] = m[f'{a}_U'] - m[f'{a}_H']
    nn = sum(m[f'new_U{p}'].fillna(0) for p in PREP.values())
    nt = sum(m[f'n_U{p}'].fillna(0) for p in PREP.values())
    m['open_new_U'] = (100 * m['new_Uopen'].fillna(0) / nn).where(nn >= MIN_NEW)
    m['open_all_U'] = (100 * m['n_Uopen'].fillna(0) / nt).where(nt >= MIN_N)
    m['open_tilt_U'] = m['open_new_U'] - m['open_all_U']
    return m.rename(columns={'store': 'store_code_uc'})


def load():
    m = store_measures()
    inc = pd.read_parquet(idir / 'what_moves' / 'store_shopper_income.parquet')
    inc = inc[(inc['n_trips'] >= MIN_TRIPS) & (inc['n_households'] >= MIN_HH) & inc['zip_income'].notna()]
    ch = pd.read_parquet(idir / 'what_moves' / 'store_choice_sets.parquet', columns=['store_code_uc', 'year', 'channel_code'])
    first = ch.groupby('store_code_uc')['year'].min().rename('first_obs')
    st = ch.merge(inc[['store_code_uc', 'zip_income']], on='store_code_uc').drop_duplicates('store_code_uc')
    cuts = st['zip_income'].quantile([0.25, 0.75]).values           # store quartiles, as figure_store_access.py
    d = m.merge(inc[['store_code_uc', 'zip_income']], on='store_code_uc', how='inner').merge(ch, on=['store_code_uc', 'year'], how='left')
    d = d[(d['n_all_t'] > 0) & d['channel_code'].notna()]
    d['first_obs'] = d['store_code_uc'].map(first)
    d['bin'] = pd.cut(d['zip_income'] / 1000, EDGES, labels=MIDS, right=False).astype(int)
    d['quartile'] = np.select([d['zip_income'] <= cuts[0], d['zip_income'] > cuts[1]], ['bottom', 'top'], default='middle')
    print(f'years {d.year.min()}-{d.year.max()}; {len(d):,} store-years, {d.store_code_uc.nunique():,} stores; quartile cuts ${cuts[0]:,.0f} / ${cuts[1]:,.0f}')
    print('store type by quartile (% of store-years):')
    print((100 * pd.crosstab(d['quartile'], d['channel_code'], normalize='index')).round(1).to_string())
    return d


def diff(d, y, fe='C(year)'):
    z = d[d['quartile'].isin(['bottom', 'top'])].dropna(subset=[y, 'channel_code']).copy()
    z['poor'] = (z['quartile'] == 'bottom').astype(float)
    r = smf.ols(f'{y} ~ poor + {fe}', data=z).fit(cov_type='cluster', cov_kwds={'groups': z['store_code_uc'].values})
    return {'bottom': z.loc[z.poor == 1, y].mean(), 'top': z.loc[z.poor == 0, y].mean(), 'b': r.params['poor'], 'se': r.bse['poor'],
            'n': int(len(z))}


def table(d):
    q = d[d.quartile.isin(['bottom', 'top'])]
    lines = [r'\begin{tabular}{lcccc}', r'\toprule',
             r' & Bottom & Top & Bottom & Bottom minus top, \\',
             r' & quartile & quartile & minus top & same store type \\', r'\midrule']
    for k, (title, rows) in enumerate(TABLE):
        if k:
            lines.append(r'\midrule')
        lines.append(r'\multicolumn{5}{l}{\textit{' + title + r'}} \\')
        for y, lab, dec in rows:
            a, f = diff(d, y), diff(d, y, 'C(year) + C(channel_code)')
            lines.append(f"{lab} & {a['bottom']:,.{dec}f} & {a['top']:,.{dec}f} & {star_num(a['b'], a['se'], dec)} & {star_num(f['b'], f['se'], dec)} \\\\")
            lines.append(f" &  &  & ({a['se']:.{dec}f}) & ({f['se']:.{dec}f}) \\\\")
            print(f"  {lab}: bottom {a['bottom']:.2f}, top {a['top']:.2f}, difference {a['b']:.3f} ({a['se']:.3f}); same store type {f['b']:.3f} ({f['se']:.3f}); N {a['n']:,}")
    n = q.groupby('quartile').size()
    lines += [r'\midrule', f"Store-years & {n['bottom']:,} & {n['top']:,} &  &  \\\\", r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'store_new_options.tex'
    out.write_text('\n'.join(lines) + '\n')
    print(f'saved {out}')


def figure(d):
    b = d.groupby('bin')[['gap_newsh', 'gap_new', 'open_new_U', 'open_all_U', 'gap_va']].mean().reindex(MIDS)
    print('bin means:'); print(pd.concat([b.round(3), d.groupby('bin').size().rename('store_years')], axis=1).to_string())
    panels = [('gap_newsh', 'New unhealthy minus new healthy\nproducts, % of products offered', True),
              ('gap_new', 'New unhealthy minus new healthy\nproducts per store', True),
              ('open_new_U', "Open and eat, % of the\nstore's unhealthy products", False),
              ('gap_va', 'Variety-adjusted price change, unhealthy\nminus healthy (log points a year)', True)]
    fig, axes = plt.subplots(2, 2, figsize=(26, 20))
    fig.subplots_adjust(wspace=0.28, hspace=0.5)
    for ax, (c, title, zero) in zip(axes.flat, panels):
        ax.scatter(MIDS, b[c].values, color=RED, s=260, zorder=5, edgecolors='white', linewidth=1, label='New unhealthy products')
        if c == 'open_new_U':
            ax.scatter(MIDS, b['open_all_U'].values, color='white', s=260, zorder=4, edgecolors=GREY, linewidth=2.5, label='All unhealthy products')
            ax.legend(frameon=False, fontsize=21, loc='best')
        if zero:
            ax.axhline(0, color=GREY, lw=1.2, ls='--', zorder=1)
        ax.grid(True, alpha=0.3)
        ax.set_title(title, pad=14); ax.set_xlabel(XLAB)
        ax.set_xticks(MIDS); ax.set_xticklabels(TICKS, rotation=30)
    out = fdir / 'what_moves' / 'variety' / 'store_new_options_by_income.png'
    fig.savefig(out, bbox_inches='tight', dpi=100); plt.close(fig)
    print(f'saved {out}')


def main():
    d = load()
    print('\nAll stores (table):')
    table(d)
    figure(d)
    for lab, dd in [('grocery stores only', d[d.channel_code == 'F']), ('without 2018 and stores first seen 2018 or later', d[(d.year != 2018) & (d.first_obs < 2018)])]:
        print(f'\n{lab}:', {y: tuple(round(v, 3) for v in (lambda r: (r['b'], r['se']))(diff(dd, y))) for y in ['gap_newsh', 'open_tilt_U', 'gap_va']})
    q = d[d.quartile.isin(['bottom', 'top'])]
    print('\nbottom minus top by year (raw means): new-share gap, variety-adjusted price gap')
    for yr, z in q.groupby('year'):
        print(yr, round(z[z.quartile == 'bottom'].gap_newsh.mean() - z[z.quartile == 'top'].gap_newsh.mean(), 3),
              round(z[z.quartile == 'bottom'].gap_va.mean() - z[z.quartile == 'top'].gap_va.mean(), 3))


if __name__ == '__main__':
    main()
