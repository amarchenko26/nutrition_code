"""
figure_time_vs_money.py -- is the money cost of food per calorie related to its time cost?
For the Time section of notes/obesity.tex.

Module level (interim/health/module_year_price_calories.parquet, build/health/
build_module_price_calories.py; preparation scores interim/rms_variety/prep_time_scores.parquet,
1 = open and eat to 9 = long cooking): price per 1,000 kcal = real spending (2013$) on purchase
rows with a weight or volume size / the calories of those rows at each UPC's time-invariant
density (build_upc_dictionary.py; until 2026-11-04 the module x year label density, which made
the 2004 point of the little-preparation group an artifact of powdered soft drinks). Modules with
calories, a preparation score and at least half of their spending on rows with a size are kept.
  time_vs_money_modules.png   spending-weighted real price per 1,000 kcal by preparation group
                              (1 / 2-3 / 4-9) by year, 2004-2020 (the module scatter that was the
                              left panel was deleted 2026-11-04 at Anya's request; the pooled
                              2016-2020 module statistics are still printed).
Household level (interim/health/nova_calorie_panel.parquet, interim/panel_dataset/
panel_prep_time.parquet, panel_hh_year.parquet; sample as in figure_nutrition_over_time.py):
  time_vs_money_households.png  left: binscatter (20 bins) of the household's real price per 1,000
                              kcal (spending / calories, labelled + imputed) on its spending-weighted
                              preparation score, both residualized on year dummies, projection-
                              weighted; slope and SE clustered on household. Right: the
                              projection-weighted mean preparation score and mean price per 1,000
                              kcal of each within-year income quartile, one point per quartile-year.
Outputs: figs/health/time/
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
import statsmodels.api as sm
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

PD_ = idir / 'panel_dataset'
FIG = fdir / 'health' / 'time'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#7a7a7a'
Q_COL = {1: RED, 2: ORANGE, 3: '#999999', 4: BLUE}
Q_LAB = {1: 'Bottom income quartile', 2: 'Second quartile', 3: 'Third quartile', 4: 'Top income quartile'}
PREP_GROUPS = [('open and eat (1)', 0, 1, RED), ('little preparation (2-3)', 1, 3, ORANGE), ('cooking (4-9)', 3, 9, BLUE)]
CPI = {2004: 188.9, 2005: 195.3, 2006: 201.6, 2007: 207.3, 2008: 215.3, 2009: 214.5, 2010: 218.1, 2011: 224.9,
       2012: 229.6, 2013: 233.0, 2014: 236.7, 2015: 237.0, 2016: 240.0, 2017: 245.1, 2018: 251.1, 2019: 255.7, 2020: 258.8}
YEARS = list(range(2004, 2021))
plt.rcParams.update({'font.size': 12, 'axes.labelsize': 13, 'axes.titlesize': 12.5, 'legend.fontsize': 10,
                     'axes.spines.top': False, 'axes.spines.right': False})


def wmean(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok]) if ok.any() else np.nan


def wcorr(x, y, w):
    mx, my = np.average(x, weights=w), np.average(y, weights=w)
    cov = np.average((x - mx) * (y - my), weights=w)
    return cov / np.sqrt(np.average((x - mx) ** 2, weights=w) * np.average((y - my) ** 2, weights=w))


def modules():
    m = pd.read_parquet(idir / 'health' / 'module_year_price_calories.parquet')
    pt = pd.read_parquet(idir / 'rms_variety' / 'prep_time_scores.parquet', columns=['product_module_code', 'prep_time'])
    m = m.merge(pt, on='product_module_code', how='left')
    m = m[(m['cals_est'] > 0) & m['prep_time'].notna() & (m['spend_grams'] >= 0.5 * m['spend_real'])].copy()
    m['price_kcal'] = 1000 * m['spend_grams'] / m['cals_est']          # 2013$ per 1,000 kcal
    pooled = m[m['panel_year'].between(2016, 2020)].groupby('product_module_code').agg(
        product_module=('product_module', 'first'), prep_time=('prep_time', 'first'), spend=('spend_grams', 'sum'), cals=('cals_est', 'sum')).reset_index()
    pooled['price_kcal'] = 1000 * pooled['spend'] / pooled['cals']
    pooled = pooled[pooled['price_kcal'].between(0.05, 100)]
    pooled['log_price'] = np.log(pooled['price_kcal'])
    print(f'{len(pooled)} modules pooled 2016-2020; spending-weighted corr(prep score, log price per 1,000 kcal) = '
          f'{wcorr(pooled.prep_time.values, pooled.log_price.values, pooled.spend.values):.3f}; '
          f'unweighted {np.corrcoef(pooled.prep_time, pooled.log_price)[0, 1]:.3f}; '
          f'Spearman {pooled[["prep_time", "log_price"]].corr(method="spearman").iloc[0, 1]:.3f}')
    by_score = pooled.groupby('prep_time').apply(lambda g: pd.Series({'price': np.exp(wmean(g['log_price'], g['spend'])), 'price_arith': wmean(g['price_kcal'], g['spend']), 'n': len(g), 'spend_share': g['spend'].sum() / pooled['spend'].sum()}))
    print('\nby preparation score (2016-2020): spending-weighted geometric mean $ per 1,000 kcal, modules, spending share'); print(by_score.round(3).to_string())
    print('\ncheapest and dearest modules per 1,000 kcal:')
    print(pooled.sort_values('price_kcal')[['product_module', 'prep_time', 'price_kcal']].head(8).to_string(index=False))
    print(pooled.sort_values('price_kcal')[['product_module', 'prep_time', 'price_kcal']].tail(8).to_string(index=False))
    # by prep group over time: spending-weighted price per 1,000 kcal
    rows = []
    for lab, lo, hi, _ in PREP_GROUPS:
        g = m[(m['prep_time'] > lo) & (m['prep_time'] <= hi)]
        s = g.groupby('panel_year').apply(lambda x: 1000 * x['spend_grams'].sum() / x['cals_est'].sum())
        rows.append(s.rename(lab))
    allm = m.groupby('panel_year').apply(lambda x: 1000 * x['spend_grams'].sum() / x['cals_est'].sum()).rename('all modules')
    over_time = pd.concat(rows + [allm], axis=1)
    print('\nreal $ per 1,000 kcal by preparation group and year:'); print(over_time.round(2).to_string())
    return pooled, over_time


def households():
    hh = pd.read_parquet(PD_ / 'panel_hh_year.parquet', columns=['household_code', 'panel_year', 'real_income', 'projection_factor'])
    hh = hh[hh['panel_year'].isin(YEARS)].dropna(subset=['real_income', 'projection_factor'])
    p1, p99 = hh['real_income'].quantile([0.01, 0.99]); hh = hh[hh['real_income'].between(p1, p99)]
    cal = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet', columns=['household_code', 'panel_year', 'cals_all', 'spend_all', 'ae_total', 'ae_complete'])
    pt = pd.read_parquet(PD_ / 'panel_prep_time.parquet', columns=['household_code', 'panel_year', 'prep_time_raw'])
    d = hh.merge(cal, on=['household_code', 'panel_year']).merge(pt, on=['household_code', 'panel_year'])
    d = d[(d['ae_complete'] == 1) & (d['cals_all'] > 0)].copy()
    d['cals_ae_day'] = d['cals_all'] / 365 / d['ae_total']
    c1, c99 = d['cals_ae_day'].quantile([0.01, 0.99]); d = d[d['cals_ae_day'].between(c1, c99)]
    d['price_kcal'] = 1000 * d['spend_all'] / d['cals_all'] * (CPI[2013] / d['panel_year'].map(CPI))
    q1, q99 = d['price_kcal'].quantile([0.01, 0.99]); d = d[d['price_kcal'].between(q1, q99)]
    d['inc_q'] = d.groupby('panel_year')['real_income'].transform(lambda s: pd.qcut(s.rank(method='first'), 4, labels=[1, 2, 3, 4])).astype(int)
    print(f'\nhouseholds: {len(d):,} household-years')
    return d.reset_index(drop=True)


def resid_year(df, col, w):
    Y = pd.get_dummies(df['panel_year'], drop_first=True, dtype=float).values
    X = np.column_stack([np.ones(len(df)), Y]); y = df[col].values.astype(float); sw = np.sqrt(w)
    beta = np.linalg.lstsq(X * sw[:, None], y * sw, rcond=None)[0]
    return y - X @ beta + np.average(y, weights=w)


def main():
    pooled, over_time = modules()
    fig, ax = plt.subplots(figsize=(9, 6))
    for lab, lo, hi, color in PREP_GROUPS:
        ax.plot(over_time.index, over_time[lab], color=color, lw=2.5, marker='o', ms=4, label=lab)
    ax.plot(over_time.index, over_time['all modules'], color='black', lw=1.5, ls='--', label='all scored modules')
    ax.set_xlabel('Year'); ax.set_ylabel('Real price per 1,000 kcal (2013\\$)')
    ax.set_ylim(0, over_time.max().max() * 1.25)
    ax.set_title('Price per calorie by preparation group, spending-weighted'); ax.legend(frameon=False)
    fig.savefig(FIG / 'time_vs_money_modules.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "time_vs_money_modules.png"}')

    d = households()
    w = d['projection_factor'].values
    y = resid_year(d, 'price_kcal', w); x = resid_year(d, 'prep_time_raw', w)
    edges = np.percentile(x, np.linspace(0, 100, 21)); edges[-1] += 1; b = np.digitize(x, edges)
    pts = pd.DataFrame([(np.average(x[b == i], weights=w[b == i]), np.average(y[b == i], weights=w[b == i])) for i in range(1, 21)], columns=['x', 'y'])
    fit = sm.WLS(y, sm.add_constant(x), weights=w).fit(cov_type='cluster', cov_kwds={'groups': d['household_code'].values})
    print(f'household price per 1,000 kcal on preparation score: slope {fit.params[1]:.3f} $ per score point (SE {fit.bse[1]:.3f}); '
          f'weighted corr {wcorr(x, y, w):.3f}')
    byq = d.groupby(['inc_q', 'panel_year']).apply(lambda g: pd.Series({'prep': wmean(g['prep_time_raw'], g['projection_factor']), 'price': wmean(g['price_kcal'], g['projection_factor'])})).reset_index()
    print('\nby income quartile (pooled years): mean preparation score and $ per 1,000 kcal')
    print(byq.groupby('inc_q')[['prep', 'price']].mean().round(3).to_string())
    fig, axes = plt.subplots(1, 2, figsize=(17, 6.5))
    ax = axes[0]
    ax.scatter(pts['x'], pts['y'], s=55, color=RED, zorder=4, edgecolors='white', linewidth=0.5)
    xx = np.linspace(pts['x'].min(), pts['x'].max(), 10); ax.plot(xx, fit.params[0] + fit.params[1] * xx, color=GREY, lw=1.5)
    ax.set_xlabel('Household preparation score (spending-weighted mean module score), net of year')
    ax.set_ylabel('Real price per 1,000 kcal (2013\\$), net of year')
    ax.set_title(f'Households: {fit.params[1]:+.2f}\\$ per 1,000 kcal per score point (SE {fit.bse[1]:.2f})')
    ax = axes[1]
    for q in (1, 2, 3, 4):
        g = byq[byq['inc_q'] == q]
        ax.scatter(g['prep'], g['price'], color=Q_COL[q], s=40, label=Q_LAB[q], zorder=4)
        for _, r_ in g.iterrows():
            if r_['panel_year'] in (2004, 2020):
                ax.annotate(str(int(r_['panel_year'])), (r_['prep'], r_['price']), fontsize=8, color=Q_COL[q], xytext=(3, 3), textcoords='offset points')
    ax.set_xlabel('Mean household preparation score'); ax.set_ylabel('Mean real price per 1,000 kcal (2013\\$)')
    ax.set_title('Income quartiles, one point per quartile and year (2004 and 2020 labelled)'); ax.legend(frameon=False)
    fig.savefig(FIG / 'time_vs_money_households.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "time_vs_money_households.png"}')


if __name__ == '__main__':
    main()
