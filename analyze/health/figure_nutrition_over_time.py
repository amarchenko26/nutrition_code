"""
figure_nutrition_over_time.py -- the one script for grocery nutrition over time and by income
(notes/obesity.tex). Replaces analyze/hi/syndigo/figure_hi_over_time.py and
analyze/hi/figure_gradient_by_year.py, both deleted 2026-11-03 at Anya's request.

Data: interim/health/nova_calorie_panel.parquet (build/health/build_nova_calorie_panel.py:
household-year grams, calories of all purchase rows at the UPC's time-invariant calorie density
from build_upc_dictionary.py (cals_all: own label, else nearest-sized labelled UPC of the same
brand and module, else brand x module mean, else pooled module density) and of the rows with
their own label (cals_matched), calories by NOVA group, produce calories, adult equivalents),
interim/panel_dataset/hei_usda_panel.parquet (HEI from USDA labels),
interim/panel_dataset/hi_usda_panel.parquet (sugar per 1,000 kcal),
interim/panel_dataset/panel_hh_year.parquet (the paper's household-year sample: real income in
2013$, projection factors, head age, household size).
Sample: household-years in panel_hh_year.parquet, 2004-2020, with a complete roster (adult
equivalents computed), real income trimmed at the pooled 1st and 99th percentiles, calories per
adult equivalent trimmed at the 1st and 99th percentiles. Income quartiles are
within-year quartiles of real income among these households. All means are projection-weighted.
"Ultra-processed" = NOVA group 4 of clean/usda/build_usda_nova.py (ingredient-list markers).

Figures (figs/health/diet/):
  calories_over_time.png          calories per person (per adult equivalent) per day, all purchases
                                  at the product-level density (solid) and products with their own
                                  label only (dashed); right: all purchases by income quartile
  calorie_sources_over_time.png   produce share of all calories; NOVA group shares of labelled
                                  calories (ultra-processed against minimally processed)
  upf_share_over_time.png         ultra-processed share of labelled calories, all households
  upf_by_income_over_time.png     ultra-processed and minimally processed calorie shares by income
                                  quartile
  nutrition_by_income_over_time.png  1x3: HEI by income quartile; sugar per 1,000 kcal by quartile;
                                  produce share of all calories by quartile
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
from matplotlib.ticker import MaxNLocator

PD_ = idir / 'panel_dataset'
FIG = fdir / 'health' / 'diet'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, ORANGE, GREY, GREY2 = '#2c5f8a', '#a63d40', '#fc8d59', '#7a7a7a', '#bbbbbb'
Q_COL = {1: RED, 2: ORANGE, 3: '#999999', 4: BLUE}
Q_LAB = {1: 'Bottom income quartile', 2: 'Second quartile', 3: 'Third quartile', 4: 'Top income quartile'}
NOVA_LAB = {1: 'Unprocessed or minimally processed (NOVA 1)', 2: 'Culinary ingredients (NOVA 2)',
            3: 'Processed (NOVA 3)', 4: 'Ultra-processed (NOVA 4)'}
NOVA_COL = {1: BLUE, 2: GREY2, 3: GREY, 4: RED}
YEARS = list(range(2004, 2021))
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10.5,
                     'axes.spines.top': False, 'axes.spines.right': False})


def wmean(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok]) if ok.any() else np.nan


def by_year(df, col, w='projection_factor'):
    return df.groupby('panel_year').apply(lambda g: wmean(g[col], g[w]))


def by_year_q(df, col, w='projection_factor'):
    return df.groupby(['panel_year', 'inc_q']).apply(lambda g: wmean(g[col], g[w])).unstack()


def plot_q(ax, tab, scale=1.0):
    for q in (1, 2, 3, 4):
        ax.plot(tab.index, scale * tab[q], color=Q_COL[q], lw=2.5 if q in (1, 4) else 1.8, marker='o',
                ms=5 if q in (1, 4) else 4, label=Q_LAB[q])


def load():
    hh = pd.read_parquet(PD_ / 'panel_hh_year.parquet',
                         columns=['household_code', 'panel_year', 'real_income', 'projection_factor', 'avg_age_hh_head', 'household_size'])
    hh = hh[hh['panel_year'].isin(YEARS)].dropna(subset=['real_income', 'projection_factor'])
    p1, p99 = hh['real_income'].quantile([0.01, 0.99])
    hh = hh[hh['real_income'].between(p1, p99)]
    cal = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet')
    hei = pd.read_parquet(PD_ / 'hei_usda_panel.parquet', columns=['household_code', 'panel_year', 'hei_usda'])
    usd = pd.read_parquet(PD_ / 'hi_usda_panel.parquet', columns=['household_code', 'panel_year', 'sugar_per_1000cal_usda'])
    df = hh.merge(cal, on=['household_code', 'panel_year'], how='inner').merge(hei, on=['household_code', 'panel_year'], how='left') \
           .merge(usd, on=['household_code', 'panel_year'], how='left')
    df = df[df['ae_complete'] == 1]
    df['inc_q'] = df.groupby('panel_year')['real_income'].transform(lambda s: pd.qcut(s.rank(method='first'), 4, labels=[1, 2, 3, 4])).astype(int)
    df['cals_ae_day'] = df['cals_all'] / 365 / df['ae_total']
    df['cals_matched_ae_day'] = df['cals_matched'] / 365 / df['ae_total']
    c1, c99 = df['cals_ae_day'].quantile([0.01, 0.99])
    df = df[df['cals_ae_day'].between(c1, c99)]
    nova_cals = df[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    for g in (1, 2, 3, 4):
        df[f'share_nova{g}'] = df[f'cals_nova{g}'] / nova_cals.replace(0, np.nan)
    df['produce_share_all'] = df['produce_cals_all'] / df['cals_all'].replace(0, np.nan)
    df['label_share'] = df['cals_matched'] / df['cals_all'].replace(0, np.nan)
    print(f'{len(df):,} household-years, {df.household_code.nunique():,} households')
    return df


def main():
    df = load()
    cov = df.groupby('panel_year').apply(lambda g: pd.Series({'matched_share': g.cals_matched.sum() / g.cals_all.sum(),
                                                             'nova_share': g[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum().sum() / g.cals_all.sum()}))
    print('label coverage of calories by year (matched / all), NOVA coverage:'); print(cov.round(3).to_string())

    # ---------------------------------------------------------- calories per person
    cal = pd.DataFrame({'all': by_year(df, 'cals_ae_day'), 'matched': by_year(df, 'cals_matched_ae_day'), 'label_share': cov['matched_share']})
    tiers = df.groupby('panel_year').apply(lambda g: pd.Series({f'tier{t}': g[f'cals_tier{t}'].sum() / g['cals_all'].sum() for t in (2, 3, 4)}))
    print('\ncalories per adult equivalent per day (all purchases at the product-level density; own-label rows only):'); print(cal.join(tiers).round(3).to_string())
    calq = by_year_q(df, 'cals_ae_day')
    print(calq.round(0).to_string())
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax = axes[0]
    ax.plot(cal.index, cal['all'], color=RED, lw=2.5, marker='o', label='All grocery purchases (product-level calorie density)')
    ax.plot(cal.index, cal['matched'], color=GREY, lw=1.8, ls='--', marker='s', ms=3, label='Products with their own USDA label only')
    ax.set_ylabel('Calories per adult equivalent per day'); ax.set_xlabel('Year')
    ax.set_ylim(0, cal['all'].max() * 1.25)
    ax.legend(frameon=False, loc='lower left', fontsize=10.5)
    ax.set_title('Calories per person, barcoded grocery purchases')
    ax = axes[1]
    plot_q(ax, calq)
    ax.set_ylabel('Calories per adult equivalent per day'); ax.set_xlabel('Year')
    ax.set_title('Calories per person by income quartile')
    ax.legend(frameon=False, loc='lower left')
    for ax in axes:
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.savefig(FIG / 'calories_over_time.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "calories_over_time.png"}')

    # ---------------------------------------------------------- where the calories come from
    src = by_year(df, 'produce_share_all')
    nova = pd.DataFrame({g: by_year(df, f'share_nova{g}') for g in (1, 2, 3, 4)})
    print('\nproduce share of all calories:'); print((100 * src).round(2).to_string())
    print('\nNOVA shares of labelled calories:'); print((100 * nova).round(1).to_string())
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax = axes[0]
    ax.plot(src.index, 100 * src, color=BLUE, lw=2.5, marker='o')
    ax.set_ylabel('Percent of calories'); ax.set_xlabel('Year'); ax.set_ylim(0, max(8, 100 * src.max() * 1.3))
    ax.set_title('Fruit and vegetables, share of grocery calories')
    ax = axes[1]
    for g in (4, 1, 3, 2):
        ax.plot(nova.index, 100 * nova[g], color=NOVA_COL[g], lw=2.5 if g in (1, 4) else 1.5, marker='o', ms=5 if g in (1, 4) else 3, label=NOVA_LAB[g])
    ax.set_ylabel('Percent of labelled calories'); ax.set_xlabel('Year'); ax.set_ylim(0, 100)
    ax.set_title('Ultra-processed and other calories, share of calories with an ingredient list'); ax.legend(frameon=False, loc='center left')
    for ax in axes:
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.savefig(FIG / 'calorie_sources_over_time.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "calorie_sources_over_time.png"}')

    # ---------------------------------------------------------- ultra-processed share, all households
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot(nova.index, 100 * nova[4], color=RED, lw=2.5, marker='o')
    ax.set_ylabel('Percent of labelled calories'); ax.set_xlabel('Year'); ax.set_ylim(50, 100)
    ax.set_title('Ultra-processed share of grocery calories')
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.savefig(FIG / 'upf_share_over_time.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "upf_share_over_time.png"}')

    # ---------------------------------------------------------- ultra-processed by income
    n4 = by_year_q(df, 'share_nova4'); n1 = by_year_q(df, 'share_nova1')
    print('\nultra-processed share by income quartile:'); print((100 * n4).round(1).to_string())
    print('\nminimally processed share by income quartile:'); print((100 * n1).round(1).to_string())
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax = axes[0]; plot_q(ax, n4, 100)
    ax.set_ylabel('Percent of labelled calories'); ax.set_xlabel('Year')
    ax.set_title('Ultra-processed share of calories, by income quartile'); ax.legend(frameon=False)
    ax = axes[1]; plot_q(ax, n1, 100)
    ax.set_ylabel('Percent of labelled calories'); ax.set_xlabel('Year')
    ax.set_title('Unprocessed or minimally processed share of calories, by income quartile'); ax.legend(frameon=False)
    for ax in axes:
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.savefig(FIG / 'upf_by_income_over_time.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "upf_by_income_over_time.png"}')

    # ---------------------------------------------------------- HEI, sugar, produce by income
    heiq = by_year_q(df, 'hei_usda'); sugq = by_year_q(df, 'sugar_per_1000cal_usda'); prodq = by_year_q(df, 'produce_share_all')
    print('\nHEI by income quartile:'); print(heiq.round(2).to_string())
    print('\nsugar per 1,000 kcal by income quartile:'); print(sugq.round(2).to_string())
    fig, axes = plt.subplots(1, 3, figsize=(21, 6))
    ax = axes[0]; plot_q(ax, heiq)
    ax.set_ylabel('HEI (0-100)'); ax.set_title('HEI by income quartile'); ax.legend(frameon=False)
    ax = axes[1]; plot_q(ax, sugq)
    ax.set_ylabel('Grams of sugar per 1,000 kcal'); ax.set_title('Sugar density by income quartile'); ax.legend(frameon=False)
    ax = axes[2]; plot_q(ax, prodq, 100)
    ax.set_ylabel('Percent of calories'); ax.set_title('Fruit and vegetable share of calories by income quartile'); ax.legend(frameon=False)
    for ax in axes:
        ax.set_xlabel('Year'); ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.savefig(FIG / 'nutrition_by_income_over_time.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "nutrition_by_income_over_time.png"}')


if __name__ == '__main__':
    main()
