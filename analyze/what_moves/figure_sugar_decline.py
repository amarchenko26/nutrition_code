"""The fall in sugar per 1,000 kcal, 2004-2020: is it real, which products, who cut (what-moves review, 2026-11-05).

Reads interim/what_moves/sugar_module_year.parquet and sugar_hh_year.parquet (build_sugar_decomposition.py:
fixed per-UPC values, so these series move only through the mix of products bought), panel_hh_year
(Syndigo sugar per 1,000 kcal, income, age, weights), hei_usda_panel (HEI added-sugar component),
final/labor_supply_reg_data.dta (USDA-label series, soda spending share, education, kids) and
interim/what_moves/nhanes_sugar.parquet (store-bought sugar per 1,000 kcal from NHANES recalls, no package labels).

Figures (figs/what_moves/sugar/): sugar_series_check.png, sugar_by_group.png
Prints the decomposition of the 2004-2020 change by module that the text of 0_main.tex reports (figure and table deleted 2026-11-06).
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
import pyreadstat
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, RED, ORANGE, GREY, DGREY = '#2c5f8a', '#a63d40', '#fc8d59', '#999999', '#555555'
FIG = fdir / 'what_moves' / 'sugar'
plt.rcParams.update({'font.size': 14, 'axes.labelsize': 15, 'axes.titlesize': 15, 'xtick.labelsize': 13,
                     'ytick.labelsize': 13, 'legend.fontsize': 12, 'axes.spines.top': False, 'axes.spines.right': False})


def wmean(df, col, w='projection_factor'):
    m = df[col].notna() & df[w].notna()
    return np.average(df.loc[m, col], weights=df.loc[m, w]) if m.any() else np.nan


def by_year(df, col, w='projection_factor'):
    return df.groupby('panel_year').apply(lambda g: wmean(g, col, w))


def load():
    mod = pd.read_parquet(idir / 'what_moves' / 'sugar_module_year.parquet')
    hh = pd.read_parquet(idir / 'what_moves' / 'sugar_hh_year.parquet')
    pan = pd.read_parquet(idir / 'panel_dataset' / 'panel_hh_year.parquet',
                          columns=['household_code', 'panel_year', 'sugar_per_1000cal', 'real_income', 'avg_age_hh_head', 'projection_factor'])
    hei = pd.read_parquet(idir / 'panel_dataset' / 'hei_usda_panel.parquet', columns=['household_code', 'panel_year', 'hei_c_addsugar'])
    reg, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'),
                                 usecols=['hhid', 'year', 'sugar_per_1000cal_usda', 'addsugar_per_1000cal_usda', 'spend_soda', 'spend_total', 'hh_avg_yrsofschool', 'kids'])
    reg = reg.rename(columns={'hhid': 'household_code', 'year': 'panel_year'})
    reg['household_code'] = reg['household_code'].astype('int64'); reg['panel_year'] = reg['panel_year'].astype(int)
    reg['soda_share'] = 100 * reg['spend_soda'] / reg['spend_total']
    hh['sugar_fixed'] = 1000 * hh['sugar_g'] / hh['kcal']
    hh['sugar_fixed_lab'] = 1000 * hh['sugar_lab'] / hh['kcal_lab']
    hh['addsug_fixed'] = 1000 * hh['addsug_g'] / hh['kcal']
    hh['lab_share'] = hh['kcal_lab'] / hh['kcal']
    d = pan.merge(hh, on=['household_code', 'panel_year'], how='left').merge(hei, on=['household_code', 'panel_year'], how='left') \
           .merge(reg, on=['household_code', 'panel_year'], how='left')
    for c in ['sugar_fixed', 'sugar_fixed_lab', 'addsug_fixed']:
        lo, hi = d[c].quantile([0.005, 0.995]); d[c] = d[c].clip(lo, hi)
    return mod, d


def fig_series(mod, d):
    yrs = sorted(d['panel_year'].unique())
    s = pd.DataFrame({'Syndigo labels, all purchases': by_year(d, 'sugar_per_1000cal'),
                      'USDA labels, all purchases': by_year(d, 'sugar_per_1000cal_usda'),
                      'USDA labels, products with their own label only': by_year(d, 'sugar_fixed_lab')})
    nh = pd.read_parquet(idir / 'what_moves' / 'nhanes_sugar.parquet')
    nat = mod.groupby('panel_year').agg(sugar=('sugar_g', 'sum'), kcal=('kcal', 'sum'))
    nat['S'] = 1000 * nat['sugar'] / nat['kcal']
    soda = mod[mod['product_module_code'].isin([1484, 1553])].pivot_table(index='panel_year', columns='product_module_code', values='kcal', aggfunc='sum')
    tot_k = mod.groupby('panel_year')['kcal'].sum()
    reg_share = 100 * soda[1484] / tot_k
    diet_share = 100 * soda[1553] / tot_k
    soda_spend = by_year(d, 'soda_share')
    lab = by_year(d, 'lab_share')
    fig, axes = plt.subplots(1, 3, figsize=(21, 6.5))
    fig.subplots_adjust(wspace=0.28)
    ax = axes[0]
    for (lab_, ser), c, ls in zip(s.items(), [BLUE, RED, ORANGE], ['-', '-', '--']):
        ax.plot(ser.index, ser.values, color=c, lw=2.5, ls=ls, marker='o', ms=4, label=f'{lab_}: {ser.iloc[0]:.0f} to {ser.iloc[-1]:.0f} g')
    ax.plot(nh['year_mid'], nh['store_sugar_1000'], color=DGREY, lw=2.5, ls='-', marker='D', ms=6, label=f"NHANES recalls, store-bought food: {nh.store_sugar_1000.iloc[0]:.0f} to {nh.store_sugar_1000.iloc[-1]:.0f} g")
    ax.set_title('Sugar per 1,000 kcal')
    ax.set_ylabel('grams per 1,000 kcal')
    ax.legend(frameon=False, fontsize=10, loc='upper center', bbox_to_anchor=(0.5, -0.14))
    ax = axes[1]
    ax.plot(reg_share.index, reg_share.values, color=RED, lw=2.5, marker='o', ms=4, label=f'regular soda, share of all calories: {reg_share.iloc[0]:.1f}% to {reg_share.iloc[-1]:.1f}%')
    ax.plot(diet_share.index, diet_share.values, color=BLUE, lw=2.5, marker='o', ms=4, label=f'diet soda, share of all calories: {diet_share.iloc[0]:.2f}% to {diet_share.iloc[-1]:.2f}%')
    ax.plot(soda_spend.index, soda_spend.values, color=DGREY, lw=2.5, ls='--', marker='s', ms=4, label=f'soda, share of grocery spending: {soda_spend.iloc[0]:.1f}% to {soda_spend.iloc[-1]:.1f}%')
    ax.set_title('Soda')
    ax.set_ylabel('percent')
    ax.legend(frameon=False, fontsize=10, loc='upper center', bbox_to_anchor=(0.5, -0.14))
    ax = axes[2]
    ax.plot(lab.index, 100 * lab.values, color=DGREY, lw=2.5, marker='o', ms=4, label='share of calories from products with their own USDA label')
    addsug = by_year(d, 'addsugar_per_1000cal_usda'); hc = by_year(d, 'hei_c_addsugar')
    ax2 = ax.twinx()
    ax2.plot(addsug.index, addsug.values, color=ORANGE, lw=2.5, marker='o', ms=4, label=f'USDA added sugar per 1,000 kcal: {addsug.iloc[0]:.0f} to {addsug.iloc[-1]:.0f} g')
    ax2.plot(hc.index, 10 * hc.values, color=BLUE, lw=2.5, ls='--', marker='s', ms=4, label=f'HEI added-sugar component x 10: {10*hc.iloc[0]:.0f} to {10*hc.iloc[-1]:.0f}')
    ax2.set_ylabel('grams; HEI component x 10')
    ax2.spines['right'].set_visible(True)
    ax.set_ylabel('percent of calories')
    ax.set_title('Label coverage and the added-sugar measures')
    h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, frameon=False, fontsize=10, loc='upper center', bbox_to_anchor=(0.5, -0.14))
    for a in axes:
        a.set_xticks(range(2004, 2021, 2)); a.set_xlabel('Panel year')
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / 'sugar_series_check.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print('saved sugar_series_check.png')
    return s, nat, reg_share, diet_share, soda_spend, lab


def decompose(mod, y0, y1):
    a = mod[mod['panel_year'] == y0].groupby(['product_module_code', 'product_module'])[['kcal', 'sugar_g']].sum()
    b = mod[mod['panel_year'] == y1].groupby(['product_module_code', 'product_module'])[['kcal', 'sugar_g']].sum()
    m = a.join(b, lsuffix='0', rsuffix='1', how='outer').fillna(0)
    m['k0'] = m['kcal0'] / m['kcal0'].sum(); m['k1'] = m['kcal1'] / m['kcal1'].sum()
    m['s0'] = 1000 * m['sugar_g0'] / m['kcal0'].replace(0, np.nan); m['s1'] = 1000 * m['sugar_g1'] / m['kcal1'].replace(0, np.nan)
    for c in ['s0', 's1']:
        m[c] = m[c].fillna(m[['s0', 's1']].mean(axis=1)).fillna(0)
    S_avg = ((m['k0'] * m['s0']).sum() + (m['k1'] * m['s1']).sum()) / 2
    # calorie share moved, density held at its average, relative to the average basket: a module with average sugar
    # contributes nothing however much its share moves; the sum over modules is unchanged because shares sum to one
    m['between'] = (m['k1'] - m['k0']) * ((m['s0'] + m['s1']) / 2 - S_avg)
    m['within'] = (m['s1'] - m['s0']) * (m['k0'] + m['k1']) / 2       # density moved, calorie share held at its average
    S0 = (m['k0'] * m['s0']).sum(); S1 = (m['k1'] * m['s1']).sum()
    return m, S0, S1


def print_decomposition(mod):
    m, S0, S1 = decompose(mod, 2004, 2020)
    print(f"national sugar per 1,000 kcal (fixed densities): {S0:.1f} (2004) -> {S1:.1f} (2020), change {S1 - S0:.1f}; between-module {m['between'].sum():.1f}, within-module {m['within'].sum():.1f}")
    m = m.reset_index()
    top = m.reindex(m['between'].abs().sort_values(ascending=False).index).head(10)
    print(top[['product_module', 'k0', 'k1', 's0', 'between']].round(3).to_string(index=False))
    for a, b in [(2004, 2012), (2012, 2020)]:
        mm, s0, s1 = decompose(mod, a, b)
        print(f"  {a}->{b}: {s0:.1f} -> {s1:.1f}; between {mm['between'].sum():+.1f}, within {mm['within'].sum():+.1f}")


def fig_groups(d):
    d = d.copy()
    d['inc_q'] = d.groupby('panel_year')['real_income'].transform(lambda x: pd.qcut(x.rank(method='first'), 4, labels=[1, 2, 3, 4]))
    d['educ'] = pd.cut(d['hh_avg_yrsofschool'], [0, 12, 15.9, np.inf], labels=['12 years or less', '13-15', '16 or more'])
    d['kids_f'] = np.where(d['kids'].isin([9]), 'no children', np.where(d['kids'].notna(), 'children under 18', None))
    d['age_g'] = pd.cut(d['avg_age_hh_head'], [0, 39.9, 64.9, np.inf], labels=['under 40', '40-64', '65 and over'])
    groups = [('inc_q', 'Income quartile (within year)', {1: 'bottom', 2: 'second', 3: 'third', 4: 'top'}), ('educ', 'Education of the heads', None),
              ('kids_f', 'Children', None), ('age_g', 'Average age of the heads', None)]
    fig, axes = plt.subplots(2, 2, figsize=(16, 11))
    fig.subplots_adjust(hspace=0.35, wspace=0.22)
    cols = [BLUE, RED, ORANGE, DGREY]
    table = []
    for ax, (g, title, names) in zip(axes.flat, groups):
        for i, (lev, x) in enumerate(d.dropna(subset=[g]).groupby(g, observed=True)):
            ser = by_year(x, 'sugar_fixed')
            name = names[lev] if names else str(lev)
            ax.plot(ser.index, ser.values, color=cols[i % 4], lw=2.5, marker='o', ms=4,
                    label=f'{name}: {ser.get(2004, np.nan):.0f} (2004), {ser.get(2012, np.nan):.0f} (2012), {ser.get(2020, np.nan):.0f} (2020)')
            table.append((title, name, ser.get(2004, np.nan), ser.get(2012, np.nan), ser.get(2020, np.nan)))
        ax.set_title(title); ax.set_xticks(range(2004, 2021, 2)); ax.legend(frameon=False, fontsize=10)
        ax.set_ylabel('sugar, g per 1,000 kcal')
    fig.suptitle('Sugar per 1,000 kcal by group, all purchases at fixed per-UPC values, projection-weighted household means', fontsize=14)
    fig.savefig(FIG / 'sugar_by_group.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print('saved sugar_by_group.png')
    print(pd.DataFrame(table, columns=['group', 'level', '2004', '2012', '2020']).round(1).to_string(index=False))


def main():
    mod, d = load()
    s, nat, reg_share, diet_share, soda_spend, lab = fig_series(mod, d)
    print(pd.concat([s.round(1), pd.DataFrame({'regular soda kcal %': reg_share.round(2), 'diet soda kcal %': diet_share.round(2), 'soda spend %': soda_spend.round(2), 'labelled kcal share': lab.round(2)})], axis=1).to_string())
    print_decomposition(mod)
    fig_groups(d)


if __name__ == '__main__':
    main()
