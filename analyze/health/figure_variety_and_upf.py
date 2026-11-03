"""
figure_variety_and_upf.py -- product variety and ultra-processed products over time, for
notes/obesity.tex.

  upc_counts_by_category.png   number of UPCs sold in a county in the year, averaged over the
                               counties present in every year 2007-2020, for selected categories;
                               from the Nielsen Retail Scanner module x county x year file
                               (interim/rms_variety/rms_variety_module_fips_year.parquet, n_upcs).
                               Levels (right) and indexed to 2007 = 100 (left).
  upf_counts.png               ultra-processed products among the UPCs bought by panelists in the year
                               (interim/health/upc_year_spending.parquet, build/health/) that match a
                               USDA ingredient list (interim/usda_nielsen_merged/usda_upf.parquet,
                               clean/usda/build_usda_upf.py, NOVA group-4 markers): the share of matched
                               UPCs and of matched spending that is ultra-processed, the match rate, and
                               the matched counts. The USDA file is a 2024 snapshot, so products gone
                               before about 2018 have no ingredient list and the match rate rises over
                               time; the counts are therefore not a measure of how many such products
                               existed, only the shares are comparable across years.
Outputs: figs/health/
"""
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim')
FIG = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs') / 'health'
FIG.mkdir(parents=True, exist_ok=True)
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10,
                     'axes.spines.top': False, 'axes.spines.right': False})
BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
# categories: Nielsen module descriptions grouped by name
CATS = {
    'Candy': lambda d: d.str.startswith('CANDY'),
    'Ready-to-eat cereal': lambda d: d == 'CEREAL - READY TO EAT',
    'Yogurt': lambda d: d.str.startswith('YOGURT'),
    'Milk': lambda d: d.str.startswith('DAIRY-MILK'),
    'Carbonated soft drinks': lambda d: d.str.startswith('SOFT DRINKS'),
    'Fresh bread': lambda d: d == 'BAKERY - BREAD - FRESH',
    'Salty snacks': lambda d: d.str.startswith('SNACKS'),
    'Ice cream': lambda d: d.str.startswith('ICE CREAM'),
    'Frozen entrees': lambda d: d.str.startswith('ENTREES') & d.str.contains('FROZEN'),
    'Cheese': lambda d: d.str.startswith('CHEESE'),
}
COLORS = plt.cm.tab10(np.linspace(0, 1, 10))


def save(fig, name):
    fig.savefig(FIG / name, bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / name}')


def main():
    u = pd.read_parquet(BASE / 'health' / 'upc_year_spending.parquet')
    u['descr'] = u['product_module_descr'].astype(str).str.upper()
    years = sorted(u['panel_year'].unique())
    hh = u.groupby('panel_year')['n_households'].max()
    print('max households buying any one UPC, by year (panel size proxy):', hh.to_dict())

    # ---- UPC counts by category, from the retail scanner: UPCs per county, balanced counties
    v = pd.read_parquet(BASE / 'rms_variety' / 'rms_variety_module_fips_year.parquet',
                        columns=['product_module_code', 'product_module_descr', 'fips', 'year', 'n_upcs', 'total_spending'])
    v = v[(v['year'] >= 2007) & (v['year'] <= 2020)]
    v['descr'] = v['product_module_descr'].astype(str).str.upper()
    cty_years = v.groupby('fips')['year'].nunique()
    balanced = cty_years[cty_years == 14].index
    v = v[v['fips'].isin(balanced)]
    print(f'counties present in all of 2007-2020: {len(balanced):,}')
    rows = {}
    for name, f in CATS.items():
        m = v[f(v['descr'])]
        per_cty = m.groupby(['fips', 'year'])['n_upcs'].sum()          # UPCs of the category sold in the county that year
        rows[name] = per_cty.groupby('year').mean()
    cnt = pd.DataFrame(rows)
    print('\nmean UPCs per county, by category and year:'); print(cnt.round(0).to_string())
    fig, axes = plt.subplots(1, 2, figsize=(17, 6.5))
    ax = axes[0]
    base_year = 2007
    for (name, s_), c in zip(cnt.items(), COLORS):
        idx = 100 * s_ / s_.loc[base_year]
        ax.plot(idx.index, idx, color=c, lw=2, marker='o', ms=4, label=name)
    ax.axhline(100, color=GREY, lw=1, ls='--')
    ax.set_ylabel(f'UPCs per county, {base_year} = 100')
    ax.set_xlabel('Year'); ax.set_title('Variety by category, indexed')
    ax.legend(frameon=False, ncol=2)
    ax = axes[1]
    for (name, s_), c in zip(cnt.items(), COLORS):
        ax.plot(s_.index, s_, color=c, lw=2, marker='o', ms=4, label=name)
    ax.set_ylabel('UPCs sold per county (mean over counties)')
    ax.set_xlabel('Year'); ax.set_title('Variety by category, levels')
    save(fig, 'upc_counts_by_category.png')

    # ---- ultra-processed
    upf_path = BASE / 'usda_nielsen_merged' / 'usda_upf.parquet'
    if not upf_path.exists():
        print('usda_upf.parquet not found; skipping the ultra-processed figure'); return
    upf = pd.read_parquet(upf_path, columns=['upc13', 'upf', 'upf_strict', 'n_markers'])
    u['upc13'] = u['upc'].astype(str).str.zfill(12).map(lambda x: '0' + x)
    m = u.merge(upf, on='upc13', how='left')
    print(f'\nUPC-years matched to a USDA ingredient list: {m.upf.notna().mean():.3f}; '
          f'spending matched: {m.loc[m.upf.notna(), "spending"].sum() / m.spending.sum():.3f}')
    g = m.groupby('panel_year').apply(lambda d: pd.Series({
        'matched': d['upf'].notna().sum(), 'match_rate': d['upf'].notna().mean(),
        'upf_upcs': (d['upf'] == 1).sum(), 'nonupf_upcs': (d['upf'] == 0).sum(),
        'upf_share_upcs': (d['upf'] == 1).sum() / max(d['upf'].notna().sum(), 1),
        'upf_share_spend': d.loc[d['upf'] == 1, 'spending'].sum() / max(d.loc[d['upf'].notna(), 'spending'].sum(), 1e-9),
        'upf_strict_share_spend': d.loc[d['upf_strict'] == 1, 'spending'].sum() / max(d.loc[d['upf'].notna(), 'spending'].sum(), 1e-9),
    }))
    print(g.round(3).to_string())
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax = axes[0]
    ax.plot(g.index, 100 * g['upf_share_upcs'], color=RED, lw=2.5, marker='o', ms=5, label='Share of matched UPCs bought')
    ax.plot(g.index, 100 * g['upf_share_spend'], color=RED, lw=2, ls='--', marker='s', ms=4, label='Share of matched spending')
    ax.plot(g.index, 100 * g['upf_strict_share_spend'], color=GREY, lw=2, ls=':', marker='^', ms=4, label='Share of matched spending, two or more markers')
    ax.set_ylabel('Percent ultra-processed'); ax.set_ylim(0, 100)
    ax.set_xlabel('Year'); ax.set_title('Ultra-processed share among products with an ingredient list')
    ax.legend(frameon=False, loc='lower left')
    ax = axes[1]
    ax.plot(g.index, g['upf_upcs'] / 1000, color=RED, lw=2.5, marker='o', ms=5, label='Ultra-processed UPCs bought (thousands)')
    ax.plot(g.index, g['nonupf_upcs'] / 1000, color=BLUE, lw=2.5, marker='o', ms=5, label='Not ultra-processed UPCs bought (thousands)')
    ax2 = ax.twinx()
    ax2.plot(g.index, 100 * g['match_rate'], color='black', lw=1.2, ls='-.', label='Share of UPCs bought that have an ingredient list (%, right axis)')
    ax2.set_ylim(0, 100); ax2.set_ylabel('Percent with an ingredient list'); ax2.spines['top'].set_visible(False)
    ax.set_ylabel('UPCs bought by panelists (thousands)')
    ax.set_xlabel('Year'); ax.set_title('Matched counts, and why they rise: label coverage')
    h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, frameon=False, loc='upper left', fontsize=9)
    save(fig, 'upf_counts.png')


if __name__ == '__main__':
    main()
