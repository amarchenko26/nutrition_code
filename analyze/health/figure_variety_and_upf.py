"""
figure_variety_and_upf.py -- product variety and ultra-processed products over time, from the
household purchase files, for notes/obesity.tex.
Reads interim/health/upc_year_spending.parquet (build/health/build_upc_year_spending.py: one
row per UPC x panel year with spending, quantity, households) and, when present,
interim/usda_nielsen_merged/usda_upf.parquet (clean/usda/build_usda_upf.py: NOVA-marker
ultra-processed flag per UPC from USDA ingredient lists).

  upc_counts_by_category.png   distinct UPCs bought by panelists in the year, selected categories,
                               indexed to 2004 = 100 (left) and in levels (right)
  upf_counts.png               distinct UPCs bought per year by ultra-processed status (matched UPCs
                               only), and the ultra-processed share of UPCs and of spending
A UPC counts in a year if at least one panelist bought it; counts therefore depend on the panel's
size (about 40,000 households 2004-2006, 60,000 from 2007), so the indexed panel starts in 2007.
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

    # ---- UPC counts by category
    rows = {}
    for name, f in CATS.items():
        m = u[f(u['descr'])]
        rows[name] = m.groupby('panel_year')['upc'].nunique().reindex(years)
    cnt = pd.DataFrame(rows)
    print('\ndistinct UPCs bought per year:'); print(cnt.to_string())
    fig, axes = plt.subplots(1, 2, figsize=(17, 6.5))
    ax = axes[0]
    base_year = 2007
    for (name, s), c in zip(cnt.items(), COLORS):
        idx = 100 * s / s.loc[base_year]
        ax.plot(idx.index[idx.index >= base_year], idx[idx.index >= base_year], color=c, lw=2, marker='o', ms=4, label=name)
    ax.axhline(100, color=GREY, lw=1, ls='--')
    ax.set_ylabel(f'Distinct UPCs bought, {base_year} = 100')
    ax.set_xlabel('Year'); ax.set_title('Variety by category, indexed')
    ax.legend(frameon=False, ncol=2)
    ax = axes[1]
    for (name, s), c in zip(cnt.items(), COLORS):
        ax.plot(s.index, s / 1000, color=c, lw=2, marker='o', ms=4, label=name)
    ax.set_ylabel('Distinct UPCs bought (thousands)')
    ax.set_xlabel('Year'); ax.set_title('Variety by category, levels')
    ax.axvline(2006.5, color=GREY, lw=1, ls=':')
    ax.text(2006.6, ax.get_ylim()[1] * 0.97, 'panel enlarged', fontsize=9, color=GREY, va='top')
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
    ax.plot(g.index, g['upf_upcs'] / 1000, color=RED, lw=2.5, marker='o', ms=5, label='Ultra-processed (at least one NOVA marker)')
    ax.plot(g.index, g['nonupf_upcs'] / 1000, color=BLUE, lw=2.5, marker='o', ms=5, label='Not ultra-processed')
    ax.set_ylabel('Distinct UPCs bought (thousands), matched to an ingredient list')
    ax.set_xlabel('Year'); ax.set_title('Number of products by ultra-processed status')
    ax.legend(frameon=False)
    ax = axes[1]
    ax.plot(g.index, 100 * g['upf_share_upcs'], color=RED, lw=2.5, marker='o', ms=5, label='Share of matched UPCs')
    ax.plot(g.index, 100 * g['upf_share_spend'], color=RED, lw=2, ls='--', marker='s', ms=4, label='Share of matched spending')
    ax.plot(g.index, 100 * g['upf_strict_share_spend'], color=GREY, lw=2, ls=':', marker='^', ms=4, label='Share of matched spending, two or more markers')
    ax.plot(g.index, 100 * g['match_rate'], color='black', lw=1.2, ls='-.', label='Match rate: UPCs with an ingredient list')
    ax.set_ylabel('Percent'); ax.set_ylim(0, 100)
    ax.set_xlabel('Year'); ax.set_title('Ultra-processed share')
    ax.legend(frameon=False, loc='lower left')
    save(fig, 'upf_counts.png')


if __name__ == '__main__':
    main()
