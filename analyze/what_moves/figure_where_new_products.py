"""Where the new food products are, and the national module-level Jaravel facts (2026-11-05).

Figure figs/what_moves/variety/where_new_products.png, rows = module groupings (healthiness, ultra-processed share,
preparation time), columns: (1) share of spending on products new to the nation that year, RMS, spending-weighted across
modules; (2) UPCs sold per module, modules present in every year 2009-2020, 2009 = 100; (3) what the new UPCs of 2008-2020 are
(build_new_product_types.py): new brand, new variant of an existing brand, new size of an existing product, store brand, re-coded.
Table tabs/what_moves/variety_national_modules.tex: across modules (WLS on 2009-2011 spending, robust SE), the change from
2009-2011 to 2018-2020 in spending, in UPCs, in the price of continuing products and in the variety-adjusted price, and the mean
new-product share, on the module's buyer-income tilt (rich_z, capped at +/-4) and on its group; prints the slope of price
change on nominal spending growth (nominal, so no division bias from deflating by the same price) and the group gaps without the
four chip modules, whose new-product share is 30-36% in odd years and 3-5% in even years.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.stars import stars, star_num, est

import numpy as np
import pandas as pd
import pyreadstat
import statsmodels.formula.api as smf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, RED, GREY, ORANGE, LGREY = '#2c5f8a', '#a63d40', '#999999', '#fc8d59', '#cccccc'
plt.rcParams.update({'font.size': 24, 'axes.labelsize': 24, 'axes.titlesize': 25, 'xtick.labelsize': 22, 'ytick.labelsize': 22,
                     'legend.fontsize': 20, 'axes.spines.top': False, 'axes.spines.right': False})
GROUPS = {'hi_group': ('Healthiness', {1.0: ('Unhealthy', RED), 2.0: ('Middle', ORANGE), 3.0: ('Healthy', BLUE)}),
          'upf_group': ('Ultra-processed', {2.0: ('Mostly UPF', RED), 1.0: ('Mostly not', BLUE)}),
          'prep_group': ('Preparation', {1.0: ('Open and eat', RED), 2.0: ('Little prep', ORANGE), 3.0: ('Cooking', BLUE)})}
TYPES = [('new variant', 'New variant, same brand', BLUE), ('new size', 'New size, same product', ORANGE), ('new brand', 'New brand', RED),
         ('store brand', 'Store brand', GREY), ('re-coded', 'Re-coded', LGREY)]


def load_modules():
    d, _ = pyreadstat.read_dta(str(idir / 'panel_dataset' / 'innovation_reg_data.dta'), usecols=['module_code', 'year', 'spending', 'ssnp', 'n_upcs', 'n_new_upcs', 'share_new_upcs'])
    d = d.rename(columns={'module_code': 'product_module_code'}); d['year'] = d['year'].astype(int); d['product_module_code'] = d['product_module_code'].astype(int)
    g = pd.read_parquet(idir / 'what_moves' / 'module_groups.parquet', columns=['product_module_code', 'product_module_descr', 'hi_group', 'upf_group', 'prep_group', 'eta_iv', 'upf_share', 'claude_hi'])
    r = pd.read_parquet(idir / 'what_moves' / 'module_buyer_income.parquet', columns=['product_module_code', 'rich_z'])
    g = g.merge(r, on='product_module_code', how='left')
    g['rich_z'] = g['rich_z'].clip(-4, 4)
    return d, g


def fig_where(d, g):
    ty = pd.read_parquet(idir / 'what_moves' / 'new_product_types_by_group.parquet')
    m = d.merge(g, on='product_module_code')
    bal = d[d['spending'] > 0].groupby('product_module_code')['year'].nunique()
    bal = bal[bal == d['year'].nunique()].index
    fig, axes = plt.subplots(3, 3, figsize=(30, 24), gridspec_kw={'width_ratios': [1, 1, 1.1]})
    fig.subplots_adjust(wspace=0.34, hspace=0.42)
    summary = {}
    for i, (gv, (gname, gl)) in enumerate(GROUPS.items()):
        ax = axes[i, 0]
        for k, (lab, col) in gl.items():
            z = m[m[gv] == k].groupby('year').apply(lambda t: 100 * np.average(t['ssnp'], weights=t['spending']))
            ax.plot(z.index, z.values, color=col, lw=3, marker='o', ms=5, label=lab)
            summary[(gname, lab)] = {'ssnp_mean': z.mean(), 'ssnp_2010': z.get(2010), 'ssnp_2019': z.get(2019)}
        ax.set_ylabel('% of spending'); ax.set_title(f'{gname}: new-product share'); ax.set_ylim(bottom=0); ax.set_xticks([2010, 2013, 2016, 2019])
        ax = axes[i, 1]
        for k, (lab, col) in gl.items():
            t = m[(m[gv] == k) & m['product_module_code'].isin(bal)].groupby('year')['n_upcs'].sum()
            z = 100 * t / t.loc[t.index.min()]
            ax.plot(z.index, z.values, color=col, lw=3, marker='o', ms=5, label=lab)
            n = m[(m[gv] == k) & m['product_module_code'].isin(bal)]['product_module_code'].nunique()
            summary[(gname, lab)].update({'upcs_per_module_2009': t.loc[t.index.min()] / n, 'upcs_2020_index': z.iloc[-1], 'upcs_2019_index': z.get(2019)})
        ax.axhline(100, color=GREY, lw=1, ls='--'); ax.set_ylabel('2009 = 100'); ax.set_title(f'{gname}: UPCs sold'); ax.set_xticks([2010, 2013, 2016, 2019]); ax.legend(frameon=False, loc='upper left')
        ax = axes[i, 2]
        t = ty[ty['grouping'] == gv].groupby(['group', 'type'])['n'].sum().unstack(fill_value=0)
        t = t.div(t.sum(axis=1), axis=0) * 100
        keys = list(gl.keys())
        ypos = np.arange(len(keys))[::-1]
        left = np.zeros(len(keys))
        for tkey, tlab, col in TYPES:
            vals = np.array([t.loc[k, tkey] for k in keys])
            ax.barh(ypos, vals, left=left, color=col, label=tlab, height=0.62)
            for yy, v, l in zip(ypos, vals, left):
                if v >= 7:
                    ax.text(l + v / 2, yy, f'{v:.0f}', ha='center', va='center', fontsize=20, color='white' if col in (BLUE, RED, GREY) else 'black')
            left += vals
        ax.set_yticks(ypos); ax.set_yticklabels([gl[k][0] for k in keys]); ax.set_xlim(0, 100); ax.set_xlabel('% of new UPCs, 2008-2020'); ax.set_title(f'{gname}: types of new UPCs')
        if i == 0:
            fig.legend(*ax.get_legend_handles_labels(), frameon=False, ncol=5, loc='upper center', bbox_to_anchor=(0.5, 0.945), fontsize=22)
    for ax in axes[:, :2].flat:
        ax.set_xlabel('Year')
    out = fdir / 'what_moves' / 'variety' / 'where_new_products.png'
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches='tight', dpi=130)
    print(f'saved {out}')
    print(pd.DataFrame(summary).T.round(2).to_string())
    # new UPCs per year per module by group, and rate relative to the stock
    tt = ty.groupby(['grouping', 'group'])['n'].sum()
    print(tt.to_string())


def module_long_diff(d, g):
    pi = pd.read_parquet(idir / 'rms_variety' / 'price_index_module_year.parquet')
    w0, w1 = [2009, 2010, 2011], [2018, 2019, 2020]
    a0 = d[d.year.isin(w0)].groupby('product_module_code').agg(sp0=('spending', 'mean'), n0=('n_upcs', 'mean'))
    a1 = d[d.year.isin(w1)].groupby('product_module_code').agg(sp1=('spending', 'mean'), n1=('n_upcs', 'mean'))
    m = a0.join(a1, how='inner')
    m = m[(m['sp0'] > 0) & (m['sp1'] > 0)]
    m['dlog_spend'] = np.log(m['sp1'] / m['sp0']); m['dlog_nupcs'] = np.log(m['n1'] / m['n0'])
    w = d[d.year.between(2009, 2020)].groupby('product_module_code').apply(lambda t: 100 * np.average(t['ssnp'], weights=t['spending']) if t['spending'].sum() > 0 else np.nan).rename('ssnp_mean')
    m = m.join(w)
    p0 = pi[pi.year.isin(w0)].groupby('product_module_code')[['level_ces', 'level_va']].mean(); p1 = pi[pi.year.isin(w1)].groupby('product_module_code')[['level_ces', 'level_va']].mean()
    m['dlog_ces'] = np.log(p1['level_ces'] / p0['level_ces']); m['dlog_va'] = np.log(p1['level_va'] / p0['level_va'])
    m = m.reset_index().merge(g, on='product_module_code', how='left')
    m = m[m['dlog_ces'].abs() < 1.5]
    return m


def table_modules(m):
    ys = [('dlog_spend', 'Spending, log change'), ('dlog_nupcs', 'UPCs, log change'), ('ssnp_mean', 'New-product share of spending, \\%'),
          ('dlog_ces', 'Price of continuing products, log change'), ('dlog_va', 'Variety-adjusted price, log change')]
    lines = ['\\begin{tabular}{lccccc}', '\\toprule', ' & ' + ' & '.join(f'({i})' for i in range(1, 6)) + ' \\\\',
             ' & Spending & UPCs & New-product & Continuing- & Variety-adj. \\\\', ' & log chg. & log chg. & share, \\% & product price & price \\\\', '\\midrule']
    res = {}
    terms = [('rich_z', 'Buyers richer by 1 SD')]
    fit = {}
    for y, _ in ys:
        fit[y] = smf.wls(f'{y} ~ rich_z', data=m.dropna(subset=['rich_z', y]), weights=m.dropna(subset=['rich_z', y])['sp0']).fit(cov_type='HC1')
    lines.append('Buyers richer by 1 SD & ' + ' & '.join(star_num(fit[y].params['rich_z'], fit[y].bse['rich_z'], 3) for y, _ in ys) + ' \\\\')
    lines.append(' & ' + ' & '.join(f"({fit[y].bse['rich_z']:.3f})" for y, _ in ys) + ' \\\\')
    lines.append('\\midrule')
    for gv, base, labs in [('upf_group', 1.0, {2.0: 'Mostly ultra-processed (vs not)'}), ('hi_group', 3.0, {1.0: 'Unhealthy (vs healthy)', 2.0: 'Middle (vs healthy)'}), ('prep_group', 3.0, {1.0: 'Open and eat (vs cooking)', 2.0: 'Little preparation (vs cooking)'})]:
        fg = {}
        for y, _ in ys:
            z = m.dropna(subset=[gv, y])
            fg[y] = smf.wls(f'{y} ~ C({gv}, Treatment(reference={base}))', data=z, weights=z['sp0']).fit(cov_type='HC1')
        for k, lab in labs.items():
            nm = f'C({gv}, Treatment(reference={base}))[T.{k}]'
            lines.append(f'{lab} & ' + ' & '.join(star_num(fg[y].params[nm], fg[y].bse[nm], 3) for y, _ in ys) + ' \\\\')
            lines.append(' & ' + ' & '.join(f"({fg[y].bse[nm]:.3f})" for y, _ in ys) + ' \\\\')
            res[lab] = {y: (round(fg[y].params[nm], 3), round(fg[y].bse[nm], 3)) for y, _ in ys}
        lines.append('\\midrule')
    lines.append('Modules & ' + ' & '.join(str(int(fit[y].nobs)) for y, _ in ys) + ' \\\\')
    lines += ['\\bottomrule', '\\end{tabular}']
    out = tdir / 'what_moves' / 'variety_national_modules.tex'
    out.write_text('\n'.join(lines) + '\n')
    print(f'saved {out}')
    print('rich_z:', {y: (round(fit[y].params['rich_z'], 3), round(fit[y].bse['rich_z'], 3)) for y, _ in ys})
    for k, v in res.items():
        print(k, v)
    # group means
    for gv in GROUPS:
        print(gv, m.dropna(subset=[gv]).groupby(gv).apply(lambda z: pd.Series({y: np.average(z[y].dropna(), weights=z.loc[z[y].notna(), 'sp0']) for y, _ in ys})).round(3).to_dict('index'))


def print_without_chips(m):
    """Salty-snack modules count 30-36% of spending as new in odd years, 3-5% in even years: a manufacturer re-coding its bags."""
    chips = m['product_module_descr'].fillna('').str.contains('POTATO CHIPS|TORTILLA CHIPS|PUFFED CHEESE|CORN CHIPS')
    print(f'chip modules {int(chips.sum())}, {100 * m.loc[chips, "sp0"].sum() / m["sp0"].sum():.1f}% of spending')
    for gv, base, others in [('hi_group', 3.0, [1.0, 2.0]), ('upf_group', 1.0, [2.0]), ('prep_group', 3.0, [1.0, 2.0])]:
        for label, z in [('all', m), ('no chips', m[~chips])]:
            z = z.dropna(subset=[gv])
            for y in ['ssnp_mean', 'dlog_va', 'dlog_ces']:
                r = smf.wls(f'{y} ~ C({gv}, Treatment(reference={base}))', data=z.dropna(subset=[y]), weights=z.dropna(subset=[y])['sp0']).fit(cov_type='HC1')
                print(gv, label, y, {k: (round(r.params[f'C({gv}, Treatment(reference={base}))[T.{k}]'], 3), round(r.bse[f'C({gv}, Treatment(reference={base}))[T.{k}]'], 3)) for k in others})


def print_price_slope(m):
    """The one clean spending-growth slope: the variety-adjusted price and the new-product share contain new-product spending, so
    their slopes on spending growth are mechanical. Continuing-product price on nominal spending growth, across modules."""
    z = m.dropna(subset=['dlog_spend', 'dlog_ces'])
    z = z[z['dlog_spend'].abs() < 2]
    r = smf.wls('dlog_ces ~ dlog_spend', data=z, weights=z['sp0']).fit(cov_type='HC1')
    print(f'continuing-product price on spending growth: {r.params["dlog_spend"]:.3f} ({r.bse["dlog_spend"]:.3f}), {int(r.nobs)} modules')


def main():
    d, g = load_modules()
    fig_where(d, g)
    m = module_long_diff(d, g)
    print(f'{len(m)} modules; spending growth mean {np.average(m.dlog_spend, weights=m.sp0):.3f}, CES {np.average(m.dlog_ces, weights=m.sp0):.3f}, VA {np.average(m.dlog_va, weights=m.sp0):.3f}')
    table_modules(m)
    print_without_chips(m)
    print_price_slope(m)


if __name__ == '__main__':
    main()
