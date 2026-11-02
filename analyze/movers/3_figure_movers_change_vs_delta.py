"""
FGW (2016) Figures III-V with occupations as places and HEI as the outcome,
plus the descriptive 'does nutrition improve when households move up'.
Reads interim/movers/occupation_movers_panel.parquet (1_).

delta_i = mean HEI in the destination occupation minus the origin occupation.
Pre window: relative years -3..-1; post window: 1..3 (FGW use 2..5; the Nielsen
panels are shorter). Matched non-movers: non-movers in the mover's ORIGIN
occupation in the same calendar year, education bin, size bin and 5-year age
bin (projection-weighted cell mean; fallback year x origin x age bin, then
year x origin).

Outputs (figs/movers/):
  delta_distribution.png       FGW Fig. III: distribution of delta across movers
  change_vs_delta.png          FGW Fig. IV: change in HEI on move vs delta (ventiles;
                               right panel one point per origin -> destination)
  premove_vs_delta.png         FGW Fig. V: pre-move HEI, mover minus matched non-mover, vs delta
  hei_change_by_rank_move.png  change in HEI (net of matched non-movers) by income-rank change
Outputs (tabs/movers/): change_vs_delta_summary.tex
"""

import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data') / 'interim' / 'movers'
OVERLEAF = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition')
FIG = OVERLEAF / 'figs' / 'movers'
TAB = OVERLEAF / 'tabs' / 'movers'
BLUE, RED, GREY = '#3b6b9a', '#a63d40', '#7a7a7a'
PRE, POST = (-3, -1), (1, 3)
OCC_LABELS = {1: 'Professional', 2: 'Manager/owner', 3: 'Clerical', 4: 'Sales',
              5: 'Craftsman', 6: 'Operative/driver', 8: 'Service', 9: 'Farmer',
              11: 'Laborer', 12: 'Not employed'}

plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 14,
                     'xtick.labelsize': 12, 'ytick.labelsize': 12, 'legend.fontsize': 12,
                     'axes.spines.top': False, 'axes.spines.right': False})


def wmean(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok]) if ok.any() else np.nan


def save(fig, name):
    fig.savefig(FIG / name, bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / name}')


def cell_means(nm, keys):
    g = nm.groupby(keys)
    out = g.apply(lambda d: wmean(d['hei_usda'], d['projection_factor'])).rename('m')
    return out.reset_index()


def build_mover_frame(df):
    an = df[(df['analysis'] == 1) & df['hei_usda'].notna()]
    nm = an[an['mover_type'] == 0]
    mv = an[an['mover_type'] == 1].copy()
    # matched non-mover cell means in the ORIGIN occupation
    keys_full = ['year', 'occ', 'educ_bin', 'size_bin', 'age5']
    full = cell_means(nm, keys_full).rename(columns={'m': 'm_full'})
    mid = cell_means(nm, ['year', 'occ', 'age5']).rename(columns={'m': 'm_mid'})
    lo = cell_means(nm, ['year', 'occ']).rename(columns={'m': 'm_lo'})
    mv['occ_orig'] = mv['origin'].astype(int)
    mv = mv.merge(full.rename(columns={'occ': 'occ_orig'}), on=['year', 'occ_orig', 'educ_bin', 'size_bin', 'age5'], how='left')
    mv = mv.merge(mid.rename(columns={'occ': 'occ_orig'}), on=['year', 'occ_orig', 'age5'], how='left')
    mv = mv.merge(lo.rename(columns={'occ': 'occ_orig'}), on=['year', 'occ_orig'], how='left')
    mv['y_match'] = mv['m_full'].fillna(mv['m_mid']).fillna(mv['m_lo'])
    print(f'matched on full cell: {mv["m_full"].notna().mean():.3f} of mover-years')
    mv['pre'] = mv['r'].between(*PRE)
    mv['post'] = mv['r'].between(*POST)
    agg = []
    for flag in ['pre', 'post']:
        s = mv[mv[flag]].groupby('hhid').agg(**{f'y_{flag}': ('hei_usda', 'mean'),
                                                 f'm_{flag}': ('y_match', 'mean'),
                                                 f'n_{flag}': ('hei_usda', 'size')})
        agg.append(s)
    hh = mv.groupby('hhid').agg(delta=('delta_hei_usda', 'first'), origin=('origin', 'first'),
                                dest=('dest', 'first'), w=('projection_factor', 'mean'),
                                rank_orig=('rank_orig', 'first'), rank_dest=('rank_dest', 'first'),
                                age_move=('avg_age_hh_head', 'mean'))
    hh = hh.join(agg[0]).join(agg[1])
    hh['dy'] = hh['y_post'] - hh['y_pre']
    hh['dm'] = hh['m_post'] - hh['m_pre']
    hh['pre_gap'] = hh['y_pre'] - hh['m_pre']
    hh['drank'] = hh['rank_dest'] - hh['rank_orig']
    return hh


def slope(x, y, w=None):
    X = sm.add_constant(x)
    res = (sm.WLS(y, X, weights=w) if w is not None else sm.OLS(y, X)).fit(cov_type='HC1')
    return res.params[1], res.bse[1]


def fig_delta_distribution(hh):
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(hh['delta'], bins=np.arange(-5, 5.25, 0.25), color=BLUE, weights=np.ones(len(hh)) / len(hh))
    ax.axvline(0, color=GREY, lw=1.5, ls='--')
    ax.set_xlabel('Destination minus origin mean HEI, $\\hat\\delta_i$')
    ax.set_ylabel('Share of movers')
    ax.set_title('Distribution of the destination–origin HEI gap across movers')
    ax.text(0.98, 0.95, f'Mean = {hh["delta"].mean():.2f}\nSD = {hh["delta"].std():.2f}\nN = {len(hh):,}',
            transform=ax.transAxes, ha='right', va='top', fontsize=12)
    save(fig, 'delta_distribution.png')


def fig_change_vs_delta(hh):
    s = hh.dropna(subset=['dy', 'dm']).copy()
    s['vent'] = pd.qcut(s['delta'].rank(method='first'), 20, labels=False)
    pts = s.groupby('vent').apply(lambda d: pd.Series({'x': wmean(d['delta'], d['w']), 'y': wmean(d['dy'], d['w']), 'n': len(d)}))
    b, se = slope(pts['x'].values, pts['y'].values)
    b_micro, se_micro = slope(s['delta'].values, s['dy'].values, s['w'].values)
    match_pt = wmean(s['dm'], s['w'])

    pairs = s.groupby(['origin', 'dest']).apply(lambda d: pd.Series({'x': d['delta'].iloc[0], 'y': wmean(d['dy'], d['w']), 'n': len(d)}))
    pairs = pairs[pairs['n'] >= 30]
    bp, sep = slope(pairs['x'].values, pairs['y'].values, pairs['n'].values)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.8), sharey=True)
    ax = axes[0]
    ax.scatter(pts['x'], pts['y'], s=50, color=BLUE, zorder=3, label='Movers, ventiles of $\\hat\\delta_i$')
    xx = np.linspace(pts['x'].min(), pts['x'].max(), 10)
    c = pts['y'].mean() - b * pts['x'].mean()
    ax.plot(xx, c + b * xx, color=BLUE, lw=2.5)
    ax.plot(xx, c + 1.0 * (xx - 0) + (-c + c), color=GREY, lw=1.2, ls=':', alpha=0)  # placeholder keeps limits
    ax.scatter([0], [match_pt], marker='x', s=120, color=RED, lw=2.5, zorder=4, label='Matched non-movers')
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.set_xlabel('Destination minus origin mean HEI, $\\hat\\delta_i$')
    ax.set_ylabel('Change in HEI on move (post minus pre)')
    ax.set_title(f'Ventiles: slope = {b:.2f} (SE {se:.2f})')
    ax.legend(loc='upper left', frameon=False)
    ax = axes[1]
    ax.scatter(pairs['x'], pairs['y'], s=np.sqrt(pairs['n']) * 4, color=BLUE, alpha=0.6, zorder=3)
    xx = np.linspace(pairs['x'].min(), pairs['x'].max(), 10)
    cp = np.average(pairs['y'], weights=pairs['n']) - bp * np.average(pairs['x'], weights=pairs['n'])
    ax.plot(xx, cp + bp * xx, color=BLUE, lw=2.5)
    ax.axhline(0, color=GREY, lw=1, ls='--')
    big = pairs.sort_values('n', ascending=False).head(3)
    for i, ((o, d), r) in enumerate(big.iterrows()):
        ax.annotate(f'{OCC_LABELS[int(o)]}→{OCC_LABELS[int(d)]}', (r['x'], r['y']), fontsize=9,
                    xytext=(8, 10 - 14 * i), textcoords='offset points')
    ax.set_ylim(-3, 3)
    ax.set_xlabel('Destination minus origin mean HEI, $\\hat\\delta_i$')
    ax.set_title(f'One point per origin→destination (size = movers): slope = {bp:.2f} (SE {sep:.2f})')
    save(fig, 'change_vs_delta.png')
    return {'slope_ventiles': b, 'se_ventiles': se, 'slope_micro': b_micro, 'se_micro': se_micro,
            'slope_pairs': bp, 'se_pairs': sep, 'n_movers': len(s), 'match_change': match_pt,
            'mean_dy': wmean(s['dy'], s['w'])}


def fig_premove_vs_delta(hh):
    s = hh.dropna(subset=['pre_gap']).copy()
    s['vent'] = pd.qcut(s['delta'].rank(method='first'), 20, labels=False)
    pts = s.groupby('vent').apply(lambda d: pd.Series({'x': wmean(d['delta'], d['w']), 'y': wmean(d['pre_gap'], d['w'])}))
    b, se = slope(pts['x'].values, pts['y'].values)
    fig, ax = plt.subplots(figsize=(8, 5.8))
    ax.scatter(pts['x'], pts['y'], s=50, color=BLUE, zorder=3)
    xx = np.linspace(pts['x'].min(), pts['x'].max(), 10)
    c = pts['y'].mean() - b * pts['x'].mean()
    ax.plot(xx, c + b * xx, color=BLUE, lw=2.5)
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.set_xlabel('Destination minus origin mean HEI, $\\hat\\delta_i$')
    ax.set_ylabel('Pre-move HEI: mover minus matched non-mover')
    ax.set_title(f'Pre-move differences by size of the later move: slope = {b:.2f} (SE {se:.2f})')
    save(fig, 'premove_vs_delta.png')
    return {'slope_pre': b, 'se_pre': se, 'n_pre': len(s), 'mean_pre_gap': wmean(s['pre_gap'], s['w'])}


def fig_rank_change(hh):
    s = hh.dropna(subset=['dy', 'dm']).copy()
    s['net'] = s['dy'] - s['dm']
    s['grp'] = s['drank'].clip(-4, 4)
    order = [-4, -3, -2, -1, 1, 2, 3, 4]
    g = s.groupby('grp').apply(lambda d: pd.Series({'m': wmean(d['net'], d['w']), 'se': d['net'].std() / np.sqrt(len(d)), 'n': len(d)}))
    g = g.reindex(order)
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.5), gridspec_kw={'width_ratios': [2, 1.1]})
    ax = axes[0]
    x = np.arange(len(order))
    ax.bar(x, g['m'], yerr=1.96 * g['se'], color=[RED if k < 0 else BLUE for k in order], capsize=3, error_kw={'lw': 1.5})
    for i, k in enumerate(order):
        ax.text(i, (g['m'].iloc[i] + 1.96 * g['se'].iloc[i]) + 0.05, f'n={int(g["n"].iloc[i]):,}', ha='center', fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(['−4 or more', '−3', '−2', '−1', '+1', '+2', '+3', '+4 or more'])
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.set_xlabel('Change in income rank of occupation (12 codes), origin → destination')
    ax.set_ylabel('Change in HEI, net of matched non-movers')
    ax.set_title('Change in HEI on move by direction and size of the move')
    ax = axes[1]
    up = s[s['drank'] > 0]
    dn = s[s['drank'] < 0]
    vals = [wmean(dn['dy'], dn['w']), wmean(dn['dm'], dn['w']), wmean(up['dy'], up['w']), wmean(up['dm'], up['w'])]
    ses = [dn['dy'].std() / np.sqrt(len(dn)), dn['dm'].std() / np.sqrt(len(dn)), up['dy'].std() / np.sqrt(len(up)), up['dm'].std() / np.sqrt(len(up))]
    ax.bar([0, 1, 3, 4], vals, yerr=[1.96 * v for v in ses], color=[RED, GREY, BLUE, GREY], capsize=3, error_kw={'lw': 1.5})
    ax.set_xticks([0, 1, 3, 4])
    ax.set_xticklabels([f'Down\n(n={len(dn):,})', 'matched\nnon-movers', f'Up\n(n={len(up):,})', 'matched\nnon-movers'], fontsize=10)
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.set_ylabel('Change in HEI (post minus pre)')
    ax.set_title('Raw changes')
    save(fig, 'hei_change_by_rank_move.png')
    return {'up_dy': vals[2], 'up_dm': vals[3], 'dn_dy': vals[0], 'dn_dm': vals[1], 'n_up': len(up), 'n_dn': len(dn)}


def main():
    df = pd.read_parquet(BASE / 'occupation_movers_panel.parquet')
    hh = build_mover_frame(df)
    print(f'single movers: {len(hh):,}; with pre & post windows: {hh.dropna(subset=["dy"]).shape[0]:,}')
    fig_delta_distribution(hh)
    s4 = fig_change_vs_delta(hh)
    s5 = fig_premove_vs_delta(hh)
    s6 = fig_rank_change(hh)
    rows = [
        ('Movers with both windows', f'{s4["n_movers"]:,}'),
        ('Mean change in HEI on move (post minus pre)', f'{s4["mean_dy"]:.2f}'),
        ('\\quad matched non-movers over the same years', f'{s4["match_change"]:.2f}'),
        ('Slope of change on $\\hat\\delta_i$, ventiles (SE)', f'{s4["slope_ventiles"]:.2f} ({s4["se_ventiles"]:.2f})'),
        ('\\quad mover-level regression (SE)', f'{s4["slope_micro"]:.2f} ({s4["se_micro"]:.2f})'),
        ('\\quad origin--destination pairs, weighted (SE)', f'{s4["slope_pairs"]:.2f} ({s4["se_pairs"]:.2f})'),
        ('Pre-move mover minus matched non-mover, mean', f'{s5["mean_pre_gap"]:.2f}'),
        ('\\quad slope on $\\hat\\delta_i$, ventiles (SE)', f'{s5["slope_pre"]:.2f} ({s5["se_pre"]:.2f})'),
        ('Up-movers: change in HEI / matched non-movers', f'{s6["up_dy"]:.2f} / {s6["up_dm"]:.2f}'),
        ('Down-movers: change in HEI / matched non-movers', f'{s6["dn_dy"]:.2f} / {s6["dn_dm"]:.2f}'),
    ]
    with open(TAB / 'change_vs_delta_summary.tex', 'w') as fh:
        fh.write('\\begin{tabular}{lr}\n\\toprule\n')
        for a, b in rows:
            fh.write(f'{a} & {b} \\\\\n')
        fh.write('\\bottomrule\n\\end{tabular}\n')
    for a, b in rows:
        print(f'{a:60s} {b}')
    hh.to_parquet(BASE / 'mover_changes.parquet')


if __name__ == '__main__':
    main()
