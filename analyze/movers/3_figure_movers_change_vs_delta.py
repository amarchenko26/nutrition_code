"""
FGW (2016) Figures III-V with occupations as places and HEI as the outcome, plus
the change in HEI by direction of the move. Reads interim/movers/
movers_events_panel.parquet and occupation_movers_panel.parquet (1_).

Sample: every dated move between employed occupations (emp2emp == 1), all movers,
one-year reversals smoothed in 1_. delta_e = mean HEI in the destination occupation
minus the origin. Pre window: relative years -3..-1; post window: 1..3 (FGW use
2..5; the Nielsen panels are shorter). Matched non-movers: households that never
change code, in the event's ORIGIN occupation in the same calendar year, education
bin, size bin and 5-year age bin (projection-weighted cell mean; fallback
year x origin x age bin, then year x origin).

Outputs (figs/movers/):
  delta_distribution.png       FGW Fig. III: distribution of delta across events
  change_vs_delta.png          FGW Fig. IV: change in HEI on move vs delta (ventiles;
                               right panel one point per origin -> destination)
  premove_vs_delta.png         FGW Fig. V: pre-move HEI, mover minus matched non-mover, vs delta
  hei_change_by_rank_move.png  change in HEI (net of matched non-movers) by income-rank change
Prints (no file) the number of events with both windows and the mean change
under alternative window definitions.
"""

import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data') / 'interim' / 'movers'
OVERLEAF = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf')
FIG = OVERLEAF / 'figs' / 'movers'
BLUE, RED, GREY = '#3b6b9a', '#a63d40', '#7a7a7a'
PRE, POST = (-3, -1), (1, 3)
OCC_LABELS = {1: 'Professional', 2: 'Manager/owner', 3: 'Clerical', 4: 'Sales', 5: 'Craftsman',
              6: 'Operative/driver', 8: 'Service', 9: 'Farmer', 11: 'Laborer', 12: 'Not employed'}

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
    return nm.groupby(keys).apply(lambda d: wmean(d['hei_usda'], d['projection_factor'])).rename('m').reset_index()


def attach_matches(st, df):
    """Matched non-mover cell mean (origin occupation) for every event-year."""
    nm = df[(df['analysis'] == 1) & (df['mover_type'] == 0) & df['hei_usda'].notna()]
    full = cell_means(nm, ['year', 'occ', 'educ_bin', 'size_bin', 'age5']).rename(columns={'m': 'm_full', 'occ': 'origin'})
    mid = cell_means(nm, ['year', 'occ', 'age5']).rename(columns={'m': 'm_mid', 'occ': 'origin'})
    lo = cell_means(nm, ['year', 'occ']).rename(columns={'m': 'm_lo', 'occ': 'origin'})
    st = st.merge(full, on=['year', 'origin', 'educ_bin', 'size_bin', 'age5'], how='left')
    st = st.merge(mid, on=['year', 'origin', 'age5'], how='left')
    st = st.merge(lo, on=['year', 'origin'], how='left')
    st['y_match'] = st['m_full'].fillna(st['m_mid']).fillna(st['m_lo'])
    print(f'matched on the full cell: {st["m_full"].notna().mean():.3f} of event-years')
    return st


def event_frame(st, pre=PRE, post=POST):
    """One row per event: mean HEI and matched mean in the pre and post windows."""
    st = st.copy()
    st['pre'] = st['r'].between(*pre) if pre is not None else st['r'] <= -1
    st['post'] = st['r'].between(*post) if post is not None else st['r'] >= 1
    agg = []
    for flag in ['pre', 'post']:
        s = st[st[flag]].groupby('event_id').agg(**{f'y_{flag}': ('hei_usda', 'mean'),
                                                     f'm_{flag}': ('y_match', 'mean'),
                                                     f'n_{flag}': ('hei_usda', 'size')})
        agg.append(s)
    ev = st.groupby('event_id').agg(hhid=('hhid', 'first'), delta=('delta_hei_usda', 'first'),
                                    origin=('origin', 'first'), dest=('dest', 'first'),
                                    w=('projection_factor', 'mean'), rank_orig=('rank_orig', 'first'),
                                    rank_dest=('rank_dest', 'first'))
    ev = ev.join(agg[0]).join(agg[1])
    ev['dy'] = ev['y_post'] - ev['y_pre']
    ev['dm'] = ev['m_post'] - ev['m_pre']
    ev['pre_gap'] = ev['y_pre'] - ev['m_pre']
    ev['drank'] = ev['rank_dest'] - ev['rank_orig']
    return ev


def slope(x, y, w=None):
    X = sm.add_constant(x)
    res = (sm.WLS(y, X, weights=w) if w is not None else sm.OLS(y, X)).fit(cov_type='HC1')
    return res.params[1], res.bse[1]


def fig_delta_distribution(ev):
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(ev['delta'], bins=np.arange(-5, 5.25, 0.25), color=BLUE, weights=np.ones(len(ev)) / len(ev))
    ax.axvline(0, color=GREY, lw=1.5, ls='--')
    ax.set_xlabel('Destination minus origin mean HEI, $\\hat\\delta_e$')
    ax.set_ylabel('Share of moves')
    ax.set_title('Distribution of the destination–origin HEI gap across moves')
    ax.text(0.98, 0.95, f'Mean = {ev["delta"].mean():.2f}\nSD = {ev["delta"].std():.2f}\nN = {len(ev):,}',
            transform=ax.transAxes, ha='right', va='top', fontsize=12)
    save(fig, 'delta_distribution.png')


def fig_change_vs_delta(ev):
    s = ev.dropna(subset=['dy', 'dm']).copy()
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
    ax.scatter(pts['x'], pts['y'], s=50, color=BLUE, zorder=3, label='Moves, ventiles of $\\hat\\delta_e$')
    xx = np.linspace(pts['x'].min(), pts['x'].max(), 10)
    c = pts['y'].mean() - b * pts['x'].mean()
    ax.plot(xx, c + b * xx, color=BLUE, lw=2.5)
    ax.scatter([0], [match_pt], marker='x', s=120, color=RED, lw=2.5, zorder=4, label='Matched non-movers')
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.set_xlabel('Destination minus origin mean HEI, $\\hat\\delta_e$')
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
                    xytext=(0, 2.4 - 0.45 * i), textcoords='data', ha='center',
                    arrowprops={'arrowstyle': '-', 'color': GREY, 'lw': 0.8})
    ax.set_ylim(-3, 3)
    ax.set_xlabel('Destination minus origin mean HEI, $\\hat\\delta_e$')
    ax.set_title(f'One point per origin→destination (size = moves): slope = {bp:.2f} (SE {sep:.2f})')
    save(fig, 'change_vs_delta.png')
    return {'slope_ventiles': b, 'se_ventiles': se, 'slope_micro': b_micro, 'se_micro': se_micro,
            'slope_pairs': bp, 'se_pairs': sep, 'n_events': len(s), 'match_change': match_pt, 'mean_dy': wmean(s['dy'], s['w'])}


def fig_premove_vs_delta(ev):
    s = ev.dropna(subset=['pre_gap']).copy()
    s['vent'] = pd.qcut(s['delta'].rank(method='first'), 20, labels=False)
    pts = s.groupby('vent').apply(lambda d: pd.Series({'x': wmean(d['delta'], d['w']), 'y': wmean(d['pre_gap'], d['w'])}))
    b, se = slope(pts['x'].values, pts['y'].values)
    fig, ax = plt.subplots(figsize=(8, 5.8))
    ax.scatter(pts['x'], pts['y'], s=50, color=BLUE, zorder=3)
    xx = np.linspace(pts['x'].min(), pts['x'].max(), 10)
    c = pts['y'].mean() - b * pts['x'].mean()
    ax.plot(xx, c + b * xx, color=BLUE, lw=2.5)
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.set_xlabel('Destination minus origin mean HEI, $\\hat\\delta_e$')
    ax.set_ylabel('Pre-move HEI: mover minus matched non-mover')
    ax.set_title(f'Pre-move differences by size of the later move: slope = {b:.2f} (SE {se:.2f})')
    save(fig, 'premove_vs_delta.png')
    return {'slope_pre': b, 'se_pre': se, 'n_pre': len(s), 'mean_pre_gap': wmean(s['pre_gap'], s['w'])}


def fig_rank_change(ev):
    s = ev.dropna(subset=['dy', 'dm']).copy()
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
    ax.set_xticklabels([f'Down\n(n={len(dn):,})', 'matched', f'Up\n(n={len(up):,})', 'matched'], fontsize=10)
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.set_ylabel('Change in HEI (post minus pre)')
    ax.set_title('Raw changes (grey = matched non-movers)')
    save(fig, 'hei_change_by_rank_move.png')
    return {'up_dy': vals[2], 'up_dm': vals[3], 'dn_dy': vals[0], 'dn_dm': vals[1], 'n_up': len(up), 'n_dn': len(dn)}


def window_check(st):
    """How many events have both windows, and the mean change, under other window definitions."""
    print('\nalternative windows (events with both windows; weighted mean change in HEI, SE; slope on delta, SE):')
    for lab, pre, post in [('-3..-1 vs 1..3 (used)', (-3, -1), (1, 3)), ('-2..-1 vs 1..2', (-2, -1), (1, 2)),
                           ('-1 vs 1', (-1, -1), (1, 1)), ('all pre vs all post', None, None),
                           ('-5..-1 vs 1..5', (-5, -1), (1, 5))]:
        e = event_frame(st, pre, post).dropna(subset=['dy'])
        b, se = slope(e['delta'].values, e['dy'].values, e['w'].values)
        print(f'  {lab:24s} n = {len(e):7,}   mean dy = {wmean(e["dy"], e["w"]):+.3f} ({e["dy"].std() / np.sqrt(len(e)):.3f})'
              f'   slope = {b:+.3f} ({se:.3f})')


def main():
    st = pd.read_parquet(BASE / 'movers_events_panel.parquet')
    st = st[(st['emp2emp'] == 1) & st['hei_usda'].notna()].copy()
    df = pd.read_parquet(BASE / 'occupation_movers_panel.parquet')
    st = attach_matches(st, df)
    ev = event_frame(st)
    print(f'events (employed to employed): {len(ev):,}; with pre & post windows: {ev.dropna(subset=["dy"]).shape[0]:,}')
    fig_delta_distribution(ev)
    s4 = fig_change_vs_delta(ev)
    s5 = fig_premove_vs_delta(ev)
    s6 = fig_rank_change(ev)
    for k, v in {**s4, **s5, **s6}.items():
        print(f'{k:16s} {v:.3f}' if isinstance(v, float) else f'{k:16s} {v}')
    window_check(st)
    ev.to_parquet(BASE / 'mover_changes.parquet')


if __name__ == '__main__':
    main()
