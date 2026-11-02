"""
Descriptive figures and tables on occupation moves and the occupation gradient
in nutrition (occupation-movers design). Reads interim/movers/
occupation_movers_panel.parquet, movers_events_panel.parquet and
occupation_means.csv (1_). Occupation codes are the reversal-smoothed ones.

Outputs (figs/movers/, tabs/movers/):
  occupation_histogram.png               household-years by Nielsen occupation code (12 codes)
  transition_heatmap.png                 origin (t-1) x destination (t) transition matrix, row percent
  top_moves.png                          the 20 most common origin -> destination moves
  hei_by_occupation.png                  mean HEI by occupation, raw and age/year-adjusted
  hei_gradient_income_vs_occupation.png  HEI net of age, kids and year: income binscatter and the
                                         occupation means on the same income axis, with both slopes
  occupation_means.tex                   table: N, income, age, HEI, simple indices, in/out-move rates
  moves_summary.tex                      table: ever-move shares, reversals removed, events, up/down counts
Retired 2026-11-02: ever_move_by_tenure.png, move_symmetry_pairs.png.
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

OCC_LABELS = {1: 'Professional', 2: 'Manager/owner', 3: 'Clerical', 4: 'Sales',
              5: 'Craftsman', 6: 'Operative/driver', 7: 'Armed forces',
              8: 'Service', 9: 'Farmer', 10: 'Student <30h', 11: 'Laborer',
              12: 'Not employed'}
SHORT = {1: 'Prof', 2: 'Mgr', 3: 'Cler', 4: 'Sales', 5: 'Craft', 6: 'Oper', 7: 'Army',
         8: 'Serv', 9: 'Farm', 10: 'Stud', 11: 'Lab', 12: 'NotEmp'}
EMPLOYED = [1, 2, 3, 4, 5, 6, 8, 9, 11]
BLUE, RED, GREY = '#3b6b9a', '#a63d40', '#7a7a7a'

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


def occupation_histogram(df):
    counts = df.groupby('occ').agg(hh_years=('hhid', 'size'), households=('hhid', 'nunique'))
    counts = counts.reindex(range(1, 13)).fillna(0)
    fig, ax = plt.subplots(figsize=(11, 5.5))
    x = np.arange(12)
    ax.bar(x, counts['hh_years'] / 1000, color=BLUE, width=0.7)
    for i, v in enumerate(counts['hh_years']):
        ax.text(i, v / 1000 + 3, f'{100 * v / counts["hh_years"].sum():.1f}%', ha='center', fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels([f'{OCC_LABELS[k]}\n({k})' for k in range(1, 13)], rotation=45, ha='right')
    ax.set_ylabel('Household-years (thousands)')
    ax.set_title('Household-years by occupation code')
    save(fig, 'occupation_histogram.png')
    return counts


def transitions(df):
    pairs = df[(df['gap'] == 1) & df['occ_prev'].notna()].copy()
    pairs['occ_prev'] = pairs['occ_prev'].astype(int)
    tm = pd.crosstab(pairs['occ_prev'], pairs['occ']).reindex(index=range(1, 13), columns=range(1, 13)).fillna(0)
    stay = np.diag(tm.values).copy()
    moves = tm.copy()
    np.fill_diagonal(moves.values, 0)
    row_pct = 100 * moves.div(tm.sum(axis=1), axis=0)          # % of origin's household-years going to each destination
    stay_pct = 100 * stay / tm.sum(axis=1).values

    fig, ax = plt.subplots(figsize=(11, 9))
    im = ax.imshow(row_pct.values, cmap='Blues', vmin=0, vmax=np.nanmax(row_pct.values))
    for i in range(12):
        for j in range(12):
            if i == j:
                ax.text(j, i, f'stay\n{stay_pct[i]:.0f}%', ha='center', va='center', fontsize=9, color=GREY)
            else:
                v = row_pct.values[i, j]
                ax.text(j, i, f'{v:.1f}', ha='center', va='center', fontsize=9,
                        color='white' if v > 0.6 * np.nanmax(row_pct.values) else 'black')
    ax.set_xticks(range(12)); ax.set_yticks(range(12))
    ax.set_xticklabels([SHORT[k] for k in range(1, 13)], rotation=45, ha='right')
    ax.set_yticklabels([f'{OCC_LABELS[k]} (n={int(tm.sum(axis=1)[k]):,})' for k in range(1, 13)], fontsize=10)
    ax.set_xlabel('Occupation code at t')
    ax.set_ylabel('Occupation code at t-1')
    ax.set_title('Year-to-year occupation transitions (% of origin household-years)')
    ax.tick_params(length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    cb = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02)
    cb.set_label('% of origin household-years')
    save(fig, 'transition_heatmap.png')

    # top 20 moves
    mv = pairs[pairs['occ'] != pairs['occ_prev']]
    top = mv.groupby(['occ_prev', 'occ']).size().sort_values(ascending=False).head(20)
    fig, ax = plt.subplots(figsize=(9, 8))
    labels = [f'{OCC_LABELS[a]} → {OCC_LABELS[b]}' for (a, b) in top.index]
    colors = [RED if (a == 12 or b == 12) else BLUE for (a, b) in top.index]
    ax.barh(range(len(top))[::-1], 100 * top.values / len(mv), color=colors)
    ax.set_yticks(range(len(top))[::-1]); ax.set_yticklabels(labels, fontsize=11)
    ax.set_xlabel('Share of all occupation changes (%)')
    ax.set_title(f'The 20 most common moves ({len(mv):,} changes)')
    ax.text(0.98, 0.04, 'red = into or out of not employed', transform=ax.transAxes,
            ha='right', color=RED, fontsize=11)
    save(fig, 'top_moves.png')
    return pairs, mv


def move_stats(mv, means):
    rank = means['income_rank'].to_dict()
    mv = mv.copy()
    mv['up'] = mv['occ'].map(rank) > mv['occ_prev'].map(rank)
    e2e = mv[mv['occ'].isin(EMPLOYED) & mv['occ_prev'].isin(EMPLOYED)]
    return {'n_moves': len(mv), 'n_e2e': len(e2e), 'share_up_e2e': e2e['up'].mean(),
            'n_into_notemp': int((mv['occ'] == 12).sum()), 'n_outof_notemp': int((mv['occ_prev'] == 12).sum())}


def demeaned_dummies(sub, cols, w):
    parts = [pd.get_dummies(sub[c].astype(int), prefix=c, drop_first=True).astype(float) for c in cols]
    C = pd.concat(parts, axis=1)
    return C - np.average(C.values, weights=w, axis=0)


def occupation_adjusted(sub, controls):
    """Coefficients on a full set of occupation dummies with demeaned control dummies:
    each is the occupation's mean HEI at the sample's average mix of the controls;
    household-clustered SE."""
    w = sub['projection_factor'].values
    O = pd.get_dummies(sub['occ'].astype(int), prefix='o', drop_first=False).astype(float)
    C = demeaned_dummies(sub, controls, w)
    res = sm.WLS(sub['hei_usda'].values, np.column_stack([O.values, C.values]), weights=w).fit(
        cov_type='cluster', cov_kwds={'groups': sub['hhid'].values})
    ids = [int(c.split('_')[1]) for c in O.columns]
    return pd.Series(res.params[:len(ids)], index=ids), pd.Series(res.bse[:len(ids)], index=ids)


def gradient(df, means):
    sub = df[~df['occ'].isin([7, 10]) & df['hei_usda'].notna()].copy()
    order = means.loc[[k for k in means.index if k not in (7, 10)]].sort_values('income_w').index.tolist()
    raw = sub.groupby('occ').apply(lambda d: pd.Series({
        'mean': wmean(d['hei_usda'], d['projection_factor']),
        'se': d.groupby('hhid')['hei_usda'].mean().std() / np.sqrt(d['hhid'].nunique())}))
    adj, se = occupation_adjusted(sub, ['age5', 'year'])

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.8), sharey=True)
    x = np.arange(len(order))
    for ax, vals, ses, title in [(axes[0], raw.loc[order, 'mean'], raw.loc[order, 'se'], 'Raw'),
                                 (axes[1], adj.loc[order], se.loc[order], 'Adjusted for age bins and year')]:
        ax.bar(x, vals, yerr=1.96 * ses, color=[RED if k == 12 else BLUE for k in order], width=0.7,
               capsize=3, error_kw={'lw': 1.5})
        for i, k in enumerate(order):
            ax.text(i, vals.iloc[i] + 0.35, f'{vals.iloc[i]:.1f}', ha='center', fontsize=10)
        ax.set_xticks(x)
        ax.set_xticklabels([f'{OCC_LABELS[k]}\n${means.loc[k, "income_w"] * 10:.0f}k' for k in order],
                           rotation=45, ha='right', fontsize=10)
        ax.set_title(title)
        ax.set_ylim(43, 51)
    axes[0].set_ylabel('Mean HEI (0–100)')
    fig.suptitle('HEI by occupation, sorted by mean household income (2013$)', y=1.02, fontsize=15)
    save(fig, 'hei_by_occupation.png')
    return raw, adj, se


def gradient_income_vs_occupation(df, nq=20):
    """HEI net of 5-year age bins, kids and year, against household income: a binscatter over
    income quantiles (Allcott-style: outcome and income both residualized on the controls, means
    added back) and, on the same axes, each occupation's adjusted mean HEI at its mean income."""
    sub = df[~df['occ'].isin([7, 10]) & df['hei_usda'].notna() & df['real_income'].notna()].copy()
    p1, p99 = sub['real_income'].quantile([0.01, 0.99])
    sub = sub[sub['real_income'].between(p1, p99)].reset_index(drop=True)
    w = sub['projection_factor'].values
    C = demeaned_dummies(sub, ['age5', 'kids', 'year'], w)
    X = np.column_stack([np.ones(len(sub)), C.values])
    sw = np.sqrt(w)

    def resid(y):
        beta = np.linalg.lstsq(X * sw[:, None], y * sw, rcond=None)[0]
        return y - X @ beta + np.average(y, weights=w)

    y = resid(sub['hei_usda'].values.astype(float))
    x = resid(sub['real_income'].values.astype(float) * 10)       # $000s, 2013$
    edges = np.percentile(x, np.linspace(0, 100, nq + 1)); edges[-1] += 1
    b = np.digitize(x, edges)
    pts = pd.DataFrame([(np.average(x[b == i], weights=w[b == i]), np.average(y[b == i], weights=w[b == i]))
                        for i in range(1, nq + 1)], columns=['x', 'y'])
    inc = sm.WLS(y, sm.add_constant(x / 10), weights=w).fit(cov_type='cluster', cov_kwds={'groups': sub['hhid'].values})
    b_inc, se_inc = inc.params[1], inc.bse[1]                        # HEI points per $10k

    adj, se = occupation_adjusted(sub, ['age5', 'kids', 'year'])
    occ_x = sub.groupby('occ').apply(lambda d: wmean(d['real_income'], d['projection_factor'])) * 10
    occ_w = sub.groupby('occ')['projection_factor'].sum()
    emp = [k for k in adj.index if k in EMPLOYED]
    occ = sm.WLS(adj.loc[emp].values, sm.add_constant(occ_x.loc[emp].values / 10), weights=occ_w.loc[emp].values).fit()
    b_occ, se_occ = occ.params[1], occ.bse[1]

    fig, ax = plt.subplots(figsize=(11, 7))
    ax.scatter(pts['x'], pts['y'], s=55, color=BLUE, zorder=4,
               label=f'Households, {nq} income bins: slope {b_inc:.2f} per \\$10k (SE {se_inc:.2f})')
    xx = np.linspace(pts['x'].min(), pts['x'].max(), 10)
    ax.plot(xx, inc.params[0] + b_inc * xx / 10, color=BLUE, lw=2, zorder=3)
    ax.errorbar(occ_x.loc[emp], adj.loc[emp], yerr=1.96 * se.loc[emp], fmt='s', ms=9, color=RED, ecolor=RED,
                elinewidth=1.2, capsize=3, zorder=5,
                label=f'Employed occupations, adjusted means: slope {b_occ:.2f} per \\$10k of occupation mean income (SE {se_occ:.2f})')
    xo = np.linspace(occ_x.loc[emp].min(), occ_x.loc[emp].max(), 10)
    ax.plot(xo, occ.params[0] + b_occ * xo / 10, color=RED, lw=2, ls='--', zorder=3)
    if 12 in adj.index:
        ax.errorbar([occ_x.loc[12]], [adj.loc[12]], yerr=[1.96 * se.loc[12]], fmt='s', ms=9, mfc='white',
                    color=RED, ecolor=RED, elinewidth=1.2, capsize=3, zorder=5, label='Not employed (not in the occupation fit)')
    # label placement: (dx, dy) in points and horizontal alignment, chosen by hand to avoid overlaps
    place = {1: (8, -4, 'left'), 2: (8, -14, 'left'), 3: (-8, 6, 'right'), 4: (8, 2, 'left'), 5: (8, -4, 'left'),
             6: (8, -14, 'left'), 8: (-8, -16, 'right'), 9: (-8, -4, 'right'), 11: (-8, -14, 'right'), 12: (-8, 6, 'right')}
    for k in adj.index:
        dx, dy, ha = place.get(k, (8, -4, 'left'))
        ax.annotate(OCC_LABELS[k], (occ_x.loc[k], adj.loc[k]), xytext=(dx, dy), textcoords='offset points',
                    fontsize=10, color=RED, ha=ha)
    ax.set_xlabel('Household income (\\$000s, 2013\\$)')
    ax.set_ylabel('HEI (0–100), net of age bins, kids and year')
    ax.set_title('HEI by household income and by occupation')
    ax.legend(loc='lower right', frameon=False, fontsize=10.5)
    save(fig, 'hei_gradient_income_vs_occupation.png')
    rng_inc = pts['y'].iloc[-1] - pts['y'].iloc[0]
    rng_occ = adj.loc[emp].max() - adj.loc[emp].min()
    print(f'income gradient: {b_inc:.3f} (SE {se_inc:.3f}) HEI points per $10k; bottom to top income bin '
          f'({pts["x"].iloc[0]:.0f}k to {pts["x"].iloc[-1]:.0f}k): {rng_inc:.2f} points')
    print(f'occupation gradient: {b_occ:.3f} (SE {se_occ:.3f}) points per $10k of occupation mean income; '
          f'range across employed occupations: {rng_occ:.2f} points '
          f'({OCC_LABELS[adj.loc[emp].idxmin()]} {adj.loc[emp].min():.1f} to {OCC_LABELS[adj.loc[emp].idxmax()]} {adj.loc[emp].max():.1f})')
    return {'b_inc': b_inc, 'se_inc': se_inc, 'b_occ': b_occ, 'se_occ': se_occ, 'rng_inc': rng_inc, 'rng_occ': rng_occ,
            'x_lo': pts['x'].iloc[0], 'x_hi': pts['x'].iloc[-1], 'n': len(sub)}


def tables(df, ev, means, counts, mv, stats, n_blip):
    pairs = df[(df['gap'] == 1) & df['occ_prev'].notna()]
    out_rate = pairs.groupby('occ_prev').apply(lambda d: 100 * (d['occ'] != d['occ_prev']).mean())
    in_share = mv.groupby('occ').size() / len(mv) * 100
    t = pd.DataFrame({
        'Households': counts['households'].astype(int),
        'HH-years': counts['hh_years'].astype(int),
        'Income (\\$k)': (means['income_w'] * 10).round(1),
        'Age': means['age_w'].round(1),
        'HEI': means['ybar_hei_usda'].round(1),
        'Simple index (USDA)': means['ybar_hi_usda'].round(2),
        'Simple index (Syndigo)': means['ybar_hi'].round(2),
        'Leave rate (\\%)': out_rate.reindex(range(1, 13)).round(1),
        'Share of arrivals (\\%)': in_share.reindex(range(1, 13)).round(1),
    })
    t.index = [f'{OCC_LABELS[k]} ({k})'.replace('<', '$<$') for k in t.index]
    fmt = {'Households': '{:,}'.format, 'HH-years': '{:,}'.format,
           'Simple index (USDA)': '{:.2f}'.format, 'Simple index (Syndigo)': '{:.2f}'.format}
    for c in t.columns:
        fmt.setdefault(c, '{:.1f}'.format)
    with open(TAB / 'occupation_means.tex', 'w') as fh:
        fh.write(t.to_latex(escape=False, column_format='l' + 'r' * t.shape[1], formatters=fmt))

    hh = df.groupby('hhid').agg(n_obs=('n_obs', 'first'), n_switch=('n_switch', 'first'), n_ee=('n_events_ee', 'first'))
    hh2 = hh[hh['n_obs'] >= 2]
    ee = ev[ev['emp2emp'] == 1]
    n_pairs = int(((df['gap'] == 1) & df['occ_prev'].notna()).sum())
    rows = [
        ('Households observed at least twice', f'{len(hh2):,}'),
        ('\\quad ever change occupation code (\\%)', f'{100 * (hh2["n_switch"] > 0).mean():.1f}'),
        ('\\quad with a dated move between employed occupations', f'{int((hh2["n_ee"] >= 1).sum()):,}'),
        ('\\quad\\quad one such move', f'{int((hh2["n_ee"] == 1).sum()):,}'),
        ('\\quad\\quad two or more', f'{int((hh2["n_ee"] >= 2).sum()):,}'),
        ('One-year reversals set back to the origin code (household-years)', f'{n_blip:,}'),
        ('Consecutive household-year pairs', f'{n_pairs:,}'),
        ('\\quad with an occupation change (\\%)', f'{100 * stats["n_moves"] / n_pairs:.1f}'),
        ('Occupation changes between consecutive years', f'{stats["n_moves"]:,}'),
        ('\\quad between employed occupations (the events)', f'{len(ee):,}'),
        ('\\quad\\quad of which up the income ladder (\\%)', f'{100 * ee["up_income"].mean():.1f}'),
        ('\\quad into not employed', f'{stats["n_into_notemp"]:,}'),
        ('\\quad out of not employed', f'{stats["n_outof_notemp"]:,}'),
    ]
    with open(TAB / 'moves_summary.tex', 'w') as fh:
        fh.write('\\begin{tabular}{lr}\n\\toprule\n')
        for a, b in rows:
            fh.write(f'{a} & {b} \\\\\n')
        fh.write('\\bottomrule\n\\end{tabular}\n')
    print(t.to_string())
    for a, b in rows:
        print(f'{a:70s} {b}')


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    TAB.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(BASE / 'occupation_movers_panel.parquet')
    ev = pd.read_parquet(BASE / 'movers_events_panel.parquet').drop_duplicates('event_id')
    means = pd.read_csv(BASE / 'occupation_means.csv').set_index('occ')
    n_blip = int((df['occ'] != df['occ_raw']).sum())
    print(f'{len(df):,} household-years, {df.hhid.nunique():,} households (geographic non-movers, 12 codes); '
          f'{n_blip:,} one-year reversals smoothed')

    counts = occupation_histogram(df)
    pairs, mv = transitions(df)
    stats = move_stats(mv, means)
    gradient(df, means)
    gradient_income_vs_occupation(df)
    tables(df, ev, means, counts, mv, stats, n_blip)
    print(stats)


if __name__ == '__main__':
    main()
