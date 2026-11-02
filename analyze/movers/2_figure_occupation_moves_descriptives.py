"""
Descriptive figures and tables on occupation moves and the occupation gradient
in nutrition (occupation-movers design). Reads interim/movers/
occupation_movers_panel.parquet and occupation_means.csv (1_).

Outputs (figs/movers/, tabs/movers/):
  occupation_histogram.png        household-years by Nielsen occupation code (12 codes)
  ever_move_by_tenure.png         share of households that ever change code, by years observed
  transition_heatmap.png          origin (t-1) x destination (t) transition matrix, row percent
  top_moves.png                   the 20 most common origin -> destination moves
  move_symmetry_pairs.png         moves up vs down the income ladder, one point per pair
  hei_by_occupation.png           mean HEI by occupation, raw and age/year-adjusted
  occupation_means.tex            table: N, income, age, HEI, simple indices, in/out-move rates
  moves_summary.tex               table: ever-move shares, moves per household, up/down counts
"""

import numpy as np
import pandas as pd
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


def occupation_histogram(df, means):
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


def ever_move(df):
    hh = df.groupby('hhid').agg(n_obs=('n_obs', 'first'), n_switch=('n_switch', 'first'),
                                mover_type=('mover_type', 'first'))
    hh['ever'] = hh['n_switch'] > 0
    hh['tenure'] = hh['n_obs'].clip(upper=10)
    by = hh[hh['n_obs'] >= 2].groupby('tenure').agg(share=('ever', 'mean'), n=('ever', 'size'))
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(by.index, 100 * by['share'], color=BLUE, width=0.7)
    for t, r in by.iterrows():
        ax.text(t, 100 * r['share'] + 1, f'{100 * r["share"]:.0f}%', ha='center', fontsize=11)
    ax.set_xticks(by.index)
    ax.set_xticklabels([str(t) if t < 10 else '10+' for t in by.index])
    ax.set_xlabel('Years the household is observed')
    ax.set_ylabel('Share ever changing occupation code (%)')
    ax.set_title('Households that ever change occupation code')
    ax.set_ylim(0, 100)
    save(fig, 'ever_move_by_tenure.png')
    return hh


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
    return pairs, mv, tm


def symmetry(mv, means):
    rank = means['income_rank'].to_dict()
    mv = mv.copy()
    mv['up'] = mv['occ'].map(rank) > mv['occ_prev'].map(rank)
    mv['lo'] = np.minimum(mv['occ'], mv['occ_prev'])
    mv['hi_'] = np.maximum(mv['occ'], mv['occ_prev'])
    pair = mv.groupby(['lo', 'hi_']).agg(n=('up', 'size'), n_up=('up', 'sum'))
    pair['n_down'] = pair['n'] - pair['n_up']
    pair = pair[pair['n'] >= 30]
    fig, ax = plt.subplots(figsize=(7.5, 7))
    inv = pair.index.map(lambda t: 12 in t)
    ax.scatter(pair.loc[~inv, 'n_up'], pair.loc[~inv, 'n_down'], s=40, color=BLUE, label='between employed occupations')
    ax.scatter(pair.loc[inv, 'n_up'], pair.loc[inv, 'n_down'], s=40, color=RED, label='into / out of not employed')
    lim = [20, pair[['n_up', 'n_down']].max().max() * 1.5]
    ax.plot(lim, lim, color=GREY, lw=1.5, ls='--')
    ax.set_xscale('log'); ax.set_yscale('log'); ax.set_xlim(lim); ax.set_ylim(lim)
    big = pair.sort_values('n', ascending=False).head(8)
    for i, ((lo, hi_), r) in enumerate(big.iterrows()):
        dx, dy = (6, 6) if i % 2 == 0 else (6, -12)
        ax.annotate(f'{SHORT[lo]}–{SHORT[hi_]}', (r['n_up'], r['n_down']), fontsize=9,
                    xytext=(dx, dy), textcoords='offset points')
    ax.set_xlabel('Moves up the income ladder (count)')
    ax.set_ylabel('Moves down the income ladder (count)')
    ax.set_title('Up vs down moves, one point per occupation pair')
    ax.legend(loc='upper left', frameon=False)
    save(fig, 'move_symmetry_pairs.png')
    e2e = mv[mv['occ'].isin(EMPLOYED) & mv['occ_prev'].isin(EMPLOYED)]
    stats = {'n_moves': len(mv), 'n_e2e': len(e2e), 'share_up_e2e': e2e['up'].mean(),
             'n_into_notemp': int((mv['occ'] == 12).sum()), 'n_outof_notemp': int((mv['occ_prev'] == 12).sum()),
             'share_up_all': mv['up'].mean(),
             'n_bounce': int(((mv['occ'] != mv['occ_prev'])).sum())}
    return stats, pair


def gradient(df, means):
    sub = df[~df['occ'].isin([7, 10]) & df['hei_usda'].notna()].copy()
    order = means.loc[[k for k in means.index if k not in (7, 10)]].sort_values('income_w').index.tolist()
    # raw weighted mean and SE (clustered by household: SE of the household means)
    raw = sub.groupby('occ').apply(lambda d: pd.Series({
        'mean': wmean(d['hei_usda'], d['projection_factor']),
        'se': d.groupby('hhid')['hei_usda'].mean().std() / np.sqrt(d['hhid'].nunique())}))
    # adjusted: weighted regression of HEI on a full set of occupation dummies plus
    # DEMEANED 5-year age-bin and year dummies, so each occupation coefficient is the
    # mean HEI at the sample's average age and year mix, with a household-clustered SE
    import statsmodels.api as sm
    X = pd.get_dummies(sub['occ'].astype(int), prefix='o', drop_first=False).astype(float)
    A = pd.get_dummies(sub['age5'].astype(int), prefix='a', drop_first=True).astype(float)
    Y = pd.get_dummies(sub['year'].astype(int), prefix='y', drop_first=True).astype(float)
    w = sub['projection_factor'].values
    for Z in (A, Y):
        Z -= np.average(Z.values, weights=w, axis=0)
    Xf = pd.concat([X, A, Y], axis=1)
    res = sm.WLS(sub['hei_usda'].values, Xf.values, weights=w).fit(
        cov_type='cluster', cov_kwds={'groups': sub['hhid'].values})
    occ_cols = [i for i, c in enumerate(Xf.columns) if c.startswith('o_')]
    occ_ids = [int(Xf.columns[i].split('_')[1]) for i in occ_cols]
    adj = pd.Series(res.params[occ_cols], index=occ_ids)
    se = pd.Series(res.bse[occ_cols], index=occ_ids)

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


def tables(df, means, counts, hh, mv, stats, raw, adj, adj_se):
    # occupation means table (12 codes, descriptives sample)
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
    # moves summary table
    hh2 = hh[hh['n_obs'] >= 2]
    rows = [
        ('Households observed at least twice', f'{len(hh2):,}'),
        ('\\quad ever change occupation code (\\%)', f'{100 * hh2["ever"].mean():.1f}'),
        ('\\quad change exactly once, dated (single movers)', f'{int((hh2["mover_type"] == 1).sum()):,}'),
        ('\\quad change more than once', f'{int((hh2["mover_type"] == 2).sum()):,}'),
        ('\\quad change across a gap in the panel (undated)', f'{int((hh2["mover_type"] == 3).sum()):,}'),
        ('Consecutive household-year pairs', f'{int(((df["gap"] == 1) & df["occ_prev"].notna()).sum()):,}'),
        ('\\quad with an occupation change (\\%)', f'{100 * stats["n_moves"] / int(((df["gap"] == 1) & df["occ_prev"].notna()).sum()):.1f}'),
        ('Occupation changes', f'{stats["n_moves"]:,}'),
        ('\\quad between employed occupations', f'{stats["n_e2e"]:,}'),
        ('\\quad\\quad of which up the income ladder (\\%)', f'{100 * stats["share_up_e2e"]:.1f}'),
        ('\\quad into not employed', f'{stats["n_into_notemp"]:,}'),
        ('\\quad out of not employed', f'{stats["n_outof_notemp"]:,}'),
        ('\\quad reversed the following year (\\%)', f'{100 * stats["share_bounce"]:.1f}'),
    ]
    with open(TAB / 'moves_summary.tex', 'w') as fh:
        fh.write('\\begin{tabular}{lr}\n\\toprule\n')
        for a, b in rows:
            fh.write(f'{a} & {b} \\\\\n')
        fh.write('\\bottomrule\n\\end{tabular}\n')
    print(t.to_string())
    for a, b in rows:
        print(f'{a:60s} {b}')


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    TAB.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(BASE / 'occupation_movers_panel.parquet')
    means = pd.read_csv(BASE / 'occupation_means.csv').set_index('occ')
    print(f'{len(df):,} household-years, {df.hhid.nunique():,} households (geographic non-movers, 12 codes)')

    counts = occupation_histogram(df, means)
    hh = ever_move(df)
    pairs, mv, tm = transitions(df)
    stats, pair = symmetry(mv, means)
    # reversal rate: the next observed year returns to the origin code
    nxt = df[['hhid', 'year', 'occ']].copy(); nxt['year'] -= 1
    nxt = nxt.rename(columns={'occ': 'occ_next'})
    mv2 = mv.merge(nxt, on=['hhid', 'year'], how='left')
    stats['share_bounce'] = (mv2['occ_next'] == mv2['occ_prev']).mean()
    raw, adj, adj_se = gradient(df, means)
    tables(df, means, counts, hh, mv, stats, raw, adj, adj_se)
    print(stats)


if __name__ == '__main__':
    main()
