"""
Figures from the Stata event studies of the occupation-movers design.
Reads interim/movers/{event_study_coefs, event_study_summary, pairs_coefs,
pairs_summary, decomposition}.csv and occupation_effects_hei_usda.csv
(4_build_movers_event_study.do, 5_build_symmetric_pairs.do).

Outputs (figs/movers/):
  event_study_hei.png          FGW Fig. VI: theta_r for HEI, single movers
  event_study_variants.png     theta_r across outcomes and samples
  event_study_controls.png     theta_r for HEI under the income IV's exact control sets
  event_study_income.png       income realized on move (share of the occupation income gap) and HEI per
                               $10k of that gap against what the income IV coefficient implies
  pairs_mirror.png             A -> B movers vs -1 x (B -> A movers), the most common pairs
  pairs_pooled_fe.png          pooled up-movers vs -1 x down-movers, by fixed-effect set
  pairs_pooled_samples.png     pooled up vs -1 x down, by sample
  occupation_effects.png       each occupation's HEI gap split into occupation and household parts
  other_switches.png           the event study for births, deaths, retirement, job loss, re-employment
  event_study_more_movers.png  employed-to-employed theta_r with single movers, first moves, all moves stacked
"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data') / 'interim' / 'movers'
FIG = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition') / 'figs' / 'movers'
BLUE, RED, GREY = '#3b6b9a', '#a63d40', '#7a7a7a'
OCC = {1: 'Professional', 2: 'Manager/owner', 3: 'Clerical', 4: 'Sales', 5: 'Craftsman',
       6: 'Operative/driver', 8: 'Service', 9: 'Farmer', 11: 'Laborer', 12: 'Not employed'}

plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13,
                     'xtick.labelsize': 12, 'ytick.labelsize': 12, 'legend.fontsize': 11,
                     'axes.spines.top': False, 'axes.spines.right': False})


def save(fig, name):
    fig.savefig(FIG / name, bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / name}')


def es_panel(ax, d, color=BLUE, label=None, marker='o', ls='-', flip=False, rmax=6):
    d = d[(d['r'] >= -rmax) & (d['r'] <= rmax)].sort_values('r')
    b = -d['b'] if flip else d['b']
    ax.plot(d['r'], b, color=color, lw=2.5, marker=marker, ms=6, ls=ls, label=label)
    ax.fill_between(d['r'], b - 1.96 * d['se'], b + 1.96 * d['se'], color=color, alpha=0.15, lw=0)
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.axvline(-0.5, color=GREY, lw=1, ls=':')
    ax.set_xticks(range(-rmax, rmax + 1))


def fig_event_study(coefs, summ):
    d = coefs[(coefs['spec'] == 'main') & (coefs['outcome'] == 'hei_usda')]
    s = summ[(summ['spec'] == 'main') & (summ['outcome'] == 'hei_usda')].iloc[0]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    es_panel(ax, d)
    ax.axhline(1, color=GREY, lw=1, ls='--')
    ax.text(-6, 1.03, 'gap entirely due to occupation', va='bottom', fontsize=10, color=GREY)
    ax.text(-6, 0.03, 'gap entirely due to households', va='bottom', fontsize=10, color=GREY)
    ax.set_xlabel('Year relative to the move')
    ax.set_ylabel('$\\theta_r$: share of the origin–destination HEI gap')
    ax.set_title(f'Event study, single movers (N = {int(s["n_movers"]):,}): '
                 f'post-move share = {s["jump"]:.2f} (SE {s["jump_se"]:.2f})')
    ax.set_ylim(-0.6, 1.2)
    save(fig, 'event_study_hei.png')


def fig_variants(coefs, summ):
    panels = [('main', 'hei_usda', 'HEI (main)'), ('main', 'hi_usda', 'Simple index, USDA'),
              ('main', 'hi', 'Simple index, Syndigo'), ('emp2emp', 'hei_usda', 'HEI: employed → employed'),
              ('notemp', 'hei_usda', 'HEI: into or out of not employed'), ('under62', 'hei_usda', 'HEI: under 62 at the move'),
              ('pure', 'hei_usda', 'HEI: education and size bins unchanged'), ('multi', 'hei_usda', 'HEI: including multiple movers'),
              ('unweighted', 'hei_usda', 'HEI: unweighted')]
    fig, axes = plt.subplots(3, 3, figsize=(17, 12), sharex=True, sharey=True)
    fig.subplots_adjust(hspace=0.35, wspace=0.15)
    for ax, (spec, out, title) in zip(axes.flat, panels):
        d = coefs[(coefs['spec'] == spec) & (coefs['outcome'] == out)]
        s = summ[(summ['spec'] == spec) & (summ['outcome'] == out)].iloc[0]
        es_panel(ax, d)
        ax.axhline(1, color=GREY, lw=1, ls='--')
        ax.set_title(f'{title}\nN = {int(s["n_movers"]):,} · post {s["jump"]:.2f} ({s["jump_se"]:.2f}) · pre-trend p = {s["p_pretrend"]:.2f}', fontsize=10)
        ax.set_ylim(-0.8, 1.4)
    for ax in axes[-1]:
        ax.set_xlabel('Year relative to the move')
    for ax in axes[:, 0]:
        ax.set_ylabel('$\\theta_r$')
    save(fig, 'event_study_variants.png')


IV_BETA_SD = 0.035      # income IV, full HEI, clean sample: SD per $10k (tabs/labor_supply/hi_clean.tex)


def fig_controls(coefs, summ):
    panels = [('main', 'Household, year, 5-yr age bin, kids, composition FE'),
              ('ivcontrols', 'Household, year, exact age, kids, composition FE'),
              ('ivcontrols_ys', 'Household, year x state, exact age, kids, composition FE')]
    fig, axes = plt.subplots(1, 3, figsize=(17, 5), sharey=True)
    for ax, (spec, title) in zip(axes, panels):
        d = coefs[(coefs['spec'] == spec) & (coefs['outcome'] == 'hei_usda')]
        s = summ[(summ['spec'] == spec) & (summ['outcome'] == 'hei_usda')].iloc[0]
        es_panel(ax, d)
        ax.axhline(1, color=GREY, lw=1, ls='--')
        ax.set_title(f'{title}\npost {s["jump"]:.2f} ({s["jump_se"]:.2f}) · pre-trend p = {s["p_pretrend"]:.2f}', fontsize=10.5)
        ax.set_xlabel('Year relative to the move')
        ax.set_ylim(-0.6, 1.2)
    axes[0].set_ylabel('$\\theta_r$: share of the origin–destination HEI gap')
    save(fig, 'event_study_controls.png')


def fig_income(coefs, summ, hei_sd):
    inc = coefs[(coefs['spec'] == 'income') & (coefs['outcome'] == 'real_income')].sort_values('r')
    hei = coefs[(coefs['spec'] == 'hei_per_inc') & (coefs['outcome'] == 'hei_usda')].sort_values('r')
    si = summ[(summ['spec'] == 'income')].iloc[0]
    sh = summ[(summ['spec'] == 'hei_per_inc')].iloc[0]
    beta_pts = IV_BETA_SD * hei_sd
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.5))
    ax = axes[0]
    es_panel(ax, inc)
    ax.axhline(1, color=GREY, lw=1, ls='--')
    ax.set_title(f'Income: share of the occupation income gap realized\npost {si["jump"]:.2f} ({si["jump_se"]:.2f}) · pre-trend p = {si["p_pretrend"]:.2f}', fontsize=11)
    ax.set_ylabel('$\\theta_r$ for household income')
    ax.set_xlabel('Year relative to the move')
    ax.set_ylim(-0.3, 1.1)
    ax = axes[1]
    es_panel(ax, hei)
    implied = inc.set_index('r')['b'] * beta_pts
    ax.plot(implied.index[(implied.index >= -6) & (implied.index <= 6)], implied[(implied.index >= -6) & (implied.index <= 6)],
            color=RED, lw=2.5, ls='--', marker='s', ms=5, label=f'implied by the income IV ({IV_BETA_SD:.3f} SD = {beta_pts:.2f} points per \\$10k)')
    ax.plot([], [], color=BLUE, lw=2.5, marker='o', label='movers, estimated')
    ax.legend(loc='upper left', frameon=False, fontsize=10)
    ax.set_title(f'HEI per \\$10k of occupation income gap\npost {sh["jump"]:.3f} ({sh["jump_se"]:.3f}); IV-implied {beta_pts * si["jump"]:.3f}', fontsize=11)
    ax.set_ylabel('HEI points per \\$10k of cross-sectional income gap')
    ax.set_xlabel('Year relative to the move')
    save(fig, 'event_study_income.png')


def fig_other_switches(oc, os_):
    designs = [('birth', 'Birth: child under 6 arrives'), ('death', 'Death of a head (widowed)'),
               ('retirement', 'Retirement: into not employed, 62+'), ('jobloss', 'Job loss: into not employed, under 62'),
               ('reemploy', 'Re-employment: out of not employed')]
    fig, axes = plt.subplots(2, 3, figsize=(19, 10), sharex=True, sharey=True)
    fig.subplots_adjust(hspace=0.45, wspace=0.12)
    for ax, (d, title) in zip(axes.flat, designs):
        c = oc[(oc['design'] == d) & (oc['spec'] == 'points')]
        r = os_[os_['design'] == d].iloc[0]
        es_panel(ax, c, rmax=4)
        ax.axhline(r['gap'], color=GREY, lw=1.2, ls='-.')
        ax.plot([1, 4], [r['iv_implied'], r['iv_implied']], color=RED, lw=2.5, ls='--')
        ax.set_title(f'{title}\nN = {int(r["n_movers"]):,} · age {r["age_post"]:.0f} · gap {r["gap"]:+.2f} · pre-trend p = {r["p_pretrend"]:.2f}\n'
                     f'HEI jump {r["jump"]:+.2f} ({r["jump_se"]:.2f}) · income {r["inc_jump"]:+.2f} ({r["inc_jump_se"]:.2f}) $10k → IV-implied {r["iv_implied"]:+.2f}', fontsize=10)
    axes.flat[-1].set_visible(False)
    for ax in axes[-1]:
        ax.set_xlabel('Year relative to the switch')
    axes[0, 2].set_xlabel('Year relative to the switch')
    for ax in axes[:, 0]:
        ax.set_ylabel('Change in HEI (points)')
    fig.suptitle('Event studies for other switches, HEI points relative to the year before the switch; dash-dot line = HEI gap between the two states; red = jump implied by the income IV', y=1.0, fontsize=13)
    save(fig, 'other_switches.png')


def fig_more_movers(sc, ss):
    specs = [('single', 'Single movers'), ('firstmove', 'First move of every household, cut before the second'),
             ('stacked', 'All moves, stacked'), ('noreversal', 'All moves, excluding moves later reversed')]
    fig, axes = plt.subplots(1, 4, figsize=(21, 5), sharey=True)
    for ax, (spec, title) in zip(axes, specs):
        d = sc[(sc['spec'] == spec) & (sc['sample'] == 'emp2emp')]
        r = ss[(ss['spec'] == spec) & (ss['sample'] == 'emp2emp')].iloc[0]
        es_panel(ax, d)
        ax.axhline(1, color=GREY, lw=1, ls='--')
        ax.set_title(f'{title}\n{int(r["n_events"]):,} moves, {int(r["n_households"]):,} households\npost {r["jump"]:.2f} ({r["jump_se"]:.2f}) · pre-trend p = {r["p_pretrend"]:.2f}', fontsize=10)
        ax.set_xlabel('Year relative to the move')
        ax.set_ylim(-0.6, 1.2)
    axes[0].set_ylabel('$\\theta_r$ (share of the HEI gap)')
    fig.suptitle('Employed-to-employed moves: adding households that move more than once', y=1.06, fontsize=13)
    save(fig, 'event_study_more_movers.png')


def fig_pairs_mirror(pc, ps):
    d = pc[(pc['block'] == 'pairs') & (pc['spec'] == 'cell')]
    s = ps[(ps['block'] == 'pairs') & (ps['spec'] == 'cell')].copy()
    s['n_tot'] = s['n_up'] + s['n_dn']
    s = s.sort_values('n_tot', ascending=False)
    pairs = s['pair'].tolist()[:8]
    fig, axes = plt.subplots(2, 4, figsize=(20, 9.5), sharex=True, sharey=True)
    fig.subplots_adjust(hspace=0.3)
    for ax, p in zip(axes.flat, pairs):
        r = s[s['pair'] == p].iloc[0]
        A, B = int(r['A']), int(r['B'])
        es_panel(ax, d[(d['pair'] == p) & (d['direction'] == 'up')], color=BLUE, label=f'{OCC[A]} → {OCC[B]} (n={int(r["n_up"]):,})', rmax=4)
        es_panel(ax, d[(d['pair'] == p) & (d['direction'] == 'down')], color=RED, label=f'−1 × [{OCC[B]} → {OCC[A]}] (n={int(r["n_dn"]):,})',
                 marker='s', ls='--', flip=True, rmax=4)
        ax.axhline(r['gap'], color=GREY, lw=1.2, ls='-.')
        ax.set_title(f'{OCC[A]} ↔ {OCC[B]}\ngap {r["gap"]:.2f} · p(mirror) = {r["p_mirror"]:.2f} · p(pre) = {r["p_pretrend"]:.2f}', fontsize=10.5)
        ax.legend(loc='lower left', frameon=False, fontsize=9)
    for ax in axes[-1]:
        ax.set_xlabel('Year relative to the move')
    for ax in axes[:, 0]:
        ax.set_ylabel('Change in HEI (points)')
    fig.suptitle('Movers A → B against movers B → A (times −1), household and education × size × year fixed effects; '
                 'dash-dot line = cross-sectional HEI gap between B and A', y=1.0, fontsize=13)
    save(fig, 'pairs_mirror.png')


def pooled_figure(pc, ps, specs, name, title):
    fig, axes = plt.subplots(1, len(specs), figsize=(5.2 * len(specs), 5), sharey=True)
    for ax, (spec, lab) in zip(np.atleast_1d(axes), specs):
        d = pc[(pc['block'] == 'pooled') & (pc['spec'] == spec)]
        r = ps[(ps['block'] == 'pooled') & (ps['spec'] == spec)].iloc[0]
        es_panel(ax, d[d['direction'] == 'up'], color=BLUE, label=f'up-movers (n={int(r["n_up"]):,})', rmax=4)
        es_panel(ax, d[d['direction'] == 'down'], color=RED, label=f'−1 × down-movers (n={int(r["n_dn"]):,})', marker='s', ls='--', flip=True, rmax=4)
        ax.axhline(r['gap'], color=GREY, lw=1.2, ls='-.')
        ax.set_title(f'{lab}\njump up {r["jump_up"]:.2f} ({r["jump_up_se"]:.2f}), −down {-r["jump_dn"]:.2f} ({r["jump_dn_se"]:.2f}); p(pre) = {r["p_pretrend"]:.2f}', fontsize=10.5)
        ax.set_xlabel('Year relative to the move')
        ax.legend(loc='lower left', frameon=False, fontsize=9)
    np.atleast_1d(axes)[0].set_ylabel('Change in HEI (points)')
    fig.suptitle(title, y=1.02, fontsize=13)
    save(fig, name)


def fig_occupation_effects(eff):
    eff = eff.sort_values('ybar')
    w = eff['n'] / eff['n'].sum()
    gm_y = np.average(eff['ybar'], weights=w)
    gm_g = np.average(eff['gam'], weights=w)
    gm_s = np.average(eff['ystar'], weights=w)
    place = eff['gam'] - gm_g
    hh = eff['ystar'] - gm_s
    fig, ax = plt.subplots(figsize=(11, 5.8))
    x = np.arange(len(eff))
    ax.bar(x, place, color=BLUE, width=0.7, label='due to occupation ($\\gamma_j$)')
    ax.bar(x, hh, bottom=np.where(np.sign(hh) == np.sign(place), place, 0), color=RED, width=0.7, alpha=0.85, label='due to households ($\\bar y^*_j$)')
    ax.plot(x, eff['ybar'] - gm_y, color='black', marker='D', ms=7, lw=0, label='total gap ($\\bar y_j$ − mean)')
    ax.axhline(0, color=GREY, lw=1)
    ax.set_xticks(x)
    ax.set_xticklabels([OCC[int(o)] for o in eff['occ']], rotation=45, ha='right')
    ax.set_ylabel('HEI points relative to the mean')
    ax.set_title('Each occupation\'s HEI gap split into occupation and household components')
    ax.legend(frameon=False, loc='upper left')
    save(fig, 'occupation_effects.png')


def main():
    coefs = pd.read_csv(BASE / 'event_study_coefs.csv')
    summ = pd.read_csv(BASE / 'event_study_summary.csv')
    fig_event_study(coefs, summ)
    fig_variants(coefs, summ)
    if (coefs['spec'] == 'ivcontrols_ys').any():
        fig_controls(coefs, summ)
    if (coefs['spec'] == 'hei_per_inc').any():
        panel = pd.read_parquet(BASE / 'occupation_movers_panel.parquet', columns=['analysis', 'hei_usda'])
        fig_income(coefs, summ, panel.loc[panel['analysis'] == 1, 'hei_usda'].std())
    if (BASE / 'pairs_coefs.csv').exists():
        pc = pd.read_csv(BASE / 'pairs_coefs.csv')
        ps = pd.read_csv(BASE / 'pairs_summary.csv')
        if (pc['block'] == 'pairs').any():
            fig_pairs_mirror(pc, ps)
        if (pc['block'] == 'pooled').any():
            pooled_figure(pc, ps, [('year', 'Year FE'), ('cell', 'Education × size × year FE'),
                                   ('cellstate', '+ state'), ('cellcounty', '+ county')],
                          'pairs_pooled_fe.png', 'All pairs pooled: up-movers vs −1 × down-movers, by fixed-effect set (household FE throughout)')
            pooled_figure(pc, ps, [('emp2emp', 'Employed → employed'), ('notemp', 'Into / out of not employed'),
                                   ('allmoves', 'All moves (not only pure)'), ('moversonly', 'Movers only')],
                          'pairs_pooled_samples.png', 'All pairs pooled: up-movers vs −1 × down-movers, by sample (education × size × year and household FE)')
    if (BASE / 'stacked_event_study_summary.csv').exists():
        fig_more_movers(pd.read_csv(BASE / 'stacked_event_study_coefs.csv'), pd.read_csv(BASE / 'stacked_event_study_summary.csv'))
    if (BASE / 'other_switches_summary.csv').exists():
        fig_other_switches(pd.read_csv(BASE / 'other_switches_coefs.csv'), pd.read_csv(BASE / 'other_switches_summary.csv'))
    if (BASE / 'occupation_effects_hei_usda.csv').exists():
        fig_occupation_effects(pd.read_csv(BASE / 'occupation_effects_hei_usda.csv'))


if __name__ == '__main__':
    main()
