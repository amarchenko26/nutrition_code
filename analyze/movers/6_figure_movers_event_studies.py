"""
Figures from the Stata event study of the occupation-movers design.
Reads interim/movers/event_study_coefs.csv and event_study_summary.csv
(4_build_movers_event_study.do).

Outputs (figs/movers/):
  event_study_hei.png        FGW Fig. VI: theta_r for HEI, all moves between employed occupations
  event_study_outcomes.png   theta_r for HEI, the simple USDA index and the simple Syndigo index, by sample (4 x 3)
  other_switches.png         the event study for births, deaths, education increases, retirement, job loss,
                             re-employment (8_build_life_event_studies.do), HEI points
  event_study_income.png     income realized on move (share of the occupation income gap), employed-to-employed
                             and into/out of not employed; HEI per $10k of the gap against the income IV's implied path
  event_study_samples.png    theta_r for HEI: all moves, moves not later reversed, first moves, single-change households
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data') / 'interim' / 'movers'
FIG = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition') / 'figs' / 'movers'
BLUE, RED, GREY = '#3b6b9a', '#a63d40', '#7a7a7a'
IV_BETA_SD = 0.035      # income IV, full HEI, clean sample: SD per $10k (tabs/labor_supply/hi_clean.tex)

plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13,
                     'xtick.labelsize': 12, 'ytick.labelsize': 12, 'legend.fontsize': 11,
                     'axes.spines.top': False, 'axes.spines.right': False})


def save(fig, name):
    fig.savefig(FIG / name, bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / name}')


def es_panel(ax, d, rmax=6):
    d = d[(d['r'] >= -rmax) & (d['r'] <= rmax)].sort_values('r')
    ax.plot(d['r'], d['b'], color=BLUE, lw=2.5, marker='o', ms=6)
    ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.axhline(1, color=GREY, lw=1, ls='--')
    ax.axvline(-0.5, color=GREY, lw=1, ls=':')
    ax.set_xticks(range(-rmax, rmax + 1))
    ax.set_ylim(-0.6, 1.2)


def header(s, short=False, lines=False):
    if lines:
        return f'{int(s["n_events"]):,} moves\npost-move share {s["jump"]:.2f} (SE {s["jump_se"]:.2f}) · pre-trend p = {s["p_pretrend"]:.2f}'
    if short:
        return f'{int(s["n_events"]):,} moves · post-move share {s["jump"]:.2f} (SE {s["jump_se"]:.2f}) · pre-trend p = {s["p_pretrend"]:.2f}'
    return (f'{int(s["n_events"]):,} moves, {int(s["n_households"]):,} households · '
            f'post-move share {s["jump"]:.2f} (SE {s["jump_se"]:.2f}) · pre-trend p = {s["p_pretrend"]:.2f}')


def fig_event_study(coefs, summ):
    d = coefs[(coefs['spec'] == 'main') & (coefs['outcome'] == 'hei_usda')]
    s = summ[(summ['spec'] == 'main') & (summ['outcome'] == 'hei_usda')].iloc[0]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    es_panel(ax, d)
    ax.text(-6, 1.03, 'gap entirely due to occupation', va='bottom', fontsize=10, color=GREY)
    ax.text(-6, 0.03, 'gap entirely due to households', va='bottom', fontsize=10, color=GREY)
    ax.set_xlabel('Year relative to the move')
    ax.set_ylabel('$\\theta_r$: share of the origin–destination HEI gap')
    ax.set_title(f'Moves between employed occupations\n{header(s)}')
    save(fig, 'event_study_hei.png')


def fig_outcomes(coefs, summ):
    outcomes = [('hei_usda', 'HEI (full index, USDA labels)'), ('hi_usda', 'Simple index, USDA labels'),
                ('hi', 'Simple index, Syndigo labels')]
    samples = [('main', 'All moves'), ('noreversal', 'Not later reversed'),
               ('firstmove', 'First move of each household'), ('single', 'Single-change households')]
    fig, axes = plt.subplots(4, 3, figsize=(17, 18), sharex=True, sharey=True)
    fig.subplots_adjust(wspace=0.12, hspace=0.45)
    for i, (spec, stitle) in enumerate(samples):
        for j, (out, otitle) in enumerate(outcomes):
            ax = axes[i, j]
            d = coefs[(coefs['spec'] == spec) & (coefs['outcome'] == out)]
            s = summ[(summ['spec'] == spec) & (summ['outcome'] == out)]
            if d.empty or s.empty:
                ax.set_visible(False); continue
            es_panel(ax, d)
            ax.set_title(f'{stitle}\n{otitle}\n{header(s.iloc[0], lines=True)}', fontsize=9.5)
    for ax in axes[-1]:
        ax.set_xlabel('Year relative to the move')
    for ax in axes[:, 0]:
        ax.set_ylabel('$\\theta_r$: share of the gap')
    save(fig, 'event_study_outcomes.png')


def fig_income(coefs, summ, hei_sd):
    inc = coefs[(coefs['spec'] == 'income') & (coefs['outcome'] == 'real_income')].sort_values('r')
    inc_ne = coefs[(coefs['spec'] == 'income_notemp') & (coefs['outcome'] == 'real_income')].sort_values('r')
    hei = coefs[(coefs['spec'] == 'hei_per_inc') & (coefs['outcome'] == 'hei_usda')].sort_values('r')
    si = summ[summ['spec'] == 'income'].iloc[0]
    sn = summ[summ['spec'] == 'income_notemp'].iloc[0]
    sh = summ[summ['spec'] == 'hei_per_inc'].iloc[0]
    beta_pts = IV_BETA_SD * hei_sd
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax = axes[0]
    d = inc[(inc['r'] >= -6) & (inc['r'] <= 6)]
    ax.plot(d['r'], d['b'], color=BLUE, lw=2.5, marker='o', ms=6,
            label=f'between employed occupations ({int(si["n_events"]):,} moves): post {si["jump"]:.2f} (SE {si["jump_se"]:.2f})')
    ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
    d = inc_ne[(inc_ne['r'] >= -6) & (inc_ne['r'] <= 6)]
    ax.plot(d['r'], d['b'], color=GREY, lw=2, ls='--', marker='s', ms=5,
            label=f'into or out of not employed ({int(sn["n_events"]):,} moves): post {sn["jump"]:.2f} (SE {sn["jump_se"]:.2f})')
    ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=GREY, alpha=0.15, lw=0)
    ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axhline(1, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
    ax.set_xticks(range(-6, 7))
    ax.set_ylim(-0.4, 1.1)
    ax.set_title('Household income: share of the occupation income gap realized')
    ax.set_ylabel('$\\theta_r$ for household income (2013 dollars)')
    ax.set_xlabel('Year relative to the move')
    ax.legend(loc='lower right', frameon=False, fontsize=9.5)
    ax = axes[1]
    d = hei[(hei['r'] >= -6) & (hei['r'] <= 6)]
    ax.plot(d['r'], d['b'], color=BLUE, lw=2.5, marker='o', ms=6, label='movers, estimated')
    ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
    implied = inc.set_index('r')['b'] * beta_pts
    implied = implied[(implied.index >= -6) & (implied.index <= 6)]
    ax.plot(implied.index, implied.values, color=RED, lw=2.5, ls='--', marker='s', ms=5,
            label=f'implied by the income IV: {IV_BETA_SD:.3f} SD = {beta_pts:.2f} HEI points per \\$10k, times the income path')
    ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
    ax.set_xticks(range(-6, 7))
    ax.set_title(f'HEI per \\$10k of the occupation income gap\npost {sh["jump"]:.3f} (SE {sh["jump_se"]:.3f}); IV-implied {beta_pts * si["jump"]:.3f}')
    ax.set_ylabel('HEI points per \\$10k of cross-sectional income gap')
    ax.set_xlabel('Year relative to the move')
    ax.legend(loc='upper left', frameon=False, fontsize=9.5)
    save(fig, 'event_study_income.png')


def fig_other_switches(oc, os_):
    designs = [('birth', 'Birth: child under 6 arrives'), ('death', 'Death of a head (widowed)'),
               ('educ', 'Education increase (a head\'s schooling +2 years)'),
               ('retirement', 'Retirement: into not employed, 62+'), ('jobloss', 'Job loss: into not employed, under 62'),
               ('reemploy', 'Re-employment: out of not employed')]
    fig, axes = plt.subplots(2, 3, figsize=(19, 10.5), sharex=True, sharey=True)
    fig.subplots_adjust(hspace=0.45, wspace=0.12)
    for ax, (d, title) in zip(axes.flat, designs):
        c = oc[(oc['design'] == d) & (oc['spec'] == 'points')].sort_values('r')
        c = c[(c['r'] >= -4) & (c['r'] <= 4)]
        rr = os_[os_['design'] == d]
        if c.empty or rr.empty:
            ax.set_visible(False); continue
        r = rr.iloc[0]
        ax.plot(c['r'], c['b'], color=BLUE, lw=2.5, marker='o', ms=6)
        ax.fill_between(c['r'], c['b'] - 1.96 * c['se'], c['b'] + 1.96 * c['se'], color=BLUE, alpha=0.15, lw=0)
        ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
        ax.axhline(r['gap'], color=GREY, lw=1.2, ls='-.')
        ax.plot([1, 4], [r['iv_implied'], r['iv_implied']], color=RED, lw=2.5, ls='--')
        ax.set_xticks(range(-4, 5))
        ax.set_title(f'{title}\nN = {int(r["n_movers"]):,} · age {r["age_post"]:.0f} · gap {r["gap"]:+.2f} · pre-trend p = {r["p_pretrend"]:.2f}\n'
                     f'HEI jump {r["jump"]:+.2f} ({r["jump_se"]:.2f}) · income {r["inc_jump"]:+.2f} ({r["inc_jump_se"]:.2f}) $10k, IV-implied {r["iv_implied"]:+.2f}', fontsize=10)
    for ax in axes[-1]:
        ax.set_xlabel('Year relative to the switch')
    for ax in axes[:, 0]:
        ax.set_ylabel('Change in HEI (points)')
    save(fig, 'other_switches.png')


def fig_samples(coefs, summ):
    panels = [('main', 'All moves between employed occupations'), ('noreversal', 'Moves not later reversed'),
              ('firstmove', 'First move of each household'), ('single', 'Single-change households')]
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True, sharey=True)
    fig.subplots_adjust(wspace=0.12, hspace=0.35)
    for ax, (spec, title) in zip(axes.flat, panels):
        d = coefs[(coefs['spec'] == spec) & (coefs['outcome'] == 'hei_usda')]
        s = summ[(summ['spec'] == spec) & (summ['outcome'] == 'hei_usda')].iloc[0]
        es_panel(ax, d)
        ax.set_title(f'{title}\n{header(s, lines=True)}', fontsize=11)
    for ax in axes[-1]:
        ax.set_xlabel('Year relative to the move')
    for ax in axes[:, 0]:
        ax.set_ylabel('$\\theta_r$: share of the origin–destination HEI gap')
    save(fig, 'event_study_samples.png')


def main():
    coefs = pd.read_csv(BASE / 'event_study_coefs.csv')
    summ = pd.read_csv(BASE / 'event_study_summary.csv')
    fig_event_study(coefs, summ)
    fig_outcomes(coefs, summ)
    fig_samples(coefs, summ)
    if (coefs['spec'] == 'income').any():
        panel = pd.read_parquet(BASE / 'occupation_movers_panel.parquet', columns=['analysis', 'hei_usda'])
        fig_income(coefs, summ, panel.loc[panel['analysis'] == 1, 'hei_usda'].std())
    if (BASE / 'other_switches_summary.csv').exists():
        fig_other_switches(pd.read_csv(BASE / 'other_switches_coefs.csv'), pd.read_csv(BASE / 'other_switches_summary.csv'))
    print(summ.round(3).to_string())


if __name__ == '__main__':
    main()
