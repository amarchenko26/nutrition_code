"""
Figures from the Stata event study of the occupation-movers design.
Reads interim/movers/event_study_coefs.csv and event_study_summary.csv
(4_build_movers_event_study.do).

Outputs (figs/movers/):
  event_study_hei.png        FGW Fig. VI, 1 x 2 since 2026-11-05: theta_r for HEI on all moves (E->E plus into/out of
                             not employed) and on moves from an employed occupation (origin != 12, employed at t-1);
                             the E->E-only version lives on in event_study_samples / event_study_outcomes
  event_study_outcomes.png   theta_r for HEI, the simple USDA index and the simple Syndigo index, by sample (4 x 3)
  other_switches.png         the event study for births, deaths, education increases, retirement, job loss,
                             re-employment (8_build_life_event_studies.do), HEI points
  event_study_first_stage.png   theta_r for household income on the occupation INCOME gap (the first stage):
                                all moves / employed origin (the movers analogue of the IV's employed-at-t-1 sample)
  event_study_reduced_form.png  theta_r for HEI per $10k of the occupation income gap (the reduced form), same two
                                samples, each against the path the income IV implies (0.035 SD per $10k x income theta_r)
  (event_study_income.png, the two-panel predecessor, deleted 2026-11-05)
  tabs/movers/movers_results.tex  post-move means for the three objects by sample, with / without controls
                                  (+ income-quintile movers rows when 4_ has been run with `income`)
  event_study_income_movers.png   income-quintile movers (1_ --places income, 4_ income): HEI on the quintile HEI gap,
                                  income on the quintile income gap, HEI per $10k of it vs the IV-implied path
  event_study_samples.png    theta_r for HEI: all moves, moves not later reversed, first moves, single-change households
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data') / 'interim' / 'movers'
FIG = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf') / 'figs' / 'movers'
TAB = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf') / 'tabs' / 'movers'
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
    """1 x 2: theta_r for HEI (share of the HEI gap) on all moves and on moves from an employed occupation."""
    panels = [('all', 'All moves (between employed occupations, and into or out of not employed)'),
              ('emporig', 'Moves from an employed occupation (employed at t−1)')]
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.8), sharey=True)
    fig.subplots_adjust(wspace=0.08)
    for ax, (spec, title) in zip(axes, panels):
        d = coefs[(coefs['spec'] == spec) & (coefs['outcome'] == 'hei_usda')]
        s = summ[(summ['spec'] == spec) & (summ['outcome'] == 'hei_usda')].iloc[0]
        es_panel(ax, d)
        ax.text(-6, 1.03, 'gap entirely due to occupation', va='bottom', fontsize=10, color=GREY)
        ax.text(-6, 0.03, 'gap entirely due to households', va='bottom', fontsize=10, color=GREY)
        ax.set_title(f'{title}\n{header(s, lines=True)}', fontsize=11)
        ax.set_xlabel('Year relative to the move')
    axes[0].set_ylabel('$\\theta_r$: share of the origin–destination HEI gap')
    save(fig, 'event_study_hei.png')


def fig_outcomes(coefs, summ):
    outcomes = [('hei_usda', 'HEI (full index, USDA labels)'), ('hi_usda', 'Simple index, USDA labels'),
                ('hi', 'Simple index, Syndigo labels')]
    samples = [('main', 'All moves between employed occupations'), ('noreversal', 'Not later reversed'),
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


SAMPLES = [('all', 'All moves (between employed occupations, and into or out of not employed)'),
           ('emporig', 'Moves from an employed occupation (employed at t−1)')]


def _spec(base, suffix):
    return base if suffix == '' else f'{base}_{suffix}'


def fig_first_stage(coefs, summ):
    """theta_r for household income on the occupation income gap: the share of the cross-sectional gap realized."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.8), sharey=True)
    fig.subplots_adjust(wspace=0.08)
    for ax, (suf, title) in zip(axes, SAMPLES):
        spec = _spec('income', suf)
        d = coefs[(coefs['spec'] == spec) & (coefs['outcome'] == 'real_income')].sort_values('r')
        s = summ[(summ['spec'] == spec) & (summ['outcome'] == 'real_income')].iloc[0]
        d = d[(d['r'] >= -6) & (d['r'] <= 6)]
        ax.plot(d['r'], d['b'], color=BLUE, lw=2.5, marker='o', ms=6)
        ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
        ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axhline(1, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
        ax.set_xticks(range(-6, 7))
        ax.set_ylim(-0.4, 1.1)
        ax.set_title(f'{title}\n{header(s, lines=True)}', fontsize=11)
        ax.set_xlabel('Year relative to the move')
    axes[0].set_ylabel('$\\theta_r$: share of the occupation income gap in household income')
    save(fig, 'event_study_first_stage.png')


def fig_reduced_form(coefs, summ, hei_sd):
    """theta_r for HEI per $10k of the occupation income gap, against the path the income IV implies."""
    beta_pts = IV_BETA_SD * hei_sd
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.8), sharey=True)
    fig.subplots_adjust(wspace=0.08)
    for ax, (suf, title) in zip(axes, SAMPLES):
        spec, ispec = _spec('hei_per_inc', suf), _spec('income', suf)
        d = coefs[(coefs['spec'] == spec) & (coefs['outcome'] == 'hei_usda')].sort_values('r')
        s = summ[(summ['spec'] == spec) & (summ['outcome'] == 'hei_usda')].iloc[0]
        inc = coefs[(coefs['spec'] == ispec) & (coefs['outcome'] == 'real_income')].sort_values('r')
        si = summ[(summ['spec'] == ispec) & (summ['outcome'] == 'real_income')].iloc[0]
        d = d[(d['r'] >= -6) & (d['r'] <= 6)]
        ax.plot(d['r'], d['b'], color=BLUE, lw=2.5, marker='o', ms=6, label='movers, estimated')
        ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
        implied = inc.set_index('r')['b'] * beta_pts
        implied = implied[(implied.index >= -6) & (implied.index <= 6)]
        ax.plot(implied.index, implied.values, color=RED, lw=2.5, ls='--', marker='s', ms=5,
                label=f'implied by the income IV: {beta_pts:.2f} HEI points per \\$10k times the income path')
        ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
        ax.set_xticks(range(-6, 7))
        ax.set_title(f'{title}\n{int(s["n_events"]):,} moves · post {s["jump"]:.3f} (SE {s["jump_se"]:.3f}) · '
                     f'IV-implied {beta_pts * si["jump"]:.3f} · pre-trend p = {s["p_pretrend"]:.2f}', fontsize=11)
        ax.set_xlabel('Year relative to the move')
    axes[0].set_ylabel('HEI points per \\$10k of the occupation income gap')
    axes[0].legend(loc='upper left', frameon=False, fontsize=9.5)
    save(fig, 'event_study_reduced_form.png')


def table_results(summ, hei_sd, isum=None):
    """tabs/movers/movers_results.tex: post-move means of theta_1..theta_3 for the three outcome-gap pairs, by sample,
    with and without the age / kids / composition controls (labor_supply.tex Movers section, Anya 2026-11-05)."""
    beta_pts = IV_BETA_SD * hei_sd
    def g(spec, outcome):
        r = summ[(summ['spec'] == spec) & (summ['outcome'] == outcome)]
        return None if r.empty else r.iloc[0]
    out = ['\\begin{tabular}{llrcccccc}', '\\toprule',
           ' & & & \\multicolumn{2}{c}{HEI on the HEI gap} & \\multicolumn{2}{c}{Income on the income gap} & \\multicolumn{2}{c}{HEI on the income gap} \\\\',
           '\\cmidrule(lr){4-5} \\cmidrule(lr){6-7} \\cmidrule(lr){8-9}',
           'Sample & Controls & Moves & share of gap & pre $p$ & share of gap & pre $p$ & HEI pts per \\$10k & IV-implied \\\\', '\\midrule']
    blocks = [(summ, 'All moves', 'all'), (summ, 'From an employed occupation', 'emporig')]
    if isum is not None:
        blocks.append((isum, 'Income-quintile movers', 'all'))
    for frame, label, base in blocks:
        if label == 'Income-quintile movers':
            out.append('\\midrule')
        for ctl, suf in [('age, kids, comp.', ''), ('none', '_nox')]:
            gg = lambda spec, outcome: (lambda r: None if r.empty else r.iloc[0])(frame[(frame['spec'] == spec) & (frame['outcome'] == outcome)])
            h, i, pr = gg(base + suf, 'hei_usda'), gg('income_' + base + suf, 'real_income'), gg('hei_per_inc_' + base + suf, 'hei_usda')
            if h is None or i is None or pr is None:
                continue
            out.append(f'{label if suf == "" else ""} & {ctl} & {int(h["n_events"]):,} & {h["jump"]:.3f} ({h["jump_se"]:.3f}) & {h["p_pretrend"]:.2f} & '
                       f'{i["jump"]:.3f} ({i["jump_se"]:.3f}) & {i["p_pretrend"]:.2f} & {pr["jump"]:.3f} ({pr["jump_se"]:.3f}) & {beta_pts * i["jump"]:.3f} \\\\')
    out += ['\\bottomrule', '\\end{tabular}']
    TAB.mkdir(parents=True, exist_ok=True)
    (TAB / 'movers_results.tex').write_text('\n'.join(out) + '\n')
    print(f'saved {TAB / "movers_results.tex"}')


def fig_income_movers(ic, isum, hei_sd):
    """Income-quintile movers (1_ --places income, 4_ income): theta_r for HEI on the quintile HEI gap, income on the
    quintile income gap, HEI per $10k of the quintile income gap against the IV-implied path."""
    beta_pts = IV_BETA_SD * hei_sd
    fig, axes = plt.subplots(1, 3, figsize=(19, 5.8))
    fig.subplots_adjust(wspace=0.3)
    specs = [('all', 'hei_usda', 'HEI on the quintile HEI gap', '$\\theta_r$: share of the HEI gap', (-0.6, 1.2)),
             ('income_all', 'real_income', 'Income on the quintile income gap', '$\\theta_r$: share of the income gap', (-0.4, 1.1)),
             ('hei_per_inc_all', 'hei_usda', 'HEI per \\$10k of the quintile income gap', 'HEI points per \\$10k of income gap', None)]
    inc = ic[(ic['spec'] == 'income_all') & (ic['outcome'] == 'real_income')].sort_values('r')
    for ax, (spec, outcome, title, ylab, ylim) in zip(axes, specs):
        d = ic[(ic['spec'] == spec) & (ic['outcome'] == outcome)].sort_values('r')
        s = isum[(isum['spec'] == spec) & (isum['outcome'] == outcome)].iloc[0]
        d = d[(d['r'] >= -6) & (d['r'] <= 6)]
        ax.plot(d['r'], d['b'], color=BLUE, lw=2.5, marker='o', ms=6, label='movers, estimated')
        ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
        ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
        extra = ''
        if ylim is not None:
            ax.axhline(1, color=GREY, lw=1, ls='--'); ax.set_ylim(*ylim)
        else:
            si = isum[(isum['spec'] == 'income_all') & (isum['outcome'] == 'real_income')].iloc[0]
            implied = inc.set_index('r')['b'] * beta_pts
            implied = implied[(implied.index >= -6) & (implied.index <= 6)]
            ax.plot(implied.index, implied.values, color=RED, lw=2.5, ls='--', marker='s', ms=5,
                    label=f'implied by the income IV: {beta_pts:.2f} HEI points per \\$10k times the income path')
            ax.legend(loc='upper left', frameon=False, fontsize=9.5)
            extra = f' · IV-implied {beta_pts * si["jump"]:.3f}'
        ax.set_xticks(range(-6, 7))
        ax.set_title(f'{title}\n{int(s["n_events"]):,} moves · post {s["jump"]:.3f} (SE {s["jump_se"]:.3f}){extra} · pre-trend p = {s["p_pretrend"]:.2f}', fontsize=11)
        ax.set_xlabel('Year relative to the move')
        ax.set_ylabel(ylab)
    save(fig, 'event_study_income_movers.png')


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
    panel = pd.read_parquet(BASE / 'occupation_movers_panel.parquet', columns=['analysis', 'hei_usda'])
    hei_sd = panel.loc[panel['analysis'] == 1, 'hei_usda'].std()
    fig_first_stage(coefs, summ)
    fig_reduced_form(coefs, summ, hei_sd)
    isum = None
    if (BASE / 'income_event_study_summary.csv').exists():
        ic, isum = pd.read_csv(BASE / 'income_event_study_coefs.csv'), pd.read_csv(BASE / 'income_event_study_summary.csv')
        fig_income_movers(ic, isum, hei_sd)
    table_results(summ, hei_sd, isum)
    if (BASE / 'other_switches_summary.csv').exists():
        fig_other_switches(pd.read_csv(BASE / 'other_switches_coefs.csv'), pd.read_csv(BASE / 'other_switches_summary.csv'))
    print(summ.round(3).to_string())


if __name__ == '__main__':
    main()
