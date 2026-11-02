"""
Figures from the Stata event study of the occupation-movers design.
Reads interim/movers/event_study_coefs.csv and event_study_summary.csv
(4_build_movers_event_study.do).

Outputs (figs/movers/):
  event_study_hei.png        FGW Fig. VI: theta_r for HEI, all moves between employed occupations
  event_study_outcomes.png   theta_r for HEI, the simple USDA index and the simple Syndigo index
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data') / 'interim' / 'movers'
FIG = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition') / 'figs' / 'movers'
BLUE, GREY = '#3b6b9a', '#7a7a7a'

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


def header(s, short=False):
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
    panels = [('hei_usda', 'HEI (full index, USDA labels)'), ('hi_usda', 'Simple index, USDA labels'),
              ('hi', 'Simple index, Syndigo labels')]
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5), sharey=True)
    fig.subplots_adjust(wspace=0.2)
    for ax, (out, title) in zip(axes, panels):
        d = coefs[(coefs['spec'] == 'main') & (coefs['outcome'] == out)]
        s = summ[(summ['spec'] == 'main') & (summ['outcome'] == out)].iloc[0]
        es_panel(ax, d)
        ax.set_title(f'{title}\n{header(s, short=True)}', fontsize=9.5)
        ax.set_xlabel('Year relative to the move')
    axes[0].set_ylabel('$\\theta_r$: share of the origin–destination gap')
    save(fig, 'event_study_outcomes.png')


def main():
    coefs = pd.read_csv(BASE / 'event_study_coefs.csv')
    summ = pd.read_csv(BASE / 'event_study_summary.csv')
    fig_event_study(coefs, summ)
    fig_outcomes(coefs, summ)
    print(summ.round(3).to_string())


if __name__ == '__main__':
    main()
