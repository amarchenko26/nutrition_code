"""Figure and tables for the employment-change event studies (build_time_events.do).

Reads interim/what_moves/time_events_coefs.csv and time_events_summary.csv (columns spec, outcome,
event, ...; specs main, balanced, samecell, male, female).
Outputs:
  figs/what_moves/time/time_events.png   columns = events, rows = seven outcomes: the change in the
      outcome r years from the event for movers relative to the year before it, net of the
      never-changers (blue, 95% band), and the same for movers observed in every year r = -2..2
      (orange dashed). Inside each panel: the mean of the r = 1 to 3 coefficients, its SE, the
      pre-trend p-value.
  tabs/what_moves/time_events.tex         outcomes x events: post mean (r = 1 to 3), SE, pre-trend p
  tabs/what_moves/time_events_checks.tex  the same outcomes on the balanced window (post mean of
      r = 1, 2, and the r = -2 coefficient) and against never-changers of the same starting
      employment state and age group
  tabs/what_moves/time_events_by_head.tex post mean by event x which head changes
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.hei_units import to_sd, hei_sd, hei_mean_sd
from utils.stars import stars, star_num, est

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BASE = idir / 'what_moves'
FIG = fdir / 'what_moves' / 'time'
TAB = tdir / 'what_moves'
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#999999'
plt.rcParams.update({'font.size': 24, 'axes.labelsize': 24, 'axes.titlesize': 26, 'xtick.labelsize': 20, 'ytick.labelsize': 20,
                     'axes.spines.top': False, 'axes.spines.right': False})

EVENTS = ['retirement', 'job loss', 'hours cut', 're-employment', 'hours increase']
EVENT_TITLES = {'retirement': 'Retirement', 'job loss': 'Job loss', 'hours cut': 'Hours cut', 're-employment': 'Re-employment', 'hours increase': 'Hours increase'}
FIG_OUTCOMES = [('real_income', 'Income ($10k)'), ('hei_usda', 'HEI (SD)'), ('prep_time_raw', 'Preparation score'),
                ('upf_share', 'Ultra-processed\nshare (pct. pts)'), ('price_1000', 'Price paid per\n1,000 kcal ($)'), ('cals_ae_day', 'Calories per adult\nequivalent per day'), ('n_trips', 'Trips per year')]
TAB_OUTCOMES = [('real_income', 'Household income (\\$10k)', 2), ('hei_usda', 'HEI (SD)', 3), ('prep_time_raw', 'Preparation-time score (1--9)', 3),
                ('upf_share', 'Ultra-processed share of calories (\\%)', 2), ('produce_share', 'Produce share of calories (\\%)', 2),
                ('sugar_1000', 'Sugar per 1,000 kcal (g)', 2), ('price_1000', 'Price paid per 1,000 kcal, all food (2013\\$)', 3), ('price_upf_1000', 'Price paid per 1,000 kcal, ultra-processed (2013\\$)', 3),
                ('price_other_1000', 'Price paid per 1,000 kcal, other food (2013\\$)', 3), ('cals_ae_day', 'Calories per adult equivalent per day', 1), ('n_trips', 'Trips with food per year', 1),
                ('spend_ae', 'Food spending per adult equivalent (2013\\$)', 1), ('nonfood_ae', 'Non-food spending on food trips, per adult equivalent (2013\\$)', 1),
                ('food_share_trip', 'Food share of trip totals (\\%)', 2)]
CHECK_OUTCOMES = ['hei_usda', 'prep_time_raw', 'upf_share', 'cals_ae_day', 'n_trips', 'spend_ae', 'nonfood_ae', 'food_share_trip']


def fig_grid(c, s):
    fig, axes = plt.subplots(len(FIG_OUTCOMES), len(EVENTS), figsize=(30, 33), sharex=True, sharey='row')
    fig.subplots_adjust(wspace=0.08, hspace=0.14)
    for j, ev in enumerate(EVENTS):
        n = int(s[(s['spec'] == 'main') & (s['event'] == ev)]['n_movers'].iloc[0])
        axes[0, j].set_title(f'{EVENT_TITLES[ev]}\n{n:,} households', pad=16)
        for i, (y, lab) in enumerate(FIG_OUTCOMES):
            ax = axes[i, j]
            d = c[(c['spec'] == 'main') & (c['outcome'] == y) & (c['event'] == ev)].sort_values('r')
            b = c[(c['spec'] == 'balanced') & (c['outcome'] == y) & (c['event'] == ev) & c['r'].between(-2, 2)].sort_values('r')
            r = s[(s['spec'] == 'main') & (s['outcome'] == y) & (s['event'] == ev)].iloc[0]
            ax.plot(d['r'], d['b'], color=BLUE, lw=3, marker='o', ms=7, label='all movers')
            ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
            ax.plot(b['r'], b['b'], color=ORANGE, lw=3, ls='--', marker='s', ms=6, label='movers observed in every year from $-2$ to $2$')
            ax.axhline(0, color=GREY, lw=1.2, ls='--'); ax.axvline(-0.5, color=GREY, lw=1.2, ls=':')
            ax.set_xticks([-4, -2, 0, 2, 4])
            dec = 3 if y in ('prep_time_raw', 'price_1000', 'hei_usda') else (0 if y in ('cals_ae_day',) else (1 if y == 'n_trips' else 2))
            ax.text(0.03, 0.97, f'post {r["jump"]:+.{dec}f} ({r["jump_se"]:.{dec}f})\npre-trend p {r["p_pretrend"]:.2f}', transform=ax.transAxes, va='top', fontsize=19)
            if j == 0:
                ax.set_ylabel(lab)
            if i == len(FIG_OUTCOMES) - 1:
                ax.set_xlabel('Year relative to the event')
    h, l = axes[0, 0].get_legend_handles_labels()
    fig.legend(h, l, loc='lower center', ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.045))
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / 'time_events.png', bbox_inches='tight', dpi=110); plt.close(fig)
    print(f'saved {FIG / "time_events.png"}')


def fmt(v, dec):
    return f'{v:.{dec}f}'


def table_main(s):
    m = s[s['spec'] == 'main']
    ns = {ev: int(m[m['event'] == ev]['n_movers'].iloc[0]) for ev in EVENTS}
    out = ['\\begin{tabular}{l' + 'c' * len(EVENTS) + '}', '\\toprule',
           ' & \\multicolumn{3}{c}{More time at home} & \\multicolumn{2}{c}{Less time at home} \\\\', '\\cmidrule(lr){2-4} \\cmidrule(lr){5-6}',
           ' & ' + ' & '.join(EVENT_TITLES[e] for e in EVENTS) + ' \\\\', 'Households & ' + ' & '.join(f'{ns[e]:,}' for e in EVENTS) + ' \\\\', '\\midrule']
    for y, lab, dec in TAB_OUTCOMES:
        rows = [m[(m['outcome'] == y) & (m['event'] == e)].iloc[0] for e in EVENTS]
        lead = lab
        out.append(lead + ' & ' + ' & '.join(star_num(r['jump'], r['jump_se'], dec) for r in rows) + ' \\\\')
        out.append(f'\\quad mean before {sum(r["mean_pre"] for r in rows) / len(rows):.{max(dec - 1, 0) if y != "prep_time_raw" else 2}f} & ' + ' & '.join(f'({fmt(r["jump_se"], dec)}) [{r["p_pretrend"]:.2f}]' for r in rows) + ' \\\\')
    n0 = int(m['n_controls'].iloc[0])
    out += ['\\midrule', f'\\multicolumn{{{len(EVENTS) + 1}}}{{l}}{{Comparison group: {n0:,} households whose heads\' employment never changes.}} \\\\', '\\bottomrule', '\\end{tabular}']
    TAB.mkdir(parents=True, exist_ok=True)
    (TAB / 'time_events.tex').write_text('\n'.join(out) + '\n')
    print(f'saved {TAB / "time_events.tex"}')


def table_checks(s):
    labs = {y: (lab, dec) for y, lab, dec in TAB_OUTCOMES}
    b = s[s['spec'] == 'balanced']; c = s[s['spec'] == 'samecell']
    ns = {ev: int(b[b['event'] == ev]['n_movers'].iloc[0]) for ev in EVENTS}
    out = ['\\begin{tabular}{l' + 'c' * len(EVENTS) + '}', '\\toprule', ' & ' + ' & '.join(EVENT_TITLES[e] for e in EVENTS) + ' \\\\', '\\midrule',
           '\\multicolumn{6}{l}{\\textit{A. Movers observed in every year from $r=-2$ to $r=2$: mean of $r=1,2$ (SE) [coefficient at $r=-2$]}} \\\\',
           'Households & ' + ' & '.join(f'{ns[e]:,}' for e in EVENTS) + ' \\\\']
    for y in CHECK_OUTCOMES:
        lab, dec = labs[y]
        rows = [b[(b['outcome'] == y) & (b['event'] == e)].iloc[0] for e in EVENTS]
        out.append(lab + ' & ' + ' & '.join(star_num(r['jump12'], r['jump12_se'], dec) for r in rows) + ' \\\\')
        out.append(' & ' + ' & '.join(f'({fmt(r["jump12_se"], dec)}) [{fmt(r["lead2"], dec)}]' for r in rows) + ' \\\\')
    out += ['\\midrule', '\\multicolumn{6}{l}{\\textit{B. Compared with never-changers of the same starting employment state and age group: mean of $r=1,2,3$ (SE) [pre-trend $p$]}} \\\\']
    for y in CHECK_OUTCOMES:
        lab, dec = labs[y]
        rows = [c[(c['outcome'] == y) & (c['event'] == e)].iloc[0] for e in EVENTS]
        out.append(lab + ' & ' + ' & '.join(fmt(r['jump'], dec) for r in rows) + ' \\\\')
        out.append(' & ' + ' & '.join(f'({fmt(r["jump_se"], dec)}) [{r["p_pretrend"]:.2f}]' for r in rows) + ' \\\\')
    out += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'time_events_checks.tex').write_text('\n'.join(out) + '\n')
    print(f'saved {TAB / "time_events_checks.tex"}')


def table_by_head(s):
    labs = {y: (lab, dec) for y, lab, dec in TAB_OUTCOMES}
    outcomes = ['real_income', 'hei_usda', 'prep_time_raw', 'upf_share', 'cals_ae_day', 'n_trips', 'spend_ae', 'nonfood_ae', 'food_share_trip']
    d = s[s['spec'].isin(['male', 'female'])]
    out = ['\\begin{tabular}{l' + 'cc' * len(EVENTS) + '}', '\\toprule',
           ' & ' + ' & '.join(f'\\multicolumn{{2}}{{c}}{{{EVENT_TITLES[e]}}}' for e in EVENTS) + ' \\\\',
           ' '.join(f'\\cmidrule(lr){{{2 + 2 * k}-{3 + 2 * k}}}' for k in range(len(EVENTS))),
           'Head whose employment changes & ' + ' & '.join(['Male', 'Female'] * len(EVENTS)) + ' \\\\']
    cells = [(e, sp) for e in EVENTS for sp in ('male', 'female')]
    n = [int(d[(d['event'] == e) & (d['spec'] == sp)]['n_movers'].iloc[0]) for e, sp in cells]
    out.append('Households & ' + ' & '.join(f'{v:,}' for v in n) + ' \\\\')
    out.append('\\midrule')
    for y in outcomes:
        lab, dec = labs[y]
        rows = [d[(d['outcome'] == y) & (d['event'] == e) & (d['spec'] == sp)].iloc[0] for e, sp in cells]
        out.append(lab + ' & ' + ' & '.join(fmt(r['jump'], dec) for r in rows) + ' \\\\')
        out.append(' & ' + ' & '.join(f'({fmt(r["jump_se"], dec)})' for r in rows) + ' \\\\')
    out += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'time_events_by_head.tex').write_text('\n'.join(out) + '\n')
    print(f'saved {TAB / "time_events_by_head.tex"}')


def main():
    for stem in ('time_events_coefs', 'time_events_summary'):     # one file per Stata process -> one file; a new part replaces the same spec and outcome
        parts = sorted(BASE.glob(f'{stem}_*.csv'))
        if parts:
            new = pd.concat([pd.read_csv(f) for f in parts], ignore_index=True)
            old_path = BASE / f'{stem}.csv'
            if old_path.exists():
                old = pd.read_csv(old_path)
                keys = new[['spec', 'outcome']].drop_duplicates().assign(_new=1)
                old = old.merge(keys, on=['spec', 'outcome'], how='left')
                new = pd.concat([old[old['_new'].isna()].drop(columns='_new'), new], ignore_index=True)
            new.to_csv(old_path, index=False)
            for f in parts:
                f.unlink()
    c = pd.read_csv(BASE / 'time_events_coefs.csv'); s = pd.read_csv(BASE / 'time_events_summary.csv')
    c, s = to_sd(c), to_sd(s)     # HEI in standard deviations, the paper's unit
    fig_grid(c, s); table_main(s); table_checks(s); table_by_head(s)


if __name__ == '__main__':
    main()
