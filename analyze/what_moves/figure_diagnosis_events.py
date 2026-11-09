"""Figures and tables for the health-diagnosis event studies (build_diagnosis_events.do).

Combines interim/what_moves/diagnosis_parts/{coefs,summary}_<part>.csv into diagnosis_events_coefs.csv and
diagnosis_events_summary.csv in interim/what_moves/ (the parts are then deleted) and reads
diagnosis_event_quality.csv (build_diagnosis_panel.py).

Outputs:
  figs/what_moves/health/diagnosis_events.png    rows: conditions; columns: HEI, sugar, calories, ultra-processed share;
                                                 households with a dated new diagnosis (blue, 95% band) and with a first
                                                 survey report (grey), against households that never report it
  figs/what_moves/health/diagnosis_products.png  the same two lines for a diabetes diagnosis: regular soda ounces, diet
                                                 share of soda, any sugar substitute, any dietetic candy
  tabs/what_moves/diagnosis_events.tex           years 1-3 and 4-6 after the diagnosis year, joint test of the years before
  tabs/what_moves/diagnosis_events_regime.tex    first reports: repeated next survey or not, 2013-2015 vs 2017-2019
  tabs/what_moves/diagnosis_products.tex         diabetes products, by event type, with the other conditions as placebos
  tabs/what_moves/diagnosis_event_quality.tex    how good the events are: recency answers and repeat rates of first reports
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.hei_units import to_sd, hei_sd, hei_mean_sd

import shutil
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
plt.rcParams.update({'font.size': 22, 'axes.labelsize': 22, 'axes.titlesize': 21, 'xtick.labelsize': 19, 'ytick.labelsize': 19,
                     'legend.fontsize': 21, 'axes.spines.top': False, 'axes.spines.right': False})
CONDS = [('diabetes', 'Type-2 diabetes'), ('hypertension', 'Hypertension'), ('cholesterol', 'High cholesterol'),
         ('heart', 'Heart disease'), ('obesity', 'Obesity')]
MAIN = [('hei_usda', 'HEI', 'SD'), ('sugar_1000', 'Sugar per 1,000 kcal', 'grams'), ('sodium_1000', 'Sodium per 1,000 kcal', 'mg'),
        ('soda_share', 'Soda share of spending', 'pct. points'), ('cals_ae_day', 'Calories per adult per day', 'kcal'),
        ('upf_share', 'Ultra-processed share', 'pct. points'), ('produce_share', 'Produce share of calories', 'pct. points')]
GRID = [m for m in MAIN if m[0] in ('hei_usda', 'sugar_1000', 'cals_ae_day', 'upf_share')]
CORE = [m for m in MAIN if m[0] in ('hei_usda', 'sugar_1000', 'cals_ae_day')]
PROD = [('reg_soda_oz_wk', 'Regular soda, ounces per week'), ('diet_share', 'Diet share of soda ounces (%)'),
        ('sweetener_any', 'Bought a sugar substitute (%)'), ('diet_candy_any', 'Bought dietetic candy (%)')]
W = idir / 'what_moves'
FIG = fdir / 'what_moves' / 'health'
TAB = tdir / 'what_moves'


def load():
    parts = W / 'diagnosis_parts'
    if parts.exists() and list(parts.glob('coefs_*.csv')):
        pd.concat([pd.read_csv(f) for f in sorted(parts.glob('coefs_*.csv'))]).to_csv(W / 'diagnosis_events_coefs.csv', index=False)
        pd.concat([pd.read_csv(f) for f in sorted(parts.glob('summary_*.csv'))]).to_csv(W / 'diagnosis_events_summary.csv', index=False)
        shutil.rmtree(parts)
    # HEI in standard deviations, the paper's unit
    return to_sd(pd.read_csv(W / 'diagnosis_events_coefs.csv')), to_sd(pd.read_csv(W / 'diagnosis_events_summary.csv'))


def pick(df, cond, regime, outcome):
    return df[(df['condition'] == cond) & (df['regime'] == regime) & (df['outcome'] == outcome)]


def event_lines(ax, coefs, cond, outcome):
    d = pick(coefs, cond, 'dated', outcome).sort_values('r')
    f = pick(coefs, cond, 'first', outcome).sort_values('r')
    ax.plot(f['r'], f['b'], color=GREY, lw=2.2, ls='--', marker='s', ms=5, label='first survey report')
    ax.plot(d['r'], d['b'], color=BLUE, lw=3, marker='o', ms=7, label='dated new diagnosis')
    ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
    ax.axhline(0, color=GREY, lw=1, ls='-'); ax.axvline(-0.5, color=GREY, lw=1.2, ls=':')
    ax.set_xticks([-5, -3, -1, 1, 3, 5])


def fig_events(coefs, summ):
    fig, axes = plt.subplots(len(CONDS), len(GRID), figsize=(24, 27))
    fig.subplots_adjust(wspace=0.30, hspace=0.42)
    for i, (c, clab) in enumerate(CONDS):
        for j, (o, olab, unit) in enumerate(GRID):
            ax = axes[i, j]
            event_lines(ax, coefs, c, o)
            s = pick(summ, c, 'dated', o).iloc[0]
            head = f'{s["post"]:+.3f} ({s["post_se"]:.3f})' if o == 'hei_usda' else f'{s["post"]:+.2f} ({s["post_se"]:.2f})' if abs(s['post']) < 100 else f'{s["post"]:+.0f} ({s["post_se"]:.0f})'
            ax.set_title((f'{olab}\n' if i == 0 else '') + f'after {head}', fontsize=21)
            if j == 0:
                ax.set_ylabel(f'{clab}\n{int(s["n_events"]):,} households\n\nchange ({unit})')
            else:
                ax.set_ylabel(unit)
            if i == len(CONDS) - 1:
                ax.set_xlabel('Year relative to diagnosis')
    h, l = axes[0, 0].get_legend_handles_labels()
    fig.legend(h[::-1], l[::-1], loc='upper center', ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.945))
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / 'diagnosis_events.png', bbox_inches='tight', dpi=120)
    plt.close(fig)
    print(f'saved {FIG / "diagnosis_events.png"}')


def fig_products(coefs, summ):
    fig, axes = plt.subplots(2, 2, figsize=(20, 14))
    fig.subplots_adjust(wspace=0.22, hspace=0.42)
    for ax, (o, olab) in zip(axes.flat, PROD):
        event_lines(ax, coefs, 'diabetes', o)
        s = pick(summ, 'diabetes', 'dated', o).iloc[0]
        ax.set_title(f'{olab}\nafter {s["post"]:+.2f} ({s["post_se"]:.2f}), mean {s["mean_m1"]:.1f}')
        ax.set_ylabel('change')
    for ax in axes[-1]:
        ax.set_xlabel('Year relative to diabetes diagnosis')
    axes[0, 0].legend(frameon=False, loc='best')
    fig.savefig(FIG / 'diagnosis_products.png', bbox_inches='tight', dpi=120)
    plt.close(fig)
    print(f'saved {FIG / "diagnosis_products.png"}')


def cell(s, a='post', b='post_se'):
    if s['outcome'] == 'hei_usda':
        return f'{s[a]:.3f} ({s[b]:.3f})'
    return f'{s[a]:.0f} ({s[b]:.0f})' if abs(s[a]) >= 100 or s[b] >= 5 else f'{s[a]:.2f} ({s[b]:.2f})'


def table_events(summ):
    out = ['\\begin{tabular}{l' + 'c' * len(MAIN) + '}', '\\toprule',
           ' & ' + ' & '.join(l for _, l, _ in MAIN) + ' \\\\', ' & ' + ' & '.join(u for _, _, u in MAIN) + ' \\\\', '\\midrule']
    for regime, title in [('dated', 'Panel A: a member says diagnosed less than a year ago'),
                          ('first', 'Panel B: first survey report of the condition')]:
        out.append(f'\\multicolumn{{{len(MAIN) + 1}}}{{l}}{{\\textit{{{title}}}}} \\\\')
        means = [summ[(summ['regime'] == regime) & (summ['outcome'] == o)]['mean_m1'].mean() for o, _, _ in MAIN]
        out.append('Mean, year before diagnosis & ' + ' & '.join(f'{m:,.2f}' if o == 'hei_usda' else f'{m:,.1f}' for m, (o, _, _) in zip(means, MAIN)) + ' \\\\')
        for c, clab in CONDS:
            rows = [pick(summ, c, regime, o).iloc[0] for o, _, _ in MAIN]
            n = max(int(r['n_events']) for r in rows)
            out.append(f'{clab} ({n:,}): years 1--3 after & ' + ' & '.join(cell(r) for r in rows) + ' \\\\')
            out.append('\\quad years 4--6 after & ' + ' & '.join(cell(r, 'late', 'late_se') for r in rows) + ' \\\\')
            out.append('\\quad years before, joint $p$ & ' + ' & '.join(f'{r["p_pretrend"]:.2f}' for r in rows) + ' \\\\')
        if regime == 'dated':
            out.append('\\midrule')
    out += ['\\bottomrule', '\\end{tabular}']
    TAB.mkdir(parents=True, exist_ok=True)
    (TAB / 'diagnosis_events.tex').write_text('\n'.join(out) + '\n')


def table_regime(summ):
    out = ['\\begin{tabular}{ll' + 'c' * len(CORE) + '}', '\\toprule',
           'Condition & First reports (households) & ' + ' & '.join(f'{l} ({u})' for _, l, u in CORE) + ' \\\\', '\\midrule']
    regs = [('first_conf', 'repeated in the next survey'), ('first_unconf', 'not repeated'), ('first_early', 'first report 2013--2015'), ('first_late', 'first report 2017--2019')]
    for c, clab in CONDS:
        for reg, rlab in regs:
            rows = [pick(summ, c, reg, o).iloc[0] for o, _, _ in CORE]
            out.append(f'{clab if reg == "first_conf" else ""} & {rlab} ({max(int(r["n_events"]) for r in rows):,}) & ' + ' & '.join(cell(r) for r in rows) + ' \\\\')
        if c != CONDS[-1][0]:
            out.append('\\midrule')
    out += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'diagnosis_events_regime.tex').write_text('\n'.join(out) + '\n')


def table_products(summ):
    rows = [('diabetes', 'dated', 'Diabetes, dated new diagnosis'), ('diabetes', 'first', 'Diabetes, first report'), ('diabetes', 'first_conf', 'Diabetes, first report repeated next survey'),
            ('hypertension', 'dated', 'Hypertension, dated'), ('cholesterol', 'dated', 'High cholesterol, dated'), ('heart', 'dated', 'Heart disease, dated'), ('obesity', 'dated', 'Obesity, dated')]
    out = ['\\begin{tabular}{l' + 'c' * len(PROD) + '}', '\\toprule', 'Event (households) & ' + ' & '.join(l.replace('%', '\\%') for _, l in PROD) + ' \\\\', '\\midrule',
           'Mean, year before a dated diabetes diagnosis & ' + ' & '.join(f'{pick(summ, "diabetes", "dated", o).iloc[0]["mean_m1"]:.1f}' for o, _ in PROD) + ' \\\\']
    for c, reg, lab in rows:
        rr = [pick(summ, c, reg, o).iloc[0] for o, _ in PROD]
        out.append(f'{lab} ({max(int(r["n_events"]) for r in rr):,}) & ' + ' & '.join(cell(r) for r in rr) + ' \\\\')
    out += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'diagnosis_products.tex').write_text('\n'.join(out) + '\n')


def table_quality():
    q = pd.read_csv(W / 'diagnosis_event_quality.csv').set_index('condition')
    out = ['\\begin{tabular}{lrrrrrrrrrr}', '\\toprule',
           ' & \\multicolumn{2}{c}{First reports, 2013--2019} & \\multicolumn{4}{c}{Answer in the first-report survey, 2013--2015 (\\%)} & & \\multicolumn{3}{c}{Members diagnosed $<$1 year ago (\\%)} \\\\',
           '\\cmidrule(lr){2-3} \\cmidrule(lr){4-7} \\cmidrule(lr){9-11}',
           'Condition & Households & Repeated next survey (\\%) & $<$1 yr & 1--2 yrs & 3--4 yrs & 5+ yrs & Dated events & Manage by diet & Prescription & No treatment \\\\', '\\midrule']
    for c, clab in CONDS:
        r = q.loc[c]
        out.append(f'{clab} & {int(r.first_reports_2013_19):,} & {r.repeat_next:.0f} & {r.lt1yr:.0f} & {r.yrs1_2:.0f} & {r.yrs3_4:.0f} & {r.yrs5plus:.0f} & {int(r.dated_events):,} & {r.diet_new:.0f} & {r.rx_new:.0f} & {r.notreat_new:.0f} \\\\')
    out += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'diagnosis_event_quality.tex').write_text('\n'.join(out) + '\n')


def main():
    coefs, summ = load()
    fig_events(coefs, summ)
    fig_products(coefs, summ)
    table_events(summ); table_regime(summ); table_products(summ); table_quality()
    print(f'saved tables in {TAB}')
    pd.set_option('display.width', 220)
    print(summ[summ['regime'] == 'dated'].pivot(index='condition', columns='outcome', values='post').round(3).to_string())


if __name__ == '__main__':
    main()
