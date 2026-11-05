"""Figure and table for the employment-change event studies (build_time_events.do).

Reads interim/what_moves/time_events_coefs.csv and time_events_summary.csv.
Outputs: figs/what_moves/time/time_events.png (rows = events, columns = outcomes: each panel the
change in the outcome r years from the event for movers relative to the year before, net of the
never-changers, 95% bands) and tabs/what_moves/time_events.tex (post-event mean of the r = 1 to 3
coefficients with SE, and the pre-trend p, per event x outcome).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BASE = idir / 'what_moves'
FIG = fdir / 'what_moves' / 'time'
TAB = tdir / 'what_moves'
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#999999'
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 13, 'axes.titlesize': 12, 'xtick.labelsize': 11, 'ytick.labelsize': 11,
                     'axes.spines.top': False, 'axes.spines.right': False})

EVENTS = ['retirement', 'job loss', 're-employment', 'hours cut', 'hours increase']
OUTCOMES = [('real_income', 'Household income', '$10k'), ('hei_usda', 'HEI', 'points'), ('prep_time_raw', 'Preparation-time score of the basket', 'points, 1-9'),
            ('upf_share', 'Ultra-processed share of calories', 'pct. points'), ('cals_ae_day', 'Calories per adult equivalent per day', 'kcal'),
            ('produce_share', 'Produce share of calories', 'pct. points'), ('sugar_1000', 'Sugar per 1,000 kcal', 'grams'),
            ('n_trips', 'Shopping trips per year', 'trips'), ('spend_ae', 'Grocery spending per adult equivalent', 'nominal $ per year')]


def main():
    c = pd.read_csv(BASE / 'time_events_coefs.csv'); s = pd.read_csv(BASE / 'time_events_summary.csv')
    fig, axes = plt.subplots(len(EVENTS), len(OUTCOMES), figsize=(4.2 * len(OUTCOMES), 3.6 * len(EVENTS)))
    fig.subplots_adjust(wspace=0.35, hspace=0.6)
    for i, ev in enumerate(EVENTS):
        for j, (y, title, unit) in enumerate(OUTCOMES):
            ax = axes[i, j]
            d = c[(c['outcome'] == y) & (c['event'] == ev)].sort_values('r')
            r = s[(s['outcome'] == y) & (s['event'] == ev)].iloc[0]
            ax.plot(d['r'], d['b'], color=BLUE, lw=2.2, marker='o', ms=4.5)
            ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
            ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
            ax.set_xticks(range(-5, 6))
            ax.set_title(f'{ev} ({int(r["n_movers"]):,} hh): {title}\npost {r["jump"]:+.2f} (SE {r["jump_se"]:.2f}) · pre-trend p {r["p_pretrend"]:.2f} · mean {r["mean_pre"]:.1f}', fontsize=9.5)
            if j == 0:
                ax.set_ylabel('Change vs year −1')
            if i == len(EVENTS) - 1:
                ax.set_xlabel('Year relative to the event')
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / 'time_events.png', bbox_inches='tight', dpi=130); plt.close(fig)
    print(f'saved {FIG / "time_events.png"}')
    # table: rows events (two lines: post, pre-trend p), columns outcomes
    hdr = ' & '.join(f'\\shortstack{{{t}\\\\({u})}}' for _, t, u in OUTCOMES)
    out = ['\\begin{tabular}{l' + 'c' * len(OUTCOMES) + '}', '\\toprule', 'Event (households) & ' + hdr + ' \\\\', '\\midrule']
    for ev in EVENTS:
        rows = {y: s[(s['outcome'] == y) & (s['event'] == ev)].iloc[0] for y, _, _ in OUTCOMES}
        n = int(next(iter(rows.values()))['n_movers'])
        out.append(f'{ev} ({n:,}) & ' + ' & '.join(f'{r["jump"]:.2f} ({r["jump_se"]:.2f})' for r in rows.values()) + ' \\\\')
        out.append('\\quad pre-trend $p$ & ' + ' & '.join(f'{r["p_pretrend"]:.2f}' for r in rows.values()) + ' \\\\')
        out.append('\\quad level at $r=-1$ & ' + ' & '.join(f'{r["mean_pre"]:.1f}' for r in rows.values()) + ' \\\\')
    n0 = int(s['n_controls'].iloc[0])
    out += ['\\midrule', f'\\multicolumn{{{len(OUTCOMES) + 1}}}{{l}}{{Controls: {n0:,} households whose heads\' employment never changes.}} \\\\', '\\bottomrule', '\\end{tabular}']
    TAB.mkdir(parents=True, exist_ok=True)
    (TAB / 'time_events.tex').write_text('\n'.join(out) + '\n')
    print(f'saved {TAB / "time_events.tex"}')
    print(s.pivot(index='event', columns='outcome', values='jump').round(3).to_string())


if __name__ == '__main__':
    main()
