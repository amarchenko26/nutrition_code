"""Figure for the cash-timing comparisons, households with children on both sides (build_cash_timing_checks.do, 2026-11-06).

Refund-eligible households all have children, while most SNAP-only households do not, and households with children shop differently
in late January and February whether or not a refund arrives. So both panels keep households with children only.
Left: EITC refund window 2017-2020, the daily difference between refund-eligible households and other low-income households with
children in the healthy share of a shopping day's grocery spending (percentage points, March 1 = 0; household and date fixed
effects; 95% bands). Right: SNAP-eligible households with children minus EITC-only households by week of the month, weeks 2-4 relative
to week 1, for the healthy, produce and unhealthy shares. Reads interim/what_moves/cash_timing_checks.csv.
Output: figs/what_moves/income/cash_timing.png
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import datetime
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, RED, GREY, ORANGE = '#2c5f8a', '#a63d40', '#7a7a7a', '#fc8d59'
plt.rcParams.update({'font.size': 22, 'axes.labelsize': 22, 'axes.titlesize': 22, 'xtick.labelsize': 19,
                     'ytick.labelsize': 19, 'legend.fontsize': 19, 'axes.spines.top': False, 'axes.spines.right': False})


def main():
    d = pd.read_csv(idir / 'what_moves' / 'cash_timing_checks.csv')
    fig, axes = plt.subplots(1, 2, figsize=(22, 9), gridspec_kw={'width_ratios': [1.4, 1]})
    fig.subplots_adjust(wspace=0.22)
    # left: EITC daily path
    ax = axes[0]
    x = d[d['check'] == 'kids_daily'].copy()
    x['doy'] = x['window'].astype(int)
    x = x.sort_values('doy')
    x['date'] = [datetime.date(2019, 1, 1) + datetime.timedelta(days=int(k) - 1) for k in x['doy']]
    ax.plot(x['date'], x['b'], color=BLUE, lw=3, marker='o', ms=5)
    ax.fill_between(x['date'], x['b'] - 1.96 * x['se'], x['b'] + 1.96 * x['se'], color=BLUE, alpha=0.15, lw=0)
    ax.axhline(0, color=GREY, lw=1.2, ls='--')
    ax.axvspan(datetime.date(2019, 2, 22), datetime.date(2019, 3, 7), color=ORANGE, alpha=0.2, lw=0)
    ax.set_title('EITC refunds, 2017-2020: healthy share of the day\'s spending,\nrefund window shaded')
    ax.set_ylabel('Eligible minus other households (pct. points)')
    ax.set_xlabel('Date')
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%b %d'))
    # right: SNAP week of month
    ax = axes[1]
    labs = {'hs': 'Healthy', 'ps': 'Produce', 'us': 'Unhealthy'}
    cols = {'hs': BLUE, 'ps': ORANGE, 'us': RED}
    w = np.arange(2, 5)
    for i, (o, lab) in enumerate(labs.items()):
        y = d[(d['check'] == 'snap_kids') & (d['outcome'] == o)].sort_values('window')
        ax.errorbar(w + (i - 1) * 0.2, y['b'], yerr=1.96 * y['se'], fmt='o', color=cols[o], ms=9, capsize=5, lw=2.5, label=lab)
    ax.axhline(0, color=GREY, lw=1.2, ls='--')
    ax.set_xticks(w)
    ax.set_xticklabels(['Week 2', 'Week 3', 'Week 4'])
    ax.set_title('SNAP, 2004-2020: shares of the day\'s spending\nby week of the month, week 1 = 0')
    ax.set_ylabel('SNAP minus EITC-only households (pct. points)')
    ax.set_ylim(top=0.55)
    ax.legend(frameon=False, loc='upper center', ncol=3, columnspacing=1.0, handletextpad=0.3)
    out = fdir / 'what_moves' / 'income' / 'cash_timing.png'
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches='tight', dpi=120)
    print(f'saved {out}')
    print(d[d['check'] != 'kids_daily'].round(3).to_string(index=False))


if __name__ == '__main__':
    main()
