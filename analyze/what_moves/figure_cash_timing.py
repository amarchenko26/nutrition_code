"""Figure for the cash-timing comparisons (build_cash_timing_did.do, 2026-11-05).

Left: EITC refund window 2017-2020, the daily difference between refund-eligible households and households eligible only for SNAP in the
healthy share of a shopping day's grocery spending (percentage points, March 1 = 0; household and date fixed
effects; 95% bands). Right: SNAP-eligible households minus households eligible only for the EITC by week of the month (the daily panel keeps only households eligible for one of the two programs, so there is no high-income comparison group), weeks 2-4 relative to
week 1, for the healthy, produce and unhealthy shares. Reads interim/what_moves/cash_timing_coefs.csv.
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
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'xtick.labelsize': 12,
                     'ytick.labelsize': 12, 'legend.fontsize': 11, 'axes.spines.top': False, 'axes.spines.right': False})


def main():
    d = pd.read_csv(idir / 'what_moves' / 'cash_timing_coefs.csv')
    fig, axes = plt.subplots(1, 2, figsize=(16, 6), gridspec_kw={'width_ratios': [1.5, 1]})
    # left: EITC daily path
    ax = axes[0]
    x = d[d['design'] == 'eitc_daily'].copy()
    x['doy'] = x['term'].astype(int)
    x = x.sort_values('doy')
    x['date'] = [datetime.date(2019, 1, 1) + datetime.timedelta(days=int(k) - 1) for k in x['doy']]
    ax.plot(x['date'], x['b'], color=BLUE, lw=2.5, marker='o', ms=4)
    ax.fill_between(x['date'], x['b'] - 1.96 * x['se'], x['b'] + 1.96 * x['se'], color=BLUE, alpha=0.15, lw=0)
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.axvspan(datetime.date(2019, 2, 22), datetime.date(2019, 3, 7), color=ORANGE, alpha=0.15, lw=0)
    s = d[(d['design'] == 'eitc') & (d['years'] == '2017-2020') & (d['outcome'] == 'hs')].iloc[0]
    ax.set_title(f'EITC refunds, 2017-2020: eligible households minus SNAP-only households, healthy share of the day\'s spending\n'
                 f'refund window Feb 22 - Mar 7 (shaded): {s["b"]:+.2f} pct. points (SE {s["se"]:.2f}); '
                 f'{int(s["n_treated_hh"]):,} eligible households', fontsize=11)
    ax.set_ylabel('Difference in healthy share (pct. points), Mar 1 = 0')
    ax.set_xlabel('Date')
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%b %d'))
    # right: SNAP week of month
    ax = axes[1]
    labs = {'hs': 'healthy share', 'ps': 'produce share', 'us': 'unhealthy share'}
    cols = {'hs': BLUE, 'ps': ORANGE, 'us': RED}
    w = np.arange(2, 5)
    for i, (o, lab) in enumerate(labs.items()):
        y = d[(d['design'] == 'snap') & (d['outcome'] == o)].sort_values('term')
        ax.errorbar(w + (i - 1) * 0.25, y['b'], yerr=1.96 * y['se'], fmt='o', color=cols[o], ms=7, capsize=4, lw=2, label=lab)
    ax.axhline(0, color=GREY, lw=1, ls='--')
    ax.set_xticks(w); ax.set_xticklabels(['week 2\n(days 8-14)', 'week 3\n(days 15-21)', 'week 4\n(days 22-31)'])
    n = int(d[d['design'] == 'snap'].iloc[0]['n_treated_hh'])
    ax.set_title(f'SNAP-eligible minus EITC-only households, by week of the month\n(week 1 = days 1-7 omitted; {n:,} eligible households, 2004-2020)', fontsize=11)
    ax.set_ylabel('Difference in share of the day\'s spending (pct. points)')
    ax.legend(frameon=False)
    out = fdir / 'what_moves' / 'income' / 'cash_timing.png'
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches='tight', dpi=150)
    print(f'saved {out}')
    print(d[d['design'] != 'eitc_daily'].round(3).to_string(index=False))


if __name__ == '__main__':
    main()
