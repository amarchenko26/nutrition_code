"""Match-rate and selectivity of coverage over Nielsen panel years.

Left panel:  spend-weighted share of purchases matched to each source by year
             (USDA also unweighted / row-share).
Right panel: non-produce selectivity -- spend-weighted module healthiness
             (claude_hi_norm) of the true basket vs the USDA-matched and
             USDA-unmatched baskets, nutrient-scored non-produce goods only.

Input:   interim/usda_comparison/coverage_by_year.parquet
Output:  Overleaf figs/usda_diagnostics/coverage_over_time.pdf
"""
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
FIG_DIR = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/usda_diagnostics')
plt.rcParams.update({
    'axes.labelsize': 17, 'axes.titlesize': 18, 'xtick.labelsize': 14,
    'ytick.labelsize': 14, 'legend.fontsize': 14, 'lines.linewidth': 3,
    'axes.spines.top': False, 'axes.spines.right': False, 'figure.dpi': 150,
})
SYN_C, USDA_C = '#1f77b4', '#d62728'

c = pd.read_parquet(BASE / 'interim' / 'usda_comparison' / 'coverage_by_year.parquet'
                    ).sort_values('panel_year')
yr = c['panel_year']

fig, axes = plt.subplots(1, 2, figsize=(14.5, 6))

ax = axes[0]
ax.plot(yr, c['spend_syn_cal'] / c['spend_total'], color=SYN_C, label='Syndigo (spend-wt.)')
ax.plot(yr, c['spend_usda_cal'] / c['spend_total'], color=USDA_C, label='USDA (spend-wt.)')
ax.plot(yr, c['rows_usda_cal'] / c['rows_total'], color=USDA_C, linestyle=':',
        linewidth=2.5, label='USDA (unweighted)')
ax.set_xlabel('Panel year'); ax.set_ylabel('Share of purchases matched')
ax.set_title('Match rate to each source')
ax.legend(frameon=False); ax.set_ylim(0, 1)

ax = axes[1]
# unmatched derived by subtraction (all - matched): robust to the mask-dtype bug
# that corrupted the stored np_usda_un columns in early builder runs
unm_hi = (c['hiwt_np_all'] - c['hiwt_np_usda']) / (c['spend_np_all'] - c['spend_np_usda'])
ax.plot(yr, c['hiwt_np_all'] / c['spend_np_all'], color='black', label='All non-produce (true basket)')
ax.plot(yr, c['hiwt_np_usda'] / c['spend_np_usda'], color=USDA_C, label='USDA-matched')
ax.plot(yr, unm_hi, color=USDA_C, linestyle='--', label='USDA-unmatched')
ax.set_xlabel('Panel year'); ax.set_ylabel('Module healthiness (claude_hi, 0-1)')
ax.set_title('Healthiness of covered vs. missed spend\n(nutrient-scored non-produce goods)')
ax.legend(frameon=False)

fig.tight_layout()
fig.savefig(FIG_DIR / 'coverage_over_time.pdf')

print("Spend-weighted match by year:")
out = pd.DataFrame({'year': yr,
                    'syn': (c['spend_syn_cal'] / c['spend_total']).round(3),
                    'usda': (c['spend_usda_cal'] / c['spend_total']).round(3),
                    'np_true_hi': (c['hiwt_np_all'] / c['spend_np_all']).round(3),
                    'np_usda_hi': (c['hiwt_np_usda'] / c['spend_np_usda']).round(3),
                    'np_unm_hi': ((c['hiwt_np_all'] - c['hiwt_np_usda'])
                                  / (c['spend_np_all'] - c['spend_np_usda'])).round(3)})
print(out.to_string(index=False))
print("Saved coverage_over_time.pdf")
