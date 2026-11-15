"""Analysis cells to Stata, with module healthiness (claude_hi; unhealthy = 0-3; groups 0-3 / 4-5 / 6-10).

Usage: python 5_export_cells_dta.py <END>
Reads  interim/variety/jaravel_food/cells_2006_<END>/analysis_main.parquet, interim/what_moves/module_groups.parquet.
Writes interim/variety/jaravel_food/cells_2006_<END>/analysis_main.dta.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403

END = int(sys.argv[1])
d = DATA / f'cells_2006_{END}'
a = pd.read_parquet(d / 'analysis_main.parquet')
a = a.merge(pd.read_parquet(idir / 'what_moves' / 'module_groups.parquet')[['product_module_code', 'claude_hi', 'hi_group']],
            on='product_module_code', how='left')
a['unhealthy'] = np.where(a['claude_hi'].notna(), (a['claude_hi'] <= 3).astype(float), np.nan)
a['hi3'] = a['hi_group']
keep = ['product_module_code', 'quality_rank', 'product_group_code', 'department_code', 'claude_hi', 'unhealthy', 'hi3',
        'RMS_avg_spending', 'RMS_lces_price_index_win', 'RMS_ssnp', 'RMS_ssep', 'RMS_spend_new', 'RMS_spend_all',
        'RMS_avg_Dlog_spending', 'inst_raw', 'inst_linageres', 'inst_allres']
keep += [c for c in a.columns if c.startswith('RMS_lfullces_pi_win_') or c.startswith('RMS_avg_Dlog_realspending_')]
a[keep].to_stata(d / 'analysis_main.dta', write_index=False, version=118)
print(f"{len(a):,} cells; modules without a healthiness score: {a.loc[a.claude_hi.isna(), 'product_module_code'].nunique()}")
