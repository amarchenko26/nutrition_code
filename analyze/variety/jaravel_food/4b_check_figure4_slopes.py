"""Check against Jaravel (2019) Figure IV (prints only): slopes of the Feenstra ratio and continuing-product inflation on
real spending growth and on the price decile, weights log mean cell spending, clustered by module (his 4e_Figure4.do).

Reads interim/variety/jaravel_food/cells_2006_<END>/analysis_main.parquet.   Usage: python 4b_check_figure4_slopes.py <END>
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
import statsmodels.api as sm  # noqa: E402

END = int(sys.argv[1])
a = pd.read_parquet(DATA / f'cells_2006_{END}' / 'analysis_main.parquet')
a['ces_infl'] = (a['RMS_ces_price_index_win'] - 1) * 100
S = lambda y: a[f'RMS_total_spending_QM_{y}']  # noqa: E731
a['real_growth_pc'] = ((((S(END - 1) + S(END)) / (S(2006) + S(2007))) ** (1 / (END - 2007)) - 1) * 100
                       - (np.exp(a['RMS_lfullces_pi_win_s57']) - 1) * 100)


def ols(y, x, d):
    r = sm.WLS(d[y], sm.add_constant(d[x]), weights=np.log(d['RMS_avg_spending'])).fit(cov_type='cluster', cov_kwds={'groups': d['product_module_code']})
    return r.params[x], r.bse[x]


lo, hi = a['real_growth_pc'].quantile([.05, .95])
t = a[a['real_growth_pc'].between(lo, hi)]
print('B: CES inflation on real growth  %.5f (%.5f)   his -0.04713 (0.00542)' % ols('ces_infl', 'real_growth_pc', t))
print('A: Feenstra ratio on real growth %.5f (%.5f)   his -0.00139 (0.00014)' % ols('RMS_feenstra_win', 'real_growth_pc', t))
print('E: Feenstra ratio on decile      %.5f (%.5f)   his -0.00313 (0.00023)' % ols('RMS_feenstra_win', 'quality_rank', a))
print('F: CES inflation on decile       %.5f (%.5f)   his -0.24418 (0.01060)' % ols('ces_infl', 'quality_rank', a))
