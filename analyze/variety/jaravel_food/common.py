"""Shared paths and helpers for the Jaravel (2019) Table 5 replication on food cells (analyze/variety/jaravel_food).

Reads nothing itself.
  DATA     = interim/variety/jaravel_food: every intermediate file of the pipeline.
  RMS_NAT  = DATA/rms_national: national RMS barcode totals copied from Oscar (0_collapse_rms_national.py).
  FIG_OUT, TAB_OUT = default exhibit folders in the Overleaf clone (figs|tabs/variety/jaravel_food). Scripts that
           write exhibits take --out <dir> to write elsewhere, and --extra to also write the exhibits that are not in the paper.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *  # noqa: E402,F401,F403

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

DATA = idir / 'variety' / 'jaravel_food'
RMS_NAT = DATA / 'rms_national'
FIG_OUT = fdir / 'variety' / 'jaravel_food'
TAB_OUT = tdir / 'variety' / 'jaravel_food'
DATA.mkdir(parents=True, exist_ok=True)


def out_dirs(argv):
    """(--out dir or the Overleaf defaults, --extra flag)."""
    extra = '--extra' in argv
    if '--out' in argv:
        d = Path(argv[argv.index('--out') + 1])
        return d, d, extra
    return FIG_OUT, TAB_OUT, extra


def stata_pctile(x, p):
    """Percentile as Stata's `summarize, detail` computes it (r(p1), r(p99), ...)."""
    x = np.sort(np.asarray(x, dtype=float))
    x = x[~np.isnan(x)]
    n = len(x)
    if n == 0:
        return np.nan
    np_ = n * p / 100.0
    i = int(np.floor(np_))
    if np_ == i:            # integer: average of x_(i) and x_(i+1) (1-based)
        if i == 0:
            return x[0]
        if i >= n:
            return x[-1]
        return (x[i - 1] + x[i]) / 2.0
    return x[min(i, n - 1)]  # x_(floor+1), 1-based


def assign_deciles(df, price_col, module_col='product_module_code', key_col='upc'):
    """His rule (4b / 1d / 1e): within module, sort barcodes by price per size unit, rank = n/N*100 (stored as float),
    decile = smallest r in 1..10 with rank <= 10r.  Ties are broken by UPC (his Stata sort is random)."""
    df = df.sort_values([module_col, price_col, key_col], kind='mergesort').copy()
    n = df.groupby(module_col).cumcount() + 1
    N = df.groupby(module_col)[key_col].transform('size')
    rank = (n / N * 100.0).astype(np.float32).astype(np.float64)
    edges = np.arange(10, 101, 10, dtype=np.float64)
    df['quality_rank'] = np.searchsorted(edges, rank.to_numpy(), side='left') + 1
    return df


def load_products():
    """Products master (raw/products.tsv): one row per UPC, version 1 (his RMS merge sets upc_ver_uc = 1)."""
    p = pd.read_csv(rdir / 'products.tsv', sep='\t', quoting=3, encoding='latin-1',
                    usecols=['upc', 'upc_ver_uc', 'product_module_code', 'product_group_code',
                             'department_code', 'department_descr', 'multi', 'size1_amount', 'size1_units'],
                    dtype={'upc': 'int64'})
    p = p[p['upc_ver_uc'] == 1].drop(columns='upc_ver_uc')
    return p
