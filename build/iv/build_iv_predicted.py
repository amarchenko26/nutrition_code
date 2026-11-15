"""
Predicted-share income IV (Anya's advisor, planned 2026-09-17, built 2026-09-29).

The production instrument (build_iv.py) is the change in the leave-one-county-
out income of the household's OWN cell:
    d_iv(t) = z[cell at t](t) - z[cell at t-1](t-1)
88% of its variance comes from households switching cells (review memo §7),
and the switch (job loss, retirement, a child leaving) is realized at t, so it
can move nutrition through channels other than income.

This builder replaces the realized year-t cell with a distribution over cells
PREDICTED FROM t-1 INFORMATION ONLY ("predicted shares"):
    z_pred(t)   = sum_c P(c | info at t-1) * z_c(t)
    d_iv_pred   = z_pred(t) - z[cell at t-1](t-1)
which splits into
    d_iv_frozen    = z[cell at t-1](t)  - z[cell at t-1](t-1)   within-cell shift
    d_iv_expswitch = z_pred(t)          - z[cell at t-1](t)     expected switch gain

THE LADDER (Anya 2026-09-29: start simple, add predictors only if needed;
`--version 1|2|3`, default 1). All versions predict the 16 (occupation x size)
bins at t; education is held at its t-1 bin (education-code changes are
symmetric reporting noise, review memo §7), so each household has positive
probability on 16 of the 64 cells. Every predictor is measured at t-1.
  V1  empirical transition frequencies of a 96-group table: origin occupation
      bin (4) x origin size bin (4) x cell-defining head's age bin
      (<30, 30-44, 45-54, 55-61, 62-66, 67+).
  V2  the same table refined by the cell-defining head's hours bin (<30,
      30-34, 35+, not employed) and the age-and-presence-of-children code;
      thin groups fall back to the V1 group, then to occupation x age.
  V3  multinomial logit (sklearn, lbfgs, light ridge) on the full t-1
      predictor set: cell-defining head's raw occupation code, 5-year age
      bin and hours bin; the other head's presence, occupation bin, hours
      bin and age bin; exact household size; education bin; marital status;
      children code; age of the oldest child (member codes 1/2 = son/
      daughter); number of children aged 18-25; plus origin occupation x
      age bin and origin size x oldest-child interactions. Income, health
      conditions, WIC and geography are excluded by design.

CROSS-FITTING: households are hashed into 5 folds (fixed seed); the table for
fold k is estimated on the other four folds, so a household's own move never
enters its own predicted shares. Groups with fewer than MIN_GROUP_N training
pairs fall back to the coarser occupation x age table.

Cell incomes z_c(t) are computed for EVERY cell x county x year with the same
leave-one-county-out formula, weights, deflator, and MIN_CELL_N donor floor as
build_iv.py; the own-cell values reproduce iv_income_fips exactly (asserted).
Destination cells below the floor are dropped from the sum and the shares are
renormalised; the row is set missing if less than MIN_DEFINED_MASS of the
predicted mass has a defined cell income.

Outputs (V1 as named; V2/V3 add the suffix _v2 / _v3 to the data and csv
files and write no Overleaf table -- Anya 2026-09-29, "don't change the
Overleaf")
  interim/panel_dataset/iv_predicted.parquet / .dta   (one row per HH-year t
      with a defined cell at t-1 and a t-1 row in the panel; $1,000s 2013$,
      like iv_income_fips -- the .do files divide by 10)
    household_code, panel_year, fold, grp, grp_n_train, grp_coarse,
    p_stay, p_switch, p_notemp, p_realized, defined_mass,
    iv_pred_level, iv_frozen_level, iv_own_lag,
    d_iv_pred, d_iv_frozen, d_iv_expswitch,
    dest_defined, switched, dest_notemp        (realized, for the fit report)
  interim/labor_supply/predicted_shares_fit.csv, predicted_shares_calib.csv
  Overleaf tabs/labor_supply/predicted_shares_fit.tex   (out-of-fold fit)
"""

import sys
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_iv import (PANELISTS, BASE, MIN_CELL_N, deflate_income_k,
                      bin_occupation, SIZE_BINS, SIZE_LABELS,
                      EDUC_BINS, EDUC_LABELS)

OUT_PATH  = BASE / 'interim' / 'panel_dataset' / 'iv_predicted.parquet'
FIT_DIR   = BASE / 'interim' / 'labor_supply'
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
OVERLEAF  = overleaf
FIT_TEX   = OVERLEAF / 'tabs' / 'labor_supply' / 'predicted_shares_fit.tex'
IV_PROD   = BASE / 'interim' / 'panel_dataset' / 'iv_income.parquet'

AGE_BINS   = [0, 29.999, 44.999, 54.999, 61.999, 66.999, np.inf]
AGE_LABELS = ['lt30', '30_44', '45_54', '55_61', '62_66', '67p']
OCC_LABELS = ['white', 'blue', 'service', 'not_emp']

N_FOLDS          = 5
SEED             = 20260929
MIN_GROUP_N      = 50      # training pairs; below this use the occ x age table
MIN_DEFINED_MASS = 0.95    # predicted mass that must have a defined cell income

GROUP_COLS    = ['occ_lag', 'size_lag', 'age_lag']
GROUP_COLS_V2 = ['occ_lag', 'size_lag', 'age_lag', 'hours_lag', 'kids_lag']
COARSE_COLS   = ['occ_lag', 'age_lag']
DEST_COLS     = ['occ_t', 'size_t']

# V3 logit features (all measured at t-1; categorical, one-hot encoded)
V3_FEATURES = ['occ_code_lag', 'hsize_lag', 'educ_lag', 'age_fine_lag', 'hours_lag',
               'oth_present_lag', 'oth_occ_lag', 'oth_hours_lag', 'oth_age_lag',
               'marital_lag', 'kids_lag', 'oldest_child_lag', 'n_child_18_25_lag',
               'occ_x_age_lag', 'size_x_oldest_lag']
LOGIT_C        = 100.0   # sklearn inverse ridge penalty (light regularisation)
LOGIT_MAX_ITER = 2000

CHILD_CODES = {1, 2}     # member relationship codes: son, daughter
AGE_FINE_BINS   = [0, 24.999, 29.999, 34.999, 39.999, 44.999, 49.999, 54.999, 59.999, 64.999, 69.999, 74.999, np.inf]
AGE_FINE_LABELS = ['lt25', '25_29', '30_34', '35_39', '40_44', '45_49', '50_54', '55_59', '60_64', '65_69', '70_74', '75p']
OLDEST_BINS   = [-np.inf, 12.999, 16.999, 18.999, 24.999, np.inf]
OLDEST_LABELS = ['lt13', '13_16', '17_18', '19_24', '25p']


# ============================================================ preparation
def prep_panel():
    pan = pd.read_parquet(PANELISTS)
    print(f"  {len(pan):,} HH-year rows in the panelist file")
    pan['inc']  = deflate_income_k(pan)
    pan['fips'] = (pan['fips_state_code'].astype(str).str.zfill(2) +
                   pan['fips_county_code'].astype(str).str.zfill(3))
    pan['w']    = pd.to_numeric(pan['projection_factor'], errors='coerce')
    pan['educ_bin'] = pd.cut(pan['hh_avg_yrsofschool'], bins=EDUC_BINS, labels=EDUC_LABELS).astype(object)
    pan['size_bin'] = pd.cut(pd.to_numeric(pan['household_size'], errors='coerce'),
                             bins=SIZE_BINS, labels=SIZE_LABELS).astype(object)
    pan['occ_bin']  = bin_occupation(pan)

    # cell-defining head's exact age: the male head's when he defines the cell
    # (present, not armed forces), else the female head's -- same rule as
    # bin_occupation()
    male_occ = pd.to_numeric(pan['male_head_occupation'], errors='coerce')
    from build_iv import OCC_MAP
    male_defines = male_occ.map(OCC_MAP).notna()
    pan['head_age'] = np.where(male_defines, pan['male_head_age_exact'], pan['female_head_age_exact'])
    pan['age_bin']  = pd.cut(pan['head_age'], bins=AGE_BINS, labels=AGE_LABELS).astype(object)

    pan['cell_key'] = pan['size_bin'].astype(str) + '|' + pan['educ_bin'].astype(str) + '|' + pan['occ_bin'].astype(str)
    pan.loc[pan[['size_bin', 'educ_bin', 'occ_bin']].isna().any(axis=1), 'cell_key'] = np.nan
    add_finer_predictors(pan, male_defines)
    return pan


def add_finer_predictors(pan, male_defines):
    """V2/V3 predictors (all categorical strings). 'head' = the cell-defining
    head (bin_occupation's rule), 'oth' = the other head."""
    from build_iv import OCC_MAP
    def col(sex, name):
        return pd.to_numeric(pan[f'{sex}_head_{name}'], errors='coerce')
    m, f = male_defines, ~male_defines
    # the cell-defining head's raw occupation code (10 and 12 pooled: not employed)
    occ_code = np.where(m, col('male', 'occupation'), col('female', 'occupation'))
    occ_code = pd.Series(occ_code, index=pan.index)
    pan['occ_code'] = occ_code.where(~occ_code.isin([10, 12]), 12).astype('Int64').astype(str)
    # hours bin: employment code 1 <30h, 2 30-34h, 3 35+h, 9 not employed
    hours = pd.Series(np.where(m, col('male', 'employment'), col('female', 'employment')), index=pan.index)
    pan['hours_bin'] = hours.where(hours.isin([1, 2, 3, 9])).astype('Int64').astype(str)
    pan['age_fine'] = pd.cut(pan['head_age'], bins=AGE_FINE_BINS, labels=AGE_FINE_LABELS).astype(object)
    # the other head: present if their exact age is known (no-head rows have no birth year)
    oth_age   = pd.Series(np.where(m, pan['female_head_age_exact'], pan['male_head_age_exact']), index=pan.index)
    oth_occ   = pd.Series(np.where(m, col('female', 'occupation'), col('male', 'occupation')), index=pan.index)
    oth_hours = pd.Series(np.where(m, col('female', 'employment'), col('male', 'employment')), index=pan.index)
    present = oth_age.notna()
    pan['oth_present'] = present.astype(int).astype(str)
    pan['oth_occ']   = np.where(present, oth_occ.map(OCC_MAP).fillna('other'), 'absent')
    pan['oth_hours'] = np.where(present, oth_hours.where(oth_hours.isin([1, 2, 3, 9])).astype('Int64').astype(str), 'absent')
    pan['oth_age']   = np.where(present, pd.cut(oth_age, bins=AGE_BINS, labels=AGE_LABELS).astype(object), 'absent')
    pan['marital'] = pd.to_numeric(pan['marital_status'], errors='coerce').astype('Int64').astype(str)
    pan['kids']    = pd.to_numeric(pan['age_and_presence_of_children'], errors='coerce').astype('Int64').astype(str)
    pan['hsize']   = pd.to_numeric(pan['household_size'], errors='coerce').clip(upper=7).astype('Int64').astype(str)
    # children from the member records: oldest child's age, children aged 18-25
    yr = pd.to_numeric(pan['panel_year'], errors='coerce')
    oldest = pd.Series(np.nan, index=pan.index)
    n1825  = pd.Series(0, index=pan.index)
    for i in range(1, 8):
        rel = pd.to_numeric(pan[f'member_{i}_relationship_sex'], errors='coerce')
        age = (yr - pd.to_numeric(pan[f'member_{i}_birth'], errors='coerce')).where(rel.isin(CHILD_CODES))
        oldest = np.fmax(oldest, age)
        n1825 += age.between(18, 25).astype(int)
    pan['oldest_child']  = pd.cut(oldest, bins=OLDEST_BINS, labels=OLDEST_LABELS).astype(object)
    pan['oldest_child']  = pan['oldest_child'].where(oldest.notna(), 'none')
    pan['n_child_18_25'] = n1825.clip(upper=2).astype(str)
    pan['occ_x_age']     = pan['occ_bin'].astype(str) + 'x' + pan['age_bin'].astype(str)
    pan['size_x_oldest'] = pan['size_bin'].astype(str) + 'x' + pan['oldest_child'].astype(str)


def cell_income_table(pan):
    """Leave-one-county-out cell income for EVERY cell x county x year (build_iv formula)."""
    v = pan.dropna(subset=['inc', 'fips', 'w', 'cell_key']).copy()
    v = v[v['w'] > 0]
    v['wi'] = v['w'] * v['inc']
    cell_tot = (v.groupby(['panel_year', 'cell_key'])
                 .agg(cell_wi=('wi', 'sum'), cell_w=('w', 'sum'), cell_n=('w', 'size')).reset_index())
    cz_tot = (v.groupby(['panel_year', 'cell_key', 'fips'])
               .agg(cz_wi=('wi', 'sum'), cz_w=('w', 'sum'), cz_n=('w', 'size')).reset_index())
    counties = v[['panel_year', 'fips']].drop_duplicates()
    z = cell_tot.merge(counties, on='panel_year', how='inner')          # cell x county x year
    z = z.merge(cz_tot, on=['panel_year', 'cell_key', 'fips'], how='left')
    for c in ['cz_wi', 'cz_w', 'cz_n']:
        z[c] = z[c].fillna(0)
    lo_w = z['cell_w'] - z['cz_w']
    z['z'] = (z['cell_wi'] - z['cz_wi']) / lo_w.replace(0, np.nan)
    z['n_lo'] = z['cell_n'] - z['cz_n']
    z.loc[z['n_lo'] < MIN_CELL_N, 'z'] = np.nan
    print(f"  cell income table: {len(z):,} cell x county x year rows, "
          f"{z['z'].isna().mean():.1%} below the {MIN_CELL_N}-donor floor")
    return z[['panel_year', 'fips', 'cell_key', 'z']]


def validate_against_production(pan, z):
    own = pan.dropna(subset=['cell_key'])[['household_code', 'panel_year', 'fips', 'cell_key']]
    own = own.merge(z, on=['panel_year', 'fips', 'cell_key'], how='left')
    prod = pd.read_parquet(IV_PROD, columns=['household_code', 'panel_year', 'iv_income_fips'])
    chk = own.merge(prod, on=['household_code', 'panel_year'], how='inner')
    both = chk.dropna(subset=['z', 'iv_income_fips'])
    gap = (both['z'] - both['iv_income_fips']).abs().max()
    n_mismatch_na = (chk['z'].isna() != chk['iv_income_fips'].isna()).sum()
    print(f"  validation vs iv_income_fips: {len(both):,} rows, max |diff| = {gap:.2e}, "
          f"missingness mismatches = {n_mismatch_na:,}")
    assert gap < 1e-6 and n_mismatch_na == 0, "own-cell z does not reproduce iv_income_fips"


# ============================================================ transitions
def build_pairs(pan):
    """One row per HH-year t with a t-1 row: origin (t-1) variables + destination (t) bins."""
    keep_t   = ['household_code', 'panel_year', 'fips', 'occ_bin', 'size_bin', 'educ_bin', 'cell_key']
    extra = ['occ_code', 'hours_bin', 'age_fine', 'oth_present', 'oth_occ', 'oth_hours', 'oth_age',
             'marital', 'kids', 'hsize', 'oldest_child', 'n_child_18_25', 'occ_x_age', 'size_x_oldest']
    keep_lag = ['household_code', 'panel_year', 'fips', 'occ_bin', 'size_bin', 'educ_bin',
                'cell_key', 'head_age', 'age_bin', 'inc', 'w'] + extra
    t   = pan[keep_t].rename(columns={'occ_bin': 'occ_t', 'size_bin': 'size_t', 'educ_bin': 'educ_t',
                                      'cell_key': 'cell_t', 'fips': 'fips_t'})
    lag = pan[keep_lag].rename(columns={'occ_bin': 'occ_lag', 'size_bin': 'size_lag', 'educ_bin': 'educ_lag',
                                        'cell_key': 'cell_lag', 'fips': 'fips_lag', 'head_age': 'age_lag_exact',
                                        'age_bin': 'age_lag', 'inc': 'inc_lag', 'w': 'w_lag',
                                        'occ_code': 'occ_code_lag', 'hours_bin': 'hours_lag', 'age_fine': 'age_fine_lag',
                                        'oth_present': 'oth_present_lag', 'oth_occ': 'oth_occ_lag',
                                        'oth_hours': 'oth_hours_lag', 'oth_age': 'oth_age_lag',
                                        'marital': 'marital_lag', 'kids': 'kids_lag', 'hsize': 'hsize_lag',
                                        'oldest_child': 'oldest_child_lag', 'n_child_18_25': 'n_child_18_25_lag',
                                        'occ_x_age': 'occ_x_age_lag', 'size_x_oldest': 'size_x_oldest_lag'})
    lag['panel_year'] = lag['panel_year'] + 1
    pairs = t.merge(lag, on=['household_code', 'panel_year'], how='inner')
    # origin cell must exist (and the t-1 row must be a valid donor row, as
    # the production instrument requires), and the head's age must be known
    pairs = pairs.dropna(subset=['cell_lag', 'age_lag', 'inc_lag', 'w_lag'])
    pairs = pairs[pairs['w_lag'] > 0].copy()
    pairs['dest_defined'] = pairs[DEST_COLS].notna().all(axis=1)
    pairs['switched']     = ((pairs['occ_t'] != pairs['occ_lag']) | (pairs['size_t'] != pairs['size_lag'])).where(pairs['dest_defined'])
    pairs['dest_notemp']  = (pairs['occ_t'] == 'not_emp').where(pairs['dest_defined'])
    print(f"  {len(pairs):,} consecutive-year pairs with a defined origin cell; "
          f"{pairs['dest_defined'].mean():.1%} have a defined destination cell")
    return pairs


def assign_folds(pairs):
    hh = pairs['household_code'].unique()
    rng = np.random.default_rng(SEED)
    fold = pd.Series(rng.permutation(len(hh)) % N_FOLDS, index=hh)
    pairs['fold'] = pairs['household_code'].map(fold).astype(int)


def transition_probs(train, group_cols):
    """Rows: group; columns: the 16 destination bins; values: frequencies. Plus n_train."""
    cnt = train.groupby(group_cols + DEST_COLS).size().rename('n').reset_index()
    cnt['dest'] = cnt['occ_t'] + '|' + cnt['size_t']
    wide = cnt.pivot_table(index=group_cols, columns='dest', values='n', fill_value=0)
    n = wide.sum(axis=1)
    probs = wide.div(n, axis=0)
    probs['n_train'] = n
    return probs.reset_index()


def table_predict(train, test, levels, dest_names):
    """Empirical transition frequencies, finest level first; a row takes the
    finest level with at least MIN_GROUP_N training pairs (the last level
    regardless). grp_coarse = index of the level used (0 = finest)."""
    pred = test.copy()
    for d in dest_names:
        pred[d] = np.nan
    pred['n_train'] = np.nan
    pred['grp_coarse'] = -1
    for li, cols in enumerate(levels):
        tbl = transition_probs(train, cols)
        for d in dest_names:
            if d not in tbl.columns:
                tbl[d] = 0.0
        m = pred[cols].merge(tbl, on=cols, how='left')
        m.index = pred.index
        ok = m['n_train'].notna() & ((m['n_train'] >= MIN_GROUP_N) | (li == len(levels) - 1))
        fill = ok & (pred['grp_coarse'] < 0)
        for d in dest_names + ['n_train']:
            pred.loc[fill, d] = m.loc[fill, d].values
        pred.loc[fill, 'grp_coarse'] = li
    return pred


def logit_predict(train, test, dest_names):
    """Multinomial logit on one-hot V3_FEATURES; out-of-fold predicted probabilities."""
    from sklearn.linear_model import LogisticRegression
    Xtr = pd.get_dummies(train[V3_FEATURES].astype(str), drop_first=True, dtype=np.float32)
    Xte = pd.get_dummies(test[V3_FEATURES].astype(str), drop_first=True, dtype=np.float32)
    Xte = Xte.reindex(columns=Xtr.columns, fill_value=0.0)
    ytr = (train['occ_t'] + '|' + train['size_t']).to_numpy()
    model = LogisticRegression(multi_class='multinomial', solver='lbfgs', C=LOGIT_C,
                               max_iter=LOGIT_MAX_ITER, tol=1e-5)
    model.fit(Xtr.to_numpy(), ytr)
    P = pd.DataFrame(model.predict_proba(Xte.to_numpy()), columns=model.classes_, index=test.index)
    P = P.reindex(columns=dest_names, fill_value=0.0)
    pred = test.copy()
    for d in dest_names:
        pred[d] = P[d].values
    pred['n_train'] = float(len(train))
    pred['grp_coarse'] = 0
    print(f"    logit: {Xtr.shape[1]} dummies, {len(train):,} training pairs, "
          f"{model.n_iter_[0]} iterations, in-sample mean log-lik "
          f"{np.mean(np.log(np.clip(model.predict_proba(Xtr.to_numpy())[np.arange(len(ytr)), pd.Index(model.classes_).get_indexer(ytr)], 1e-9, None))):.4f}")
    return pred


def crossfit_shares(pairs, version):
    """Out-of-fold predicted shares over the 16 destinations for every pair."""
    train_all = pairs[pairs['dest_defined']]
    dest_names = sorted((train_all['occ_t'] + '|' + train_all['size_t']).unique())
    assert len(dest_names) == 16, dest_names
    levels = {1: [GROUP_COLS, COARSE_COLS], 2: [GROUP_COLS_V2, GROUP_COLS, COARSE_COLS]}
    out = []
    for k in range(N_FOLDS):
        train = train_all[train_all['fold'] != k]
        test  = pairs[pairs['fold'] == k]
        print(f"  fold {k}: {len(train):,} training pairs -> {len(test):,} predictions")
        if version in (1, 2):
            pred = table_predict(train, test, levels[version], dest_names)
        else:
            pred = logit_predict(train, test, dest_names)
        out.append(pred)
    pred = pd.concat(out).sort_values(['household_code', 'panel_year'])
    pred['grp'] = pred['occ_lag'] + '|' + pred['size_lag'] + '|' + pred['age_lag']   # V1 group, for FE
    pred['grp_n_train'] = pred['n_train'].astype(int)
    # summaries of the predicted distribution
    stay_key = pred['occ_lag'] + '|' + pred['size_lag']
    P = pred[dest_names].to_numpy()
    pred['p_stay']   = P[np.arange(len(pred)), pd.Index(dest_names).get_indexer(stay_key)]
    pred['p_switch'] = 1 - pred['p_stay']
    pred['p_notemp'] = pred[[d for d in dest_names if d.startswith('not_emp|')]].sum(axis=1)
    pred['p_max']    = P.max(axis=1)
    real_key = (pred['occ_t'].astype(str) + '|' + pred['size_t'].astype(str))
    idx = pd.Index(dest_names).get_indexer(real_key)
    pred['p_realized'] = np.where(idx >= 0, P[np.arange(len(pred)), np.clip(idx, 0, None)], np.nan)
    return pred, dest_names


# ============================================================ instrument
def build_instrument(pred, dest_names, z):
    # expected year-t cell income under the predicted shares (education fixed at t-1)
    long = pred[['household_code', 'panel_year', 'fips_t', 'educ_lag'] + dest_names].melt(
        id_vars=['household_code', 'panel_year', 'fips_t', 'educ_lag'], var_name='dest', value_name='p')
    long = long[long['p'] > 0]
    occ = long['dest'].str.split('|').str[0]
    size = long['dest'].str.split('|').str[1]
    long['cell_key'] = size + '|' + long['educ_lag'] + '|' + occ
    long = long.merge(z.rename(columns={'fips': 'fips_t'}), on=['panel_year', 'fips_t', 'cell_key'], how='left')
    long['pz'] = long['p'] * long['z']
    long['p_def'] = long['p'].where(long['z'].notna(), 0)
    agg = (long.groupby(['household_code', 'panel_year'])
               .agg(pz=('pz', 'sum'), p_def=('p_def', 'sum')).reset_index())
    agg['iv_pred_level'] = agg['pz'] / agg['p_def'].replace(0, np.nan)
    agg.loc[agg['p_def'] < MIN_DEFINED_MASS, 'iv_pred_level'] = np.nan
    agg = agg.rename(columns={'p_def': 'defined_mass'})[['household_code', 'panel_year', 'iv_pred_level', 'defined_mass']]
    pred = pred.merge(agg, on=['household_code', 'panel_year'], how='left')

    # frozen t-1 cell at year t (own county at t) and the t-1 own-cell income
    zt = z.rename(columns={'fips': 'fips_t', 'cell_key': 'cell_lag', 'z': 'iv_frozen_level'})
    pred = pred.merge(zt, on=['panel_year', 'fips_t', 'cell_lag'], how='left')
    zl = z.rename(columns={'fips': 'fips_lag', 'cell_key': 'cell_lag', 'z': 'iv_own_lag'}).copy()
    zl['panel_year'] = zl['panel_year'] + 1
    pred = pred.merge(zl, on=['panel_year', 'fips_lag', 'cell_lag'], how='left')

    pred['d_iv_pred']      = pred['iv_pred_level']   - pred['iv_own_lag']
    pred['d_iv_frozen']    = pred['iv_frozen_level'] - pred['iv_own_lag']
    pred['d_iv_expswitch'] = pred['iv_pred_level']   - pred['iv_frozen_level']
    return pred


# ============================================================ fit report
def fit_report(pred, version):
    ok = pred[pred['dest_defined']].copy()
    ok['log_p_realized'] = np.log(np.clip(ok['p_realized'], 1e-9, None))
    def block(df, label):
        return pd.Series({
            'group': label, 'n_pairs': len(df),
            'switch_actual': df['switched'].astype(float).mean(), 'switch_pred': df['p_switch'].mean(),
            'notemp_actual': df['dest_notemp'].astype(float).mean(), 'notemp_pred': df['p_notemp'].mean(),
            'p_realized': df['p_realized'].mean(), 'log_p_realized': df['log_p_realized'].mean(),
            'p_max': df['p_max'].mean(), 'share_coarse': (df['grp_coarse'] > 0).mean()})
    rows = [block(ok, 'All')]
    rows += [block(ok[ok['occ_lag'] == o], f'occ:{o}') for o in OCC_LABELS]
    rows += [block(ok[ok['age_lag'] == a], f'age:{a}') for a in AGE_LABELS]
    fit = pd.DataFrame(rows)
    # calibration of the predicted not-employed probability, deciles
    ok['dec'] = pd.qcut(ok['p_notemp'].rank(method='first'), 10, labels=False) + 1
    calib = (ok.groupby('dec').agg(n=('p_notemp', 'size'), notemp_pred=('p_notemp', 'mean'),
                                   notemp_actual=('dest_notemp', lambda s: s.astype(float).mean()),
                                   switch_pred=('p_switch', 'mean'),
                                   switch_actual=('switched', lambda s: s.astype(float).mean())).reset_index())
    FIT_DIR.mkdir(parents=True, exist_ok=True)
    sfx = '' if version == 1 else f'_v{version}'
    fit.to_csv(FIT_DIR / f'predicted_shares_fit{sfx}.csv', index=False)
    calib.to_csv(FIT_DIR / f'predicted_shares_calib{sfx}.csv', index=False)
    print(f"\n  Out-of-fold fit, V{version} (unweighted; dest_defined pairs):")
    print(fit.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    print("\n  Calibration by decile of predicted P(not employed at t):")
    print(calib.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    if version == 1:
        write_fit_tex(fit)
    return fit, calib


def write_fit_tex(fit):
    occ_lab = {'white': 'White collar', 'blue': 'Blue collar', 'service': 'Service/other', 'not_emp': 'Not employed'}
    age_lab = {'lt30': 'Under 30', '30_44': '30--44', '45_54': '45--54', '55_61': '55--61', '62_66': '62--66', '67p': '67+'}
    def line(r, name):
        return (f"{name} & {int(r['n_pairs']):,} & {100*r['switch_actual']:.1f} & {100*r['switch_pred']:.1f}"
                f" & {100*r['notemp_actual']:.1f} & {100*r['notemp_pred']:.1f} & {r['p_realized']:.2f} \\\\")
    L = ["\\begin{tabular}{lrccccc}", "\\toprule",
         " & & \\multicolumn{2}{c}{Switch cell (\\%)} & \\multicolumn{2}{c}{To not employed (\\%)} & Prob. of \\\\",
         "\\cmidrule(lr){3-4} \\cmidrule(lr){5-6}",
         "Origin at \\(t-1\\) & Pairs & Actual & Predicted & Actual & Predicted & realized cell \\\\",
         "\\midrule"]
    L.append(line(fit.iloc[0], 'All'))
    L.append("\\midrule")
    for o in OCC_LABELS:
        L.append(line(fit[fit['group'] == f'occ:{o}'].iloc[0], occ_lab[o]))
    L.append("\\midrule")
    for a in AGE_LABELS:
        L.append(line(fit[fit['group'] == f'age:{a}'].iloc[0], age_lab[a]))
    L += ["\\bottomrule", "\\end{tabular}"]
    FIT_TEX.parent.mkdir(parents=True, exist_ok=True)
    FIT_TEX.write_text("\n".join(L) + "\n")
    print(f"  wrote {FIT_TEX}")


# ============================================================ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--version', type=int, default=1, choices=[1, 2, 3])
    version = ap.parse_args().version
    sfx = '' if version == 1 else f'_v{version}'
    out_path = OUT_PATH.with_name(OUT_PATH.stem + sfx + OUT_PATH.suffix)
    print(f"Predicted-share IV, version {version} -> {out_path.name}")
    print("Loading and preparing the panelist file...")
    pan = prep_panel()
    print("Cell incomes for every cell x county x year...")
    z = cell_income_table(pan)
    validate_against_production(pan, z)
    print("Consecutive-year transition pairs...")
    pairs = build_pairs(pan)
    assign_folds(pairs)
    print(f"Cross-fitted predicted shares ({N_FOLDS} folds, version {version})...")
    pred, dest_names = crossfit_shares(pairs, version)
    print(f"  {(pred['grp_coarse'] > 0).mean():.2%} of rows fall back to a coarser table")
    print("Instrument...")
    pred = build_instrument(pred, dest_names, z)
    fit_report(pred, version)

    keep = ['household_code', 'panel_year', 'fold', 'grp', 'grp_n_train', 'grp_coarse',
            'p_stay', 'p_switch', 'p_notemp', 'p_max', 'p_realized', 'defined_mass',
            'iv_pred_level', 'iv_frozen_level', 'iv_own_lag',
            'd_iv_pred', 'd_iv_frozen', 'd_iv_expswitch',
            'dest_defined', 'switched', 'dest_notemp']
    out = pred[keep].copy()
    for c in ['dest_defined', 'switched', 'dest_notemp']:
        out[c] = out[c].astype(float)
    print("\n  Instrument summary ($1,000s, 2013$):")
    print(out[['d_iv_pred', 'd_iv_frozen', 'd_iv_expswitch', 'p_switch', 'p_notemp', 'defined_mass']]
          .describe().T.to_string(float_format=lambda x: f"{x:.3f}"))
    print(f"  d_iv_pred defined: {out['d_iv_pred'].notna().sum():,} / {len(out):,}")
    print("  corr(d_iv_frozen, d_iv_expswitch) = "
          f"{out[['d_iv_frozen', 'd_iv_expswitch']].corr().iloc[0, 1]:.3f}")

    out.to_parquet(out_path, index=False)
    out.to_stata(out_path.with_suffix('.dta'), write_index=False)
    print(f"\nSaved {out_path} and .dta\nDone.")


if __name__ == '__main__':
    main()
