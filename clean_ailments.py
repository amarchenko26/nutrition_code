"""
Clean Nielsen Ailments Data

Extracts dietary/metabolic disease indicators from Nielsen Health Care Survey
data across all available years (2011-2023) and saves a household × year panel.

Target conditions:
  cholesterol    - Cholesterol Problems (high cholesterol, triglycerides)
  prediabetes    - Pre-Diabetes
  diabetes_type1 - Diabetes Type I
  diabetes_type2 - Diabetes Type II
  heart_disease  - Heart Disease / Heart Attack / Angina / Heart Failure
  hypertension   - High Blood Pressure / Hypertension
  obesity        - Obesity / Overweight

Format changes across years (verified from format/layout files):

  2011-2015  Q1_Ailment## Description    positions {11,13,14,15,21,22,30}
  2016       Q16_Ailment# (exact)        positions {16,20,21,22,30,32,43}
  2017       Q16_Ailment# (exact)        positions {16,20,21,22,31,33,44}
  2018       Q10_Ailment# (exact)        positions {16,20,21,22,31,33,44}
  2019-2021  Q36_## - Description        positions {16,20,21,22,31,33,44}
  2022-2023  10 (Q36_##) (exact)         positions {14,17,18,19,28,29,38}

Note: 2020 and 2021 data files have the parsed data on a non-primary sheet.

Output:
  interim/ailments/dietary_ailments_by_household.parquet
  Columns: household_code, panel_year, cholesterol, prediabetes,
           diabetes_type1, diabetes_type2, heart_disease, hypertension,
           obesity, any_diabetes, any_metabolic_disease, n_dietary_conditions
"""

import os
import re
import pandas as pd
import numpy as np

BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
AILMENTS_DIR  = os.path.join(BASE_DATA_DIR, 'raw', 'ailments')
OUTPUT_DIR    = os.path.join(BASE_DATA_DIR, 'interim', 'ailments')

TARGET_CONDITIONS = [
    'cholesterol', 'prediabetes', 'diabetes_type1', 'diabetes_type2',
    'heart_disease', 'hypertension', 'obesity',
]

# ---------------------------------------------------------------------------
# Hardcoded column position → condition mappings (from format files)
# ---------------------------------------------------------------------------

Q1_TARGETS = {
    11: 'cholesterol', 13: 'prediabetes', 14: 'diabetes_type1',
    15: 'diabetes_type2', 21: 'heart_disease', 22: 'hypertension', 30: 'obesity',
}

Q16_TARGETS_2016 = {
    16: 'cholesterol', 20: 'prediabetes', 21: 'diabetes_type1',
    22: 'diabetes_type2', 30: 'heart_disease', 32: 'hypertension', 43: 'obesity',
}

# 2017-2021: heart/hypertension/obesity shifted by 1-1-1 position
Q16_TARGETS_2017 = {
    16: 'cholesterol', 20: 'prediabetes', 21: 'diabetes_type1',
    22: 'diabetes_type2', 31: 'heart_disease', 33: 'hypertension', 44: 'obesity',
}

Q36_TARGETS_2022 = {
    14: 'cholesterol', 17: 'prediabetes', 18: 'diabetes_type1',
    19: 'diabetes_type2', 28: 'heart_disease', 29: 'hypertension', 38: 'obesity',
}

# fmt: column prefix style ('Q1', 'Q16', 'Q10', 'Q36d'=Q36 with desc suffix, '10Q36')
YEAR_CONFIG = {
    2011: ('Q1',    Q1_TARGETS),
    2012: ('Q1',    Q1_TARGETS),
    2013: ('Q1',    Q1_TARGETS),
    2014: ('Q1',    Q1_TARGETS),
    2015: ('Q1',    Q1_TARGETS),
    2016: ('Q16',   Q16_TARGETS_2016),
    2017: ('Q16',   Q16_TARGETS_2017),
    2018: ('Q10',   Q16_TARGETS_2017),
    2019: ('Q36d',  Q16_TARGETS_2017),
    2020: ('Q36d',  Q16_TARGETS_2017),
    2021: ('Q36d',  Q16_TARGETS_2017),
    2022: ('10Q36', Q36_TARGETS_2022),
    2023: ('10Q36', Q36_TARGETS_2022),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_hh_col(cols):
    for c in cols:
        cl = c.lower().strip()
        if 'household id' in cl or 'hhid' in cl or 'panelistid' in cl:
            return c
    return cols[0]


def read_data_file(fpath):
    """Read the parsed data xlsx, handling multi-sheet files by finding the
    sheet with actual column headers (not unnamed/pivot tables)."""
    xl = pd.ExcelFile(fpath, engine='openpyxl')
    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        unnamed = sum(1 for c in df.columns if str(c).startswith('Unnamed:'))
        if unnamed <= 2:
            return df
    # fallback: first sheet
    return xl.parse(xl.sheet_names[0])


def find_data_file(year_dir):
    """Return filename of the main data xlsx (not format/layout/corrected)."""
    files = []
    for f in os.listdir(year_dir):
        if not f.endswith('.xlsx') or f.startswith('~'):
            continue
        fl = f.lower()
        if any(kw in fl for kw in ('format', 'layout', 'corrected')):
            continue
        files.append(f)
    if not files:
        return None
    return max(files, key=lambda f: os.path.getsize(os.path.join(year_dir, f)))


def find_corrected_diabetes_file(year_dir):
    for f in os.listdir(year_dir):
        if 'corrected' in f.lower() and f.endswith('.xlsx') and not f.startswith('~'):
            return os.path.join(year_dir, f)
    return None


def extract_conditions(df, fmt, targets):
    """
    Return dict {condition_name: Series(0/1)}.

    For formats with description suffixes (Q1, Q36d), builds a prefix lookup
    so that e.g. 'Q1_Ailment11 Cholesterol Problems ' → pos 11.
    """
    # Build prefix lookups for suffix-bearing formats
    if fmt == 'Q1':
        lookup = {}
        for c in df.columns:
            m = re.match(r'^Q1_Ailment(\d+)', c)
            if m:
                lookup[int(m.group(1))] = c
    elif fmt == 'Q36d':
        lookup = {}
        for c in df.columns:
            m = re.match(r'^Q36_(\d+)', c)
            if m:
                lookup[int(m.group(1))] = c
    else:
        lookup = None

    result = {}
    for pos, cond in targets.items():
        if lookup is not None:
            col = lookup.get(pos)
            if col is None:
                print(f"      WARNING: position {pos} not found in {fmt} lookup (skipping {cond})")
                continue
        elif fmt == 'Q16':
            col = f'Q16_Ailment{pos}'
        elif fmt == 'Q10':
            col = f'Q10_Ailment{pos}'
        elif fmt == '10Q36':
            col = f'10 (Q36_{pos})'
        else:
            continue

        if lookup is None and col not in df.columns:
            print(f"      WARNING: column '{col}' not found (skipping {cond})")
            continue

        vals = pd.to_numeric(df[col], errors='coerce').fillna(0)
        result[cond] = (vals == 1).astype(int)

    return result


# ---------------------------------------------------------------------------
# Per-year processing
# ---------------------------------------------------------------------------

def process_year(year):
    if year not in YEAR_CONFIG:
        return None

    year_dir = os.path.join(AILMENTS_DIR, str(year))
    if not os.path.isdir(year_dir):
        return None

    fname = find_data_file(year_dir)
    if fname is None:
        print(f"  {year}: no data file found")
        return None

    fpath = os.path.join(year_dir, fname)
    print(f"\n  {year}: {fname}")

    try:
        df = read_data_file(fpath)
    except Exception as e:
        print(f"    ERROR reading file: {e}")
        return None

    print(f"    {len(df):,} rows × {len(df.columns)} cols")

    fmt, targets = YEAR_CONFIG[year]
    print(f"    Format: {fmt}")

    hh_col = find_hh_col(list(df.columns))
    hh_ids = pd.to_numeric(df[hh_col], errors='coerce')

    conditions = extract_conditions(df, fmt, targets)
    print(f"    Extracted: {list(conditions.keys())}")

    # For 2020-2021, override diabetes type I/II with corrected file if available
    if year in (2020, 2021):
        corr_path = find_corrected_diabetes_file(year_dir)
        if corr_path:
            print(f"    Applying corrected diabetes file: {os.path.basename(corr_path)}")
            try:
                corr = read_data_file(corr_path)
                corr_fmt = YEAR_CONFIG[year][0]
                corr_hh_col = find_hh_col(list(corr.columns))
                corr_hh = pd.to_numeric(corr[corr_hh_col], errors='coerce')
                corr_conds = extract_conditions(corr, corr_fmt, targets)
                corr_df = pd.DataFrame({'hh': corr_hh})
                for cond in ('diabetes_type1', 'diabetes_type2'):
                    if cond in corr_conds:
                        corr_df[cond] = corr_conds[cond].values
                corr_df = corr_df.dropna(subset=['hh']).set_index('hh')
                hh_arr = hh_ids.values
                for cond in ('diabetes_type1', 'diabetes_type2'):
                    if cond in corr_df.columns and cond in conditions:
                        override = corr_df[cond].reindex(hh_arr).values
                        mask = ~np.isnan(override)
                        arr = conditions[cond].values.astype(float)
                        arr[mask] = override[mask]
                        conditions[cond] = pd.Series(arr.astype(int), index=conditions[cond].index)
            except Exception as e:
                print(f"    WARNING: could not apply corrected diabetes file: {e}")

    result = pd.DataFrame({'household_code': hh_ids, 'panel_year': year})
    for cond, vals in conditions.items():
        result[cond] = vals.values

    # Multiple family members per HH: keep any-member flag
    result = result.groupby('household_code', as_index=False).max()

    for cond in conditions:
        rate = result[cond].mean() * 100
        print(f"      {cond}: {rate:.1f}%")

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("CLEANING NIELSEN AILMENTS DATA")
    print("=" * 70)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    years = sorted(
        int(d) for d in os.listdir(AILMENTS_DIR)
        if d.isdigit() and os.path.isdir(os.path.join(AILMENTS_DIR, d)))
    print(f"Years found: {years}")

    frames = []
    for year in years:
        res = process_year(year)
        if res is not None:
            frames.append(res)

    if not frames:
        print("\nERROR: no data processed.")
        return

    combined = pd.concat(frames, ignore_index=True)

    for cond in TARGET_CONDITIONS:
        if cond not in combined.columns:
            combined[cond] = np.nan

    diabetes_cols = ['prediabetes', 'diabetes_type1', 'diabetes_type2']
    combined['any_diabetes'] = combined[diabetes_cols].max(axis=1)
    combined['any_metabolic_disease'] = combined[TARGET_CONDITIONS].max(axis=1)
    combined['n_dietary_conditions'] = combined[TARGET_CONDITIONS].sum(axis=1)

    print(f"\n{'='*70}")
    print(f"COMBINED: {len(combined):,} HH-year obs, "
          f"{combined['household_code'].nunique():,} unique HHs, "
          f"years {combined['panel_year'].min()}–{combined['panel_year'].max()}")
    print(f"\nOverall prevalence:")
    for cond in TARGET_CONDITIONS + ['any_diabetes', 'any_metabolic_disease']:
        rate = combined[cond].mean() * 100
        print(f"  {cond:<25s}: {rate:.1f}%")

    out_path = os.path.join(OUTPUT_DIR, 'dietary_ailments_by_household.parquet')
    combined.to_parquet(out_path, index=False)
    print(f"\nSaved: {out_path}")
    print(f"Columns: {list(combined.columns)}")


if __name__ == '__main__':
    main()
