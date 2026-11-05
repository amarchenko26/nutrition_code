"""Export the USDA HI panel to Stata .dta for merging into build_hi.do."""
import pandas as pd
from pathlib import Path
D = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset')
df = pd.read_parquet(D / 'hi_usda_panel.parquet')
df['household_code'] = df['household_code'].astype('int64')
df['panel_year']     = df['panel_year'].astype('int32')
value_cols = [c for c in df.columns if c not in ('household_code', 'panel_year')]
for c in value_cols:
    df[c] = df[c].astype('float64')
out = D / 'hi_usda_panel.dta'
df.to_stata(out, write_index=False, version=118)
print(f"wrote {out}  ({len(df):,} rows, {df['hi_usda'].notna().sum():,} non-missing hi_usda)")
print(df[['hi_usda','hi_usda_allcott']].describe().round(4).to_string())
