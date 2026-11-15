"""Organic flag per barcode from Nielsen products_extra, 2004-2020.

Reads  raw/consumer/Consumer_Panel_Data_<y>.tgz, member nielsen_extracts/HMS/<y>/Annual_Files/products_extra_<y>.tsv
       (streamed from the archive; nothing is extracted to disk); raw/products.tsv (upc_descr, for the comparison below).
Writes interim/variety/jaravel_food/organic_upc.parquet (upc, claim_org, seal_org, coded, organic, descr_org).
organic = USDA organic seal or an organic claim (ORG, ORGANIC, OG and variants) in any year; coded = Nielsen recorded the
seal or claim field for the barcode in at least one year (the flag is missing otherwise).  Printed: agreement with
ORG / ORGANIC in the product description, which catches about 30% of flagged organic barcodes.
"""
import io
import sys
import tarfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403

COLS = ['upc', 'upc_ver_uc', 'panel_year', 'organic_claim_code', 'organic_claim_descr', 'usda_organic_seal_code', 'usda_organic_seal_descr']
frames = []
for y in range(2004, 2021):
    target = f'nielsen_extracts/HMS/{y}/Annual_Files/products_extra_{y}.tsv'
    d = None
    with tarfile.open(rdir / 'consumer' / f'Consumer_Panel_Data_{y}.tgz', 'r|gz') as tf:
        for member in tf:
            if member.name == target:
                d = pd.read_csv(io.BytesIO(tf.extractfile(member).read()), sep='\t', quoting=3, encoding='latin-1', usecols=COLS,
                                dtype={'upc': 'int64', 'organic_claim_descr': str, 'usda_organic_seal_descr': str})
                break
    assert d is not None, f'{target} not found'
    frames.append(d)
    print(y, f'{len(d):,} rows', flush=True)
d = pd.concat(frames, ignore_index=True)
cl = d['organic_claim_descr'].astype(str).str.upper()
d['claim_org'] = cl.str.contains(r'ORG|\bOG\b', regex=True) & ~cl.str.contains('NOT STATED')
d['seal_org'] = d['usda_organic_seal_descr'].astype(str).str.upper().eq('USDA ORGANIC SEAL')
d['coded'] = d['organic_claim_descr'].notna() | d['usda_organic_seal_descr'].notna()
u = d.groupby('upc').agg(claim_org=('claim_org', 'max'), seal_org=('seal_org', 'max'), coded=('coded', 'max')).reset_index()
u['organic'] = (u['claim_org'] | u['seal_org']).astype(int)
p = pd.read_csv(rdir / 'products.tsv', sep='\t', quoting=3, encoding='latin-1', dtype={'upc': 'int64'},
                usecols=['upc', 'upc_ver_uc', 'upc_descr', 'department_descr']).sort_values('upc_ver_uc').drop_duplicates('upc')
p['descr_org'] = p['upc_descr'].fillna('').str.upper().str.contains(r'\bORG\b|ORGANIC|\bORGNC\b|\bOG\b', regex=True).astype(int)
u = u.merge(p[['upc', 'descr_org', 'department_descr']], on='upc', how='left')
c = u[u.coded & ~u['department_descr'].isin(['HEALTH & BEAUTY CARE', 'GENERAL MERCHANDISE', 'NON-FOOD GROCERY', 'ALCOHOLIC BEVERAGES'])]
print(f"barcodes {len(u):,}; coded food barcodes {len(c):,}, organic {int(c.organic.sum()):,}; description says organic for "
      f"{c.loc[c.organic == 1, 'descr_org'].mean():.3f} of flagged organic barcodes; flagged among description hits {c.loc[c.descr_org == 1, 'organic'].mean():.3f}")
u.drop(columns='department_descr').to_parquet(DATA / 'organic_upc.parquet', index=False)
