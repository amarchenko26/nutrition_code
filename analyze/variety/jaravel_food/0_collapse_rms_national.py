"""Run on OSCAR (0_run_collapse_rms_national.sh). National barcode totals of RMS food sales for one year,
Jaravel (2019) 1b_RMS_data_building.do: collapse (sum) total_quantity total_spending, by(product_group_code product_module_code upc).

Reads  /users/amarche4/data/rms_variety/rms_upc_fips_spending_<y>.parquet (build/variety/build_product_variety.py).
Writes /users/amarche4/scratch/jaravel_food/rms_upc_national_<y>.parquet; copy these 15 files by hand to
interim/variety/jaravel_food/rms_national/ (see README.md).
Note (2026-11-15): the barcode x county files map stores to counties from the 2006 store list only, so they hold about
100% of RMS food spending through 2011, 94% in 2015, 90% in 2017 and 67-69% in 2018-2020.
Usage: python 0_collapse_rms_national.py <year>
"""
import sys
import time
import resource
import pyarrow as pa
import pyarrow.parquet as pq

year = int(sys.argv[1])
t0 = time.time()
src = f"/users/amarche4/data/rms_variety/rms_upc_fips_spending_{year}.parquet"
dst = f"/users/amarche4/scratch/jaravel_food/rms_upc_national_{year}.parquet"
tab = pq.read_table(src, columns=["upc", "product_group_code", "product_module_code", "total_spending", "total_units"])
n_in = tab.num_rows
out = tab.group_by(["upc", "product_group_code", "product_module_code"]).aggregate(
    [("total_spending", "sum"), ("total_units", "sum")])
# pyarrow puts aggregates first and names them <col>_sum: select by name, not by position
out = pa.table({"upc": out["upc"], "product_group_code": out["product_group_code"],
                "product_module_code": out["product_module_code"],
                "total_spending": out["total_spending_sum"], "total_units": out["total_units_sum"]})
pq.write_table(out, dst)
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6
print(f"{year}: {n_in:,} UPC-county rows -> {out.num_rows:,} UPC rows; "
      f"spending {pa.compute.sum(out['total_spending']).as_py():,.0f}; {time.time()-t0:.0f}s; max RSS {rss:.1f} GB", flush=True)
