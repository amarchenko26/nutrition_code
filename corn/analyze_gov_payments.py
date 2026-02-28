#!/usr/bin/env python3
"""
Generate time series plots of government payments and CCC loans from
the merged Agricultural Census dataset (1992–2022).
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
DATA_FILE_PATH = "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/census_merged_1992_2022_deflated.tsv"
OUTPUT_DIR     = "/Users/anyamarchenko/Documents/GitHub/corn/output"
FIGS_DIR       = "figs"

FARM_BILL_YEARS  = [1996, 2002, 2008, 2014, 2018, 2025]
FARM_BILL_LABELS = [
    "1996 Farm Bill", "2002 Farm Bill", "2008 Farm Bill",
    "2014 Farm Bill", "2018 Farm Bill", "2025 BBB"
]

# ---------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------
print("Loading merged data...")
df = pd.read_csv(DATA_FILE_PATH, sep="\t", low_memory=False)
df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')

# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

def _get_years(df: pd.DataFrame) -> list[int]:
    yrs = pd.to_numeric(df['year'], errors='coerce').dropna().astype(int).unique().tolist()
    return sorted(yrs)

def make_series_simple(df: pd.DataFrame,
                       y_col: str,
                       geo: str = 'us',            # 'us' uses level==3; 'county' uses level==1
                       county_agg: str = 'mean',   # aggregation across counties per year
                       corn_positive: bool = False, # if True (county mode), keep counties with corn_for_grain_acres > 0
                       corn_filter_col: str = 'corn_for_grain_acres'
                      ) -> tuple[list[int], list[float]]:
    """
    Build a single series:
      - geo='us'    -> take the national (level==3) value each year (first non-missing).
      - geo='county'-> aggregate county (level==1) values by year using county_agg.
    No arithmetic is performed beyond aggregation for county mode.
    """
    if geo not in {'us', 'county'}:
        raise ValueError("geo must be 'us' or 'county'")

    years = _get_years(df)

    if geo == 'us':
        g = df[df['level'] == 3].copy()
        series = []
        for y in years:
            vals = pd.to_numeric(g.loc[g['year'] == y, y_col], errors='coerce').dropna()
            series.append(vals.iloc[0] if len(vals) else np.nan)
        return years, series

    # county mode
    g = df[df['level'] == 1].copy()
    if corn_positive and (corn_filter_col in g.columns):
        g[corn_filter_col] = pd.to_numeric(g[corn_filter_col], errors='coerce')
        g = g[g[corn_filter_col] > 0]

    g[y_col] = pd.to_numeric(g[y_col], errors='coerce')

    aggfunc = {'mean': 'mean', 'sum': 'sum', 'median': 'median'}.get(county_agg, 'mean')
    by_year = g.groupby('year', as_index=True)[y_col].agg(aggfunc)

    series = [by_year.get(y, np.nan) for y in years]
    return years, series


def plot_series_simple(years: list[int],
                       series: list[float],
                       title: str,
                       y_label: str,
                       filename: str,
                       label: str | None = None,
                       annotate_points: bool = True,
                       farm_bill_years: list[int] | None = None,
                       farm_bill_labels: list[str] | None = None):
    """Single line chart with optional farm-bill markers."""
    plt.style.use('seaborn-v0_8')
    fig, ax = plt.subplots(1, 1, figsize=(12, 6))

    ax.plot(years, series, marker='o', linewidth=2, markersize=7, label=label or None)

    # Optional farm-bill verticals
    from matplotlib import transforms

    fby = farm_bill_years or FARM_BILL_YEARS
    fbl = farm_bill_labels or FARM_BILL_LABELS

    # blended transform: x in data coords, y in axes fraction (0–1)
    trans = transforms.blended_transform_factory(ax.transData, ax.transAxes)

    for yr, lab in zip(fby, fbl):
        if years[0] <= yr <= years[-1]:
            ax.axvline(x=yr, color='gray', linestyle='--', alpha=0.6, linewidth=1.25, zorder=0)
            ax.text(yr, 0.98, lab, transform=trans, rotation=90,
                    va='top', ha='right', fontsize=9, alpha=0.8, color='gray')

    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xlabel('Census Year', fontsize=12)
    ax.set_ylabel(y_label, fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.set_xticks(years)
    if label:
        ax.legend()

    if annotate_points:
        finite = [float(x) for x in series if pd.notna(x) and np.isfinite(x)]
        if finite and max(finite) <= 1.0 + 1e-9:
            fmt = "{:.2f}"   # or "{:.1%}" for percent style
        else:
            fmt = "{:,.0f}"
        for (y, v) in zip(years, series):
            if pd.notna(v):
                ax.annotate(fmt.format(v), (y, v), textcoords="offset points", xytext=(0,10),
                            ha='center', fontsize=9)


    plt.tight_layout()
    out_dir = Path(OUTPUT_DIR) / FIGS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"✓ Saved plot to {out_path}")
    return out_path


def quick_timeseries(df: pd.DataFrame,
                     y_col: str,
                     title: str,
                     y_label: str,
                     filename: str,
                     geo: str = 'us',             # 'us' or 'county'
                     county_agg: str = 'mean',    # used only if geo='county'
                     corn_positive: bool = False, # used only if geo='county'
                     annotate_points: bool = True):
    """Tiny wrapper: choose level, build series, plot."""
    years, series = make_series_simple(
        df=df,
        y_col=y_col,
        geo=geo,
        county_agg=county_agg,
        corn_positive=corn_positive
    )
    label = "United States (level 3)" if geo == 'us' else f"Counties ({county_agg})"
    return plot_series_simple(
        years=years,
        series=series,
        title=title,
        y_label=y_label,
        filename=filename,
        label=label,
        annotate_points=annotate_points
    )

# ---------------------------------------------------------------------
# Create plots
# ---------------------------------------------------------------------

quick_timeseries(
    df,
    y_col=("gov_all_amt_real"),
    title="Total Federal Subsidies, Excluding CCC Loans (2017$)\nAg Census 1992–2022",
    y_label="2017 $s",
    geo="us",
    filename="gov_all_amt_real.png"
)

quick_timeseries(
    df,
    y_col=("gov_all_amt_real"),
    title="Total Federal Subsidies per County, Excluding CCC Loans (2017$)\nAg Census 1992–2022",
    y_label="2017 $s",
    geo="county",
    filename="gov_all_amt_real_percounty.png"
)

quick_timeseries(
    df,
    y_col=("farms_n"),
    title="Total Farms\nAg Census 1992–2022",
    y_label="Number of farms per county",
    geo="county",
    filename="farms_n.png"
)


quick_timeseries(
    df,
    y_col=("gov_all_n"),
    title="Farms Receiving Federal Subsidies\nAg Census 1992–2022",
    y_label="Number of farms",
    geo="us",
    filename="gov_all_n.png"
)

quick_timeseries(
    df,
    y_col=("share_corn_harvested_acres"),
    title="Share of Acres Harvested per County that are Corn\nAg Census 1992–2022",
    y_label="Corn acres as share of all harvested acres",
    geo="county",
    filename="share_corn_harvested_acres.png"
)

# 1) Government payments per farm (real)
quick_timeseries(
    df,
    y_col="gov_all_pf_real",
    title="Average Federal Subsidies per Farm (2017$)\nAg Census 1992–2022",
    y_label="2017 $ per farm",
    geo="us",
    filename="gov_pay_pf_timeseries.png"
)

# 2) Non-conservation government payments per farm (real)
quick_timeseries(
    df,
    y_col=("gov_noncons_pf_calc_real"),
    title="Non-Conservation Federal Subsidies per Farm (2017$)\nAg Census 1992–2022",
    y_label="2017 $ per farm",
    geo="county",
    corn_positive=True,        # only counties with corn acres > 0
    filename="gov_noncons_pf_timeseries.png"
)


quick_timeseries(
    df,
    y_col=("ccc_loan_amt_real"),
    title="Total CCC Loans Disbursed (2017$)\nAg Census 1992–2022",
    y_label="2017 $s",
    filename="ccc_loans_amt_timeseries.png"
)

quick_timeseries(
    df,
    y_col=("ccc_loan_n"),
    title="Number of Farms Receiving CCC Loans (2017$)\nAg Census 1992–2022",
    y_label="Number of farms",
    filename="ccc_loans_n_timeseries.png"
)

# 3) CCC loans per farm (real, amounts in $1,000s)
quick_timeseries(
    df,
    y_col=("ccc_loan_pf_real"),
    title="CCC Loans per Farm (2017$)\nAg Census 1992–2022",
    y_label="2017 $ per farm",
    filename="ccc_loans_pf_timeseries.png"
)

print("All plots created successfully!")
