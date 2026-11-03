"""
figure_cdc_benchmarks.py -- national obesity and physical-activity series from CDC
sources, for notes/obesity.tex. No Nielsen data.

  cdc_obesity_trend.png            NHANES measured obesity (adults 20+, age-adjusted, by survey
                                   cycle, NCHS Data Briefs 360 and 508); the BRFSS series are kept
                                   below for build_obesity_series.py but not drawn (Anya 2026-11-03)
  cdc_physical_activity_trend.png  BRFSS: share meeting the 150-minute aerobic guideline (Q043,
                                   asked in odd years) and share with no leisure-time physical
                                   activity (Q047)
Values downloaded from the CDC open-data API on 2026-11-02 and typed in below with their source.
Outputs: figs/health/
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

FIG = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs') / 'health'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10.5,
                     'axes.spines.top': False, 'axes.spines.right': False})

# NHANES, adults 20+, age-adjusted percent with BMI >= 30, by survey cycle (plotted at the cycle midpoint).
# NCHS Data Brief 360 (Hales et al. 2020, Figure 4 / Table 1) through 2017-2018; Data Brief 508 for Aug 2021-Aug 2023.
NHANES = {1999.5: 30.5, 2001.5: 30.5, 2003.5: 32.2, 2005.5: 34.3, 2007.5: 33.7, 2009.5: 35.7,
          2011.5: 34.9, 2013.5: 37.7, 2015.5: 39.6, 2017.5: 42.4, 2022.5: 40.3}
# BRFSS nationwide, adults 18+, self-reported height and weight (data.cdc.gov/resource/hn4x-zwk7, locationabbr=US)
BRFSS_OBESE = {2011: 27.4, 2012: 27.7, 2013: 28.3, 2014: 28.9, 2015: 28.9, 2016: 29.6, 2017: 30.1, 2018: 30.9,
               2019: 31.4, 2020: 31.9, 2021: 33.0, 2022: 33.3, 2023: 32.8, 2024: 33.1, 2025: 33.5}
BRFSS_OVERW = {2011: 35.8, 2012: 35.7, 2013: 35.5, 2014: 35.2, 2015: 35.7, 2016: 35.2, 2017: 35.3, 2018: 35.0,
               2019: 35.2, 2020: 34.8, 2021: 34.4, 2022: 34.1, 2023: 34.7, 2024: 34.7, 2025: 34.7}
BRFSS_AEROBIC = {2011: 51.6, 2013: 50.2, 2015: 50.9, 2017: 50.3, 2019: 51.5, 2023: 59.9}   # Q043
BRFSS_NOLTPA = {2011: 25.4, 2012: 23.3, 2013: 26.3, 2014: 23.7, 2015: 25.9, 2016: 24.2, 2017: 26.6, 2018: 24.2,
                2019: 26.0, 2020: 23.5, 2021: 23.7, 2022: 23.7, 2023: 24.5, 2024: 22.5, 2025: 22.3}  # Q047


def save(fig, name):
    fig.savefig(FIG / name, bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / name}')


fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(list(NHANES), list(NHANES.values()), color='black', lw=2.5, marker='D', ms=6, label='NHANES, measured BMI ≥ 30, adults 20+, age-adjusted (by survey cycle)')
ax.set_ylabel('Percent of adults')
ax.set_xlabel('Year')
ax.set_ylim(20, 50)
ax.set_title('Adult obesity in the United States, NHANES')
ax.legend(loc='upper left', frameon=False)
save(fig, 'cdc_obesity_trend.png')

fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(list(BRFSS_AEROBIC), list(BRFSS_AEROBIC.values()), color=BLUE, lw=2.5, marker='o', ms=6,
        label='Meets the aerobic guideline: 150+ min/week moderate or 75+ min vigorous (asked in odd years)')
ax.plot(list(BRFSS_NOLTPA), list(BRFSS_NOLTPA.values()), color=RED, lw=2.5, marker='s', ms=5, label='No leisure-time physical activity in the past month')
ax.set_ylabel('Percent of adults')
ax.set_xlabel('Year')
ax.set_ylim(0, 70)
ax.set_title('Adult physical activity, BRFSS self-reports')
ax.legend(loc='center left', frameon=False, fontsize=10)
save(fig, 'cdc_physical_activity_trend.png')
