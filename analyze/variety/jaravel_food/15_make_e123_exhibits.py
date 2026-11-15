"""Figures (house style: red #a63d40 dots with white edges, blue #2c5f8a squares for a second series, light grid, large
text) and table fragments for E1-E3.

Usage: python 15_make_e123_exhibits.py [--out DIR] [--extra]
Reads  interim/variety/jaravel_food/e123/e123_results.dta (14_), e3_joint.parquet, e3_coverage.parquet (13_).
Writes to figs|tabs/variety/jaravel_food (or --out DIR): e1_growth_by_decile.png/.tex, e2_health_by_decile.png/.tex,
       e3_growth_by_health.png/.tex; with --extra also e3_joint_hms.tex and e3_joint_rms.tex (not in the paper).
Stars: * 10%, ** 5%, *** 1% (utils/stars.py, normal distribution).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
from utils.stars import stars  # noqa: E402
import matplotlib  # noqa: E402
matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402

D = DATA / 'e123'
FIG, TAB, EXTRA = out_dirs(sys.argv)
FIG.mkdir(parents=True, exist_ok=True)
TAB.mkdir(parents=True, exist_ok=True)
RED, BLUE, GRAY = '#a63d40', '#2c5f8a', '#999999'
plt.rcParams.update({'font.size': 22, 'axes.titlesize': 22, 'axes.labelsize': 22, 'xtick.labelsize': 20, 'ytick.labelsize': 20,
                     'legend.fontsize': 19, 'font.family': 'DejaVu Sans'})
r = pd.read_stata(D / 'e123_results.dta')
r['window'] = r['window'].fillna('')
WLAB = {('hms', 'his'): '2004–06 to 2013–15', ('hms', 'ext'): '2004–06 to 2018–20',
        ('rms', 'his'): '2006–07 to 2014–15', ('rms', 'ext'): '2006–07 to 2019–20'}


def coefs(block, src, window, outcome, weight, prefix='d', rng=range(2, 11)):
    t = r[(r.block == block) & (r.src == src) & (r.window == window) & (r.outcome == outcome) & (r.weight == weight)]
    rows = [(1, 0.0, 0.0)]
    for q in rng:
        x = t[t.term == f'{prefix}{q}'].iloc[0]
        rows.append((q, x.b, x.se))
    return pd.DataFrame(rows, columns=['k', 'b', 'se']), t


def style(ax):
    ax.grid(True, color='#dddddd', linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ('top', 'right'):
        ax.spines[s].set_visible(False)
    ax.axhline(0, color=GRAY, linewidth=1)


def dots(ax, c, color, marker, label, shift=0.0):
    ax.errorbar(c.k + shift, c.b, yerr=1.96 * c.se, fmt='none', ecolor=color, elinewidth=1.6, capsize=0, alpha=0.6)
    ax.scatter(c.k + shift, c.b, s=150, color=color, edgecolor='white', linewidth=1.5, marker=marker, zorder=3, label=label)


def slope_txt(t, term='slope_fe'):
    x = t[t.term == term].iloc[0]
    return f"{x.b:.2f}{stars(x.b, x.se).replace('$^{', '').replace('}$', '')} ({x.se:.2f})"


# ---------------- Figure E1 ----------------
fig, axes = plt.subplots(2, 2, figsize=(16, 12), sharex=True)
for i, src in enumerate(['hms', 'rms']):
    for j, w in enumerate(['his', 'ext']):
        ax = axes[i, j]
        c, t = coefs('E1', src, w, 'growth_nom', 'logsp')
        dots(ax, c, RED, 'o', 'Nominal spending')
        txt = f"Per decile: {slope_txt(t)}"
        if src == 'rms':
            c2, t2 = coefs('E1', src, w, 'growth_real', 'logsp')
            dots(ax, c2, BLUE, 's', 'Real spending', shift=0.2)
            txt += f"\nReal: {slope_txt(t2)}"
            if j == 0:
                ax.legend(loc='lower left', frameon=True, facecolor='white', edgecolor='none', framealpha=0.85, handletextpad=0.2)
        style(ax)
        ax.set_title(f"{'Homescan' if src == 'hms' else 'RMS'}, {WLAB[(src, w)]}")
        ax.text(0.97, 0.97 if src == 'rms' else 0.05, txt, transform=ax.transAxes, ha='right', va='top' if src == 'rms' else 'bottom',
                fontsize=18, bbox=dict(facecolor='white', edgecolor='none', alpha=0.85, pad=3), zorder=5)
        ax.set_xticks(range(1, 11))
        if j == 0:
            ax.set_ylabel('Growth minus decile 1\n(pp per year)')
        if i == 1:
            ax.set_xlabel('Price decile within module')
fig.tight_layout()
fig.savefig(FIG / 'e1_growth_by_decile.png', dpi=200)
plt.close(fig)

# ---------------- Figure E2 ----------------
MEAS = [('addsugar_1000_w', 'Added sugar (g per 1,000 kcal)'), ('sugar_1000_w', 'Total sugar (g per 1,000 kcal)'),
        ('hi_1000_w', 'Health Index (per 1,000 kcal)'), ('organic_pct', 'Organic (% of barcodes)')]
fig, axes = plt.subplots(2, 2, figsize=(16, 12), sharex=True)
for ax, (m, lab) in zip(axes.flat, MEAS):
    c, t = coefs('E2', 'hms', '', m, 'none')
    c2, t2 = coefs('E2', 'hms', '', m, 'spend')
    dots(ax, c, RED, 'o', 'Each barcode once')
    dots(ax, c2, BLUE, 's', 'Weighted by spending', shift=0.2)
    style(ax)
    ax.set_title(lab)
    low = m in ('addsugar_1000_w', 'sugar_1000_w')
    ax.text(0.03, 0.04 if low else (0.95 if m != 'organic_pct' else 0.70), f"Per decile: {slope_txt(t)}\nWeighted: {slope_txt(t2)}",
            transform=ax.transAxes, ha='left', va='bottom' if low else 'top', fontsize=18, bbox=dict(facecolor='white', edgecolor='none', alpha=0.85, pad=3), zorder=5)
    ax.set_xticks(range(1, 11))
for ax in axes[1]:
    ax.set_xlabel('Price decile within module')
for ax in axes[:, 0]:
    ax.set_ylabel('Difference from decile 1')
axes[1, 1].legend(loc='upper left', frameon=False, handletextpad=0.2)
fig.tight_layout()
fig.savefig(FIG / 'e2_health_by_decile.png', dpi=200)
plt.close(fig)

# ---------------- Figure E3 ----------------
fig, axes = plt.subplots(2, 2, figsize=(16, 12), sharex=True)
for i, src in enumerate(['hms', 'rms']):
    for j, (m, lab) in enumerate([('addsugar_1000', 'added sugar'), ('hi_1000', 'Health Index')]):
        ax = axes[i, j]
        for w, color, marker, sh in (('his', RED, 'o', -0.08), ('ext', BLUE, 's', 0.08)):
            c, t = coefs('E3', src, w, m, 'logsp', prefix='t', rng=(2, 3))
            dots(ax, c, color, marker, WLAB[(src, w)], shift=sh)
        style(ax)
        ax.set_title(f"{'Homescan' if src == 'hms' else 'RMS'}, terciles of {lab}")
        ax.set_xticks([1, 2, 3])
        ax.set_xticklabels(['Least\nhealthy', 'Middle', 'Healthiest'])
        ax.set_xlim(0.6, 3.4)
        lo, hi = ax.get_ylim()
        ax.set_ylim(lo, hi + 0.35 * (hi - lo))          # room for the legend above the data
        ax.legend(loc='upper left', frameon=False, handletextpad=0.2, fontsize=17)
        if j == 0:
            ax.set_ylabel('Growth minus least healthy\n(pp per year)')
fig.tight_layout()
fig.savefig(FIG / 'e3_growth_by_health.png', dpi=200)
plt.close(fig)


# ---------------- tables ----------------
def fmt(b, se, dec=2):
    if pd.isna(b):
        return '', ''
    return f"{round(b, dec) + 0.0:.{dec}f}{stars(b, se)}", f"({se:.{dec}f})"


def get(block, src, window, outcome, weight, term):
    t = r[(r.block == block) & (r.src == src) & (r.window == window) & (r.outcome == outcome) & (r.weight == weight) & (r.term == term)]
    return t.iloc[0] if len(t) else None


# E1
COLS = [('hms', 'his', 'growth_nom'), ('hms', 'ext', 'growth_nom'), ('rms', 'his', 'growth_nom'), ('rms', 'ext', 'growth_nom'),
        ('rms', 'his', 'growth_real'), ('rms', 'ext', 'growth_real')]
L = [r'\begin{tabular}{lcccccc}', r'\toprule',
     r' & \multicolumn{2}{c}{Homescan, nominal} & \multicolumn{2}{c}{RMS, nominal} & \multicolumn{2}{c}{RMS, real} \\',
     r'\cmidrule(lr){2-3}\cmidrule(lr){4-5}\cmidrule(lr){6-7}',
     r' & 2004--06 to & 2004--06 to & 2006--07 to & 2006--07 to & 2006--07 to & 2006--07 to \\',
     r' & 2013--15 & 2018--20 & 2014--15 & 2019--20 & 2014--15 & 2019--20 \\', r'\midrule',
     r'\multicolumn{7}{l}{\textit{Difference from decile 1 (pp per year), module fixed effects}} \\']
for q in range(2, 11):
    vals = [fmt(get('E1', s, w, o, 'logsp', f'd{q}').b, get('E1', s, w, o, 'logsp', f'd{q}').se) for s, w, o in COLS]
    L.append(f'Decile {q} & ' + ' & '.join(v for v, _ in vals) + r' \\')
    L.append(' & ' + ' & '.join(s_ for _, s_ in vals) + r' \\')
L.append(r'\midrule')
for lab, term in (('One decile higher, module fixed effects', 'slope_fe'), ('One decile higher, no fixed effects (his code)', 'slope_nofe')):
    vals = [fmt(get('E1', s, w, o, 'logsp', term).b, get('E1', s, w, o, 'logsp', term).se, 3) for s, w, o in COLS]
    L.append(lab + ' & ' + ' & '.join(v for v, _ in vals) + r' \\')
    L.append(' & ' + ' & '.join(s_ for _, s_ in vals) + r' \\')
L.append('Cells (modules) & ' + ' & '.join(f"{int(get('E1', s, w, o, 'logsp', 'slope_fe').N):,} ({int(get('E1', s, w, o, 'logsp', 'slope_fe').nmod)})" for s, w, o in COLS) + r' \\')
L += [r'\bottomrule', r'\end{tabular}']
(TAB / 'e1_growth_by_decile.tex').write_text('\n'.join(L) + '\n')

# E2
L = [r'\begin{tabular}{lccccccc}', r'\toprule',
     r' & & \multicolumn{3}{c}{Each barcode once} & \multicolumn{3}{c}{Weighted by spending} \\',
     r'\cmidrule(lr){3-5}\cmidrule(lr){6-8}',
     r' & Mean & Decile 10 & One decile & Barcodes & Decile 10 & One decile & Mean \\',
     r' & & minus 1 & higher & (modules) & minus 1 & higher & \\', r'\midrule']
for m, lab in [('addsugar_1000_w', 'Added sugar, g per 1,000 kcal'), ('sugar_1000_w', 'Total sugar, g per 1,000 kcal'),
               ('hi_1000_w', 'Health Index per 1,000 kcal'), ('organic_pct', r'Organic, \% of barcodes')]:
    mu, d10, sl = get('E2', 'hms', '', m, 'none', 'mean'), get('E2', 'hms', '', m, 'none', 'd10'), get('E2', 'hms', '', m, 'none', 'slope_fe')
    muw, d10w, slw = get('E2', 'hms', '', m, 'spend', 'mean'), get('E2', 'hms', '', m, 'spend', 'd10'), get('E2', 'hms', '', m, 'spend', 'slope_fe')
    a, b_, c_, e_ = fmt(d10.b, d10.se), fmt(sl.b, sl.se, 3), fmt(d10w.b, d10w.se), fmt(slw.b, slw.se, 3)
    L.append(f"{lab} & {mu.b:.2f} & {a[0]} & {b_[0]} & {int(sl.N):,} ({int(sl.nmod)}) & {c_[0]} & {e_[0]} & {muw.b:.2f} \\\\")
    L.append(f" & & {a[1]} & {b_[1]} & & {c_[1]} & {e_[1]} & \\\\")
L += [r'\bottomrule', r'\end{tabular}']
(TAB / 'e2_health_by_decile.tex').write_text('\n'.join(L) + '\n')

# E3 groups
cov = pd.read_parquet(D / 'e3_coverage.parquet')
COLS = [(s, m, w) for s in ('hms', 'rms') for m in ('addsugar_1000', 'hi_1000') for w in ('his', 'ext')]
L = [r'\begin{tabular}{l' + 'c' * 8 + '}', r'\toprule',
     r' & \multicolumn{4}{c}{Homescan} & \multicolumn{4}{c}{RMS} \\', r'\cmidrule(lr){2-5}\cmidrule(lr){6-9}',
     r' & \multicolumn{2}{c}{Added sugar} & \multicolumn{2}{c}{Health Index} & \multicolumn{2}{c}{Added sugar} & \multicolumn{2}{c}{Health Index} \\',
     r'\cmidrule(lr){2-3}\cmidrule(lr){4-5}\cmidrule(lr){6-7}\cmidrule(lr){8-9}',
     r' & to 2013--15 & to 2018--20 & to 2013--15 & to 2018--20 & to 2014--15 & to 2019--20 & to 2014--15 & to 2019--20 \\', r'\midrule',
     r'\multicolumn{9}{l}{\textit{Growth minus growth of the least healthy tercile (pp per year), module fixed effects}} \\']
for lab, term in (('Middle tercile', 't2'), ('Healthiest tercile', 't3')):
    vals = [fmt(get('E3', s, w, m, 'logsp', term).b, get('E3', s, w, m, 'logsp', term).se) for s, m, w in COLS]
    L.append(lab + ' & ' + ' & '.join(v for v, _ in vals) + r' \\')
    L.append(' & ' + ' & '.join(s_ for _, s_ in vals) + r' \\')
L.append('Module-terciles (modules) & ' + ' & '.join(f"{int(get('E3', s, w, m, 'logsp', 't2').N):,} ({int(get('E3', s, w, m, 'logsp', 't2').nmod)})" for s, m, w in COLS) + r' \\')
covrow = []
for s, m, w in COLS:
    c = cov[(cov.src == s) & (cov.measure == m)].sort_values('year')
    covrow.append(f"{100 * c.share.iloc[0]:.0f} / {100 * c.share.iloc[-1]:.0f}")
L.append(r'Spending with an own value, first / last year (\%) & ' + ' & '.join(covrow) + r' \\')
L += [r'\bottomrule', r'\end{tabular}']
(TAB / 'e3_growth_by_health.tex').write_text('\n'.join(L) + '\n')

# E3 joint (pooled over modules), --extra only
j = pd.read_parquet(D / 'e3_joint.parquet')
for src in (('hms', 'rms') if EXTRA else ()):
    L = [r'\begin{tabular}{lcccccccc}', r'\toprule',
         r' & \multicolumn{4}{c}{Terciles of added sugar} & \multicolumn{4}{c}{Terciles of the Health Index} \\',
         r'\cmidrule(lr){2-5}\cmidrule(lr){6-9}',
         r'Price decile & Least healthy & Middle & Healthiest & All & Least healthy & Middle & Healthiest & All \\', r'\midrule']
    for pi, w in enumerate(('his', 'ext')):
        L.append(r'\multicolumn{9}{l}{\textit{Panel ' + 'AB'[pi] + ': ' + WLAB[(src, w)].replace('–', '--') + r', annualized growth (\%)}} \\')
        for q in list(range(1, 11)) + [0]:
            row = ['All' if q == 0 else str(q)]
            for m in ('addsugar_1000', 'hi_1000'):
                for t in (1, 2, 3, 0):
                    x = j[(j.src == src) & (j.measure == m) & (j.quality_rank == q) & (j.tercile == t)]
                    row.append('' if x.empty or pd.isna(x[f'growth_{w}'].iloc[0]) else f"{x[f'growth_{w}'].iloc[0]:.1f}")
            L.append(' & '.join(row) + r' \\')
        L.append(r'\midrule' if pi == 0 else r'\bottomrule')
    L.append(r'\end{tabular}')
    (TAB / f'e3_joint_{src}.tex').write_text('\n'.join(L) + '\n')
print('done')
