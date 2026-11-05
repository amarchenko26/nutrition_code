"""Central path definitions for nutrition_code (the ONLY place full paths live).

Use from any script, at any depth in the repo:

    import sys
    from pathlib import Path
    _r = Path(__file__).resolve().parent
    while not (_r / "paths.py").exists():
        _r = _r.parent
    sys.path.insert(0, str(_r))
    from paths import *  # noqa: E402,F401,F403
"""
from pathlib import Path

# code (this repo)
cdir = Path(__file__).resolve().parent

# data (Dropbox, never the repo)
data = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data")
rdir = data / "raw"       # input-only, never written
idir = data / "interim"   # cleaned / built panels, one subfolder per dataset or task
gdir = data / "final"     # regression-ready .dta only

# Overleaf (figures, tables, notes write straight here)
overleaf = Path("/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf")  # git clone of the Overleaf project (2026-11-05): pull before writing, push after; the Dropbox copy is no longer written to
fdir = overleaf / "figs"
tdir = overleaf / "tabs"
ndir = overleaf / "notes"

# aliases matching names already used across existing scripts
BASE = data
OVERLEAF = overleaf
