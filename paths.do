* Central path definitions for nutrition_code (the ONLY place full paths live).
* First line of every .do file (the one permitted full path):
*   do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

* code (this repo)
global cdir     "/Users/anyamarchenko/Documents/GitHub/nutrition_code"

* data (Dropbox, never the repo)
global data     "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data"
global rdir     "$data/raw"        // input-only, never written
global idir     "$data/interim"    // cleaned / built panels, one subfolder per dataset or task
global gdir     "$data/final"      // regression-ready .dta only

* Overleaf (figures, tables, notes write straight here)
global overleaf "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition"
global fdir     "$overleaf/figs"
global tdir     "$overleaf/tabs"
global ndir     "$overleaf/notes"

* aliases matching names already used across existing .do files
global figs     "$fdir"
global tabs     "$tdir"
global FIGS     "$fdir"
global TABS     "$tdir"
global PD       "$idir/panel_dataset"
