// ============================================================================
// build_family_events.do -- does the household's diet change when who lives in
// it changes? (what-moves-nutrition review, 2026-11-05)
//
// Who is compared with whom: a household in the years around one change in its
// membership, against households whose membership on that dimension never
// changes, in the same calendar years, with each household's own level removed.
//   emptynest  the last child under 18 leaves: children to none, household size falls
//   partner    a second head joins: one head to two
//   split      a couple separates: two heads to one, not widowed
//   widowed    two heads to one, widowed
//   birth      no child under 6 to a child under 6, household size rises
// For each event the household's FIRST change of that state between consecutive
// panel years; the window is cut before the state's next change; the comparison
// group never changes that state.
//   y_it = a_i + tau_t + sum_r b_r 1[r(i,t) = r] + x_it'g + e_it
// r = year - (m - 1), m the first file year showing the change, r = -1 omitted,
// binned at -5 and +5; 5-year age bins and kids and composition dummies except
// the one the event changes; projection weights; SE clustered on household.
// Reads interim/movers/occupation_movers_panel.dta (1_, geographic non-movers).
// Outputs (interim/what_moves/): family_events_coefs.csv, family_events_summary.csv
// ============================================================================
clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"
cap mkdir "$idir/what_moves"

use "$idir/movers/occupation_movers_panel.dta", clear
keep hhid year projection_factor age5 kids hh_comp hh_size n_heads marital_status real_income ///
	hei_usda hi hi_usda cals_ae_day upf_share produce_share sugar_1000 sodium_1000 soda_spend_share whole_share
xtset hhid year
gen byte has_kids = (kids != 9) if !missing(kids)
gen byte young = inlist(kids, 1, 4, 5, 7) if !missing(kids)
gen double L_size = L.hh_size
gen double F_dummy = .

cap file close fh
file open fh using "$idir/what_moves/family_events_coefs.csv", write replace
file write fh "event,outcome,r,b,se,N" _n
file close fh
file open fh using "$idir/what_moves/family_events_summary.csv", write replace
file write fh "event,outcome,n_events,n_controls,jump,jump_se,jump_late,jump_late_se,pre,pre_se,p_pretrend,N" _n
file close fh

cap program drop run_event
program define run_event
	args ev state evcond ctl
	// state: the variable whose first change defines the event; evcond: the change that counts
	preserve
	sort hhid year
	by hhid: gen byte _chg = (`state' != `state'[_n-1]) & _n > 1 & !missing(`state', `state'[_n-1])
	by hhid: gen int _nchg = sum(_chg)
	by hhid: egen int _totchg = total(_chg)
	gen int _m = year if _chg == 1 & _nchg == 1 & (`evcond') & (year == L.year + 1)
	by hhid: egen int m = max(_m)
	gen byte grp = cond(!missing(m), 1, cond(_totchg == 0, 0, .))
	keep if !missing(grp)
	drop if grp == 1 & _nchg >= 2          // cut the window before the state's next change
	gen int r = year - (m - 1) if grp == 1
	gen int rb = max(-5, min(5, r)) if grp == 1
	local dl
	forvalues k = -5/5 {
		if `k' == -1 continue
		local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
		gen byte d_`s' = (rb == `k')
		local dl `dl' d_`s'
	}
	bys hhid: gen byte _f = _n == 1
	quietly count if _f & grp == 1
	local nev = r(N)
	quietly count if _f & grp == 0
	local nct = r(N)
	di _n "==== `ev': `nev' event households, `nct' controls"
	foreach y in hei_usda hi hi_usda real_income cals_ae_day upf_share produce_share sugar_1000 sodium_1000 soda_spend_share whole_share {
		quietly reghdfe `y' `dl' [pw = projection_factor], absorb(hhid year age5 `ctl') cluster(hhid)
		local N = e(N)
		file open fh using "$idir/what_moves/family_events_coefs.csv", write append
		forvalues k = -5/5 {
			if `k' == -1 {
				file write fh "`ev',`y',-1,0,0,`N'" _n
				continue
			}
			local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
			file write fh "`ev',`y',`k',`=_b[d_`s']',`=_se[d_`s']',`N'" _n
		}
		file close fh
		quietly lincom (d_p1 + d_p2 + d_p3) / 3
		local j = r(estimate)
		local js = r(se)
		quietly lincom (d_p4 + d_p5) / 2
		local l = r(estimate)
		local ls = r(se)
		quietly lincom (d_m4 + d_m3 + d_m2) / 3
		local p = r(estimate)
		local ps = r(se)
		quietly test d_m4 d_m3 d_m2
		local pp = r(p)
		file open fh using "$idir/what_moves/family_events_summary.csv", write append
		file write fh "`ev',`y',`nev',`nct',`j',`js',`l',`ls',`p',`ps',`pp',`N'" _n
		file close fh
		di "`ev' / `y': post " %8.3f `j' " (" %6.3f `js' ")  pre p = " %5.3f `pp'
	}
	restore
end

run_event "emptynest" has_kids  "has_kids == 0 & L.has_kids == 1 & hh_size < L.hh_size" "hh_comp"
run_event "birth"     young     "young == 1 & L.young == 0 & hh_size > L.hh_size"       "hh_comp"
run_event "partner"   n_heads   "n_heads == 2 & L.n_heads == 1"                         "kids"
run_event "split"     n_heads   "n_heads == 1 & L.n_heads == 2 & marital_status != 2"   "kids"
run_event "widowed"   n_heads   "n_heads == 1 & L.n_heads == 2 & marital_status == 2"   "kids"
di "build_family_events.do complete"
