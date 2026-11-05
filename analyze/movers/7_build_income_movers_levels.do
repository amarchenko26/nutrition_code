// ============================================================================
// 7_build_income_movers_levels.do -- does HEI change when a household moves up
// or down the income distribution? (Anya 2026-11-05)
//
// Plain event study in LEVELS (HEI points; own income in $10k; since 2026-11-05 also
// the sub-outcomes carried by 1_: calories per adult equivalent per day, ultra-processed
// and produce shares of calories, produce and soda shares of spending, sugar and sodium
// per 1,000 kcal, whole-grain share) by direction of
// the move, with a control group so the calendar-year effects are identified:
//   movers   = households whose within-year income quintile changes exactly
//              once over their observed years, between consecutive panel years
//              (income_movers_panel.dta from 1_ --places income, mover_type 1;
//              one-year reversals already smoothed);
//   controls = households whose quintile never changes (mover_type 0).
//   y_it = a_i + tau_t + sum_r [beta_r^up 1[up, r] + beta_r^down 1[down, r]] + x_it'b + e_it
// r = year - (m_file - 1), r = -1 omitted, r binned at -5 and +5; household and
// year FE, 5-year age bins, kids and composition FE; projection-weighted; SE
// clustered on household. The stacked movers-only panel of 4_ cannot identify
// these levels (a linear trend in r is collinear with year FE + event FE).
//
// Outputs (interim/movers/): income_levels_coefs.csv, income_levels_summary.csv
// ============================================================================

clear all
set more off
do "/Users/anyamarchenko/Documents/GitHub/nutrition_code/paths.do"

use "$idir/movers/income_movers_panel.dta", clear
keep if inlist(mover_type, 0, 1)

gen m_tmp = year if switch == 1
bys hhid: egen m_file = max(m_tmp)
gen r = year - (m_file - 1) if mover_type == 1
gen q_tmp = occ if switch == 1
bys hhid: egen q_dest = max(q_tmp)
gen qo_tmp = occ_prev if switch == 1
bys hhid: egen q_orig = max(qo_tmp)
gen byte up = (q_dest > q_orig) if mover_type == 1
drop *_tmp
gen rb = max(-5, min(5, r)) if mover_type == 1

local dlist
forvalues k = -5/5 {
	if `k' == -1 continue
	local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
	gen byte u_`s' = (rb == `k' & up == 1)
	gen byte d_`s' = (rb == `k' & up == 0)
	local dlist `dlist' u_`s' d_`s'
}

quietly count if mover_type == 0
di "control household-years: " r(N)
bys hhid: gen byte _first = _n == 1
quietly count if _first & mover_type == 0
local n_ctl = r(N)
quietly count if _first & up == 1
local n_up = r(N)
quietly count if _first & up == 0
local n_dn = r(N)
di "households: controls `n_ctl', up-movers `n_up', down-movers `n_dn'"

cap file close fh
file open fh using "$idir/movers/income_levels_coefs.csv", write replace
file write fh "outcome,direction,r,b,se,N" _n
file close fh
file open fh using "$idir/movers/income_levels_summary.csv", write replace
file write fh "outcome,direction,n_movers,n_controls,jump,jump_se,jump_late,jump_late_se,pre,pre_se,p_pretrend,N" _n
file close fh
file open fh using "$idir/movers/income_levels_contrast.csv", write replace
file write fh "outcome,up_minus_down,se,up_minus_down_late,se_late,pre,pre_se,p_pretrend" _n
file close fh

foreach y in hei_usda real_income cals_ae_day upf_share produce_share produce_spend_share sugar_1000 sodium_1000 whole_share soda_spend_share {
	di _n "==== `y'"
	reghdfe `y' `dlist' [pw = projection_factor], absorb(hhid year age5 kids hh_comp) cluster(hhid)
	local N = e(N)
	foreach dir in u d {
		local dname = cond("`dir'" == "u", "up", "down")
		local nm = cond("`dir'" == "u", `n_up', `n_dn')
		file open fh using "$idir/movers/income_levels_coefs.csv", write append
		forvalues k = -5/5 {
			if `k' == -1 {
				file write fh "`y',`dname',-1,0,0,`N'" _n
				continue
			}
			local s = cond(`k' < 0, "m" + string(-`k'), "p" + string(`k'))
			file write fh "`y',`dname',`k',`=_b[`dir'_`s']',`=_se[`dir'_`s']',`N'" _n
		}
		file close fh
		quietly lincom (`dir'_p1 + `dir'_p2 + `dir'_p3) / 3
		local jump = r(estimate)
		local jump_se = r(se)
		quietly lincom (`dir'_p4 + `dir'_p5) / 2
		local jl = r(estimate)
		local jl_se = r(se)
		quietly lincom (`dir'_m4 + `dir'_m3 + `dir'_m2) / 3
		local pre = r(estimate)
		local pre_se = r(se)
		quietly test `dir'_m4 `dir'_m3 `dir'_m2
		local pp = r(p)
		file open fh using "$idir/movers/income_levels_summary.csv", write append
		file write fh "`y',`dname',`nm',`n_ctl',`jump',`jump_se',`jl',`jl_se',`pre',`pre_se',`pp',`N'" _n
		file close fh
		di "`y' / `dname': jump (1..3) = `jump' (`jump_se'); pre (-4..-2) = `pre' (`pre_se'), p = `pp'; movers `nm'"
	}
	// households moving up minus households moving down: anything common to moving cancels (2026-11-05)
	quietly lincom (u_p1 + u_p2 + u_p3) / 3 - (d_p1 + d_p2 + d_p3) / 3
	local c = r(estimate)
	local cs = r(se)
	quietly lincom (u_p4 + u_p5) / 2 - (d_p4 + d_p5) / 2
	local cl = r(estimate)
	local cls = r(se)
	quietly lincom (u_m4 + u_m3 + u_m2) / 3 - (d_m4 + d_m3 + d_m2) / 3
	local cp = r(estimate)
	local cps = r(se)
	quietly test (u_m4 = d_m4) (u_m3 = d_m3) (u_m2 = d_m2)
	local cpp = r(p)
	file open fh using "$idir/movers/income_levels_contrast.csv", write append
	file write fh "`y',`c',`cs',`cl',`cls',`cp',`cps',`cpp'" _n
	file close fh
	di "`y': up minus down = " %8.3f `c' " (" %6.3f `cs' "); before the move " %8.3f `cp' " (" %6.3f `cps' "), p = " %5.3f `cpp'
}
di "7_build_income_movers_levels.do complete"
