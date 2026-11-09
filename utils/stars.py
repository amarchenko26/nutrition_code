"""Significance stars for every regression table (Anya 2026-11-09): * 10%, ** 5%, *** 1%, two-sided, from the estimate over its
standard error with the normal distribution. `stars(b, se)` gives the LaTeX superscript, `est(b, se, dec)` the estimate with its stars
and the standard error in parentheses, `star_num(b, se, dec)` the estimate with its stars alone (for tables that put the standard error
on the next line)."""
from math import erf, sqrt


def pvalue(b, se):
    return 2 * (1 - 0.5 * (1 + erf(abs(b / se) / sqrt(2))))


def stars(b, se):
    try:
        b, se = float(b), float(se)
    except (TypeError, ValueError):
        return ''
    if not se or se != se or b != b:
        return ''
    p = pvalue(b, se)
    s = '***' if p < 0.01 else '**' if p < 0.05 else '*' if p < 0.10 else ''
    return f'$^{{{s}}}$' if s else ''


def star_num(b, se, dec, sign=False):
    s = stars(b, se)                      # from the unrounded estimate
    v = round(float(b), dec) + 0.0        # + 0.0 prints -0.000 as 0.000
    return (f'{v:+.{dec}f}' if sign else f'{v:.{dec}f}') + s


def est(b, se, dec, sign=False):
    return star_num(b, se, dec, sign) + f' ({float(se):.{dec}f})'
