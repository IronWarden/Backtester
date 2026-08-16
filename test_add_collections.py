#!/usr/bin/env python3
"""Tests for add_collections.py's return construction.

Run with:  python3 -m pytest test_add_collections.py -q

These are pure-arithmetic tests over synthetic series: nothing here opens the
database, downloads anything, or touches yfinance. The expected values are
derived from the definition of an expense ratio, not captured from a run.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from add_collections import COLLECTIONS, TRADING_DAYS, gross_up_fee


def _flat(n: int, value: float = 0.0) -> pd.Series:
    """A series of n identical daily returns on consecutive days."""
    idx = pd.date_range("2010-01-04", periods=n, freq="D")
    return pd.Series([value] * n, index=idx, dtype=float)


def test_zero_expense_ratio_is_the_identity():
    """A leg with no fee must come back bit for bit unchanged.

    This is what keeps every non-etf leg -- and any etf leg whose ratio is
    left off -- producing exactly the numbers it produced before the fee
    correction existed.
    """
    r = pd.Series([0.01, -0.02, 0.0, 0.5], dtype=float)
    out = gross_up_fee(r, 0.0)
    assert (out.to_numpy() == r.to_numpy()).all()


def test_one_year_of_fee_compounds_to_the_expense_ratio():
    """252 trading days of grossing up a 0% net return recovers exactly the fee.

    This is the property the whole correction rests on: a fund that returned
    nothing charged its holder the expense ratio, so the index it tracked must
    have returned +er over that year.
    """
    er = 0.000945  # SPY
    grossed = gross_up_fee(_flat(int(TRADING_DAYS), 0.0), er)
    compounded = float((1.0 + grossed).prod()) - 1.0
    assert compounded == pytest.approx(er, rel=1e-12)


def test_gross_up_is_the_exact_inverse_of_the_drag():
    """Applying the fee and then removing it returns the original series.

    Guards against a sign error or a wrong exponent, either of which would
    still produce plausible-looking numbers in the right ballpark.
    """
    rng = np.random.default_rng(0)
    r = pd.Series(rng.normal(0.0004, 0.01, 500))
    er = 0.0024  # MDY, the largest ratio in the table

    daily = (1.0 + er) ** (1.0 / TRADING_DAYS)
    net = (1.0 + r) / daily - 1.0          # what the fund would report
    recovered = gross_up_fee(net, er)      # what we reconstruct

    assert np.allclose(recovered.to_numpy(), r.to_numpy(), rtol=0, atol=1e-15)


def test_fee_correction_raises_return_and_scales_with_the_ratio():
    """A bigger fee must add more back, monotonically, and never subtract."""
    base = _flat(int(TRADING_DAYS), 0.0003)
    totals = []
    for er in (0.0, 0.0003, 0.000945, 0.0019, 0.0024):
        grossed = gross_up_fee(base, er)
        totals.append(float((1.0 + grossed).prod()))
    assert totals == sorted(totals)
    assert totals[0] < totals[-1]


def test_thirty_years_of_spy_fee_is_material():
    """The reason this task existed: the drag is small yearly, large compounded.

    Not an assertion about correctness so much as a guard on the magnitude --
    if this ever reads as a fraction of a percent, the correction has been
    silently disabled.
    """
    er = 0.000945
    years = 30
    grossed = gross_up_fee(_flat(int(TRADING_DAYS) * years, 0.0), er)
    total = float((1.0 + grossed).prod()) - 1.0
    assert total > 0.028  # ~2.9% of cumulative return recovered over 30y


def test_expense_ratios_are_present_and_sane():
    """Every etf leg carries a fee, and no other kind of leg does.

    An etf leg that loses its "er" silently goes back to being a fund return,
    which is the exact bug this guards. The upper bound catches a ratio
    entered as a percent (0.09) rather than a decimal (0.0009).
    """
    seen = 0
    for symbol, spec in COLLECTIONS.items():
        for leg in spec["legs"]:
            if leg["kind"] == "etf":
                assert "er" in leg, f"{symbol}: etf leg {leg['src']} has no er"
                assert 0.0 < leg["er"] < 0.01, (
                    f"{symbol}: {leg['src']} er={leg['er']} is out of range "
                    f"-- expense ratios are decimals, so 0.09% is 0.0009"
                )
                seen += 1
            else:
                assert "er" not in leg, (
                    f"{symbol}: non-etf leg {leg['src']} carries an er; only "
                    f"fund returns are net of a fee"
                )
    assert seen == 9, f"expected 9 etf legs, found {seen}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-q"]))
