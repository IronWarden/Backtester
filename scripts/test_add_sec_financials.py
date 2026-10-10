#!/usr/bin/env python3
"""Tests for add_sec_financials.py's row selection.

Run with:  python3 -m pytest scripts/test_add_sec_financials.py -q

Nothing here downloads or opens the database: `get` is replaced by a function
returning a synthetic Financial Statement Data Set ZIP built in memory, with the
same tab-separated sub.txt / num.txt shape SEC publishes.
"""
from __future__ import annotations

import io
import zipfile
from datetime import date

import pytest

import add_sec_financials as sec

SUB_HEADER = "adsh\tcik\tname\tsic\tform\tfy\tfp\tfiled\tperiod"
NUM_HEADER = "adsh\ttag\tversion\tddate\tqtrs\tuom\tsegments\tcoreg\tvalue"


def _zip(subs: list[str], nums: list[str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("sub.txt", "\n".join([SUB_HEADER, *subs]) + "\n")
        z.writestr("num.txt", "\n".join([NUM_HEADER, *nums]) + "\n")
    return buf.getvalue()


# One 10-Q for the six months to 2024-06-30, filed 2024-08-05.
Q2 = "0001-24-000002\t320193\tACME CORP\t3571\t10-Q\t2024\tQ2\t20240805\t20240630"


def _rows(monkeypatch, nums: list[str], subs: list[str] | None = None):
    blob = _zip(subs or [Q2], nums)
    monkeypatch.setattr(sec, "get", lambda *_a, **_k: blob)
    return sec.quarter_rows(2024, 3, {"320193": "ACME"})


def test_year_to_date_rows_are_kept_and_never_called_a_quarter(monkeypatch):
    """A 10-Q's cash flow statement exists only year-to-date. Dropping those
    rows lost every quarterly cash flow after Q1; mislabelling them as a
    quarter would double it."""
    df = _rows(monkeypatch, [
        "0001-24-000002\tNetCashProvidedByUsedInOperatingActivities\tus-gaap/2024"
        "\t20240630\t2\tUSD\t\t\t500",
        "0001-24-000002\tNetIncomeLoss\tus-gaap/2024\t20240630\t1\tUSD\t\t\t120",
        "0001-24-000002\tNetIncomeLoss\tus-gaap/2024\t20240630\t2\tUSD\t\t\t230",
    ])
    got = {(r["metric"], r["qtrs"]): r["frequency"] for r in df.to_dict("records")}
    assert got == {
        ("Operating Cash Flow", 2): "year_to_date",
        ("Net Income", 1): "quarterly",
        ("Net Income", 2): "year_to_date",
    }


def test_outflows_are_negated_to_match_financials(monkeypatch):
    df = _rows(monkeypatch, [
        "0001-24-000002\tPaymentsToAcquirePropertyPlantAndEquipment\tus-gaap/2024"
        "\t20240630\t2\tUSD\t\t\t80",
        "0001-24-000002\tPaymentsOfDividends\tus-gaap/2024\t20240630\t2\tUSD\t\t\t15",
        "0001-24-000002\tNetCashProvidedByUsedInOperatingActivities\tus-gaap/2024"
        "\t20240630\t2\tUSD\t\t\t500",
    ])
    got = {r["metric"]: r["value"] for r in df.to_dict("records")}
    assert got == {"Capital Expenditure": -80.0, "Cash Dividends Paid": -15.0,
                   "Operating Cash Flow": 500.0}


def test_eps_is_kept_in_its_per_share_unit(monkeypatch):
    df = _rows(monkeypatch, [
        "0001-24-000002\tEarningsPerShareDiluted\tus-gaap/2024\t20240630\t1"
        "\tUSD/shares\t\t\t1.25",
        # Some other unit entirely -- not money, shares or money per share.
        "0001-24-000002\tNetIncomeLoss\tus-gaap/2024\t20240630\t1\tEUR\t\t\t9",
    ])
    assert list(df["metric"]) == ["Diluted EPS"]
    assert df["value"].iloc[0] == pytest.approx(1.25)


def test_tag_rank_orders_tags_within_a_metric(monkeypatch):
    """Two tags for one metric in one filing, with different values. The rank
    is what lets the reader pick one instead of whichever row it saw first."""
    df = _rows(monkeypatch, [
        "0001-24-000002\tLongTermDebtNoncurrent\tus-gaap/2024\t20240630\t0\tUSD\t\t\t90",
        "0001-24-000002\tLongTermDebt\tus-gaap/2024\t20240630\t0\tUSD\t\t\t100",
    ])
    ranks = {r["tag"]: r["tag_rank"] for r in df.to_dict("records")}
    assert ranks == {"LongTermDebt": 0, "LongTermDebtNoncurrent": 1}


def test_segments_amendments_and_unknown_tags_are_skipped(monkeypatch):
    amended = ("0001-24-000009\t320193\tACME CORP\t3571\t10-Q/A\t2024\tQ2"
               "\t20240901\t20240630")
    df = _rows(monkeypatch, subs=[Q2, amended], nums=[
        "0001-24-000002\tNetIncomeLoss\tus-gaap/2024\t20240630\t1\tUSD\tSeg=A\t\t50",
        "0001-24-000009\tNetIncomeLoss\tus-gaap/2024\t20240630\t1\tUSD\t\t\t999",
        "0001-24-000002\tSomethingElse\tus-gaap/2024\t20240630\t1\tUSD\t\t\t1",
        "0001-24-000002\tAssets\tus-gaap/2024\t20240630\t0\tUSD\t\t\t7000",
    ])
    assert list(df["metric"]) == ["Total Assets"]
    row = df.iloc[0]
    assert row["ticker"] == "ACME"
    assert row["filed"] == date(2024, 8, 5)
    assert row["frequency"] == "instant"


def test_every_ranked_metric_starts_at_zero():
    """A metric whose best tag is not rank 0 would make the reader prefer
    nothing; ranks must be dense from 0 within every metric."""
    by_metric: dict[str, list[int]] = {}
    for tag, metric in sec.TAGS:
        by_metric.setdefault(metric, []).append(sec.TAG_RANK[tag])
    for metric, ranks in by_metric.items():
        assert ranks == list(range(len(ranks))), metric
    assert len(sec.TAG_TO_METRIC) == len(sec.TAGS), "a tag is listed twice"
