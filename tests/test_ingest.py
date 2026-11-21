"""Parsing tests run anywhere; database tests run only if the local Postgres is up."""
import pandas as pd
import psycopg
import pytest

from radar.ingest import DSN, num, sec_date
from radar.event_study import forward_excess


def test_sec_date_parses_and_tolerates_garbage():
    assert str(sec_date("31-MAR-2025")) == "2025-03-31"
    assert sec_date("") is None and sec_date("not a date") is None


def test_num():
    assert num("3.01") == 3.01 and num("") is None and num("x") is None


def test_forward_excess_uses_day_after_signal():
    idx = pd.bdate_range("2025-01-01", periods=80)
    px = pd.DataFrame({"AAA": range(100, 180), "SPY": [100.0] * 80}, index=idx, dtype=float)
    ev = pd.DataFrame({"ticker": ["AAA"], "signal_date": [idx[0]], "n_insiders": [3]})
    out = forward_excess(ev, px)
    # entry is idx[1] (price 101), exit 20 days later (121): 121/101 - 1, SPY flat
    assert abs(out.loc[0, "xr_20d"] - (121 / 101 - 1)) < 1e-9


def _db():
    try:
        return psycopg.connect(DSN, connect_timeout=2)
    except psycopg.OperationalError:
        pytest.skip("local Postgres not running")


def test_load_is_idempotent():
    with _db() as c:
        c.autocommit = True
        before = c.execute("SELECT count(*) FROM trade").fetchone()[0]
        from radar.ingest import ingest
        q = c.execute("SELECT quarter FROM load_log LIMIT 1").fetchone()[0]
        assert ingest(q, c)["status"].startswith("skipped")
        assert c.execute("SELECT count(*) FROM trade").fetchone()[0] == before


def test_no_duplicate_trades():
    with _db() as c:
        dup = c.execute("SELECT count(*) FROM (SELECT accession, trans_sk FROM trade "
                        "GROUP BY 1,2 HAVING count(*) > 1) d").fetchone()[0]
        assert dup == 0
