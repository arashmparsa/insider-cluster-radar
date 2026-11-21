"""Idempotent, incremental loader for SEC Form 345 quarterly data sets -> Postgres.

Each quarter is loaded in ONE transaction: delete that quarter's rows, COPY the
new ones, run data-quality checks, write load_log. If a check fails, the whole
quarter rolls back and the database is left exactly as it was.

    python -m radar.ingest 2024q1 2024q2 ...        (re-running is a no-op)
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import psycopg

DSN = os.environ.get("RADAR_DSN", "host=127.0.0.1 port=5433 user=radar dbname=insider")
DATA = Path("data")
SCHEMA = Path(__file__).with_name("schema.sql")

# Hard limits: a quarter that breaks these is rejected, not loaded.
MAX_NULL_DATE_RATE = 0.02
MAX_BAD_PURCHASE_PRICE_RATE = 0.10  # measured baseline ~7.5% (2024q1): price blank/0 in the source, mostly un-footnoted


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def rows(z: zipfile.ZipFile, name: str):
    with z.open(name) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"), delimiter="\t",
                                quoting=csv.QUOTE_NONE)
        yield from reader


def sec_date(s: str):
    """SEC uses 31-MAR-2025; return None for blanks or garbage instead of crashing."""
    try:
        return datetime.strptime(s.strip(), "%d-%b-%Y").date() if s and s.strip() else None
    except ValueError:
        return None


def num(s: str):
    try:
        return float(s) if s not in ("", None) else None
    except ValueError:
        return None


def ingest(quarter: str, conn: psycopg.Connection, force: bool = False) -> dict:
    path = DATA / f"{quarter}.zip"
    digest = sha256(path)
    prev = conn.execute("SELECT source_sha256 FROM load_log WHERE quarter = %s", (quarter,)).fetchone()
    if prev and prev[0] == digest and not force:
        return {"quarter": quarter, "status": "skipped (already loaded, same file)"}

    z = zipfile.ZipFile(path)
    with conn.transaction():
        # replace, don't append: re-loading a corrected file can't create duplicates
        conn.execute("DELETE FROM submission WHERE quarter = %s", (quarter,))
        conn.execute("DELETE FROM load_log WHERE quarter = %s", (quarter,))

        accessions, n_sub = set(), 0
        with conn.cursor().copy("COPY submission (accession, quarter, filing_date, document_type, issuer_cik, "
                                "issuer_name, ticker, aff10b5one) FROM STDIN") as cp:
            for r in rows(z, "SUBMISSION.tsv"):
                fd = sec_date(r["FILING_DATE"])
                if fd is None or r["ACCESSION_NUMBER"] in accessions:
                    continue
                accessions.add(r["ACCESSION_NUMBER"])
                cp.write_row((r["ACCESSION_NUMBER"], quarter, fd, r["DOCUMENT_TYPE"], r["ISSUERCIK"],
                              r["ISSUERNAME"], (r["ISSUERTRADINGSYMBOL"] or "").upper().strip() or None,
                              r.get("AFF10B5ONE") in ("1", "true", "True")))
                n_sub += 1

        seen, n_own = set(), 0
        with conn.cursor().copy("COPY owner (accession, owner_cik, owner_name, relationship, title) FROM STDIN") as cp:
            for r in rows(z, "REPORTINGOWNER.tsv"):
                key = (r["ACCESSION_NUMBER"], r["RPTOWNERCIK"])
                if key[0] not in accessions or key in seen:
                    continue
                seen.add(key)
                # street / city / zip columns intentionally dropped
                cp.write_row((*key, r["RPTOWNERNAME"], r["RPTOWNER_RELATIONSHIP"], r["RPTOWNER_TITLE"] or None))
                n_own += 1

        n_tr = null_dates = purchases = bad_purchase_price = 0
        with conn.cursor().copy("COPY trade (accession, trans_sk, trans_date, trans_code, shares, price, acq_disp) "
                                "FROM STDIN") as cp:
            for r in rows(z, "NONDERIV_TRANS.tsv"):
                if r["ACCESSION_NUMBER"] not in accessions:
                    continue
                td, price = sec_date(r["TRANS_DATE"]), num(r["TRANS_PRICEPERSHARE"])
                null_dates += td is None
                if r["TRANS_CODE"] == "P":
                    purchases += 1
                    bad_purchase_price += not price or price <= 0
                cp.write_row((r["ACCESSION_NUMBER"], int(r["NONDERIV_TRANS_SK"]), td, r["TRANS_CODE"],
                              num(r["TRANS_SHARES"]), price, r["TRANS_ACQUIRED_DISP_CD"]))
                n_tr += 1

        checks = {
            "null_trade_date_rate": round(null_dates / max(n_tr, 1), 4),
            "bad_purchase_price_rate": round(bad_purchase_price / max(purchases, 1), 4),
            "open_market_purchases": purchases,
        }
        if checks["null_trade_date_rate"] > MAX_NULL_DATE_RATE:
            raise ValueError(f"{quarter}: too many missing trade dates {checks}")  # rolls back the quarter
        if checks["bad_purchase_price_rate"] > MAX_BAD_PURCHASE_PRICE_RATE:
            raise ValueError(f"{quarter}: too many purchases without a price {checks}")

        conn.execute("INSERT INTO load_log (quarter, source_sha256, submissions, owners, trades, checks) "
                     "VALUES (%s, %s, %s, %s, %s, %s)", (quarter, digest, n_sub, n_own, n_tr, json.dumps(checks)))
    return {"quarter": quarter, "status": "loaded", "submissions": n_sub, "owners": n_own, "trades": n_tr, **checks}


def main(quarters: list[str]) -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(SCHEMA.read_text())
        for q in quarters:
            print(json.dumps(ingest(q, conn)))


if __name__ == "__main__":
    main(sys.argv[1:] or sorted(p.stem for p in DATA.glob("*.zip")))
