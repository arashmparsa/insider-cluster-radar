-- Insider trades from SEC Form 3/4/5 quarterly data sets.
-- Owner street addresses exist in the source but are deliberately NOT loaded.

CREATE TABLE IF NOT EXISTS load_log (
    quarter        text PRIMARY KEY,           -- e.g. 2025q1
    source_sha256  text NOT NULL,              -- same file twice = skip
    submissions    int  NOT NULL,
    owners         int  NOT NULL,
    trades         int  NOT NULL,
    checks         jsonb NOT NULL,             -- data-quality results
    loaded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS submission (
    accession      text PRIMARY KEY,
    quarter        text NOT NULL,
    filing_date    date NOT NULL,
    document_type  text NOT NULL,
    issuer_cik     text NOT NULL,
    issuer_name    text,
    ticker         text,
    aff10b5one     boolean                      -- trade under a pre-scheduled 10b5-1 plan
);

CREATE TABLE IF NOT EXISTS owner (
    accession      text NOT NULL REFERENCES submission ON DELETE CASCADE,
    owner_cik      text NOT NULL,
    owner_name     text,
    relationship   text,                        -- Director, Officer, TenPercentOwner, ...
    title          text,
    PRIMARY KEY (accession, owner_cik)
);

CREATE TABLE IF NOT EXISTS trade (
    accession      text NOT NULL REFERENCES submission ON DELETE CASCADE,
    trans_sk       bigint NOT NULL,
    trans_date     date,
    trans_code     text,                        -- P = open-market purchase, S = sale, ...
    shares         numeric,
    price          numeric,
    acq_disp       text,                        -- A acquired / D disposed
    PRIMARY KEY (accession, trans_sk)
);

CREATE INDEX IF NOT EXISTS trade_code_date ON trade (trans_code, trans_date);
CREATE INDEX IF NOT EXISTS submission_issuer ON submission (issuer_cik);
