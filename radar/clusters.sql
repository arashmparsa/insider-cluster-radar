-- Cluster buys: 3+ different officers/directors buying their own company's stock
-- on the open market within 10 days.
--
-- signal_date = the date the LAST Form 4 in the cluster became public (filing_date),
-- not the trade date: an investor could not have acted before the filings were out.

WITH buys AS (               -- one row per insider per company per trade day
    SELECT s.issuer_cik,
           max(s.ticker)                 AS ticker,
           o.owner_cik,
           t.trans_date,
           max(s.filing_date)            AS filing_date,
           sum(t.shares * t.price)       AS dollars
    FROM trade t
    JOIN submission s USING (accession)
    JOIN owner o      USING (accession)
    WHERE t.trans_code = 'P'                       -- open-market purchase
      AND t.acq_disp = 'A'
      AND t.price > 0 AND t.shares > 0             -- price-less purchases can't be valued
      AND s.document_type = '4'
      AND (o.relationship ILIKE '%%director%%' OR o.relationship ILIKE '%%officer%%')
      AND s.ticker IS NOT NULL AND s.ticker NOT IN ('NONE', 'N/A')
    GROUP BY s.issuer_cik, o.owner_cik, t.trans_date
),
windows AS (                 -- for each buy, who else bought in the 10 days up to it
    SELECT b.issuer_cik, b.ticker, b.trans_date AS window_end,
           count(DISTINCT w.owner_cik)   AS n_insiders,
           sum(w.dollars)                AS total_dollars,
           max(w.filing_date)            AS signal_date
    FROM buys b
    JOIN buys w
      ON w.issuer_cik = b.issuer_cik
     AND w.trans_date BETWEEN b.trans_date - 10 AND b.trans_date
    GROUP BY b.issuer_cik, b.ticker, b.trans_date
)
SELECT issuer_cik, ticker, window_end, n_insiders, round(total_dollars) AS total_dollars, signal_date
FROM windows
WHERE n_insiders >= %(min_insiders)s
ORDER BY issuer_cik, signal_date;
