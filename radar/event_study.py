"""Event study: do cluster buys beat the market more than lone-insider buys?

Events  : 3+ insiders within 10 days (clusters.sql), one event per company per 30 days.
Control : buys where exactly one insider bought in the 10-day window.
Return  : buy at the close of the first trading day AFTER signal_date (the filings are
          public by then), hold 20 / 60 trading days, minus SPY over the same days.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg
import yfinance as yf

from radar.ingest import DSN

SQL = Path(__file__).with_name("clusters.sql").read_text()
PRICES = Path("data/prices.pkl")
HORIZONS = (20, 60)


def events(min_insiders: int, exact: bool = False, gap_days: int = 30) -> pd.DataFrame:
    with psycopg.connect(DSN) as c:
        df = pd.DataFrame(c.execute(SQL, {"min_insiders": min_insiders}).fetchall(),
                          columns=["cik", "ticker", "window_end", "n_insiders", "dollars", "signal_date"])
    if exact:
        df = df[df["n_insiders"] == min_insiders]
    df["signal_date"] = pd.to_datetime(df["signal_date"])
    df = df.sort_values(["cik", "signal_date"])
    keep, last = [], {}
    for i, r in df.iterrows():                  # first signal per company, then a 30-day cooldown
        prev = last.get(r["cik"])
        if prev is None or (r["signal_date"] - prev).days > gap_days:
            keep.append(i)
            last[r["cik"]] = r["signal_date"]
    return df.loc[keep].reset_index(drop=True)


def load_prices(tickers: list[str], start="2023-12-01", end="2025-12-31") -> pd.DataFrame:
    cached = pd.read_pickle(PRICES) if PRICES.exists() else pd.DataFrame()
    need = sorted(set(tickers) | {"SPY"} - set(cached.columns))
    for i in range(0, len(need), 100):
        chunk = need[i:i + 100]
        for attempt in range(3):
            try:
                px = yf.download(chunk, start=start, end=end, auto_adjust=True, progress=False, timeout=60)["Close"]
                break
            except Exception:
                time.sleep(5 * (attempt + 1))
        else:
            continue
        if isinstance(px, pd.Series):
            px = px.to_frame(chunk[0])
        cached = pd.concat([cached, px], axis=1)
        cached.to_pickle(PRICES)               # checkpoint after every chunk
        print(f"prices: {min(i + 100, len(need))}/{len(need)}")
    return cached


def forward_excess(ev: pd.DataFrame, px: pd.DataFrame) -> pd.DataFrame:
    out = []
    idx = px.index
    for _, r in ev.iterrows():
        if r["ticker"] not in px.columns:
            continue
        pos = idx.searchsorted(r["signal_date"], side="right")   # first trading day AFTER the signal
        row = {"ticker": r["ticker"], "signal_date": r["signal_date"].date(), "n_insiders": r["n_insiders"]}
        for h in HORIZONS:
            if pos + h >= len(idx):
                row[f"xr_{h}d"] = np.nan
                continue
            p0, p1 = px[r["ticker"]].iloc[pos], px[r["ticker"]].iloc[pos + h]
            s0, s1 = px["SPY"].iloc[pos], px["SPY"].iloc[pos + h]
            row[f"xr_{h}d"] = (p1 / p0 - 1) - (s1 / s0 - 1) if p0 > 0 and pd.notna(p1) else np.nan
        out.append(row)
    return pd.DataFrame(out)


def summarize(df: pd.DataFrame, label: str) -> dict:
    res = {"group": label, "events_priced": int(df["xr_20d"].notna().sum())}
    for h in HORIZONS:
        x = df[f"xr_{h}d"].dropna()
        x = x.clip(x.quantile(0.01), x.quantile(0.99))          # winsorize: penny-stock blowups dominate means
        res[f"mean_{h}d"] = round(float(x.mean()), 4)
        res[f"median_{h}d"] = round(float(x.median()), 4)
        res[f"beat_spy_{h}d"] = round(float((x > 0).mean()), 3)
        res[f"t_{h}d"] = round(float(x.mean() / (x.std(ddof=1) / np.sqrt(len(x)))), 2)
    return res


def welch_t(a: pd.Series, b: pd.Series) -> float:
    a, b = a.dropna(), b.dropna()
    return float((a.mean() - b.mean()) / np.sqrt(a.var(ddof=1) / len(a) + b.var(ddof=1) / len(b)))


if __name__ == "__main__":
    clusters = events(3)
    singles = events(1, exact=True).sample(n=1500, random_state=0)
    px = load_prices(sorted(set(clusters["ticker"]) | set(singles["ticker"])))
    c, s = forward_excess(clusters, px), forward_excess(singles, px)
    Path("results").mkdir(exist_ok=True)
    c.to_csv("results/cluster_events.csv", index=False)
    s.to_csv("results/single_buy_events.csv", index=False)
    summary = {"clusters": summarize(c, "cluster (3+ insiders)"), "singles": summarize(s, "single insider"),
               "cluster_minus_single_t_60d": round(welch_t(c["xr_60d"].clip(-1, 3), s["xr_60d"].clip(-1, 3)), 2)}
    Path("results/summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
