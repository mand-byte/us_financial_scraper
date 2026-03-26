#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

# Ensure the repository root is importable when running this file directly.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.api.massive_api import MassiveApi
from src.dao.market_data_repo import MarketDataRepo
from src.utils.logger import app_logger


NYC = ZoneInfo("America/New_York")
COLD_START_DATE = "2021-03-25"


def _decode_value(value: object) -> object:
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore")
    return value


def build_state0_frame(repo: MarketDataRepo) -> pd.DataFrame:
    tasks_df = repo.get_sync_tasks("us_minutes_klines", id_column="composite_figi")
    if tasks_df.empty:
        return pd.DataFrame()

    for column in ("ticker", "composite_figi"):
        if column in tasks_df.columns:
            tasks_df[column] = tasks_df[column].apply(_decode_value)

    state0_df = tasks_df[tasks_df["sync_state"].fillna(0).astype(int) == 0].copy()
    if state0_df.empty:
        return pd.DataFrame()

    latest_ts_df = repo.get_all_stocks_latest_ts_df_by_group()
    if not latest_ts_df.empty:
        latest_ts_df["composite_figi"] = latest_ts_df["composite_figi"].apply(
            _decode_value
        )
        latest_ts_df["last_ts"] = pd.to_datetime(latest_ts_df["last_ts"], errors="coerce")

    merged = state0_df.merge(
        latest_ts_df[["composite_figi", "last_ts"]]
        if not latest_ts_df.empty
        else pd.DataFrame(columns=["composite_figi", "last_ts"]),
        on="composite_figi",
        how="left",
    )
    merged = merged.sort_values(
        ["last_ts", "ticker"], ascending=[True, True], na_position="first"
    )
    return merged


def fetch_and_print_history(api: MassiveApi, ticker: str, last_ts: pd.Timestamp) -> None:
    now_ms = int(datetime.now(NYC).timestamp() * 1000)
    if pd.isna(last_ts):
        start_ms = int(datetime.strptime(COLD_START_DATE, "%Y-%m-%d").replace(tzinfo=NYC).timestamp() * 1000)
    else:
        start_ms = int(pd.Timestamp(last_ts).timestamp() * 1000) + 1

    app_logger.info(
        f"Query Massive history: ticker={ticker}, start_ms={start_ms}, end_ms={now_ms}"
    )
    result = api.get_historical_klines(
        ticker=ticker,
        multiplier=1,
        timespan="minute",
        start=str(start_ms),
        end=str(now_ms),
        adjusted=False,
        limit=5000,
    )

    if result is None:
        print("api_result=None")
        return

    if result.empty:
        print("api_result=empty")
        return

    print(f"api_rows={len(result)}")
    print(result.head(10).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List the oldest state=0 tickers and test Massive historical kline fetch."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of tickers to print from the oldest state=0 set.",
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Ticker to query with get_historical_klines. Defaults to the oldest state=0 ticker.",
    )
    args = parser.parse_args()

    repo = MarketDataRepo()
    merged = build_state0_frame(repo)
    if merged.empty:
        print("No state=0 tickers found.")
        return

    result = merged[["ticker", "composite_figi", "last_ts", "sync_state"]].head(
        args.limit
    )
    print("## state=0 oldest tickers")
    print(result.to_string(index=False))

    if args.ticker:
        selected = merged[merged["ticker"].astype(str) == args.ticker]
        if selected.empty:
            raise ValueError(f"Ticker not found in state=0 set: {args.ticker}")
        pick = selected.iloc[0]
    else:
        pick = merged.iloc[0]

    ticker = str(pick["ticker"])
    last_ts = pick["last_ts"]
    print("\n## selected ticker")
    print(
        f"ticker={ticker}, composite_figi={pick['composite_figi']}, "
        f"last_ts={last_ts}, sync_state={pick['sync_state']}"
    )

    api = MassiveApi()
    fetch_and_print_history(api, ticker=ticker, last_ts=last_ts)


if __name__ == "__main__":
    main()
