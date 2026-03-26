#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

# Ensure repository root is importable when running directly.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.api.massive_api import MassiveApi
from src.dao.market_data_repo import MarketDataRepo
from src.utils.logger import app_logger


NYC = ZoneInfo("America/New_York")
REGULAR_SESSION_MINUTES = int(6.5 * 60)


def _decode_value(value: object) -> object:
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore")
    return value


def _normalize_tickers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    for column in ("ticker", "composite_figi"):
        if column in df.columns:
            df[column] = df[column].apply(_decode_value)
    return df


def _load_active_tickers(repo: MarketDataRepo) -> pd.DataFrame:
    active_df = repo.get_active_tickers()
    return _normalize_tickers(active_df)


def _build_window(hours: int) -> tuple[int, int, datetime, datetime]:
    end_dt = datetime.now(NYC)
    start_dt = end_dt - timedelta(hours=hours)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    return start_ms, end_ms, start_dt, end_dt


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Check how many active tickers have empty / short Massive minute history "
            "for a recent time window."
        )
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Lookback window in hours. Default: 24.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of tickers to process. 0 means all active tickers.",
    )
    args = parser.parse_args()

    repo = MarketDataRepo()
    active_df = _load_active_tickers(repo)
    if active_df.empty:
        print("No active tickers found.")
        return

    if args.limit and args.limit > 0:
        active_df = active_df.head(args.limit)

    start_ms, end_ms, start_dt, end_dt = _build_window(args.hours)
    api = MassiveApi()

    empty_rows: list[dict[str, object]] = []
    short_rows: list[dict[str, object]] = []
    failed_rows: list[dict[str, object]] = []
    non_empty_count = 0

    print(
        f"Checking {len(active_df)} active tickers from {start_dt.isoformat()} "
        f"to {end_dt.isoformat()} (threshold={REGULAR_SESSION_MINUTES})"
    )

    for idx, row in active_df.iterrows():
        ticker = str(row["ticker"])
        composite_figi = str(row.get("composite_figi", ""))
        try:
            data = api.get_historical_klines(
                ticker=ticker,
                multiplier=1,
                timespan="minute",
                start=str(start_ms),
                end=str(end_ms),
                adjusted=False,
                limit=5000,
            )

            if data is None or data.empty:
                empty_rows.append(
                    {
                        "ticker": ticker,
                        "composite_figi": composite_figi,
                    }
                )
                continue

            row_count = len(data)
            non_empty_count += 1
            if row_count < REGULAR_SESSION_MINUTES:
                short_rows.append(
                    {
                        "ticker": ticker,
                        "composite_figi": composite_figi,
                        "rows": row_count,
                    }
                )

        except Exception as exc:
            failed_rows.append(
                {
                    "ticker": ticker,
                    "composite_figi": composite_figi,
                    "error": str(exc),
                }
            )
            app_logger.warning(f"Massive history fetch failed for {ticker}: {exc}")

        if (idx + 1) % 50 == 0:
            app_logger.info(f"Processed {idx + 1}/{len(active_df)} tickers")

    print("\n## summary")
    print(f"active_tickers={len(active_df)}")
    print(f"non_empty_tickers={non_empty_count}")
    print(f"empty_tickers={len(empty_rows)}")
    print(f"short_tickers(<{REGULAR_SESSION_MINUTES})={len(short_rows)}")
    print(f"failed_tickers={len(failed_rows)}")

    if empty_rows:
        print("\n## empty tickers")
        print(pd.DataFrame(empty_rows).to_string(index=False))

    if short_rows:
        print(f"\n## short tickers (<{REGULAR_SESSION_MINUTES} rows)")
        print(pd.DataFrame(short_rows).sort_values(["rows", "ticker"]).to_string(index=False))

    if failed_rows:
        print("\n## failed tickers")
        print(pd.DataFrame(failed_rows).to_string(index=False))


if __name__ == "__main__":
    main()
