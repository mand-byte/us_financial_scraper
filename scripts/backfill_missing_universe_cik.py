#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import clickhouse_connect
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config.settings import settings  # noqa: E402
from src.model.us_stock_universe_model import UsStockUniverseModel  # noqa: E402


SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing CIK in quant_data.us_stock_universe from SEC official ticker mapping."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Insert backfilled rows back into ClickHouse.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only inspect the first N missing rows (0 means all).",
    )
    parser.add_argument(
        "--out",
        default="artifacts/missing_universe_cik_report.json",
        help="Path to write JSON report.",
    )
    return parser.parse_args()


def get_db_client():
    host = (
        os.getenv("SOURCE_CLICKHOUSE_HOST")
        or os.getenv("CLICKHOUSE_HOST")
        or settings.db.clickhouse_host
    )
    port = int(
        os.getenv("SOURCE_CLICKHOUSE_PORT")
        or os.getenv("CLICKHOUSE_PORT")
        or settings.db.clickhouse_port
    )
    user = (
        os.getenv("SOURCE_CLICKHOUSE_USER")
        or os.getenv("CLICKHOUSE_USER")
        or os.getenv("CLICKHOST_USERNAME")
        or settings.db.clickhouse_username
    )
    password = (
        os.getenv("SOURCE_CLICKHOUSE_PASSWORD")
        or os.getenv("CLICKHOUSE_PASSWORD")
        or os.getenv("CLICKHOST_PASSWORD")
        or settings.db.clickhouse_password
    )
    database = (
        os.getenv("SOURCE_CLICKHOUSE_DATABASE")
        or os.getenv("SOURCE_DATABASE")
        or os.getenv("CLICKHOUSE_DATABASE")
        or os.getenv("CLICKHOST_DATABASE")
        or "quant_data"
    )
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
    )


def get_missing_rows(client, limit: int) -> pd.DataFrame:
    limit_sql = f"LIMIT {limit}" if limit > 0 else ""
    sql = f"""
    SELECT
        ticker,
        composite_figi,
        name,
        cik,
        active,
        base_currency_name,
        base_currency_symbol,
        currency_name,
        currency_symbol,
        delisted_utc,
        last_updated_utc,
        locale,
        market,
        primary_exchange,
        share_class_figi,
        type,
        update_time
    FROM quant_data.us_stock_universe FINAL
    WHERE cik = ''
       OR trim(BOTH ' ' FROM cik) = ''
       OR replaceAll(cik, '0', '') = ''
    ORDER BY ticker
    {limit_sql}
    """
    return client.query_df(sql)


def get_sec_ticker_map() -> dict[str, str]:
    headers = {
        "User-Agent": f"{settings.api.sec_downloader_company} {settings.api.sec_downloader_email}",
        "Accept": "application/json",
    }
    response = requests.get(
        SEC_COMPANY_TICKERS_URL,
        headers=headers,
        timeout=(10, 60),
    )
    response.raise_for_status()
    payload = response.json()

    mapping: dict[str, str] = {}
    records = payload.values() if isinstance(payload, dict) else payload
    for item in records:
        ticker = str(item.get("ticker", "")).strip().upper()
        cik_raw = item.get("cik_str")
        if not ticker or cik_raw in (None, ""):
            continue
        mapping[ticker] = str(cik_raw).strip().zfill(10)
    return mapping


def clean_for_report(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: x.decode("utf-8", "ignore") if isinstance(x, bytes) else x
        )
    return df.to_dict(orient="records")


def main() -> int:
    args = parse_args()
    client = get_db_client()
    missing_df = get_missing_rows(client, args.limit)
    missing_df = UsStockUniverseModel.format_dataframe(missing_df)

    sec_map = get_sec_ticker_map()
    if missing_df.empty:
        report = {
            "missing_count": 0,
            "matched_count": 0,
            "unmatched_count": 0,
            "matched_rows": [],
            "unmatched_rows": [],
            "applied": False,
        }
    else:
        work_df = missing_df.copy()
        work_df["ticker_upper"] = work_df["ticker"].astype(str).str.upper()
        work_df["sec_cik"] = work_df["ticker_upper"].map(sec_map).fillna("")

        matched_df = work_df[work_df["sec_cik"] != ""].copy()
        unmatched_df = work_df[work_df["sec_cik"] == ""].copy()

        if args.apply and not matched_df.empty:
            matched_df["cik"] = matched_df["sec_cik"]
            to_insert = matched_df.drop(columns=["ticker_upper", "sec_cik"])
            to_insert = UsStockUniverseModel.format_dataframe(to_insert)
            client.insert_df("us_stock_universe", to_insert, database="quant_data")

        report = {
            "missing_count": int(len(work_df)),
            "matched_count": int(len(matched_df)),
            "unmatched_count": int(len(unmatched_df)),
            "applied": bool(args.apply and not matched_df.empty),
            "matched_rows": clean_for_report(
                matched_df[
                    ["ticker", "composite_figi", "name", "active", "sec_cik"]
                ].rename(columns={"sec_cik": "filled_cik"})
            ),
            "unmatched_rows": clean_for_report(
                unmatched_df[["ticker", "composite_figi", "name", "active"]]
            ),
        }

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "missing_count": report["missing_count"],
        "matched_count": report["matched_count"],
        "unmatched_count": report["unmatched_count"],
        "applied": report["applied"],
        "report": str(out_path),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
