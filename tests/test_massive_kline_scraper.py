import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.massive_kline_scraper import (  # noqa: E402
    KlineSyncTask,
    KlineTaskResult,
    MassiveKlineScraper,
)


class FakeRepo:
    def __init__(self) -> None:
        self.universe_df = pd.DataFrame()
        self.mapping_empty = False
        self.sync_tasks_df = pd.DataFrame()
        self.latest_ts_df = pd.DataFrame()
        self.inserted_universe: list[pd.DataFrame] = []
        self.inserted_klines: list[pd.DataFrame] = []
        self.sync_updates: list[tuple[str, str, str, int]] = []

    def get_universe_tickers(self) -> pd.DataFrame:
        return self.universe_df.copy()

    def insert_stock_universe(self, df: pd.DataFrame) -> None:
        self.inserted_universe.append(df.copy())

    def is_mapping_table_empty(self) -> bool:
        return self.mapping_empty

    def get_sync_tasks(
        self, table_name: str, id_column: str = "composite_figi"
    ) -> pd.DataFrame:
        assert table_name == "us_minutes_klines"
        assert id_column == "composite_figi"
        return self.sync_tasks_df.copy()

    def get_all_stocks_latest_ts_df_by_group(self) -> pd.DataFrame:
        return self.latest_ts_df.copy()

    def insert_stock_minutes_klines(self, df: pd.DataFrame) -> None:
        self.inserted_klines.append(df.copy())

    def update_sync_status(
        self,
        table_name: str,
        identifier: str,
        id_column: str = "composite_figi",
        state: int = 1,
    ) -> None:
        self.sync_updates.append((table_name, identifier, id_column, state))


class FakeMassiveApi:
    def __init__(self, responses: dict[str, pd.DataFrame | None]) -> None:
        self.responses = responses
        self.calls: list[dict[str, str]] = []

    def get_historical_klines(self, **kwargs) -> pd.DataFrame | None:
        self.calls.append(kwargs)
        return self.responses[kwargs["ticker"]]


def build_scraper() -> MassiveKlineScraper:
    scraper = MassiveKlineScraper(scheduler=None)
    scraper.repo = FakeRepo()
    scraper.massive = FakeMassiveApi({})
    return scraper


def test_load_stock_universe_inherits_identifiers_and_deduplicates(monkeypatch) -> None:
    scraper = build_scraper()
    fake_repo = scraper.repo
    assert isinstance(fake_repo, FakeRepo)

    fake_repo.universe_df = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "composite_figi": "FIGI00000001",
                "cik": "0000000001",
                "active": 1,
                "last_updated_utc": "2026-01-01T00:00:00Z",
            },
            {
                "ticker": "OLDX",
                "composite_figi": "FIGI00000002",
                "cik": "0000000002",
                "active": 0,
                "last_updated_utc": "2026-01-01T00:00:00Z",
            },
        ]
    )
    fake_repo.mapping_empty = False

    raw_universe = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "type": b"cs",
                "active": 1,
                "last_updated_utc": "2026-03-01T00:00:00Z",
            },
            {
                "ticker": "OLDX",
                "type": "OS",
                "composite_figi": "FIGI00000002",
                "active": 0,
                "last_updated_utc": "2026-02-01T00:00:00Z",
            },
            {
                "ticker": "NEWX",
                "type": "OS",
                "composite_figi": "FIGI00000002",
                "active": 1,
                "last_updated_utc": "2026-03-05T00:00:00Z",
            },
            {
                "ticker": "ETF1",
                "type": "ETF",
                "active": 1,
                "last_updated_utc": "2026-03-05T00:00:00Z",
            },
        ]
    )

    monkeypatch.setattr(
        scraper,
        "_fetch_full_stock_universe_raw",
        lambda: raw_universe.copy(),
    )
    monkeypatch.setattr(scraper, "enrich_figi", lambda df: df)
    monkeypatch.setattr(scraper, "enrich_cik", lambda df: df)

    mapping_calls: list[pd.DataFrame] = []
    monkeypatch.setattr(
        scraper,
        "load_all_figi_ticker_mapping",
        lambda df: mapping_calls.append(df.copy()),
    )

    scraper.load_stock_universe()

    assert len(fake_repo.inserted_universe) == 1
    inserted = (
        fake_repo.inserted_universe[0]
        .sort_values("ticker")
        .reset_index(drop=True)
    )
    assert inserted["ticker"].tolist() == ["AAA", "NEWX"]
    assert inserted.set_index("ticker").loc["AAA", "composite_figi"] == "FIGI00000001"
    assert inserted.set_index("ticker").loc["AAA", "cik"] == "0000000001"
    assert inserted.set_index("ticker").loc["NEWX", "cik"] == "0000000002"

    assert len(mapping_calls) == 1
    assert mapping_calls[0]["ticker"].tolist() == ["NEWX"]


def test_fetch_klines_inserts_rows_and_marks_delisted_tasks_done() -> None:
    scraper = build_scraper()
    fake_repo = scraper.repo
    assert isinstance(fake_repo, FakeRepo)

    fake_repo.sync_tasks_df = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "composite_figi": "FIGI00000001",
                "active": 1,
                "sync_state": 0,
            },
            {
                "ticker": "DEAD",
                "composite_figi": "FIGI00000002",
                "active": 0,
                "sync_state": 0,
            },
        ]
    )
    fake_repo.latest_ts_df = pd.DataFrame()

    scraper.massive = FakeMassiveApi(
        {
            "AAA": pd.DataFrame(
                [{"t": 1_700_000_000_000, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100}]
            ),
            "DEAD": pd.DataFrame(),
        }
    )
    scraper.COLD_START_DATE = "2014-01-01"

    scraper.fetch_klines()

    assert len(fake_repo.inserted_klines) == 1
    inserted = fake_repo.inserted_klines[0]
    assert inserted["composite_figi"].tolist() == ["FIGI00000001"]
    assert fake_repo.sync_updates == [
        ("us_minutes_klines", "FIGI00000002", "composite_figi", 1)
    ]


def test_sync_single_kline_task_skips_recent_records_without_api_call() -> None:
    scraper = build_scraper()
    scraper.massive = FakeMassiveApi({"AAA": pd.DataFrame()})
    fake_massive = scraper.massive

    now_ms = 2_000_000
    task = KlineSyncTask(
        ticker="AAA",
        composite_figi="FIGI00000001",
        active=1,
        sync_state=0,
    )

    result = scraper._sync_single_kline_task(
        task=task,
        latest_ts_map={"FIGI00000001": now_ms - 30_000},
        now_ms=now_ms,
        cold_start_ms=0,
    )

    assert result == KlineTaskResult(
        inserted_rows=0,
        marked_done=False,
        failed=False,
    )
    assert fake_massive.calls == []
