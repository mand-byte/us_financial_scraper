from datetime import datetime
from src.config.settings import settings
from zoneinfo import ZoneInfo

import pandas as pd

from src.dao.clickhouse_manager import get_db_manager
from src.model.gdelt_macro_sentiment_model import GdeltMacroSentimentModel
from src.model.gdelt_macro_sentiment_state_model import GdeltMacroSentimentStateModel
from src.utils.logger import app_logger


class SentimentRepo:
    GDELT_CURSOR_KEY = "gdelt_v2_15m"

    @property
    def db(self):
        return get_db_manager()

    def __init__(self):
        pass

    def insert_gdelt_macro_sentiment(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        self.db.client.insert_df(GdeltMacroSentimentModel.table_name, df)

    def get_latest_gdelt_macro_sentiment(self) -> datetime:
        try:
            res = self.db.client.query_df(
                GdeltMacroSentimentModel.QUERY_GLOBAL_LATEST_PUBLISH_TS_SQL
            )
            last_ts = res.iloc[0]["last_ts"]
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts, utc=True).to_pydatetime()
        except Exception as exc:
            app_logger.error(f"查询 GDELT 最新时间失败: {exc}")

        start_str = settings.scraper.scraping_start_date
        return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=ZoneInfo("UTC"))

    def upsert_gdelt_cursor(self, last_file_ts: datetime) -> None:
        data = pd.DataFrame(
            [
                {
                    "cursor_key": self.GDELT_CURSOR_KEY,
                    "last_file_ts": last_file_ts,
                }
            ]
        )
        clean_df = GdeltMacroSentimentStateModel.format_dataframe(data)
        if clean_df.empty:
            return
        self.db.client.insert_df(GdeltMacroSentimentStateModel.table_name, clean_df)

    def get_latest_gdelt_cursor(self) -> datetime:
        try:
            query = GdeltMacroSentimentStateModel.build_query_latest_cursor_sql(
                self.GDELT_CURSOR_KEY
            )
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]["last_ts"]
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts, utc=True).to_pydatetime()
        except Exception as exc:
            app_logger.error(f"查询 GDELT 游标时间失败: {exc}")

        start_str = settings.scraper.scraping_start_date
        return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=ZoneInfo("UTC"))
