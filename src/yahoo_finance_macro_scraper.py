import yfinance as yf
from src.dao.market_data_repo import MarketDataRepo
from src.utils.logger import app_logger
from src.utils.constants import Yahoo_Indicator_Code
from src.model.us_macro_daily_kline_model import UsMacroDailyKlineModel

from apscheduler.schedulers.blocking import BlockingScheduler


from zoneinfo import ZoneInfo


class YahooMacroScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler):
        self.scheduler = scheduler
        self.tickers = Yahoo_Indicator_Code
        self.repo = MarketDataRepo()

    def fetch_and_save(self, start_date="2014-01-01"):
        """
        Download macro daily data from Yahoo Finance.
        PIT Note: Yahoo returns adjusted close by default in recent yfinance versions.
        """
        app_logger.info(f"📡 正在拉取 Yahoo 宏观资产日线 (Since {start_date})...")

        try:
            # yfinance returns multi-index or single index depending on ticker count
            ticker_list = list(self.tickers.keys())
            df_raw = yf.download(
                ticker_list,
                start=start_date,
                interval="1d",
                progress=False,
                threads=True,
            )

            if df_raw.empty:
                return

            for yf_symbol, internal_code in self.tickers.items():
                try:
                    # Handle multi-index columns if multiple tickers were downloaded
                    if len(ticker_list) > 1:
                        if yf_symbol not in df_raw.columns.get_level_values(1):
                            continue
                        df_target = df_raw.xs(yf_symbol, axis=1, level=1).copy()
                    else:
                        df_target = df_raw.copy()

                    df_target = df_target.dropna(subset=["Close"]).reset_index()
                    df_target.rename(
                        columns={
                            "Date": "trade_date",
                            "Close": "close",
                            "Open": "open",
                            "High": "high",
                            "Low": "low",
                            "Volume": "volume",
                        },
                        inplace=True,
                    )

                    # 🌟 特殊处理：10年期美债收益率缩放
                    if yf_symbol.upper() == "^TNX":
                        df_target["close"] = df_target["close"] / 10.0
                        for col in ["open", "high", "low"]:
                            if col in df_target.columns:
                                df_target[col] = df_target[col] / 10.0

                    # 格式化并入库
                    clean_df = UsMacroDailyKlineModel.format_dataframe(
                        df_target, default_ticker=internal_code
                    )
                    if not clean_df.empty:
                        self.repo.insert_macro_daily_klines(clean_df)
                        app_logger.info(
                            f"✅ Yahoo: {internal_code} 同步完成 ({len(clean_df)} 行)。"
                        )

                except Exception as e:
                    app_logger.error(f"处理 Yahoo 标的 {yf_symbol} 失败: {e}")

        except Exception as e:
            app_logger.error(f"❌ Yahoo 宏观同步异常: {e}")

    def start(self):
        app_logger.info("✅ Yahoo 宏观搜刮器激活 (日线精度)。")

        # 1. 增量探测并运行
        last_date_str = self.repo.get_latest_trade_date_in_macro_daily_klines(
            list(self.tickers.values())
        )
        self.fetch_and_save(start_date=last_date_str)

        # 2. 每日 18:30 NYC 执行同步 (收盘后)
        self.scheduler.add_job(
            self.fetch_and_save,
            "cron",
            hour=18,
            minute=30,
            timezone=self.NYC,
            id="daily_yahoo_macro_sync",
        )

    def stop(self):
        if self.scheduler:
            self.scheduler.remove_job("daily_yahoo_macro_sync")
        app_logger.info("🛑 Yahoo 宏观搜刮器停止。")
