# -*- coding: utf-8 -*-
from datetime import datetime
from zoneinfo import ZoneInfo
from src.config.settings import settings

from apscheduler.schedulers.blocking import BlockingScheduler
from src.api.massive_api import MassiveApi
from src.dao.fundamental_repo import FundamentalRepo
from src.model.us_stock_10k_sections_raw_model import UsStock10kSectionsRawModel
from src.model.us_stock_risk_factors_model import UsStockRiskFactorsModel
from src.model.us_stock_risk_taxonomy_model import UsStockRiskTaxonomyModel
from src.utils.logger import app_logger as logger


class MassiveFilingsDisclosuresScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler = None):
        self.massive = MassiveApi()
        self.fundamental_repo = FundamentalRepo()
        self.scheduler = scheduler
        self.COLD_START_DATE = settings.scraper.scraping_start_date

    def start(self):
        if self.scheduler:
            self.scheduler.add_job(
                self.refresh_incremental_filings,
                "cron",
                day_of_week="mon-fri",
                hour="8-21",
                minute=0,
                timezone=self.NYC,
                id="sync_filings_disclosures_hourly",
                next_run_time=datetime.now(self.NYC),
                max_instances=1,
                coalesce=True,
                replace_existing=True,
            )
            self.scheduler.add_job(
                self.sync_risk_taxonomy,
                "cron",
                day_of_week="mon-fri",
                hour=21,
                minute=30,
                timezone=self.NYC,
                id="sync_risk_taxonomy_daily",
                max_instances=1,
                coalesce=True,
                replace_existing=True,
            )
            logger.info("✅ Massive Filings & Disclosures 搜刮器已启动")

    def stop(self):
        if self.scheduler:
            try:
                self.scheduler.remove_job("sync_filings_disclosures_hourly")
            except Exception:
                pass
            try:
                self.scheduler.remove_job("sync_risk_taxonomy_daily")
            except Exception:
                pass
        logger.info("🛑 Massive Filings & Disclosures 搜刮器停止。")

    def sync_10k_sections(self):
        last_date = self.fundamental_repo.get_global_latest_10k_sections_date()
        if last_date is None:
             last_date = datetime.strptime(self.COLD_START_DATE, "%Y-%m-%d").date()
             logger.info(f"10-K Sections 初次同步，使用冷启动日期: {self.COLD_START_DATE}")

        date_str = last_date.strftime("%Y-%m-%d")
        logger.info(f"🚀 拉取 10-K sections 数据 (起: {date_str})...")
        
        df_raw = self.massive.get_stock_10k_sections(
            period_end_gte=date_str, limit=1000
        )

        if df_raw is None or df_raw.empty:
            logger.info("10-K Sections 数据无新增。")
            return

        if "ticker" not in df_raw.columns:
            return
            
        df_raw = df_raw.dropna(subset=["ticker"])
        
        clean_df = UsStock10kSectionsRawModel.format_dataframe(df_raw)
        if not clean_df.empty:
            before = len(clean_df)
            clean_df = clean_df[
                (clean_df["ticker"].astype(str).str.len() > 0)
                & (clean_df["section"].astype(str).str.len() > 0)
            ]
            clean_df = clean_df.dropna(subset=["filing_date", "period_end"])
            dropped = before - len(clean_df)
            if dropped > 0:
                logger.warning(f"⚠️ 10-K Sections 清洗后丢弃 {dropped} 条异常记录。")
        if not clean_df.empty:
            self.fundamental_repo.insert_stock_10k_sections_raw(clean_df)
        logger.info(f"10-K Sections 数据拉取完成，新增 {len(clean_df)} 条记录。")

    def sync_risk_factors(self):
        last_date = self.fundamental_repo.get_global_latest_risk_factors_date()
        if last_date is None:
             last_date = datetime.strptime(self.COLD_START_DATE, "%Y-%m-%d").date()

        date_str = last_date.strftime("%Y-%m-%d")
        logger.info(f"🚀 拉取 Risk Factors 数据 (起: {date_str})...")
        df_raw = self.massive.get_risk_factors(
            filing_date_gte=date_str, limit=5000
        )

        if df_raw is None or df_raw.empty:
            logger.info("Risk Factors 数据无新增。")
            return

        if "ticker" not in df_raw.columns:
            return
            
        df_raw = df_raw.dropna(subset=["ticker"])
        
        clean_df = UsStockRiskFactorsModel.format_dataframe(df_raw)
        if not clean_df.empty:
            before = len(clean_df)
            clean_df = clean_df[
                (clean_df["ticker"].astype(str).str.len() > 0)
                & (clean_df["primary_category"].astype(str).str.len() > 0)
            ]
            clean_df = clean_df.dropna(subset=["filing_date"])
            dropped = before - len(clean_df)
            if dropped > 0:
                logger.warning(f"⚠️ Risk Factors 清洗后丢弃 {dropped} 条异常记录。")
        if not clean_df.empty:
            self.fundamental_repo.insert_stock_risk_factors(clean_df)
        logger.info(f"Risk Factors 数据拉取完成，新增 {len(clean_df)} 条记录。")

    def sync_risk_taxonomy(self):
        logger.info("🚀 拉取 Risk Taxonomy 数据 ...")
        df_raw = self.massive.get_risk_taxonomy(limit=5000)

        if df_raw is None or df_raw.empty:
            logger.info("Risk Taxonomy 数据无新增。")
            return

        clean_df = UsStockRiskTaxonomyModel.format_dataframe(df_raw)
        if not clean_df.empty:
            before = len(clean_df)
            clean_df = clean_df[
                clean_df["primary_category"].astype(str).str.len() > 0
            ]
            dropped = before - len(clean_df)
            if dropped > 0:
                logger.warning(f"⚠️ Risk Taxonomy 清洗后丢弃 {dropped} 条异常记录。")
        if not clean_df.empty:
            self.fundamental_repo.insert_stock_risk_taxonomy(clean_df)
        logger.info(f"Risk Taxonomy 数据拉取完成，更新了 {len(clean_df)} 条记录。")

    def refresh_incremental_filings(self):
        self.sync_10k_sections()
        self.sync_risk_factors()

    def refresh_all(self):
        self.refresh_incremental_filings()
        self.sync_risk_taxonomy()


if __name__ == "__main__":
    scraper = MassiveFilingsDisclosuresScraper()
    scraper.refresh_all()
