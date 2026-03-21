# -*- coding: utf-8 -*-
import os
import time
from typing import List, Dict, Any
from datetime import datetime, timezone
import pandas as pd

from sec_edgar_downloader import Downloader

from src.utils.logger import app_logger
from src.dao.clickhouse_manager import ClickHouseManager
from src.dao.sec_edgar_repo import SecEdgarRepo
from src.dao.market_data_repo import MarketDataRepo
from src.utils.sec_edgar_parsers.form345_parser import Form345Parser

from src.model.sec_form345_model import SecForm3Model, SecForm4Model, SecForm5Model

SEC_COMPANY_NAME = os.getenv("SEC_DOWNLOADER_COMPANY", "QuantResearch")
SEC_EMAIL = os.getenv("SEC_DOWNLOADER_EMAIL", "research@example.com")


class SecEdgarScraper:
    def __init__(self, scheduler=None):
        self.db_manager = ClickHouseManager()
        self.repo = SecEdgarRepo(self.db_manager)
        self.market_repo = MarketDataRepo()
        self.scheduler = scheduler
        self.download_dir = os.path.join(os.path.expanduser("~"), ".sec_edgar_cache")

    def _download_and_parse(
        self, parser: Form345Parser, ticker_or_cik: str, form_type: str, start_date: str, end_date: str
    ) -> tuple[List[Dict[str, Any]], str]:
        os.makedirs(self.download_dir, exist_ok=True)

        dl = Downloader(SEC_COMPANY_NAME, SEC_EMAIL, self.download_dir)
        try:
            dl.get(form_type, ticker_or_cik, after=start_date, before=end_date)
        except Exception as e:
            app_logger.warning(f"下载 {ticker_or_cik} Form {form_type} 报错: {e}")

        all_rows = []
        filing_dir = os.path.join(
            self.download_dir, "sec-edgar-filings", ticker_or_cik, form_type
        )

        if not os.path.exists(filing_dir):
            return all_rows, self.download_dir

        for accn_dir in os.listdir(filing_dir):
            accn_path = os.path.join(filing_dir, accn_dir)
            if not os.path.isdir(accn_path):
                continue

            parsed_flag = os.path.join(accn_path, ".parsed")
            if os.path.exists(parsed_flag):
                continue

            submission_file = os.path.join(accn_path, "full-submission.txt")
            if not os.path.exists(submission_file):
                continue

            try:
                with open(
                    submission_file, "r", encoding="utf-8", errors="replace"
                ) as f:
                    content = f.read()

                rows = parser.parse_submission(content, accn_dir, form_type)
                if rows:
                    all_rows.extend(rows)
                
                with open(parsed_flag, "w") as pf:
                    pf.write("1")

            except Exception as e:
                app_logger.warning(f"⚠️ 解析 {submission_file} 失败: {e}")
                continue

        return all_rows, self.download_dir

    def sync_form3(self):
        self._sync_form_base("3", SecForm3Model)

    def sync_form4(self):
        self._sync_form_base("4", SecForm4Model)

    def sync_form5(self):
        self._sync_form_base("5", SecForm5Model)

    def _sync_form_base(self, form_type: str, model_cls) -> None:
        table_name = model_cls.table_name
        app_logger.info(f"🔍 [Form {form_type}] 启动增量拉取检测...")

        # 每次调度实时从数据库获取映射，保持彻底无状态
        cik_figi_map = self.market_repo.get_cik_to_figi_mapping()
        parser = Form345Parser(cik_figi_map)

        tasks_df = self.market_repo.get_sync_tasks(table_name)
        if tasks_df.empty:
            app_logger.info(f"⏸️ 无 Form {form_type} 增量拉取任务。")
            return

        latest_ts_df = self.repo.get_latest_ts_df_by_figi(table_name)
        if latest_ts_df.empty:
            ts_map = {}
        else:
            ts_map = dict(zip(latest_ts_df["composite_figi"], latest_ts_df["last_ts"]))

        now = datetime.now(timezone.utc)
        # 表里没记录时从头拉取
        cold_start = datetime.strptime("2000-01-01", "%Y-%m-%d").replace(tzinfo=timezone.utc)

        all_rows = []
        for _, row in tasks_df.iterrows():
            if row["sync_state"] == 1:
                continue

            ticker = row["ticker"]
            composite_figi = row["composite_figi"]
            active = row["active"]
            
            last_ts = ts_map.get(composite_figi)
            if pd.notna(last_ts) and last_ts:
                start_dt = pd.to_datetime(last_ts).replace(tzinfo=timezone.utc)
            else:
                start_dt = cold_start

            start_str = start_dt.strftime("%Y-%m-%d")
            end_str = now.strftime("%Y-%m-%d")

            try:
                rows, _ = self._download_and_parse(parser, ticker, form_type, start_str, end_str)
                if rows:
                    df = pd.DataFrame(rows)
                    self.repo.insert_records(model_cls, df)
                    app_logger.debug(f"  {ticker}: {len(rows)} 条记录 (Form {form_type})")
                    all_rows.extend(rows)

                # 当退市(active=0)时，视为已从头拉取完毕，插入 state 为 1
                if active == 0:
                    self.market_repo.update_sync_status(table_name, composite_figi, state=1)
                    app_logger.info(f"🏁 {ticker} active=0, Form {form_type} 数据拉取见底, 置为 state=1.")

                time.sleep(0.3)
            except Exception as e:
                app_logger.warning(f"⚠️ 下载 {ticker} 失败: {e}")
                time.sleep(0.3)
                continue

        app_logger.info(f"✅ [Form {form_type}] 本轮全量迭代完成，累计获取 {len(all_rows)} 条记录")

    def start(self):
        if self.scheduler:
            self.scheduler.add_job(self.sync_form3, "interval", minutes=5, id="sync_form3", replace_existing=True)
            self.scheduler.add_job(self.sync_form4, "interval", minutes=5, id="sync_form4", replace_existing=True)
            self.scheduler.add_job(self.sync_form5, "interval", minutes=5, id="sync_form5", replace_existing=True)
            app_logger.info("✅ 已分别挂载 Form 3, 4, 5 的增量调度器 (每5分钟)。")

    def stop(self):
        if hasattr(self, 'scheduler') and self.scheduler:
            try:
                self.scheduler.remove_job("sync_form3")
                self.scheduler.remove_job("sync_form4")
                self.scheduler.remove_job("sync_form5")
            except Exception:
                pass
        app_logger.info("🛑 SEC EDGAR 搜刮器已停止。")


if __name__ == "__main__":
    from apscheduler.schedulers.blocking import BlockingScheduler

    scraper = SecEdgarScraper()
    
    app_logger.info("🚀 启动 SEC Edgar 独立拆分队列同步服务 (按 state 记录)...")
    
    scheduler = BlockingScheduler()
    scraper.scheduler = scheduler
    scraper.start()
    scheduler.start()
