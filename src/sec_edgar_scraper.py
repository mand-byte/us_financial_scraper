# -*- coding: utf-8 -*-
import os
from src.config.settings import settings
import time
from typing import List, Dict, Any
from datetime import datetime, timezone
import pandas as pd

from sec_edgar_downloader import Downloader

from src.utils.logger import app_logger
from src.dao.sec_edgar_repo import SecEdgarRepo
from src.dao.market_data_repo import MarketDataRepo
from src.utils.sec_edgar_parsers.form345_parser import Form345Parser

from src.model.sec_form345_model import SecForm3Model, SecForm4Model, SecForm5Model
from zoneinfo import ZoneInfo
SEC_COMPANY_NAME = settings.api.sec_downloader_company
SEC_EMAIL = settings.api.sec_downloader_email


class SecEdgarScraper:
    NYC = ZoneInfo("America/New_York")
    FORM_MODEL_PAIRS = [
        ("3", SecForm3Model),
        ("4", SecForm4Model),
        ("5", SecForm5Model),
    ]

    def __init__(self, scheduler=None):
        self.repo = SecEdgarRepo()
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

    def _sync_form_base(self, form_type: str, model_cls) -> None:
        table_name = model_cls.table_name
        app_logger.info(f"🔍 [Form {form_type}] 启动增量拉取检测...")

        # 取消 FIGI 映射依赖，直接解析
        parser = Form345Parser()

        tasks_df = self.market_repo.get_sync_tasks(table_name)
        if tasks_df.empty:
            app_logger.info(f"⏸️ 无 Form {form_type} 增量拉取任务。")
            return

        latest_ts_df = self.repo.get_latest_ts_df_by_cik(table_name)
        if latest_ts_df.empty:
            ts_map = {}
        else:
            ts_map = dict(zip(latest_ts_df["cik"], latest_ts_df["last_ts"]))

        now = datetime.now(timezone.utc)
        # 表里没记录或错误记录 1970 时从配置拉取
        start_date_env = settings.scraper.scraping_start_date
        cold_start = datetime.strptime(start_date_env, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        all_rows = []
        for _, row in tasks_df.iterrows():
            if row["sync_state"] == 1:
                continue

            ticker = row["ticker"]
            cik = row["cik"].decode("utf-8") if isinstance(row["cik"], bytes) else str(row["cik"])
            composite_figi = row["composite_figi"]
            active = row["active"]
            
            zfilled_cik = str(cik).zfill(10)
            last_ts = ts_map.get(zfilled_cik)
            if pd.notna(last_ts) and last_ts:
                start_dt = pd.to_datetime(last_ts).replace(tzinfo=timezone.utc)
                if start_dt < cold_start:
                    start_dt = cold_start
            else:
                start_dt = cold_start

            start_str = start_dt.strftime("%Y-%m-%d")
            end_str = now.strftime("%Y-%m-%d")

            try:
                rows, _ = self._download_and_parse(parser, cik, form_type, start_str, end_str)
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

    def sync_all_forms(self):
        try:
            for form_type, model_cls in self.FORM_MODEL_PAIRS:
                self._sync_form_base(form_type, model_cls)
        except Exception as e:
            app_logger.error(f"❌ 批量拉取 SEC Edgar 13F/3/4/5 表单发生异常: {e}")

    def start(self):
        if self.scheduler:
            self.scheduler.add_job(
                self.sync_all_forms, 
                "interval", 
                minutes=5, 
                id="sync_all_edgar_forms", 
                next_run_time=datetime.now(self.NYC),
                max_instances=1,
                coalesce=True,
                replace_existing=True
            )
            app_logger.info("✅ 已挂载 Form 3, 4, 5 的串行增量调度器 (每5分钟执行一轮)。")

    def stop(self):
        if hasattr(self, 'scheduler') and self.scheduler:
            try:
                self.scheduler.remove_job("sync_all_edgar_forms")
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
