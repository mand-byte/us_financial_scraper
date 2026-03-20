# -*- coding: utf-8 -*-
"""
SEC EDGAR 增量拉取器 (基于全文检索 API 的发现与定点拉取)
======================================================
"""
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, date, timezone
import pandas as pd

from src.utils.logger import app_logger
from src.utils.sec_edgar.client import EdgarClient
from src.utils.sec_edgar.parsers import (
    Form4Parser,
    SC13DParser,
    Form13FParser,
    Form8KParser,
    Form10QParser,
    Form10KParser,
)
from src.dao.clickhouse_manager import ClickHouseManager
from src.dao.sec_edgar_repo import SecEdgarRepo
from src.model.sec_form10k_model import SecForm10KModel
from src.model.sec_form10q_model import SecForm10QModel
from src.model.sec_form13f_model import SecForm13FModel
from src.model.sec_form4_model import SecForm4Model
from src.model.sec_form8k_model import SecForm8KModel
from src.model.sec_sc13d_model import SecSC13DModel

# 对应表单映射
FORM_CLASS_MAPPING = {
    "4": {"parser": Form4Parser, "model": "SecForm4Model"},
    "SC 13D": {"parser": SC13DParser, "model": "SecSC13DModel"},
    "13F-HR": {"parser": Form13FParser, "model": "SecForm13FModel"},
    "8-K": {"parser": Form8KParser, "model": "SecForm8KModel"},
    "10-Q": {"parser": Form10QParser, "model": "SecForm10QModel"},
    "10-K": {"parser": Form10KParser, "model": "SecForm10KModel"},
}


class SecEdgarScraper:
    def __init__(self, db_manager: ClickHouseManager):
        self.repo = SecEdgarRepo(db_manager)
        self.client = EdgarClient()

        # 加载所有解析器
        self.parsers = {
            "4": Form4Parser(),
            "SC 13D": SC13DParser(),
            "13F-HR": Form13FParser(),
            "8-K": Form8KParser(),
            "10-Q": Form10QParser(),
            "10-K": Form10KParser(),
        }

        self.models = {
            "4": SecForm4Model,
            "SC 13D": SecSC13DModel,
            "13F-HR": SecForm13FModel,
            "8-K": SecForm8KModel,
            "10-Q": SecForm10QModel,
            "10-K": SecForm10KModel,
        }

        # 缓存全局 CIK -> FIGI 映射
        self.cik_figi_map = self.repo.get_cik_to_figi_mapping()
        app_logger.info(f"✅ 成功加载 {len(self.cik_figi_map)} 条 CIK->FIGI 映射")

    def _get_zfilled_cik(self, cik: str) -> str:
        """补充为 10 位"""
        return str(cik).zfill(10)

    def _apply_composite_figi(self, results: List[Dict[str, Any]], cik_key: str) -> None:
        """为解析结果注入 composite_figi"""
        for r in results:
            cik = self._get_zfilled_cik(r.get(cik_key, ""))
            r["composite_figi"] = self.cik_figi_map.get(cik, "")

    def _discover_and_fetch(self, form_type: str, start_date: str, end_date: str) -> None:
        """执行全局发现与定点拉取管线"""
        app_logger.info(f"🔍 开始发现 {form_type} 增量任务 ({start_date} -> {end_date})")
        
        discovered_tasks = []
        start_idx = 0
        page_size = 100

        # ========== 1. Discovery 阶段 ==========
        while True:
            # Note: efts.sec.gov form names can be slightly different, SC 13D vs 13D. 
            # We will use the exact form parameter.
            res = self.client.full_text_search(
                forms=form_type if form_type != "SC 13D" else "SC+13D", 
                date_from=start_date, 
                date_to=end_date, 
                start=start_idx, 
                size=page_size
            )
            hits = res.get("hits", {}).get("hits", [])
            for hit in hits:
                source = hit.get("_source", {})
                discovered_tasks.append({
                    "cik": source.get("ciks", [""])[0],
                    "accession_number": source.get("adsh", ""),
                    "filing_date": source.get("file_date", ""),
                    "filer_name": source.get("display_names", [""])[0].split(" (")[0],
                })
            
            total = res.get("hits", {}).get("total", {}).get("value", 0)
            app_logger.debug(f"已发现 {len(discovered_tasks)} / {total} 条 {form_type} 记录...")
            
            start_idx += page_size
            if start_idx >= total or not hits:
                break
        
        app_logger.info(f"🎯 发现完毕, {form_type} 共计 {len(discovered_tasks)} 个待拉取项目")

        # ========== 2. Fetch & Parse 阶段 ==========
        all_parsed_rows = []
        for i, task in enumerate(discovered_tasks):
            cik = self._get_zfilled_cik(task["cik"])
            accn = task["accession_number"]
            meta = {
                "filingDate": task["filing_date"],
                "accessionNumber": accn,
                "filerCik": cik,
                "filerName": task["filer_name"],
            }
            
            if i > 0 and i % 50 == 0:
                app_logger.info(f"🚀 正在拉取 {form_type} {i}/{len(discovered_tasks)}...")

            try:
                # 针对 10-Q/10-K 直接走 XBRL companyfacts 接口提取
                if form_type in ("10-Q", "10-K"):
                    facts_json = self.client.get_companyfacts(cik)
                    if not facts_json:
                        continue
                    rows = self.parsers[form_type].parse(facts_json, meta)
                    
                    # 因为 companyfacts 是全量，返回的 rows 可能包含历史的，我们只过滤刚才发现的那个 accn
                    rows = [r for r in rows if r.get("accession_number", "").replace("-", "") == accn.replace("-", "")]
                    
                    # 追加 FIGI
                    self._apply_composite_figi(rows, "filer_cik")
                    all_parsed_rows.extend(rows)
                
                # 其他表单获取原始文档解析
                else:
                    index_json = self.client.get_filing_index(cik, accn)
                    if not index_json:
                        continue
                    
                    # 寻找主文件 (通常是第一个)
                    files = index_json.get("directory", {}).get("item", [])
                    primary_doc = files[0]["name"] if files else None
                    if not primary_doc:
                        continue
                        
                    doc_text = self.client.get_filing_document(cik, accn, primary_doc)
                    if not doc_text:
                        continue
                        
                    rows = self.parsers[form_type].parse(doc_text, meta)
                    
                    # 确定 mapping 用的 CIK 字段名
                    cik_key = "issuer_cik" if form_type == "4" else "filer_cik"
                    self._apply_composite_figi(rows, cik_key)
                    all_parsed_rows.extend(rows)

            except Exception as e:
                app_logger.error(f"❌ 解析 {form_type} 失败: CIK={cik} ACCN={accn} - {e}")

        # ========== 3. Insert 阶段 ==========
        if all_parsed_rows:
            df = pd.DataFrame(all_parsed_rows)
            # 对于 13F，很多时候 value 是空，需要清理
            self.repo.insert_records(self.models[form_type], df)
        else:
            app_logger.info(f"⏸️ {form_type} 在指定日期范围内无有效数据产生。")

    def sync_form(self, form_type: str, lookback_days: int = 5) -> None:
        """主入口: 同步单一表单"""
        if form_type not in self.parsers:
            raise ValueError(f"不受支持的表单类型: {form_type}")

        # 获取数据库最新日期，作为 fallback
        latest_date = self.repo.get_global_latest_filing_date(self.models[form_type])
        
        today = datetime.now(timezone.utc).date()
        
        if latest_date:
            # 为了防止跨时区或 SEC 系统延迟，退避 2 天
            start_dt = max(latest_date - timedelta(days=2), today - timedelta(days=lookback_days))
        else:
            start_dt = today - timedelta(days=lookback_days)

        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = today.strftime("%Y-%m-%d")

        self._discover_and_fetch(form_type, start_str, end_str)

    def sync_all(self, lookback_days: int = 5) -> None:
        """顺序同步所有关心的表单"""
        for form in self.parsers.keys():
            self.sync_form(form, lookback_days=lookback_days)

if __name__ == "__main__":
    from src.dao.clickhouse_manager import ClickHouseManager
    ch = ClickHouseManager()
    scraper = SecEdgarScraper(ch)
    # 沙盒试运行同步昨天的 Form 4
    scraper._discover_and_fetch("4", start_date="2024-03-19", end_date="2024-03-20")
