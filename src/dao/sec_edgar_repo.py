# -*- coding: utf-8 -*-
"""
SEC EDGAR 专用数据仓库访问层
=============================
处理所有 6 个 SEC 表的增删查改逻辑
"""
from typing import Optional, Type, Dict
import pandas as pd
from datetime import date

from src.utils.logger import app_logger
from src.dao.clickhouse_manager import ClickHouseManager
from src.model.base_clickhouse_model import BaseClickHouseModel


class SecEdgarRepo:
    def __init__(self, db_manager: ClickHouseManager):
        self.db = db_manager

    def insert_records(self, model_cls: Type[BaseClickHouseModel], df: pd.DataFrame) -> None:
        """通用插入方法，自带列对齐与格式化"""
        if df.empty:
            return

        try:
            formatted_df = model_cls.format_dataframe(df)
            self.db.insert_dataframe(model_cls.table_name, formatted_df)
            app_logger.info(f"💾 成功插入 {len(formatted_df)} 条记录至 {model_cls.table_name}")
        except Exception as e:
            app_logger.error(f"❌ 插入 {model_cls.table_name} 失败: {e}")
            raise

    def get_global_latest_filing_date(self, model_cls: Type[BaseClickHouseModel]) -> Optional[date]:
        """获取全表最新一条记录的申报或报告日期 (用于确定全局增量起点)"""
        sql = getattr(model_cls, "QUERY_GLOBAL_LATEST_FILING_SQL", None)
        if not sql:
            app_logger.warning(f"⚠️ {model_cls.__name__} 未定义 QUERY_GLOBAL_LATEST_FILING_SQL")
            return None

        try:
            df = self.db.query_dataframe(sql)
            if not df.empty and pd.notnull(df.iloc[0]["last_date"]):
                return df.iloc[0]["last_date"]
            return None
        except Exception as e:
            app_logger.error(f"❌ 查询 {model_cls.table_name} 全局最新时间失败: {e}")
            return None

    def get_cik_to_figi_mapping(self) -> Dict[str, str]:
        """从 us_stock_universe 加载 CIK 到 composite_figi 的全局映射"""
        sql = "SELECT cik, composite_figi FROM us_stock_universe WHERE cik IS NOT NULL AND cik != ''"
        try:
            df = self.db.query_dataframe(sql)
            if df.empty:
                return {}
            # Some CIKs in SEC are stripped of leading zeros, ensure mapping handles it or strips it
            # SEC usually uses 10-digit CIKs with leading zeros in universe table
            return dict(zip(df["cik"].astype(str).str.zfill(10), df["composite_figi"].astype(str)))
        except Exception as e:
            app_logger.error(f"❌ 获取 CIK -> FIGI 映射失败: {e}")
            return {}
