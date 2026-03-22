# -*- coding: utf-8 -*-
"""
SEC EDGAR 专用数据仓库访问层
=============================
处理所有 SEC 表的增删查改逻辑
"""

from datetime import date
from typing import Optional, Type

import pandas as pd

from src.model.base_clickhouse_model import BaseClickHouseModel
from src.model.sec_form345_model import _SecFormBase
from src.utils.logger import app_logger


class SecEdgarRepo:
    @property
    def db(self):
        from src.dao.clickhouse_manager import get_db_manager

        return get_db_manager()

    def insert_records(
        self, model_cls: Type[BaseClickHouseModel], df: pd.DataFrame
    ) -> None:
        """通用插入方法，自带列对齐与格式化"""
        if df.empty:
            return

        try:
            formatted_df = model_cls.format_dataframe(df)
            if formatted_df.empty:
                return
            before = len(formatted_df)
            formatted_df = formatted_df[
                (formatted_df["issuer_cik"].astype(str).str.len() > 0)
                & (formatted_df["accession_number"].astype(str).str.len() > 0)
            ]
            if "acceptance_datetime" in formatted_df.columns:
                adt = pd.to_datetime(
                    formatted_df["acceptance_datetime"], errors="coerce", utc=True
                )
                formatted_df = formatted_df[adt.notna() & (adt.dt.year > 1971)]
            if formatted_df.empty:
                app_logger.warning(f"⚠️ {model_cls.table_name} 清洗后无可入库记录，已跳过。")
                return
            dropped = before - len(formatted_df)
            if dropped > 0:
                app_logger.warning(
                    f"⚠️ {model_cls.table_name} 丢弃 {dropped} 条异常记录(空主键或非法 acceptance_datetime)。"
                )
            self.db.insert_model_df(model_cls, formatted_df)
            app_logger.info(
                f"💾 成功插入 {len(formatted_df)} 条记录至 {model_cls.table_name}"
            )
        except Exception as e:
            app_logger.error(f"❌ 插入 {model_cls.table_name} 失败: {e}")
            raise

    def get_global_latest_filing_date(
        self, model_cls: Type[BaseClickHouseModel]
    ) -> Optional[date]:
        """获取全表最新一条记录的申报或报告日期 (用于确定全局增量起点)"""
        sql = getattr(model_cls, "QUERY_GLOBAL_LATEST_FILING_SQL", None)
        if not sql:
            app_logger.warning(
                f"⚠️ {model_cls.__name__} 未定义 QUERY_GLOBAL_LATEST_FILING_SQL"
            )
            return None

        try:
            df = self.db.query_dataframe(sql)
            if not df.empty and pd.notnull(df.iloc[0]["last_date"]):
                return df.iloc[0]["last_date"]
            return None
        except Exception as e:
            app_logger.error(f"❌ 查询 {model_cls.table_name} 全局最新时间失败: {e}")
            return None

    def get_form345_latest_filing_date(self) -> Optional[date]:
        """获取 Form 3/4/5 表最新 acceptance_datetime 日期"""
        try:
            df = self.db.query_dataframe(_SecFormBase.QUERY_GLOBAL_LATEST_ACCEPTANCE_DATE_SQL)
            if not df.empty and pd.notnull(df.iloc[0]["last_date"]):
                return pd.to_datetime(df.iloc[0]["last_date"]).date()
            return None
        except Exception as e:
            app_logger.error(
                f"❌ 查询 sec_form345_insider_transactions 最新时间失败: {e}"
            )
            return None


    def get_latest_ts_df_by_cik(self, table_name: str) -> pd.DataFrame:
        """获取按 CIK 分组的最新时间戳"""
        sql = _SecFormBase.build_query_latest_ts_by_cik_sql(table_name=table_name)
        try:
            return self.db.query_dataframe(sql)
        except Exception as e:
            app_logger.error(f"❌ 查询 {table_name} 最新时间戳失败: {e}")
            return pd.DataFrame()


    def query_form345_by_figi(self, figi: str, limit: int = 100) -> pd.DataFrame:
        """根据 figi 查询内幕交易记录"""
        sql = _SecFormBase.build_query_by_figi_sql(figi=figi, limit=limit)
        try:
            return self.db.query_dataframe(sql)
        except Exception as e:
            app_logger.error(f"❌ 查询 Form 3/4/5 失败: {e}")
            return pd.DataFrame()

    def query_form345_by_owner(self, owner_name: str, limit: int = 100) -> pd.DataFrame:
        """根据内幕人姓名查询交易记录"""
        sql = _SecFormBase.build_query_by_owner_sql(
            owner_name=owner_name,
            limit=limit,
        )
        try:
            return self.db.query_dataframe(sql)
        except Exception as e:
            app_logger.error(f"❌ 查询内幕人记录失败: {e}")
            return pd.DataFrame()

    def query_form345_by_ticker(self, ticker: str, limit: int = 100) -> pd.DataFrame:
        """根据 ticker 查询内幕交易记录"""
        sql = _SecFormBase.build_query_by_ticker_sql(ticker=ticker, limit=limit)
        try:
            return self.db.query_dataframe(sql)
        except Exception as e:
            app_logger.error(f"❌ 查询 ticker 记录失败: {e}")
            return pd.DataFrame()
