# -*- coding: utf-8 -*-
"""
SEC 8-K 重大事件 Model
=======================
8-K filings 用于公司重大事件实时披露,
核心价值在于 Item 编号 → 事件类型分类。
"""
from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class SecForm8KModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "sec_form8k_events"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS sec_form8k_events
        (
            filer_cik               String       COMMENT '公司 CIK',
            filer_name              String       COMMENT '公司名称',
            composite_figi          String       COMMENT 'PIT 映射后的 FIGI',

            filing_date             Date         COMMENT 'SEC 申报日期',
            report_date             Date         COMMENT '事件报告日期',
            accession_number        String       COMMENT 'EDGAR Accession Number',

            items                   Array(String) COMMENT 'Item 编号列表 (["2.02","9.01"])',
            item_descriptions       Array(String) COMMENT 'Item 描述列表',
            document_text           String CODEC(ZSTD(3)) COMMENT '正文摘要 (前5000字符)',

            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (composite_figi, filing_date, accession_number)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "filer_cik": {"type": "str"},
        "filer_name": {"type": "str"},
        "composite_figi": {"type": "str", "default": ""},
        "filing_date": {"type": "date"},
        "report_date": {"type": "date"},
        "accession_number": {"type": "str"},
        "items": {"type": "array_str", "default": []},
        "item_descriptions": {"type": "array_str", "default": []},
        "document_text": {"type": "str", "default": ""},
    }

    QUERY_LATEST_FILING_BY_CIK_SQL: ClassVar[str] = (
        "SELECT max(filing_date) as last_date FROM sec_form8k_events WHERE filer_cik = '{filer_cik}'"
    )
    QUERY_GLOBAL_LATEST_FILING_SQL: ClassVar[str] = (
        "SELECT max(filing_date) as last_date FROM sec_form8k_events"
    )

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                default = meta.get("default", None)
                if meta["type"] == "array_str":
                    df[col] = [default or [] for _ in range(len(df))]
                else:
                    df[col] = default

        date_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        str_cols = {k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"}
        for col, length in str_cols.items():
            if col in df.columns:
                df[col] = df[col].astype(str).replace("None", "")
                if length:
                    df[col] = df[col].str.slice(0, length)

        return df[list(cls.SCHEMA_CLEAN.keys())]
