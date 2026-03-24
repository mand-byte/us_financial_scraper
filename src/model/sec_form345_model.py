# -*- coding: utf-8 -*-
from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


def _get_sec_ddl(table_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            issuer_ticker          String       COMMENT '申报时使用的 ticker',
            issuer_cik             String       COMMENT '发行人 CIK',
            reporting_owner_name   String       COMMENT '内幕人姓名',
            is_director            Bool         COMMENT '是否董事',
            is_officer             Bool         COMMENT '是否高管',
            is_ten_percent_owner   Bool         COMMENT '是否 10%+ 股东',
            officer_title          String       COMMENT '职位名称',
            filing_date            Date         COMMENT 'SEC 申报日期 (periodOfReport)',
            transaction_date       Date         COMMENT '交易执行日期 (持仓报告时为空)',
            transaction_code       String       COMMENT 'P=买入 S=卖出 M=行权 A=授予 G=赠与 W=卖出(Form5) ...',
            security_title         String       COMMENT '证券名称 (Common Stock / RSU / Option)',
            is_derivative          Bool         COMMENT '是否衍生品交易',
            transaction_shares     Float64      COMMENT '交易股数 (持仓报告时为 0)',
            transaction_price_per_share Float64  COMMENT '每股价格 (持仓报告时为 0)',
            shares_owned_following_transaction Float64 COMMENT '交易后持有股数',
            shares_owned           Float64      COMMENT '持仓数量 (仅 Form3 持仓报告使用)',
            ownership_form         String       COMMENT 'D=直接持有 I=间接持有',
            acceptance_datetime    DateTime64(3, 'UTC') COMMENT 'SEC 接收时间 (美东转 UTC)',
            accession_number       String       COMMENT 'EDGAR Accession Number',
            form_type              String       COMMENT '3, 4, 或 5',
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (issuer_cik, issuer_ticker, acceptance_datetime, reporting_owner_name, filing_date, transaction_date, transaction_code, transaction_shares)
    """


def _get_state_ddl(table_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            composite_figi String,
            state UInt8 DEFAULT 0,
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time) ORDER BY (composite_figi)
    """


class _SecFormBase(BaseClickHouseModel):
    FORM345_UNION_SQL: ClassVar[str] = (
        "SELECT * FROM sec_form3_insider_transactions "
        "UNION ALL "
        "SELECT * FROM sec_form4_insider_transactions "
        "UNION ALL "
        "SELECT * FROM sec_form5_insider_transactions"
    )
    QUERY_LATEST_TS_BY_CIK_SQL: ClassVar[str] = (
        "SELECT issuer_cik as cik, max(acceptance_datetime) as last_ts FROM {table_name} GROUP BY issuer_cik"
    )
    QUERY_GLOBAL_LATEST_ACCEPTANCE_DATE_SQL: ClassVar[str] = (
        f"SELECT max(acceptance_datetime) as last_date FROM ({FORM345_UNION_SQL}) s"
    )
    QUERY_BY_FIGI_SQL: ClassVar[str] = (
        "SELECT s.* "
        f"FROM ({FORM345_UNION_SQL}) s "
        "ANY INNER JOIN us_stock_universe u FINAL"
        "ON right(concat('0000000000', s.issuer_cik), 10) = u.cik "
        "WHERE u.composite_figi = {figi} "
        "ORDER BY acceptance_datetime DESC "
        "LIMIT {limit}"
    )
    QUERY_BY_OWNER_SQL: ClassVar[str] = (
        f"SELECT * FROM ({FORM345_UNION_SQL}) s "
        "WHERE reporting_owner_name = {owner_name} "
        "ORDER BY acceptance_datetime DESC "
        "LIMIT {limit}"
    )
    QUERY_BY_TICKER_SQL: ClassVar[str] = (
        f"SELECT * FROM ({FORM345_UNION_SQL}) s "
        "WHERE issuer_ticker = {ticker} "
        "ORDER BY acceptance_datetime DESC "
        "LIMIT {limit}"
    )

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "issuer_ticker": {"type": "str", "default": ""},
        "issuer_cik": {"type": "str", "default": ""},
        "reporting_owner_name": {"type": "str", "default": ""},
        "is_director": {"type": "bool", "default": False},
        "is_officer": {"type": "bool", "default": False},
        "is_ten_percent_owner": {"type": "bool", "default": False},
        "officer_title": {"type": "str", "default": ""},
        "filing_date": {"type": "date"},
        "transaction_date": {"type": "date"},
        "transaction_code": {"type": "str", "default": ""},
        "security_title": {"type": "str", "default": ""},
        "is_derivative": {"type": "bool", "default": False},
        "transaction_shares": {"type": "float64", "default": 0.0},
        "transaction_price_per_share": {"type": "float64", "default": 0.0},
        "shares_owned_following_transaction": {"type": "float64", "default": 0.0},
        "shares_owned": {"type": "float64", "default": 0.0},
        "ownership_form": {"type": "str", "default": ""},
        "acceptance_datetime": {"type": "datetime64"},
        "accession_number": {"type": "str", "default": ""},
        "form_type": {"type": "str", "default": ""},
    }

    @classmethod
    def build_query_latest_ts_by_cik_sql(cls, table_name: str) -> str:
        return cls.QUERY_LATEST_TS_BY_CIK_SQL.format(
            table_name=cls.sql_identifier(table_name)
        )

    @classmethod
    def build_query_by_figi_sql(cls, figi: str, limit: int) -> str:
        safe_limit = max(1, int(limit))
        return cls.QUERY_BY_FIGI_SQL.format(
            figi=cls.sql_literal(figi),
            limit=safe_limit,
        )

    @classmethod
    def build_query_by_owner_sql(cls, owner_name: str, limit: int) -> str:
        safe_limit = max(1, int(limit))
        return cls.QUERY_BY_OWNER_SQL.format(
            owner_name=cls.sql_literal(owner_name),
            limit=safe_limit,
        )

    @classmethod
    def build_query_by_ticker_sql(cls, ticker: str, limit: int) -> str:
        safe_limit = max(1, int(limit))
        return cls.QUERY_BY_TICKER_SQL.format(
            ticker=cls.sql_literal(ticker),
            limit=safe_limit,
        )

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        date_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "date"]
        for col in date_cols:
            if col in df.columns:

                def _safe_date(v):
                    if pd.isna(v) or v is None or str(v).strip() in ("", "None"):
                        return pd.to_datetime("1970-01-01").date()
                    try:
                        return pd.to_datetime(v).date()
                    except Exception:
                        return pd.to_datetime("1970-01-01").date()

                df[col] = df[col].apply(_safe_date)

        datetime_cols = [
            k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "datetime64"
        ]
        for col in datetime_cols:
            if col in df.columns:

                def _safe_datetime(v):
                    if pd.isna(v) or v is None or str(v).strip() in ("", "None"):
                        return pd.to_datetime("1970-01-01 00:00:00").tz_localize("UTC")
                    try:
                        dt = pd.to_datetime(v)
                        if dt.tzinfo is None:
                            dt = dt.tz_localize("UTC")
                        return dt
                    except Exception:
                        return pd.to_datetime("1970-01-01 00:00:00").tz_localize("UTC")

                df[col] = df[col].apply(_safe_datetime)

        str_cols = {
            k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"
        }
        for col, length in str_cols.items():
            if col in df.columns:
                df[col] = df[col].astype(str).replace("None", "")
                if length:
                    df[col] = df[col].str.slice(0, length)

        float_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "float64"]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        bool_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "bool"]
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool)

        return df[list(cls.SCHEMA_CLEAN.keys())]


class SecForm3Model(_SecFormBase):
    table_name: ClassVar[str] = "sec_form3_insider_transactions"
    __DDL__: ClassVar[str] = _get_sec_ddl("sec_form3_insider_transactions")


class SecForm4Model(_SecFormBase):
    table_name: ClassVar[str] = "sec_form4_insider_transactions"
    __DDL__: ClassVar[str] = _get_sec_ddl("sec_form4_insider_transactions")


class SecForm5Model(_SecFormBase):
    table_name: ClassVar[str] = "sec_form5_insider_transactions"
    __DDL__: ClassVar[str] = _get_sec_ddl("sec_form5_insider_transactions")


class SecForm3StateModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "sec_form3_insider_transactions_state"
    __DDL__: ClassVar[str] = _get_state_ddl("sec_form3_insider_transactions_state")


class SecForm4StateModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "sec_form4_insider_transactions_state"
    __DDL__: ClassVar[str] = _get_state_ddl("sec_form4_insider_transactions_state")


class SecForm5StateModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "sec_form5_insider_transactions_state"
    __DDL__: ClassVar[str] = _get_state_ddl("sec_form5_insider_transactions_state")
