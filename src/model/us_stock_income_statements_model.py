from src.model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class UsStockIncomeStatementsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_income_statements"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_stock_income_statements
        (
            composite_figi FixedString(12) COMMENT '全球投资工具统一识别码(固定12位)',
            timeframe LowCardinality(String) COMMENT '时间框架(quarterly/annual/trailing_twelve_months)',
            fiscal_year UInt16 COMMENT '财年',
            fiscal_quarter UInt8 COMMENT '财季',
            period_end Date COMMENT '财报期末日期',
            filing_date Date COMMENT 'SEC实际披露日期',

            -- 利润表核心营收与利润科目 (Float64 + ZSTD(3) 物理压缩)
            revenue Float64 CODEC(ZSTD(3)),
            cost_of_revenue Float64 CODEC(ZSTD(3)),
            gross_profit Float64 CODEC(ZSTD(3)),
            operating_income Float64 CODEC(ZSTD(3)),
            
            -- 运营开支明细
            total_operating_expenses Float64 CODEC(ZSTD(3)),
            selling_general_administrative Float64 CODEC(ZSTD(3)),
            research_development Float64 CODEC(ZSTD(3)),
            depreciation_depletion_amortization Float64 CODEC(ZSTD(3)),
            other_operating_expenses Float64 CODEC(ZSTD(3)),

            -- 非运营与税前/税后科目
            income_before_income_taxes Float64 CODEC(ZSTD(3)),
            income_taxes Float64 CODEC(ZSTD(3)),
            interest_income Float64 CODEC(ZSTD(3)),
            interest_expense Float64 CODEC(ZSTD(3)),
            total_other_income_expense Float64 CODEC(ZSTD(3)),
            other_income_expense Float64 CODEC(ZSTD(3)),
            equity_in_affiliates Float64 CODEC(ZSTD(3)),
            
            -- 净利润与特殊项目
            consolidated_net_income_loss Float64 CODEC(ZSTD(3)),
            net_income_loss_attributable_common_shareholders Float64 CODEC(ZSTD(3)),
            noncontrolling_interest Float64 CODEC(ZSTD(3)),
            extraordinary_items Float64 CODEC(ZSTD(3)),
            discontinued_operations Float64 CODEC(ZSTD(3)),
            preferred_stock_dividends_declared Float64 CODEC(ZSTD(3)),
            ebitda Float64 CODEC(ZSTD(3)),

            -- 每股收益与股本结构
            basic_earnings_per_share Float64 CODEC(ZSTD(3)),
            diluted_earnings_per_share Float64 CODEC(ZSTD(3)),
            basic_shares_outstanding Float64 CODEC(ZSTD(3)),
            diluted_shares_outstanding Float64 CODEC(ZSTD(3)),

            -- 系统级时间戳控制
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = ReplacingMergeTree(update_time)
        PARTITION BY toYYYY(period_end)
        ORDER BY (composite_figi, timeframe, period_end)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "composite_figi": {"type": "str", "len": 12},
        "timeframe": {"type": "str"},
        "fiscal_year": {"type": "uint64", "default": 0},
        "fiscal_quarter": {"type": "uint64", "default": 0},
        "period_end": {"type": "date"},
        "filing_date": {"type": "date"},
        "revenue": {"type": "float64", "default": 0.0},
        "cost_of_revenue": {"type": "float64", "default": 0.0},
        "gross_profit": {"type": "float64", "default": 0.0},
        "operating_income": {"type": "float64", "default": 0.0},
        "total_operating_expenses": {"type": "float64", "default": 0.0},
        "selling_general_administrative": {"type": "float64", "default": 0.0},
        "research_development": {"type": "float64", "default": 0.0},
        "depreciation_depletion_amortization": {"type": "float64", "default": 0.0},
        "other_operating_expenses": {"type": "float64", "default": 0.0},
        "income_before_income_taxes": {"type": "float64", "default": 0.0},
        "income_taxes": {"type": "float64", "default": 0.0},
        "interest_income": {"type": "float64", "default": 0.0},
        "interest_expense": {"type": "float64", "default": 0.0},
        "total_other_income_expense": {"type": "float64", "default": 0.0},
        "other_income_expense": {"type": "float64", "default": 0.0},
        "equity_in_affiliates": {"type": "float64", "default": 0.0},
        "consolidated_net_income_loss": {"type": "float64", "default": 0.0},
        "net_income_loss_attributable_common_shareholders": {
            "type": "float64",
            "default": 0.0,
        },
        "noncontrolling_interest": {"type": "float64", "default": 0.0},
        "extraordinary_items": {"type": "float64", "default": 0.0},
        "discontinued_operations": {"type": "float64", "default": 0.0},
        "preferred_stock_dividends_declared": {"type": "float64", "default": 0.0},
        "ebitda": {"type": "float64", "default": 0.0},
        "basic_earnings_per_share": {"type": "float64", "default": 0.0},
        "diluted_earnings_per_share": {"type": "float64", "default": 0.0},
        "basic_shares_outstanding": {"type": "float64", "default": 0.0},
        "diluted_shares_outstanding": {"type": "float64", "default": 0.0},
        "update_time": {"type": "datetime", "tz": "UTC"},
    }

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
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        time_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "datetime"]
        for col in time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col], errors="coerce", utc=True
                ).dt.tz_localize(None)

        str_cols = {
            k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"
        }
        for col, length in str_cols.items():
            if col in df.columns:
                df[col] = df[col].astype(str)
                if length:
                    df[col] = df[col].str.slice(0, length)

        float_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "float64"]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        int_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if "int" in v["type"]]
        for col in int_cols:
            if col in df.columns:
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce").fillna(0).astype("uint64")
                )

        return df[list(cls.SCHEMA_CLEAN.keys())]
