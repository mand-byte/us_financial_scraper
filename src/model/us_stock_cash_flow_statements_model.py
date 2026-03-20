from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class UsStockCashFlowStatementsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_cash_flow_statements"

    __DDL__: ClassVar[str] = """
            CREATE TABLE IF NOT EXISTS us_stock_cash_flow_statements
            (
            composite_figi FixedString(12) COMMENT '全球投资工具统一识别码(固定12位)',
            timeframe LowCardinality(String) COMMENT '时间框架(quarterly/annual/trailing_twelve_months)',
            fiscal_year UInt16 COMMENT '财年',
            fiscal_quarter UInt8 COMMENT '财季',
            period_end Date COMMENT '财报期末日期',
            filing_date Date COMMENT 'SEC实际披露日期',

            -- 现金流量表核心科目 (Float64 + ZSTD(3) 高效物理压缩)
            net_income Float64 CODEC(ZSTD(3)), -- 净利润用作经营现金流计算的起点。
            depreciation_depletion_and_amortization Float64 CODEC(ZSTD(3)),  -- Non-cash charges for the reduction in value of tangible and intangible assets over time.
            change_in_other_operating_assets_and_liabilities_net Float64 CODEC(ZSTD(3)),  -- Net change in working capital components including accounts receivable, inventory, accounts payable, and other operating items.
            cash_from_operating_activities_continuing_operations Float64 CODEC(ZSTD(3)),  -- Cash generated from continuing business operations before discontinued operations.
            net_cash_from_operating_activities Float64 CODEC(ZSTD(3)),    -- 经营活动产生的或使用的现金总额，代表核心业务运营产生的现金流量。
            net_cash_from_operating_activities_discontinued_operations Float64 CODEC(ZSTD(3)),    -- 已终止业务部门的经营活动现金流量。
            other_operating_activities Float64 CODEC(ZSTD(3)),      -- 其他未归类于其他类别的调整，用于将净收入与经营现金流进行核对。

            purchase_of_property_plant_and_equipment Float64 CODEC(ZSTD(3)), -- 购买或建造长期资产（如厂房、设备）的现金支出。
            sale_of_property_plant_and_equipment Float64 CODEC(ZSTD(3)), -- 处置固定资产带来的现金流入，通常以正值表示。
            net_cash_from_investing_activities Float64 CODEC(ZSTD(3)),      -- 投资活动产生的或使用的现金总额，包括资本支出、收购和资产出售。
            net_cash_from_investing_activities_continuing_operations Float64 CODEC(ZSTD(3)),    -- 持续经营业务投资活动产生的现金流量（终止经营业务之前）。
            net_cash_from_investing_activities_discontinued_operations Float64 CODEC(ZSTD(3)),    -- 已终止业务部门投资活动产生的现金流量。
            other_investing_activities Float64 CODEC(ZSTD(3)),          --  来自投资活动（未归入其他类别）的现金流量，包括收购、剥离和投资。

            dividends Float64 CODEC(ZSTD(3)),                           -- Payments made to shareholders in the form of cash distributions.Cash payments to shareholders in the form of dividends, typically reported as negative values.
            short_term_debt_issuances_repayments Float64 CODEC(ZSTD(3)),    -- 发行或偿还短期债务产生的净现金流量。
            long_term_debt_issuances_repayments Float64 CODEC(ZSTD(3)),   --  Net cash flows from issuing or repaying long-term debt obligations.
            net_cash_from_financing_activities Float64 CODEC(ZSTD(3)),    -- Total cash generated or used by financing activities, including debt issuance, debt repayment, dividends, and share transactions.
            net_cash_from_financing_activities_continuing_operations Float64 CODEC(ZSTD(3)),    -- 持续经营业务融资活动产生的现金流量（终止经营业务之前）。
            net_cash_from_financing_activities_discontinued_operations Float64 CODEC(ZSTD(3)),    -- 已终止业务部门融资活动的现金流量。
            other_financing_activities Float64 CODEC(ZSTD(3)),      -- 来自融资活动（未归类于其他项目）的现金流量，包括股份回购和其他股权交易。

            income_loss_from_discontinued_operations Float64 CODEC(ZSTD(3)),    --  After-tax income or loss from business operations that have been discontinued.
            noncontrolling_interests Float64 CODEC(ZSTD(3)),            -- 与合并子公司少数股东相关的现金流。
            effect_of_currency_exchange_rate Float64 CODEC(ZSTD(3)),    --  Impact of foreign exchange rate changes on cash and cash equivalents denominated in foreign currencies.
            other_cash_adjustments Float64 CODEC(ZSTD(3)),          -- 其他未归类于其他类别的现金流量杂项调整。
            change_in_cash_and_equivalents Float64 CODEC(ZSTD(3)), -- Net change in cash and cash equivalents during the period, representing the sum of operating, investing, and financing cash flows plus currency effects.

            -- 系统级控制
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
        "net_income": {"type": "float64", "default": 0.0},
        "depreciation_depletion_and_amortization": {"type": "float64", "default": 0.0},
        "change_in_other_operating_assets_and_liabilities_net": {
            "type": "float64",
            "default": 0.0,
        },
        "cash_from_operating_activities_continuing_operations": {
            "type": "float64",
            "default": 0.0,
        },
        "net_cash_from_operating_activities": {"type": "float64", "default": 0.0},
        "net_cash_from_operating_activities_discontinued_operations": {
            "type": "float64",
            "default": 0.0,
        },
        "other_operating_activities": {"type": "float64", "default": 0.0},
        "purchase_of_property_plant_and_equipment": {"type": "float64", "default": 0.0},
        "sale_of_property_plant_and_equipment": {"type": "float64", "default": 0.0},
        "net_cash_from_investing_activities": {"type": "float64", "default": 0.0},
        "net_cash_from_investing_activities_continuing_operations": {
            "type": "float64",
            "default": 0.0,
        },
        "net_cash_from_investing_activities_discontinued_operations": {
            "type": "float64",
            "default": 0.0,
        },
        "other_investing_activities": {"type": "float64", "default": 0.0},
        "dividends": {"type": "float64", "default": 0.0},
        "short_term_debt_issuances_repayments": {"type": "float64", "default": 0.0},
        "long_term_debt_issuances_repayments": {"type": "float64", "default": 0.0},
        "net_cash_from_financing_activities": {"type": "float64", "default": 0.0},
        "net_cash_from_financing_activities_continuing_operations": {
            "type": "float64",
            "default": 0.0,
        },
        "net_cash_from_financing_activities_discontinued_operations": {
            "type": "float64",
            "default": 0.0,
        },
        "other_financing_activities": {"type": "float64", "default": 0.0},
        "income_loss_from_discontinued_operations": {"type": "float64", "default": 0.0},
        "noncontrolling_interests": {"type": "float64", "default": 0.0},
        "effect_of_currency_exchange_rate": {"type": "float64", "default": 0.0},
        "other_cash_adjustments": {"type": "float64", "default": 0.0},
        "change_in_cash_and_equivalents": {"type": "float64", "default": 0.0},
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
