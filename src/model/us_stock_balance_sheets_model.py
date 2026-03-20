from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class UsStockBalanceSheetsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_balance_sheets"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_stock_balance_sheets
        (
            composite_figi FixedString(12) COMMENT '全球投资工具统一识别码(固定12位)',
            timeframe LowCardinality(String) COMMENT '时间框架(quarterly/annual)', -- The reporting period type. Possible values include: quarterly, annual.
            fiscal_year UInt16 COMMENT '财年', --The fiscal year for the reporting period.
            fiscal_quarter UInt8 COMMENT '财季', --The fiscal quarter number (1, 2, 3, or 4) for the reporting period.
            period_end Date COMMENT '财报期末日期',
            filing_date Date COMMENT 'SEC实际披露日期',

            accounts_payable Float64 CODEC(ZSTD(3)), -- Amounts owed to suppliers and vendors for goods and services purchased on credit.
            accrued_and_other_current_liabilities Float64 CODEC(ZSTD(3)), -- Current liabilities not classified elsewhere, including accrued expenses, taxes payable, and other obligations due within one year.
            accumulated_other_comprehensive_income Float64 CODEC(ZSTD(3)), -- Cumulative gains and losses that bypass the income statement, including foreign currency translation adjustments and unrealized gains/losses on securities.
            additional_paid_in_capital Float64 CODEC(ZSTD(3)), -- Amount received from shareholders in excess of the par or stated value of shares issued.
            cash_and_equivalents Float64 CODEC(ZSTD(3)),    -- Cash on hand and short-term, highly liquid investments that are readily convertible to known amounts of cash.
            commitments_and_contingencies Float64 CODEC(ZSTD(3)) --Disclosed amount related to contractual commitments and potential liabilities that may arise from uncertain future events.

            common_stock Float64 CODEC(ZSTD(3)), -- Par or stated value of common shares outstanding representing basic ownership in the company.
            debt_current Float64 CODEC(ZSTD(3)), --Short-term borrowings and the current portion of long-term debt due within one year.
            deferred_revenue_current Float64 CODEC(ZSTD(3)), -- Customer payments received in advance for goods or services to be delivered within one year.
            goodwill Float64 CODEC(ZSTD(3)), -- Intangible asset representing the excess of purchase price over fair value of net assets acquired in business combinations.
            intangible_assets_net Float64 CODEC(ZSTD(3)), -- Intangible assets other than goodwill, including patents, trademarks, and customer relationships, net of accumulated amortization.
            inventories Float64 CODEC(ZSTD(3)), -- Raw materials, work-in-process, and finished goods held for sale in the ordinary course of business.
            long_term_debt_and_capital_lease_obligations Float64 CODEC(ZSTD(3)), -- Long-term borrowings and capital lease obligations with maturities greater than one year.
            noncontrolling_interest Float64 CODEC(ZSTD(3)), -- Equity in consolidated subsidiaries not owned by the parent company, representing minority shareholders' ownership.
            other_assets Float64 CODEC(ZSTD(3)), -- Non-current assets not classified elsewhere, including long-term investments, deferred tax assets, and other long-term assets.
            other_current_assets Float64 CODEC(ZSTD(3)), -- Current assets not classified elsewhere, including prepaid expenses, taxes receivable, and other assets expected to be converted to cash within one year.

            other_equity Float64 CODEC(ZSTD(3)), -- Equity components not classified elsewhere in shareholders' equity.
            other_noncurrent_liabilities Float64 CODEC(ZSTD(3)), -- Non-current liabilities not classified elsewhere, including deferred tax liabilities, pension obligations, and other long-term liabilities.
            preferred_stock Float64 CODEC(ZSTD(3)), -- Par or stated value of preferred shares outstanding with preferential rights over common stock.
            property_plant_equipment_net Float64 CODEC(ZSTD(3)), -- Tangible fixed assets used in operations, reported net of accumulated depreciation.
            receivables Float64 CODEC(ZSTD(3)), -- Amounts owed to the company by customers and other parties, primarily accounts receivable, net of allowances for doubtful accounts.
            retained_earnings_deficit Float64 CODEC(ZSTD(3)), -- Cumulative net income earned by the company less dividends paid to shareholders since inception.
            short_term_investments Float64 CODEC(ZSTD(3)), -- Marketable securities and other investments with maturities of one year or less that are not classified as cash equivalents.
            total_assets Float64 CODEC(ZSTD(3)), -- Sum of all current and non-current assets representing everything the company owns or controls.
            total_current_assets Float64 CODEC(ZSTD(3)), -- Sum of all current assets expected to be converted to cash, sold, or consumed within one year.
            total_current_liabilities Float64 CODEC(ZSTD(3)), -- Sum of all liabilities expected to be settled within one year.
            total_equity Float64 CODEC(ZSTD(3)), -- Sum of all equity components representing shareholders' total ownership interest in the company.
            total_equity_attributable_to_parent Float64 CODEC(ZSTD(3)), -- Total shareholders' equity attributable to the parent company, excluding noncontrolling interests.
            total_liabilities Float64 CODEC(ZSTD(3)), -- Sum of all current and non-current liabilities representing everything the company owes.
            total_liabilities_and_equity Float64 CODEC(ZSTD(3)), -- Sum of total liabilities and total equity, which should equal total assets per the fundamental accounting equation.
            treasury_stock Float64 CODEC(ZSTD(3)), -- Cost of the company's own shares that have been repurchased and are held in treasury, typically reported as a negative value.
            update_time  DateTime64(3, 'UTC') DEFAULT now64(3)
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
        "accounts_payable": {"type": "float64", "default": 0.0},
        "accrued_and_other_current_liabilities": {"type": "float64", "default": 0.0},
        "accumulated_other_comprehensive_income": {"type": "float64", "default": 0.0},
        "additional_paid_in_capital": {"type": "float64", "default": 0.0},
        "cash_and_equivalents": {"type": "float64", "default": 0.0},
        "commitments_and_contingencies": {"type": "float64", "default": 0.0},
        "common_stock": {"type": "float64", "default": 0.0},
        "debt_current": {"type": "float64", "default": 0.0},
        "deferred_revenue_current": {"type": "float64", "default": 0.0},
        "goodwill": {"type": "float64", "default": 0.0},
        "intangible_assets_net": {"type": "float64", "default": 0.0},
        "inventories": {"type": "float64", "default": 0.0},
        "long_term_debt_and_capital_lease_obligations": {
            "type": "float64",
            "default": 0.0,
        },
        "noncontrolling_interest": {"type": "float64", "default": 0.0},
        "other_assets": {"type": "float64", "default": 0.0},
        "other_current_assets": {"type": "float64", "default": 0.0},
        "other_equity": {"type": "float64", "default": 0.0},
        "other_noncurrent_liabilities": {"type": "float64", "default": 0.0},
        "preferred_stock": {"type": "float64", "default": 0.0},
        "property_plant_equipment_net": {"type": "float64", "default": 0.0},
        "receivables": {"type": "float64", "default": 0.0},
        "retained_earnings_deficit": {"type": "float64", "default": 0.0},
        "short_term_investments": {"type": "float64", "default": 0.0},
        "total_assets": {"type": "float64", "default": 0.0},
        "total_current_assets": {"type": "float64", "default": 0.0},
        "total_current_liabilities": {"type": "float64", "default": 0.0},
        "total_equity": {"type": "float64", "default": 0.0},
        "total_equity_attributable_to_parent": {"type": "float64", "default": 0.0},
        "total_liabilities": {"type": "float64", "default": 0.0},
        "total_liabilities_and_equity": {"type": "float64", "default": 0.0},
        "treasury_stock": {"type": "float64", "default": 0.0},
        "update_time": {"type": "date", "tz": "UTC"},
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
