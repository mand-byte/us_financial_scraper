from importlib import import_module

_EXPORT_MAP = {
    "BaseClickHouseModel": ("src.model.base_clickhouse_model", "BaseClickHouseModel"),
    "BenchmarkEtfKlineModel": (
        "src.model.us_benchmark_etf_kline_model",
        "BenchmarkEtfKlineModel",
    ),
    "SecForm10KModel": ("src.model.sec_form10k_model", "SecForm10KModel"),
    "SecForm10QModel": ("src.model.sec_form10q_model", "SecForm10QModel"),
    "SecForm13FModel": ("src.model.sec_form13f_model", "SecForm13FModel"),
    "SecForm4Model": ("src.model.sec_form4_model", "SecForm4Model"),
    "SecForm8KModel": ("src.model.sec_form8k_model", "SecForm8KModel"),
    "SecSC13DModel": ("src.model.sec_sc13d_model", "SecSC13DModel"),
    "UsBenchmarkEtfKlineModel": (
        "src.model.us_benchmark_etf_kline_model",
        "BenchmarkEtfKlineModel",
    ),
    "GdeltMacroSentimentModel": (
        "src.model.gdelt_macro_sentiment_model",
        "GdeltMacroSentimentModel",
    ),
    "UsMacroDailyKlineModel": (
        "src.model.us_macro_daily_kline_model",
        "UsMacroDailyKlineModel",
    ),
    "UsMacroIndicatorsModel": (
        "src.model.us_macro_indicators_model",
        "UsMacroIndicatorsModel",
    ),
    "UsStock10kSectionsRawModel": (
        "src.model.us_stock_10k_sections_raw_model",
        "UsStock10kSectionsRawModel",
    ),
    "UsStockBalanceSheetsModel": (
        "src.model.us_stock_balance_sheets_model",
        "UsStockBalanceSheetsModel",
    ),
    "UsStockCashFlowStatementsModel": (
        "src.model.us_stock_cash_flow_statements_model",
        "UsStockCashFlowStatementsModel",
    ),
    "UsStockDividendsModel": (
        "src.model.us_stock_dividends_model",
        "UsStockDividendsModel",
    ),
    "UsStockFigiTickerMappingModel": (
        "src.model.us_stock_figi_ticker_mapping_model",
        "UsStockFigiTickerMappingModel",
    ),
    "UsStockIncomeStatementsModel": (
        "src.model.us_stock_income_statements_model",
        "UsStockIncomeStatementsModel",
    ),
    "UsStockMinutesKlineModel": (
        "src.model.us_stock_minutes_kline_model",
        "UsStockMinutesKlineModel",
    ),
    "UsStockMinutesKlineStateModel": (
        "src.model.us_stock_minutes_kline_state_model",
        "UsStockMinutesKlineStateModel",
    ),
    "UsStockNewsRawModel": ("src.model.us_stock_news_raw_model", "UsStockNewsRawModel"),
    "UsStockRiskFactorsModel": (
        "src.model.us_stock_risk_factors_model",
        "UsStockRiskFactorsModel",
    ),
    "UsStockRiskTaxonomyModel": (
        "src.model.us_stock_risk_taxonomy_model",
        "UsStockRiskTaxonomyModel",
    ),
    "UsStockSplitsModel": ("src.model.us_stock_splits_model", "UsStockSplitsModel"),
    "UsStockUniverseModel": (
        "src.model.us_stock_universe_model",
        "UsStockUniverseModel",
    ),
}


def __getattr__(name):
    if name not in _EXPORT_MAP:
        raise AttributeError(f"module 'src.model' has no attribute {name!r}")

    module_name, attr_name = _EXPORT_MAP[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


__all__ = sorted(_EXPORT_MAP)
