from importlib import import_module

_EXPORT_MAP = {
    "BaseClickHouseModel": ("src.model.base_clickhouse_model", "BaseClickHouseModel"),
    "BenchmarkEtfKlineModel": (
        "src.model.us_benchmark_etf_kline_model",
        "BenchmarkEtfKlineModel",
    ),

    "UsStockStateModel": ("src.model.us_stock_state_model", "UsStockStateModel"),

    "GdeltMacroSentimentModel": (
        "src.model.gdelt_macro_sentiment_model",
        "GdeltMacroSentimentModel",
    ),
    "GdeltMacroSentimentStateModel": (
        "src.model.gdelt_macro_sentiment_state_model",
        "GdeltMacroSentimentStateModel",
    ),
    "UsMacroDailyKlineModel": (
        "src.model.us_macro_daily_kline_model",
        "UsMacroDailyKlineModel",
    ),
    "UsMacroIndicatorsModel": (
        "src.model.us_macro_indicators_model",
        "UsMacroIndicatorsModel",
    ),
   
    "UsStockFigiTickerMappingModel": (
        "src.model.us_stock_figi_ticker_mapping_model",
        "UsStockFigiTickerMappingModel",
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
