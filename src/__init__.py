from importlib import import_module

_EXPORT_MAP = {
    "CboeScraper": ("src.cboe_scraper", "CboeScraper"),
    "ForexFactoryScraper": ("src.forex_factory_scraper", "ForexFactoryScraper"),
    "FredScraper": ("src.fred_scraper", "FredScraper"),
    "GDELTScraper": ("src.gdelt_scraper", "GDELTScraper"),
    "YahooMacroScraper": ("src.yahoo_finance_macro_scraper", "YahooMacroScraper"),
    "MassiveActionsScraper": ("src.massive_actions_scraper", "MassiveActionsScraper"),
    "MassiveBenchmarkScraper": ("src.massive_benchmark_scraper", "MassiveBenchmarkScraper"),
    "MassiveFundamentalsScraper": ("src.massive_fundamentals_scraper", "MassiveFundamentalsScraper"),
    "MassiveKlineScraper": ("src.massive_kline_scraper", "MassiveKlineScraper"),
    "MassiveNewsScraper": ("src.massive_news_scraper", "MassiveNewsScraper")
}


def __getattr__(name):
    if name not in _EXPORT_MAP:
        raise AttributeError(f"module 'src' has no attribute {name!r}")

    module_name, attr_name = _EXPORT_MAP[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


__all__ = sorted(_EXPORT_MAP)
