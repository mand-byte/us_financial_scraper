from .fred_scraper import FredScraper
from .gdelt_scraper import GDELTScraper
from .utils.cboe_scraper import CboeDataFetcher
from .yahoo_finance_macro_scraper import YahooMacroScraper
from .forex_factory_scraper import ForexFactoryScraper
from .massive_scraper import MassiveDataFetcher
from .massive_fundamentals_scraper import MassiveFundamentalsScraper
from .massive_benchmark_scraper import MassiveBenchmarkScraper
from .massive_news_scraper import MassiveNewsScraper
from .massive_financial_factor_scraper import MassiveFinancialFactorScraper
from .massive_actions_scraper import MassiveActionsScraper
from .sec_edgar_scraper import SecEdgarScraper

__all__ = [
    "FredScraper",
    "GDELTScraper",
    "CboeDataFetcher",
    "YahooMacroScraper",
    "ForexFactoryScraper",
    "MassiveDataFetcher",
    "MassiveFundamentalsScraper",
    "MassiveBenchmarkScraper",
    "MassiveNewsScraper",
    "MassiveFinancialFactorScraper",
    "MassiveActionsScraper",
    "SecEdgarScraper",
]
