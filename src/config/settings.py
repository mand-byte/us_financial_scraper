import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

# Ensure `.env` is available before reading any environment-based settings.
load_dotenv()


def _get_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return default if value is None else value


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class DBSettings:
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_username: str
    clickhouse_password: str
    clickhouse_database: str
    db_operation_retry_attempts: int
    db_retry_backoff_seconds: float
    db_circuit_open_seconds: int
    db_write_fail_exit_threshold: int


@dataclass(frozen=True)
class APISettings:
    massive_api_key: str
    massive_delay: bool
    fred_api_key: str
    openfigi_api_key: Optional[str]
    sec_api_io_key: Optional[str]
    sec_downloader_company: str
    sec_downloader_email: str


@dataclass(frozen=True)
class ScraperSettings:
    scraping_start_date: str
    kline_span: int


@dataclass(frozen=True)
class LoggingSettings:
    console_log_level: str
    file_log_level: str


@dataclass(frozen=True)
class Settings:
    db: DBSettings
    api: APISettings
    scraper: ScraperSettings
    logging: LoggingSettings

    def masked_snapshot(self) -> dict:
        return {
            "db": {
                "host": self.db.clickhouse_host,
                "port": self.db.clickhouse_port,
                "username": self.db.clickhouse_username,
                "password_set": bool(self.db.clickhouse_password),
                "database": self.db.clickhouse_database,
                "retry_attempts": self.db.db_operation_retry_attempts,
                "retry_backoff_seconds": self.db.db_retry_backoff_seconds,
                "circuit_open_seconds": self.db.db_circuit_open_seconds,
                "write_fail_exit_threshold": self.db.db_write_fail_exit_threshold,
            },
            "api": {
                "massive_api_key_set": bool(self.api.massive_api_key),
                "massive_delay": self.api.massive_delay,
                "fred_api_key_set": bool(self.api.fred_api_key),
                "openfigi_api_key_set": bool(self.api.openfigi_api_key),
                "sec_api_io_key_set": bool(self.api.sec_api_io_key),
                "sec_downloader_company": self.api.sec_downloader_company,
                "sec_downloader_email": self.api.sec_downloader_email,
            },
            "scraper": {
                "scraping_start_date": self.scraper.scraping_start_date,
                "kline_span": self.scraper.kline_span,
            },
            "logging": {
                "console_log_level": self.logging.console_log_level,
                "file_log_level": self.logging.file_log_level,
            },
        }


def load_settings() -> Settings:
    return Settings(
        db=DBSettings(
            clickhouse_host=_get_str("CLICKHOST_HOST", "localhost"),
            clickhouse_port=_get_int("CLICKHOST_PORT", 8123),
            clickhouse_username=_get_str("CLICKHOST_USERNAME", "default"),
            clickhouse_password=_get_str("CLICKHOST_PASSWORD", ""),
            clickhouse_database=_get_str("CLICKHOST_DATABASE", "quant_data"),
            db_operation_retry_attempts=max(
                1, _get_int("DB_OPERATION_RETRY_ATTEMPTS", 3)
            ),
            db_retry_backoff_seconds=max(
                0.0, _get_float("DB_RETRY_BACKOFF_SECONDS", 1.0)
            ),
            db_circuit_open_seconds=max(1, _get_int("DB_CIRCUIT_OPEN_SECONDS", 60)),
            db_write_fail_exit_threshold=max(
                1, _get_int("DB_WRITE_FAIL_EXIT_THRESHOLD", 5)
            ),
        ),
        api=APISettings(
            massive_api_key=_get_str("MASSIVE_API_KEY", ""),
            massive_delay=_get_bool("MASSIVE_DELAY", True),
            fred_api_key=_get_str("FRED_API_KEY", ""),
            openfigi_api_key=os.getenv("OPENFIGI_API_KEY"),
            sec_api_io_key=os.getenv("SEC_API_IO_KEY"),
            sec_downloader_company=_get_str(
                "SEC_DOWNLOADER_COMPANY", "QuantResearch"
            ).strip(" \"'“”"),
            sec_downloader_email=_get_str(
                "SEC_DOWNLOADER_EMAIL", "research@example.com"
            ).strip(" \"'“”"),
        ),
        scraper=ScraperSettings(
            scraping_start_date=_get_str("SCRAPING_START_DATE", "2014-01-01"),
            kline_span=max(1, _get_int("KLINE_SPAN", 5)),
        ),
        logging=LoggingSettings(
            console_log_level=_get_str("CONSOLE_LOG_LEVEL", "INFO").upper(),
            file_log_level=_get_str("FILE_LOG_LEVEL", "DEBUG").upper(),
        ),
    )


settings = load_settings()


def reload_settings() -> Settings:
    """Reload settings from current process environment."""
    global settings
    load_dotenv(override=False)
    settings = load_settings()
    return settings
