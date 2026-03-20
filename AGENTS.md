# AGENTS.md - Agentic Coding Guidelines

This file provides guidelines for AI agents working in this codebase.

---

## 1. Build / Lint / Test Commands

### Dependencies
```bash
uv sync                    # Install all dependencies from pyproject.toml
```

### Running Tests
```bash
pytest                     # Run all tests
pytest tests/              # Run tests in specific directory
pytest tests/test_ff_scraper.py -v    # Run specific test file
pytest -k test_layer_logic            # Run single test by name
```

### Linting
```bash
ruff check .               # Lint all files
ruff check src/fred_scraper.py --fix  # Lint and auto-fix
```

---

## 2. Code Style Guidelines

### General Philosophy
- Write clean, readable code with clear intent
- Keep functions focused and small (under 50 lines when possible)
- Use meaningful variable and function names
- Add type hints for all function parameters and return values

### Imports (Order)
1. Standard library (`os`, `time`, `datetime`, `logging`, etc.)
2. Third-party packages (`pandas`, `pytz`, `requests`, `pydantic`, etc.)
3. Local application imports (`from src.dao...`, `from src.model...`, etc.)

Example:
```python
import os
import time
import pandas as pd
import pytz
from datetime import datetime, timedelta
from fredapi import Fred

from src.dao.market_data_repo import MarketDataRepo
from src.model.us_macro_indicators_model import UsMacroIndicatorsModel
from src.utils.logger import app_logger
from src.utils.constants import Fred_Indicator_Code
```

### Naming Conventions
- **Classes**: PascalCase (e.g., `FredScraper`, `MarketDataRepo`)
- **Functions/variables**: snake_case (e.g., `sync_all`, `api_key`, `last_ts`)
- **Constants**: SCREAMING_SNAKE_CASE (e.g., `FRED_Indicator_Code`)
- **Private methods/attributes**: Leading underscore (e.g., `_internal_method`)

### Type Hints
Always use type hints for function signatures:
```python
def sync_all(self) -> None:
    """Synchronize all FRED macro indicators."""

def get_latest_macro_indicators(self, indicator_code: str) -> datetime | None:
    ...
```

### Pydantic Models
Use Pydantic for data validation and schema definition:
```python
from pydantic import BaseModel
from typing import List

class UsMacroIndicatorsModel(BaseModel):
    date: datetime
    indicator_code: str
    actual_value: float | None = None
    expected_value: float | None = None
    publish_timestamp: datetime

    @classmethod
    def get_columns(cls) -> List[str]:
        return list(cls.model_fields.keys())

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize DataFrame to match model columns."""
        if df.empty:
            return df
        cols = cls.get_columns()
        for col in cols:
            if col not in df.columns:
                df[col] = cls.model_fields[col].default
        return df[cols].copy()
```

### Error Handling
Use try/except blocks with specific exception types and logging:
```python
try:
    series = self.fred.get_series(fred_ticker, observation_start=start_date)
    if series.empty:
        continue
    # Process data...
except Exception as e:
    app_logger.error(f"Failed to fetch {fred_ticker}: {e}")
    continue
```

### Logging
Use loguru for structured logging:
```python
from src.utils.logger import app_logger

app_logger.info("🚀 Starting FRED sync...")
app_logger.warning("⚠️ FRED_API_KEY not set, skipping sync.")
app_logger.error(f"❌ Failed to fetch {indicator}: {e}")
```

### Database Operations
- Use the repository pattern (`src/dao/`)
- Always use timezone-aware timestamps (prefer UTC, convert to local when needed)
- Use `ReplacingMergeTree` for deduplication in ClickHouse

### Timezone Handling
Always use explicit timezones:
```python
from zoneinfo import ZoneInfo
import pytz

NYC = ZoneInfo("America/New_York")

# Convert to UTC for storage
df["publish_timestamp"] = pd.to_datetime(df["date"]).apply(
    lambda x: (
        x.replace(hour=17, minute=0, second=0)
        .replace(tzinfo=NYC)
        .astimezone(pytz.UTC)
    )
)
```

### DataFrame Operations
- Use method chaining where possible
- Rename columns with `df.rename(columns={"old": "new"}, inplace=True)`
- Use `inplace=False` by default (explicit is better than implicit)

### Configuration
- Store API keys in `.env` file (never commit secrets)
- Use `os.getenv("VAR_NAME", "default_value")` for optional config
- Required environment variables should raise warnings/errors if missing

---

## 3. Project Structure

```
src/
├── dao/              # Data Access Object (database repositories)
├── model/            # Pydantic models and ClickHouse schemas
├── api/              # External API clients
├── utils/            # Utilities (logger, constants, scrapers)
├── schema/           # SQL DDL definitions
├── fred_scraper.py           # FRED macro data scraper
├── yahoo_finance_macro_scraper.py
├── gdelt_scraper.py
├── forexfactory_economic_calendar_scraper.py
├── massive_scraper.py        # Main market data scraper
└── main.py           # Entry point

tests/
└── test_ff_scraper.py

.env                  # Environment variables (DO NOT COMMIT)
pyproject.toml        # Project dependencies
```

---

## 4. Common Patterns

### Incremental Sync Pattern
```python
def sync_all(self):
    # 1. Get last timestamp from database
    last_ts = self.repo.get_latest_record(internal_code)
    start_date = (
        last_ts.astimezone(self.NYC).strftime("%Y-%m-%d")
        if last_ts
        else self.COLD_START_DATE
    )
    
    # 2. Fetch data from source
    data = self.api.get_data(fred_ticker, observation_start=start_date)
    
    # 3. Filter to only new records
    if last_ts:
        df = df[df["publish_timestamp"] > last_ts]
    
    # 4. Insert to database
    if not df.empty:
        self.repo.insert_records(df)
```

### Scheduler Setup
```python
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler()
scraper = FredScraper(scheduler)
scheduler.add_job(scraper.sync_all, "cron", hour=17, minute=0)
scheduler.start()
```

---

## 5. Testing Guidelines

- Place tests in `tests/` directory
- Use descriptive test function names: `test_layer_logic()`
- Include both unit tests and integration tests
- Test edge cases: empty data, invalid input, network failures

---

## 6. Pre-commit Checklist

Before committing:
1. Run `ruff check .` and fix any issues
2. Run `pytest` to ensure tests pass
3. Verify no secrets in `.env` are committed
4. Check that type hints are complete
