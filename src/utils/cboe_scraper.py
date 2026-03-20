from __future__ import annotations

import pandas as pd

from src.utils.logger import app_logger


def build_vx_continuous(start_date, end_date) -> pd.DataFrame:
    """Fallback placeholder until the real CBOE helper is restored."""
    app_logger.warning(
        "build_vx_continuous helper is not implemented in this checkout; "
        "returning an empty DataFrame so imports remain functional."
    )
    return pd.DataFrame()
