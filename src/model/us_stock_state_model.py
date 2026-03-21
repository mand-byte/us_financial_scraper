# -*- coding: utf-8 -*-
from pydantic import Field
from typing import Optional, Union, List
from datetime import datetime
import pandas as pd
from src.model.base_clickhouse_model import BaseClickHouseModel

class UsStockStateModel(BaseClickHouseModel):
    """
    Generic model for all _state tables (klines, fundamentals, etc.)
    Identifier is either 'cik' or 'composite_figi' depending on the table.
    """
    identifier: str = Field(...)  # Will be mapped to 'cik' or 'composite_figi'
    state: int = Field(default=0)
    update_time: Optional[datetime] = Field(default=None)

    @classmethod
    def format_dataframe(cls, ids: Union[str, List[str]], id_column: str, state: int = 1) -> pd.DataFrame:
        """
        Standardize state update into a DataFrame.
        id_column: Name of the ID column ('cik' or 'composite_figi')
        """
        if isinstance(ids, (str, bytes)):
            ids = [ids]
        elif not isinstance(ids, (list, tuple, pd.Series)):
             ids = [ids]
            
        data = []
        for val in ids:
            cleaned_val = val.decode("utf-8") if isinstance(val, bytes) else str(val)
            data.append({
                id_column: cleaned_val,
                'state': state,
                'update_time': datetime.now()
            })
            
        return pd.DataFrame(data)
