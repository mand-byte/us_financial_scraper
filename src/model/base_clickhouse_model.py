import pandas as pd
from typing import Dict, Type, ClassVar

class BaseClickHouseModel:
    """自动化 ClickHouse 模型基类 (已弃用 Pydantic)"""

    _registry: ClassVar[Dict[str, Type['BaseClickHouseModel']]] = {}
    table_name: ClassVar[str] = ""
    __DDL__: ClassVar[str] = ""
    __abstract__: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.table_name and not getattr(cls, "__abstract__", False):
            cls._registry[cls.table_name] = cls

    @classmethod
    def get_create_table_sql(cls) -> str:
        """统一获取建表 DDL"""
        if not cls.__DDL__:
            raise ValueError(f"❌ Model [{cls.__name__}] 缺失 __DDL__ 定义")
        return cls.__DDL__.strip()
