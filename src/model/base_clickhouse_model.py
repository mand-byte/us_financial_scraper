import re
from typing import ClassVar, Dict, Type

class BaseClickHouseModel:
    """自动化 ClickHouse 模型基类 (已弃用 Pydantic)"""

    _registry: ClassVar[Dict[str, Type['BaseClickHouseModel']]] = {}
    _IDENTIFIER_PATTERN: ClassVar[re.Pattern[str]] = re.compile(
        r"^[A-Za-z_][A-Za-z0-9_]*$"
    )
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

    @classmethod
    def sql_literal(cls, value: str) -> str:
        """Safely quote a SQL string literal for simple template-based queries."""
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    @classmethod
    def sql_in_clause(cls, values: list[str]) -> str:
        """Build a SQL IN clause list: 'a','b','c'."""
        if not values:
            return "''"
        return ",".join(cls.sql_literal(v) for v in values)

    @classmethod
    def sql_identifier(cls, value: str) -> str:
        """Validate SQL identifier for table/column placeholders."""
        if not cls._IDENTIFIER_PATTERN.match(value):
            raise ValueError(f"非法 SQL 标识符: {value}")
        return value
