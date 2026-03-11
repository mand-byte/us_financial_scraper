# src/utils/__init__.py

# 从当前目录 (.) 的 logger.py 文件中引入 app_logger
from .logger import app_logger

# (可选但推荐) 明确指定对外暴露的变量/类/函数
# 这样如果别人使用 from src.utils import *，只会导入 app_logger
__all__ = ['app_logger']