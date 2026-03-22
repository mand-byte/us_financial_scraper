import os
import sys
from loguru import logger
from dotenv import load_dotenv
from src.config.settings import settings

load_dotenv()

CONSOLE_LEVEL = settings.logging.console_log_level
FILE_LEVEL = settings.logging.file_log_level


def setup_logger():
    """
    初始化并配置全局 Logger
    """
    # 1. 动态获取项目根目录 (假设 logger.py 在 src/utils/ 目录下)
    # 向上推三层：logger.py -> utils -> src -> 项目根目录
    base_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    log_dir = os.path.join(base_dir, "logs")

    # 如果 logs 文件夹不存在，则自动创建
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 2. 清除 loguru 默认的配置（防止重复打印）
    logger.remove()

    # 3. 添加控制台输出 (Stdout)
    # 特性：带有颜色高亮，默认只输出 INFO 及以上级别的信息，保持控制台干净
    logger.add(
        sys.stdout,
        level=CONSOLE_LEVEL,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        enqueue=True,  # 线程安全，多线程环境下保证日志不乱序
    )

    # 4. 添加文件输出（常规日志）
    # 常规日志文件：记录 FILE_LEVEL 及以上（默认 DEBUG）
    app_log_file = os.path.join(log_dir, "quant_app_{time:YYYY-MM-DD}.log")
    logger.add(
        app_log_file,
        level=FILE_LEVEL,
        filter=lambda record: record["level"].name not in {"ERROR", "CRITICAL"},
        rotation="00:00",
        retention="30 days",
        compression="zip",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,
    )

    # 5. 添加文件输出（错误日志）
    error_log_file = os.path.join(log_dir, "quant_error_{time:YYYY-MM-DD}.log")
    logger.add(
        error_log_file,
        level="ERROR",
        rotation="00:00",
        retention="30 days",
        compression="zip",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,
    )

    return logger


# 暴露出配置好的 logger 实例
app_logger = setup_logger()
