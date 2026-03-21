import os
import sys
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

CONSOLE_LEVEL = os.getenv("CONSOLE_LOG_LEVEL", "INFO").upper()
FILE_LEVEL = os.getenv("FILE_LOG_LEVEL", "DEBUG").upper()


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

    # 4. 添加文件输出
    # 特性：记录 DEBUG 及以上所有细节，双重切割：按大小(100MB)或按时间(每天00:00)
    # 自动归档压缩，保留 30 天
    log_file_path = os.path.join(log_dir, "quant_bot_{time:YYYY-MM-DD}.log")

    def rotation_should_occur(message, file_path):
        from datetime import datetime
        import os

        path_str = str(file_path)
        file_size = os.path.getsize(path_str) if os.path.exists(path_str) else 0
        should_rotate_by_size = file_size >= 100 * 1024 * 1024  # 100 MB
        current_hour = datetime.now().hour
        should_rotate_by_time = current_hour == 0
        return should_rotate_by_size or should_rotate_by_time

    logger.add(
        log_file_path,
        level=FILE_LEVEL,
        rotation=rotation_should_occur,
        retention="30 days",
        compression="zip",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,
    )

    return logger


# 暴露出配置好的 logger 实例
app_logger = setup_logger()
