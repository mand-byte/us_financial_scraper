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
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    log_dir = os.path.join(base_dir, 'logs')
    
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
        enqueue=True  # 线程安全，多线程环境下保证日志不乱序
    )

    # 4. 添加文件输出
    # 特性：记录 DEBUG 及以上所有细节，按天分割，自动压缩，保留 30 天
    log_file_path = os.path.join(log_dir, "quant_bot_{time:YYYY-MM-DD}.log")
    logger.add(
        log_file_path,
        level=FILE_LEVEL,         # 文件里记录更详细的 DEBUG 信息以便排错
        rotation="00:00",         # 每天午夜 00:00 自动分割文件
        retention="30 days",      # 历史日志最多保留 30 天，自动清理过期文件
        compression="zip",        # 分割后的历史日志自动压缩为 zip，极大地节省服务器空间
        encoding="utf-8",         # 避免中文字符乱码
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True
    )
    
    return logger

# 暴露出配置好的 logger 实例
app_logger = setup_logger()