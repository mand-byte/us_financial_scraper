from dotenv import load_dotenv

load_dotenv()

from apscheduler.schedulers.blocking import BlockingScheduler  # noqa: E402
from src.massive_kline_scraper import MassiveKlineScraper  # noqa: E402

# 创建调度器对象
scheduler = BlockingScheduler()
# 初始化搜刮器
scraper = MassiveKlineScraper(scheduler)

print("🚀 开始执行 load_stock_universe...")
scraper.load_stock_universe()
print("✅ 执行完毕。")
