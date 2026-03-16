import signal
import sys
from src.utils.logger import app_logger
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from dotenv import load_dotenv
from src import (
    MassiveDataFetcher,
    MassiveNewsFetcher,
    FredScraper,
    GDELTScraper,
    CboeDataFetcher,
    YahooMacroScraper,
    ForexFactoryScraper,
)

load_dotenv()
executors = {"default": ThreadPoolExecutor(20)}

job_defaults = {"coalesce": True, "max_instances": 1, "misfire_grace_time": 600}


class ScraperOrchestrator:
    def __init__(self):
        app_logger.info("🏗️ 初始化搜刮调度中心...")
        self.scheduler = BlockingScheduler(
            timezone="US/Eastern", executors=executors, job_defaults=job_defaults
        )
        # 1. 实例化所有搜刮器
        self.scrapers = [
            MassiveDataFetcher(self.scheduler),
            MassiveNewsFetcher(self.scheduler),
            FredScraper(self.scheduler),
            GDELTScraper(self.scheduler),
            YahooMacroScraper(self.scheduler),
            ForexFactoryScraper(self.scheduler),
            CboeDataFetcher(self.scheduler),
        ]

    def start_all(self):
        self.is_running = True
        app_logger.info("🚀 正在并行启动所有后台搜刮任务...")
        for scraper in self.scrapers:
            scraper.start()

    def stop_all(self):
        app_logger.info("🛑 正在尝试优雅停止所有搜刮器，请稍候...")
        self.is_running = False
        for scraper in self.scrapers:
            scraper.stop()
        app_logger.info("👋 所有后台任务已结束，服务退出。")

    def run_forever(self):
        self.start_all()

        # 注册信号处理 (处理 Ctrl+C)
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

        app_logger.info("📊 调度中心进入运行监控状态。按下 Ctrl+C 退出。")
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.handle_exit()

    def handle_exit(self, sig=None, frame=None):
        if self.is_running:
            self.is_running = False
            self.stop_all()
            self.scheduler.shutdown(wait=False)
            sys.exit(0)


if __name__ == "__main__":
    orchestrator = ScraperOrchestrator()
    orchestrator.run_forever()
