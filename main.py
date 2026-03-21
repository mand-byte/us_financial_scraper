import signal
from importlib import import_module

from src.utils.logger import app_logger
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from dotenv import load_dotenv

SCRAPER_SPECS = [
    ("src.massive_kline_scraper", "MassiveKlineScraper"),
    # ("src.massive_benchmark_scraper", "MassiveBenchmarkScraper"),
    #("src.massive_news_scraper", "MassiveNewsScraper"),
    # ("src.massive_actions_scraper", "MassiveActionsScraper"),
    # ("src.fred_scraper", "FredScraper"),
    # ("src.gdelt_scraper", "GDELTScraper"),
    # ("src.yahoo_finance_macro_scraper", "YahooMacroScraper"),
    # ("src.forex_factory_scraper", "ForexFactoryScraper"),
    # ("src.cboe_scraper", "CboeScraper"),
    # ("src.sec_edgar_scraper", "SecEdgarScraper"),
]


def _load_scraper_class(module_name: str, class_name: str):
    module = import_module(module_name)
    return getattr(module, class_name)

load_dotenv()
executors = {"default": ThreadPoolExecutor(20)}

job_defaults = {"coalesce": True, "max_instances": 1, "misfire_grace_time": 600}


class ScraperOrchestrator:
    def __init__(self):
        app_logger.info("🏗️ 初始化搜刮调度中心...")
        self.scheduler = BlockingScheduler(
            timezone="US/Eastern", executors=executors, job_defaults=job_defaults
        )
        self.scrapers = []
        for module_name, class_name in SCRAPER_SPECS:
            try:
                scraper_cls = _load_scraper_class(module_name, class_name)
                self.scrapers.append(scraper_cls(self.scheduler))
            except Exception as exc:
                app_logger.warning(f"跳过搜刮器 {class_name}: {exc}")

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
        import os
        if self.is_running:
            self.is_running = False
            try:
                self.stop_all()
                self.scheduler.shutdown(wait=False)
            except Exception as e:
                app_logger.error(f"关闭调度器报错: {e}")
            # 使用 os._exit(0) 强制越过 ThreadPoolExecutor 的 atexit 钩子
            # 否则若有爬虫线程正在进行耗时请求，Python 将一直阻塞等待它们完成
            os._exit(0)


if __name__ == "__main__":
    orchestrator = ScraperOrchestrator()
    orchestrator.run_forever()
