import time
import signal
import sys
from src.utils.logger import app_logger
from dotenv import load_dotenv
from src import (
    FredScraper, 
    GDELTScraper, 
    CboeDataFetcher, 
    YahooMacroScraper,
    ForexFactoryScraper
)
load_dotenv()
class ScraperOrchestrator:
    def __init__(self):
        app_logger.info("🏗️ 初始化搜刮调度中心...")
        
        # 1. 实例化所有搜刮器
        self.scrapers = {
            "FRED (宏观流动性)": FredScraper(),
            "GDELT (全球风险)": GDELTScraper(),
            "Yahoo-Macro (大宗/汇率)": YahooMacroScraper(),
            "ForexFactory (经济日历)": ForexFactoryScraper(),
            "cboe (VIX 数据)": CboeDataFetcher()
        }
        self.is_running = True

    def start_all(self):
        app_logger.info("🚀 正在并行启动所有后台搜刮任务...")
        for name, scraper in self.scrapers.items():
            try:
                scraper.start()
                app_logger.info(f"✅ {name} 已激活")
            except Exception as e:
                app_logger.error(f"❌ {name} 启动失败: {str(e)}")

    def stop_all(self):
        app_logger.info("🛑 正在尝试优雅停止所有搜刮器，请稍候...")
        self.is_running = False
        for name, scraper in self.scrapers.items():
            try:
                scraper.stop()
                app_logger.info(f"🔌 {name} 已安全断开")
            except Exception as e:
                app_logger.error(f"⚠️ {name} 停止时发生异常: {str(e)}")
        app_logger.info("👋 所有后台任务已结束，服务退出。")

    def run_forever(self):
        self.start_all()
        
        # 注册信号处理 (处理 Ctrl+C)
        signal.signal(signal.SIGINT, lambda sig, frame: self.handle_exit())
        signal.signal(signal.SIGTERM, lambda sig, frame: self.handle_exit())

        app_logger.info("📊 调度中心进入运行监控状态。按下 Ctrl+C 退出。")
        
        try:
            while self.is_running:
                # 每 5 分钟打印一次简单的健康检查（你可以根据需要增加更详细的状态逻辑）
                # 这里目前主要保持主线程不退出
                time.sleep(300)
                app_logger.debug("💓 调度中心健康检查: 所有子线程正在后台运行中...")
        except (KeyboardInterrupt, SystemExit):
            self.handle_exit()

    def handle_exit(self):
        if self.is_running:
            self.stop_all()
            sys.exit(0)

if __name__ == "__main__":
    orchestrator = ScraperOrchestrator()
    orchestrator.run_forever()
