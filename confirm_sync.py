from src.dao.clickhouse_manager import get_db_manager
from src.utils.logger import app_logger

def check():
    db = get_db_manager()
    
    universe_count = db.client.command("SELECT count() FROM us_stock_universe")
    mapping_count = db.client.command("SELECT count() FROM us_stock_figi_ticker_mapping")
    
    app_logger.info("📊 数据库检查结果:")
    app_logger.info(f"  - us_stock_universe 总数: {universe_count}")
    app_logger.info(f"  - us_stock_figi_ticker_mapping 总数: {mapping_count}")
    
    if universe_count > 0:
        sample = db.client.query_df("SELECT ticker, composite_figi, type FROM us_stock_universe LIMIT 5")
        app_logger.info(f"  - 宇宙表采样:\n{sample}")

if __name__ == "__main__":
    check()
