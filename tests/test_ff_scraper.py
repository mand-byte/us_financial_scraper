import pandas as pd
from datetime import datetime
import pytz
from src.forexfactory_scraper.scraper import scrape_month
from src.forexfactory_economic_calendar_scraper import ForexFactoryScraper
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)

def test_layer_logic():
    # 1. 测试底层接口 (scrape_month)
    print("🧪 测试底层月度接口: scrape_month('mar', 2024)...")
    df_raw = scrape_month('mar', 2024)
    
    if df_raw.empty:
        print("❌ 底层接口抓取失败。")
        return
    
    print(f"✅ 底层接口成功抓取到 {len(df_raw)} 条指标。")

    # 2. 测试业务层逻辑 (ForexFactoryScraper)
    scraper_service = ForexFactoryScraper()
    
    # 手动模拟业务层的过滤 (假设我们要抓取 3月10日-3月15日)
    start_dt = datetime(2024, 3, 10, tzinfo=pytz.UTC)
    end_dt = datetime(2024, 3, 15, tzinfo=pytz.UTC)
    
    print(f"\n🧪 测试业务层处理逻辑 ({start_dt.date()} -> {end_dt.date()})...")
    
    # 业务层进行范围过滤
    df_raw['DateTime'] = pd.to_datetime(df_raw['DateTime'], utc=True)
    mask = (df_raw['DateTime'] >= start_dt) & (df_raw['DateTime'] <= end_dt)
    df_filtered = df_raw[mask]
    
    print(f"📊 范围内共有 {len(df_filtered)} 条原始数据。")

    # 业务层进行 EventID 匹配和清洗
    df_final = scraper_service.process_scraped_data(df_filtered)
    
    if not df_final.empty:
        print("\n" + "="*60)
        print("✅ 业务层清洗后的标准化数据:")
        print("="*60)
        print(df_final.to_string(index=False))
        print(f"\n🎯 最终入库指标数: {len(df_final)}")
    else:
        print("❌ 业务层处理后无有效指标（请确认 constants.py 中 ID 是否覆盖了 2024-03-10 到 03-15 的关键指标）。")

if __name__ == "__main__":
    test_layer_logic()
