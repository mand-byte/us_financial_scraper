import logging
from datetime import datetime
from src.forexfactory_scraper.scraper import scrape_month

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def discover_ids(keyword=None, currency='USD', year=None, month_label=None):
    """
    指标发现工具：
    抓取指定月份（默认当月）的所有数据，并根据关键词和货币进行筛选，输出对应的 EventID。
    """
    now = datetime.now()
    target_year = year or now.year
    target_month = month_label or now.strftime('%b').lower()

    print(f"\n🔍 正在发现指标 ID (目标: {target_year}-{target_month}, 货币: {currency}, 关键词: '{keyword or 'ALL'}')...")
    
    # 1. 直接调用底层高性能月度接口
    df = scrape_month(target_month, target_year)
    
    if df.empty:
        print("❌ 未能抓取到数据，请检查网络或是否被拦截。")
        return

    # 2. 过滤货币
    if currency:
        df = df[df['Currency'] == currency.upper()]

    # 3. 过滤关键词
    if keyword:
        df = df[df['Title'].str.contains(keyword, case=False, na=False)]

    if df.empty:
        print(f"❓ 在 {target_month}.{target_year} 的数据中未找到匹配 '{keyword}' 的 {currency} 指标。")
        return

    # 4. 去重并格式化输出
    results = df[['EventID', 'Title']].drop_duplicates(subset=['EventID'])
    
    print("\n" + "="*60)
    print("🎯 匹配到的指标 ID 列表:")
    print("="*60)
    for _, row in results.sort_values('Title').iterrows():
        print(f"ID: {row['EventID']:<10} | 名称: {row['Title']}")
    print("="*60)
    print("💡 提示: 请将需要的 ID 复制到 src/utils/constants.py 的 ForexFactory_Indicator_Code 中。")

if __name__ == "__main__":
    import sys
    
    # 允许从命令行输入关键词，例如: python tools/find_ff_ids.py CPI
    search_key = sys.argv[1] if len(sys.argv) > 1 else None
    
    # 默认寻找本月或上个月的 USD 指标
    # 如果本月刚开始没数据，可以手动改月份测试，如 discover_ids(keyword=search_key, month_label='feb', year=2024)
    discover_ids(keyword=search_key)
