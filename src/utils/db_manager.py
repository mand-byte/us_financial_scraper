import clickhouse_connect
import pandas as pd
import os
from dotenv import load_dotenv
from urllib.parse import urlparse
from src.utils.logger import app_logger

load_dotenv()

class ClickHouseManager:
    def __init__(self, host=None, port=None, username=None, password=None, database=None):
        # 优先读取用户提供的环境变量
        url_str = os.getenv('CLICKHOST_URL', 'http://localhost:8123')
        parsed_url = urlparse(url_str)
        
        self.host = host or parsed_url.hostname or 'localhost'
        self.port = int(port or parsed_url.port or 8123)
        self.username = username or os.getenv('CLICKHOST_USERNAME', 'default')
        self.password = password or os.getenv('CLICKHOST_PASSWORD', '')
        self.database = database or os.getenv('CLICKHOST_DATABASE', 'quant_data')
        
        # 建立连接
        self.client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password
        )
        
        # 确保数据库存在
        self.client.command(f'CREATE DATABASE IF NOT EXISTS {self.database}')
        self.client.command(f'USE {self.database}')
        
        # 初始化表
        self._create_tables()

    def _create_tables(self):
        """完全匹配用户提供的所有 DDL (K线, Universe, 基本面, 情绪, 宏观, GDELT)"""
        # 1. 股票宇宙表 (Universe)
        self.client.command("""
            CREATE TABLE IF NOT EXISTS us_stock_universe
            (
                ticker String,
                composite_figi String,
                name String,
                type String,
                active UInt8,
                list_date Date,
                delisted_date Date,
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (ticker, composite_figi)
        """)

        # 2. 1分钟 K线表 (K-lines)
        self.client.command("""
            CREATE TABLE IF NOT EXISTS us_stock_1min
            (
                composite_figi String,
                timestamp DateTime64(3, 'UTC'),
                open Float32,
                high Float32,
                low Float32,
                close Float32,
                volume UInt64
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (composite_figi, timestamp)
            SETTINGS index_granularity = 8192
        """)

        # 3. 个股基本面与持仓表 (Fundamentals)
        self.client.command("""
            CREATE TABLE IF NOT EXISTS stock_fundamentals
            (
                composite_figi String,
                publish_timestamp DateTime64(3, 'UTC'),
                eps Float32,
                pe_ratio Float32,
                revenue_growth_yoy Float32,
                net_income_growth_yoy Float32,
                roe Float32,
                free_cash_flow Float32,
                debt_to_equity Float32,
                current_ratio Float32,
                insider_hold_pct Float32,
                inst_hold_pct Float32
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (composite_figi, publish_timestamp)
        """)

        # 4. 个股新闻与情绪张量表 (Sentiment)
        self.client.command("""
            CREATE TABLE IF NOT EXISTS stock_news_sentiment
            (
                composite_figi String,
                publish_timestamp DateTime64(3, 'UTC'),
                news_id String,
                sentiment_score Float32,
                llm_version String,
                event_category String
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (composite_figi, publish_timestamp, news_id)
        """)

        # 5. 宏观经济指标表 (Macro)
        self.client.command("""
            CREATE TABLE IF NOT EXISTS macro_indicators
            (
                publish_timestamp DateTime64(3, 'UTC'),
                indicator_code String,
                actual_value Float32,
                expected_value Nullable(Float32),
                surprise_diff Float32 MATERIALIZED (actual_value - assumeNotNull(expected_value, actual_value))
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (indicator_code, publish_timestamp)
        """)

        # 6. 全局地缘与系统性风险表 (GDELT - 聚合模式)
        self.client.command("""
            CREATE TABLE IF NOT EXISTS gdelt_macro_sentiment
            (
                publish_timestamp DateTime64(3, 'UTC'),

                -- Code 16 (经济制裁/禁运)
                count_16 Int32,
                tone_16 Float32,
                impact_16 Float32,

                -- Code 17 (资产没收/充公)
                count_17 Int32,
                tone_17 Float32,
                impact_17 Float32,

                -- Code 18 (物理袭击)
                count_18 Int32,
                tone_18 Float32,
                impact_18 Float32,

                -- Code 19 (军事冲突/战争)
                count_19 Int32,
                tone_19 Float32,
                impact_19 Float32,

                -- Code 20 (极端暴力/核恐吓)
                count_20 Int32,
                tone_20 Float32,
                impact_20 Float32
            ) ENGINE = ReplacingMergeTree()
            ORDER BY publish_timestamp
        """)
        app_logger.info("✅ 所有 DRL 核心数据表结构初始化/检查完毕。")

    def save_universe(self, df: pd.DataFrame):
        """保存或更新股票池"""
        if df.empty:
            return
        
        cols = ['ticker', 'composite_figi', 'name', 'type', 'active', 'list_date', 'delisted_date']
        df_to_save = df[cols].copy()
        df_to_save['active'] = df_to_save['active'].astype(int)
        
        self.client.insert_df('us_stock_universe', df_to_save)
        app_logger.info(f"📥 成功同步 {len(df_to_save)} 只标的至 us_stock_universe。")

    def save_klines(self, df: pd.DataFrame, composite_figi: str):
        """保存 K线数据，仅保留 FIGI 作为关联键"""
        if df.empty:
            return
        
        df_to_save = df.copy()
        df_to_save['composite_figi'] = composite_figi
        
        # 确保 volume 是整数类型
        df_to_save['volume'] = df_to_save['volume'].astype('uint64')
        
        # 映射 DDL 字段顺序
        cols = ['composite_figi', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
        df_to_save = df_to_save[cols]
        
        self.client.insert_df('us_stock_1min', df_to_save)
        app_logger.info(f"📈 成功同步 {len(df_to_save)} 条 ({composite_figi}) 的 K线数据。")

    def save_macro(self, df: pd.DataFrame):
        """保存宏观指标数据"""
        if df.empty:
            return
        
        # 严格匹配 DDL 字段顺序
        cols = ['publish_timestamp', 'indicator_code', 'actual_value', 'expected_value']
        df_to_save = df[cols].copy()
        
        self.client.insert_df('macro_indicators', df_to_save)
        app_logger.info(f"🌍 成功同步 {len(df_to_save)} 条宏观指标记录。")

    def close(self):
        self.client.close()

if __name__ == "__main__":
    try:
        db = ClickHouseManager()
        app_logger.info("🚀 ClickHouse 连接测试成功！")
        db.close()
    except Exception as e:
        app_logger.error(f"❌ ClickHouse 连接失败: {str(e)}")
