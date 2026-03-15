MARKET_DATA_TABLES = {
        # 1. 股票宇宙表 (Universe)
        "us_stock_universe": """
        CREATE TABLE IF NOT EXISTS us_stock_universe
            (
                ticker String,
                composite_figi String,
                name String,
                cik String,
                active UInt8,
                delisted_date  Nullable(Date),
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (composite_figi)
        """,
        # 2. us ticker K线表
        "us_minutes_klines":"""
            CREATE TABLE IF NOT EXISTS us_minutes_klines
            (
                composite_figi String,
                timestamp DateTime64(3, 'UTC'),
                open Float32,
                high Float32,
                low Float32,
                close Float32,
                vwap Float32,       -- 【新增】分钟内加权平均持仓成本
                trades_count UInt32,  -- 【新增】总成交笔数 (Massive 接口中的 'n')
                volume UInt64
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (composite_figi, timestamp)
            SETTINGS index_granularity = 8192
        """,
        # 3 个股纯财务基本面表 (Fundamentals)
        "us_stock_fundamentals":"""
            CREATE TABLE IF NOT EXISTS us_stock_fundamentals
            (
                cik String,
                publish_timestamp DateTime64(3, 'UTC'),  -- 财报实际披露时间 (PIT)
                period_end Date,                         -- 财报对应的自然季末 (如 2024-03-31)
                eps Float32,
                revenue_growth_yoy Float32,
                net_income_growth_yoy Float32,
                roe Float32,
                free_cash_flow Float32,
                debt_to_equity Float32,
                current_ratio Float32,
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (cik, publish_timestamp)
        """,
        # 4 机构持仓变动表 (Institutional Holdings - 13F)
        "us_stock_inst_holdings":"""
            CREATE TABLE IF NOT EXISTS us_stock_inst_holdings
            (
                cik String,
                publish_timestamp DateTime64(3, 'UTC'),  -- 13F文件的实际披露时间 (PIT)
                period_end Date,                         -- 13F对应的自然季末
                inst_hold_pct Float32,                   -- 机构总持仓比例
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (cik, publish_timestamp)
        """,
        # 5 内部人士持仓变动表 (Insider Holdings - Form 4)
        "us_stock_insider_holdings":"""
            CREATE TABLE IF NOT EXISTS us_stock_insider_holdings
            (
                cik String,
                publish_timestamp DateTime64(3, 'UTC'),  -- Form 4 文件的实际披露时间 (PIT)
                insider_hold_pct Float32,                -- 内部人士总持仓比例
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (cik, publish_timestamp)
        """,
        # 6 基准 ETF K线表 (SPY, QQQ, IWM 等)与us ticker K线表对齐
            "us_benchmark_etf_klines":"""
            CREATE TABLE IF NOT EXISTS us_benchmark_etf_klines
                (
                    ticker LowCardinality(String),  -- ETF极少退市或改名，且数量极少，用 LowCardinality(ticker) 性能最优
                    timestamp DateTime64(3, 'UTC'),
                    open Float32,
                    high Float32,
                    low Float32,
                    close Float32,
                    vwap Float32,
                    trades_count UInt32,
                    volume UInt64
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (ticker, timestamp)
                SETTINGS index_granularity = 8192
            """,
        # 7 日线 K线表 (包含yahoo的10年期美债收益率，美元指数。。及cobe的官方日k线数据)
        "us_macro_daily_klines":"""
            CREATE TABLE IF NOT EXISTS us_macro_daily_klines
            (
                ticker LowCardinality(String),  -- 'US10Y', 'DXY', 'GOLD', VIX' (现货), 'VX1' (近月), 'VX2' (次月) 等
                trade_date Date,
                open Float32,
                high Float32,
                low Float32,
                close Float32,
                volume UInt64,                  --  Yahoo的宏观指标存入时自动补0
                open_interest UInt64            -- 【核心】未平仓合约数，判断机构做空/做多恐慌盘真实资金量的关键, Yahoo数据为0, 只有VX期货有真实数值
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (ticker, trade_date)
        """,
        # 8 个股公司行动表
        "us_stock_actions": """
        CREATE TABLE IF NOT EXISTS us_stock_actions
            (
                composite_figi String,
                ex_date Date,
                action_type Enum8('split' = 1, 'dividend' = 2),
                split_ratio Float32,      -- 例如 1拆4 则为 0.25
                cash_amount Float32       -- 分红金额
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (composite_figi, ex_date, action_type)
        """,
        #9 个股公司财报表，本地提取
        "us_stock_earnings_raw": """
            CREATE TABLE IF NOT EXISTS us_stock_earnings_raw
            (
                cik String,
                publish_timestamp DateTime64(3, 'UTC'),
                period_end Date,                         -- 财报截止日
                mda_txt String CODEC(ZSTD(3)),          -- 存储 MD&A 或 管理层发言
                risk_qa_txt String CODEC(ZSTD(3)),      -- 存储 风险因素 或 Q&A
                next_quarter_revenue_low Float32,        -- 财报原文中提取的客观下限
                next_quarter_revenue_high Float32,       -- 财报原文中提取的客观上限
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (cik, publish_timestamp)
        """,
            # 10 个股新闻原始表，存储从新闻源抓取的未经处理的新闻文本与元信息，供后续LLM打分与情绪分析使用
            "us_stock_news_raw": """
            CREATE TABLE IF NOT EXISTS us_stock_news_raw
            (
                news_id String,                          -- 建议用 URL 或 (标题+时间) 的 MD5 哈希
                composite_figi String,
                publish_timestamp DateTime64(3, 'UTC'),
                title String CODEC(ZSTD(3)),             -- 长文本开启 ZSTD 高级压缩
                description String CODEC(ZSTD(3)),
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (composite_figi, publish_timestamp, news_id)
        """,
            # 11. 宏观经济指标表 (Macro)
            "us_macro_indicators": """
                CREATE TABLE IF NOT EXISTS us_macro_indicators
                (
                    publish_timestamp DateTime64(3, 'UTC'),
                    indicator_code String,
                    actual_value Float32,
                    expected_value Nullable(Float32),
                    surprise_diff Float32 MATERIALIZED (actual_value - ifNull(expected_value, actual_value))
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (indicator_code, publish_timestamp)
            """,
            # 12. 全局地缘与系统性风险表 (GDELT - 聚合模式)
            "gdelt_macro_sentiment": """
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
            """,
            # 13.个股新闻情绪张量表
            "us_stock_news_sentiment": """
            CREATE TABLE IF NOT EXISTS us_stock_news_sentiment
            (
                composite_figi String,                   -- 冗余存储，避免 JOIN
                publish_timestamp DateTime64(3, 'UTC'),  -- 冗余存储，用于 RL 环境时间轴对齐
                llm_name LowCardinality(String),         -- 如 'gemini-1.5-flash'
                news_id String, 
                sentiment_score Float32,
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            -- 排序键加入 llm_name，完美支持对同一条新闻用不同模型打分并存
            ORDER BY (composite_figi,news_id, publish_timestamp, llm_name)
        """,
        # 14. 个股财报定性分析表
        "us_stock_earnings_sentiment": """
            CREATE TABLE IF NOT EXISTS us_stock_earnings_sentiment
            (
                cik String,
                publish_timestamp DateTime64(3, 'UTC'),  -- 冗余时间戳，用于时间轴对齐
                period_end Date,                         -- 冗余截止日
                llm_name LowCardinality(String),         -- 区分打分模型版本
                
                -- LLM 提取的定性评价
                guidance_vs_consensus Int8,              -- 映射: 1=beat, 0=inline, -1=miss, 99=none
                management_tone Int8,                    -- 映射: 2=optimistic, 1=neutral, -1=cautious, -2=negative
                main_risks Array(String),
                growth_drivers Array(String),
                key_highlights String CODEC(ZSTD(3)),    -- 摘要可能有几百字，也上个压缩
                
                -- LLM 给出的量化情感得分
                overall_score Float32,                   -- 区间: -1.0 到 1.0
                revenue_sentiment Int8,
                eps_sentiment Int8,
                red_flags Array(String),
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            -- 排序键加入 llm_name，支持多模型并发对比验证
            ORDER BY (cik, publish_timestamp, llm_name)
        """,
        # 15.us_stock_daily_ratios_factors
        "us_stock_daily_ratios_factors": """
            CREATE TABLE IF NOT EXISTS us_stock_daily_ratios_factors
            (
                composite_figi String,
                trade_date Date,                         -- 对齐 K 线的交易日
                
                -- 1. 估值与市值因子 (提取自 /v1/ratios)
                market_cap Float64,                      -- 总市值 (数字极大，必须用 Float64)
                enterprise_value Float64,                -- 企业价值 (EV)
                pe_ratio Float32,                        -- 动态市盈率 (Price to Earnings)
                pb_ratio Float32,                        -- 市净率 (Price to Book)
                ps_ratio Float32,                        -- 市销率 (Price to Sales)
                ev_to_ebitda Float32,                    -- EV/EBITDA 倍数
                dividend_yield Float32,                  -- 股息率
                
                
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (composite_figi, trade_date)
        """,
        "us_stock_daily_short_interest_factors": """
            CREATE TABLE IF NOT EXISTS us_stock_daily_short_interest_factors
            (
                composite_figi String,
                trade_date Date,                         -- 对齐 K 线的交易日
                
                -- 2. 微观博弈与情绪因子 (提取自 /v1/short-interest)
   
                short_interest UInt64,                   -- 当前未平仓的做空总股数
                days_to_cover Float32,                   -- 空头回补天数 (极其危险的逼空指标)
                
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (composite_figi, trade_date)
        """,
        "us_stock_daily_short_volume_factors": """
            CREATE TABLE IF NOT EXISTS us_stock_daily_short_volume_factors
            (
                composite_figi String,
                trade_date Date,                         -- 对齐 K 线的交易日
                -- 2. 微观博弈与情绪因子 (提取自 /v1/short-volume & /short-interest)
                short_volume UInt64,                     -- 当日做空成交量
                short_volume_ratio Float32,              -- 做空成交量占比 (如 31.57)
                
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (composite_figi, trade_date)
        """,

        "us_stock_daily_float_factors": """
            CREATE TABLE IF NOT EXISTS us_stock_daily_float_factors
            (
                composite_figi String,
                trade_date Date,                         -- 对齐 K 线的交易日
                
                -- 3. 流动性与筹码结构 (提取自 /vX/float)
                free_float UInt64,                       -- 自由流通股本
                free_float_percent Float32,              -- 流通盘占比 (如 98.5)
                
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (composite_figi, trade_date)
        """,
}