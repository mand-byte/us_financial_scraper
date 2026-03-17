# US Financial Scraper (美国财经全维度数据引擎)

这是一个为强化学习（PPO）交易模型设计的全维度美股数据抓取系统。它不仅负责数据的搬运，更核心的价值在于其**针对量化特征工程的数据对齐逻辑**与**多源异构数据的冷热启动方案**。

---

## 🏗️ 数据获取策略 (Data Lifecycle)

系统将数据分为“历史冷启动”与“实盘增量”两个阶段，确保回测覆盖 10 年以上维度，实盘保持分钟级同步。

| 数据维度 | 冷启动方案 (10年+) | 增量/实盘方案 | 核心价值 |
| :--- | :--- | :--- | :--- |
| **K线行情** | Massive.com (含退市标的)REST | Massive.com REST+WSS | 确保回测无“幸存者偏差” |
| **个股财报** | Massive.com  | Massive.com + SEC | 彻底杜绝未来函数 |
| **宏观指标** | FRED / YAHOO /ForexFactory | FRED / YAHOO /ForexFactory  | 捕捉宏观“估值地心引力” |
| **新闻情绪** | Massive / Tiigon | yfinance rss | 区分“技术回调”与“基本面暴雷” |
| **地缘政治** | GDELT (1979-Now) | GDELT (15min Sync) | 捕捉黑天鹅事件波动 |

---

## 🧠 核心金融逻辑：教 AI 理解市场

本项目不仅是爬虫，更集成了针对 PPO 模型的特征解释逻辑：

### 1. 信用与风险预警 (The Parachute)
- **核心指标**: `HY_OAS_SPREAD` (垃圾债利差)。
- **AI 学习目标**: 当利差异常飙升，AI 需识别流动性枯竭信号，强制执行 **无脑清仓** 或 **反手做空**，避开系统性股灾。

### 2. 估值锚点与地心引力 (The Anchor)
- **核心指标**: `US10Y` (10年期美债收益率)、`DXY` (美元指数)。
- **AI 学习目标**: 
    - `US10Y` 飙升时，AI 应自动调低高市盈率科技股权重（杀估值）。
    - `DXY`走强时，AI 应从跨国巨头切换至营收在本土的 **罗素2000** 小盘股，规避汇兑损失。

### 3. 行业对冲与避风港 (Safe Haven)
- **核心指标**: `CL=F` (原油)、`GC=F` (黄金)。
- **AI 学习目标**: 
    - 油价暴涨时，AI 自动执行 **多能源/空航空** 的对冲策略。
    - 当“大盘跌 + 黄金涨 + VIX涨”三者共振，AI 需确认黑天鹅爆发并触发防御。

---

## 🛠️ 技术实现与架构

### 1. 实时性保障
- **SEC EDGAR RSS**: 直接订阅证监会官方 Feed，比媒体新闻快 1-5 分钟。
- **yfinance News**: 免费获取个股实时头条。

### 2. 存储方案 (ClickHouse)
采用列式存储，针对 `stock_news_sentiment`（新闻情绪）与 `us_macro_daily_kline`（宏观日线）进行优化。
- **去重逻辑**: 使用 `ReplacingMergeTree` 引擎，允许增量搬运中的重叠覆盖，自动保持最新状态。

### 3. 环境配置
```bash
# 依赖管理
uv sync

# 关键配置项 (.env)
MASSIVE_API_KEY=xxx     # 用于历史K线及退市股冷启动
SCRAPING_START_DATE=2014-01-01
```

---

## 📂 模块索引
- `fred_scraper.py`: 搜刮信贷危机与利差数据。
- `yahoo_finance_macro_scraper.py`: 搜刮 DXY, WTI, GOLD, US10Y。
- `gdelt_scraper.py`: 搜刮全球冲突与制裁事件。
- `forexfactory_economic_calendar_scraper.py`: 抓取预期差 (Surprise Diff)。
