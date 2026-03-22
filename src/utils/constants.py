"""
宏观经济指标 (Macro Indicators) 映射常量定义
采用类似 C# 静态类的结构组织，统一维护所有数据源到内部标准化编码的转换关系。
"""

Fred_Indicator_Code = {
    # --- 1. 流动性与利率 (核心定价) ---
    'DFF': 'FED_RATE_DECISION',         # 有效联邦基金利率 (日)
    'T10Y2Y': 'YIELD_CURVE_SPREAD',     # 期限利差: 衰退预警 (日)
    
    # --- 2. 货币供应 (新增) ---
    'WM2NS': 'M2_SUPPLY_WEEKLY',        # M2 供应量：流动性总量 (周)
    
    # --- 3. 核心通胀趋势 (新增) ---
    'CPILFESL': 'CORE_CPI_LEVEL',       # 核心 CPI 指数：长期价格压力 (月)
    
    # --- 4. 信用风险 (压力测试) ---
    'BAMLH0A0HYM2': 'HY_OAS_SPREAD'     # 高收益债信用利差 (日)
}

Yahoo_Indicator_Code = {
    # --- 1. 流动性与定价锚 (连续交易的市场定价) ---
    '^TNX': 'US10Y',                  # 10年期美债收益率
    
    # --- 5. 汇率与大宗商品 (慢变量) ---
    'DX-Y.NYB': 'DXY',                # 美元指数
    'CL=F': 'WTI_CRUDE',              # WTI 原油
    'GC=F': 'GOLD',                   # 现货黄金
    
    # --- 6. 恐慌与波动率现货 (Yahoo 支持极好) ---
    '^VIX': 'VIX_SPOT', # VIX 现货指数 (核心恐慌基准)
    '^VVIX': 'VVIX',                  # VIX的VIX (衡量波动率的波动率，预判极端黑天鹅的利器)
    '^VIX3M': 'VIX3M',                # 3个月期 VIX (中短期预期)
    '^VIX6M': 'VIX6M',                # 6个月期 VIX (中长期预期)
    '^MOVE': 'MOVE_BOND',             # ICE BofA 美债波动率 (债市的VIX，注意不是美股期限结构)
}
CBOE_Indicator_Code = {
    # --- 波动率现货 (无成交量与持仓量) ---
    
    # --- 波动率期货连续合约 (包含 Volume 与 Open Interest) ---
    'VX1': 'VX1',       # 近月连续合约 (Front-Month)
    'VX2': 'VX2',       # 次月连续合约 (Next-Month)
}
# ForexFactory 映射 (基于事件标题 Title)
# EventID 实际上会随具体实例(每月)变更，因此针对历史数据回溯回使用 Currency + Title 进行精确匹配。
ForexFactory_Indicator_Title_Map = {
    # --- 1. 流动性事件 ---
    'Federal Funds Rate': 'FED_RATE_DECISION',
    
    # --- 2. 通胀与消费事件 ---
    'CPI m/m': 'US_CPI_MOM',
    'CPI y/y': 'US_CPI_YOY',
    'Core CPI m/m': 'US_CORE_CPI_MOM',
    'Core PCE Price Index m/m': 'US_CORE_PCE_MOM',
    'PPI m/m': 'US_PPI_MOM',
    'Core PPI m/m': 'US_CORE_PPI_MOM',
    'Retail Sales m/m': 'US_RETAIL_SALES',
    'Core Retail Sales m/m': 'US_CORE_RETAIL_SALES',
    
    # --- 3. 就业与景气度事件 ---
    'Non-Farm Employment Change': 'US_NFP',
    'Unemployment Rate': 'US_UNEMPLOYMENT_RATE',
    'Average Hourly Earnings m/m': 'US_AVG_EARNINGS',
    'Unemployment Claims': 'US_INITIAL_CLAIMS',
    'ISM Manufacturing PMI': 'US_ISM_MFG_PMI',
    'ISM Services PMI': 'US_ISM_SERVICES_PMI',
    
    # --- 4. 其它关键事件 ---
    'ADP Non-Farm Employment Change': 'US_ADP_NFP',
    'CB Consumer Confidence': 'US_CONS_CONFIDENCE',
}

