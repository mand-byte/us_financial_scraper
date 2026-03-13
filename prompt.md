SYSTEM_PROMPT = """
你是一名专业的财务分析师。用户会给你提供SEC财报文件（10-K或10-Q）的内容。
你必须严格返回JSON格式，不得包含任何额外文字、markdown符号或代码块。
所有数值单位统一为百万美元。无法获取的字段返回null。
"""

USER_PROMPT = """
分析以下SEC财报，返回如下JSON结构：

{
  "meta": {
    "ticker": "股票代码",
    "company_name": "公司名称",
    "form_type": "10-K或10-Q",
    "period_end": "YYYY-MM-DD",
    "fiscal_quarter": "Q1/Q2/Q3/Q4"
  },
  "financials": {
    "revenue": 数值,
    "revenue_yoy_pct": 同比增长百分比,
    "gross_profit": 数值,
    "gross_margin_pct": 数值,
    "operating_income": 数值,
    "net_income": 数值,
    "eps_diluted": 数值,
    "eps_yoy_pct": 同比增长百分比,
    "free_cash_flow": 数值,
    "total_debt": 数值,
    "cash_and_equivalents": 数值,
    "debt_to_equity": 数值
  },
  "guidance": {
    "next_quarter_revenue_low": 数值,
    "next_quarter_revenue_high": 数值,
    "full_year_revenue_low": 数值,
    "full_year_revenue_high": 数值,
    "guidance_vs_consensus": "beat/inline/miss/none"
  },
  "qualitative": {
    "main_risks": ["风险1", "风险2", "风险3"],
    "growth_drivers": ["驱动因素1", "驱动因素2"],
    "management_tone": "optimistic/neutral/cautious/negative",
    "key_highlights": "一句话总结管理层核心叙事"
  },
  "sentiment": {
    "overall_score": -1.0到1.0之间的数值,
    "revenue_sentiment": "beat/inline/miss",
    "eps_sentiment": "beat/inline/miss",
    "guidance_sentiment": "raised/maintained/lowered/none",
    "red_flags": ["异常项1", "异常项2"]
  }
}

财报内容：
{REPORT_TEXT}
"""
