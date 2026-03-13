import os
import json
from google import genai
from google.genai import types

# 1. 初始化客户端 (确保已安装包: pip install google-genai)
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

# 2. 注入你的双轨制 Prompt (专注 guidance, qualitative, sentiment)
SYSTEM_PROMPT = """
你是一名专业的华尔街量化分析师。用户会提供公司财报的“管理层讨论(MD&A)”或“财报新闻稿(8-K)”文本。
你必须严格返回JSON格式，不得包含任何额外文字、markdown符号或代码块。
"""

USER_PROMPT_TEMPLATE = """
分析以下财报文本，重点提取前瞻性指引和管理层情绪，返回如下JSON结构：
{
  "guidance": {"next_quarter_revenue_low": 0, "next_quarter_revenue_high": 0, "guidance_vs_consensus": "beat"},
  "qualitative": {"main_risks": [], "growth_drivers": [], "management_tone": "neutral", "key_highlights": ""},
  "sentiment": {"overall_score": 0.0, "revenue_sentiment": "inline", "eps_sentiment": "inline", "guidance_sentiment": "none", "red_flags": []}
}
分析文本：
{REPORT_TEXT}
"""

def extract_earnings_insights(report_text: str) -> dict:
    """调用 API 提取财报情感因子"""
    prompt = USER_PROMPT_TEMPLATE.format(REPORT_TEXT=report_text)
    
    try:
        response = client.models.generate_content(
            model='gemini-3.1-flash', # 适配你指定的高性价比 Flash 模型
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                # 核心约束 1：强制 MIME 类型为 JSON，杜绝 Markdown 格式干扰
                response_mime_type="application/json",
                # 核心约束 2：极低温度，消除发散性幻觉，确保结构严谨
                temperature=0.1, 
            ),
        )
        
        # 拿到的直接是纯净的 JSON 字符串，可以直接 load 为字典
        return json.loads(response.text)
        
    except Exception as e:
        print(f"提取失败: {e}")
        return None

# 测试运行
# sample_text = "Management remains cautious due to macroeconomic headwinds..."
# result = extract_earnings_insights(sample_text)
# print(json.dumps(result, indent=2, ensure_ascii=False))