import re
import os
import logging
import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime
from bs4 import BeautifulSoup
from curl_cffi import requests

logger = logging.getLogger(__name__)
_SAVE_DEBUG_HTML = os.getenv("FOREXFACTORY_SAVE_DEBUG_HTML", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _get_session():
    """创建一个模拟 Chrome 的 curl_cffi 会话"""
    session = requests.Session(impersonate="chrome110")
    # 设置时区为 UTC (0)
    session.cookies.set("ffsetting_timezone", "0", domain=".forexfactory.com")
    return session


def scrape_month(month_str, year_int):
    """
    底层接口：仅负责抓取指定月份的所有原始数据
    :param month_str: 'jan', 'feb' ... 'dec'
    :param year_int: 2024, 2025 ...
    """
    url = f"https://www.forexfactory.com/calendar?month={month_str.lower()}.{year_int}"
    session = _get_session()

    try:
        logger.info(f"🌐 正在请求月度接口: {url}")
        response = session.get(url, timeout=30)
        response.raise_for_status()

        if _SAVE_DEBUG_HTML:
            os.makedirs("logs", exist_ok=True)
            with open(
                "logs/forexfactory_last_page_debug.html", "w", encoding="utf-8"
            ) as f:
                f.write(response.text)

        return _parse_html(response.text, year_int)
    except Exception as e:
        logger.error(f"❌ 抓取月份 {month_str}.{year_int} 失败: {str(e)}")
        return pd.DataFrame()


def _parse_html(html_content, year):
    """使用 BeautifulSoup 解析 HTML，返回该月所有货币的所有指标"""
    soup = BeautifulSoup(html_content, "lxml")
    rows = soup.find_all("tr", class_="calendar__row")

    day_data = []
    current_date_str = None
    current_time_str = None

    for row in rows:
        classes = row.get("class", [])

        # 1. 处理日期分隔行
        if "calendar__row--day-breaker" in classes:
            cell = row.find("td", class_="calendar__cell")
            if cell:
                span = cell.find("span")
                if span:
                    current_date_str = span.get_text(strip=True)
            continue

        # 2. 处理数据行
        event_id = row.get("data-event-id")
        if "calendar__row--no-event" in classes:
            continue

        try:
            # 提取时间
            time_el = row.find("td", class_="calendar__time")
            if time_el:
                t_text = time_el.get_text(strip=True).lower()
                if t_text:
                    current_time_str = t_text

            # 提取货币
            curr_el = row.find("td", class_="calendar__currency")
            currency = curr_el.get_text(strip=True) if curr_el else ""

            # 提取指标名称
            event_title_el = row.find("span", class_="calendar__event-title")
            title = event_title_el.get_text(strip=True) if event_title_el else ""
            if not title:
                continue

            # 提取实际值与预测值
            actual_el = row.find("td", class_="calendar__actual")
            actual = actual_el.get_text(strip=True) if actual_el else ""

            forecast_el = row.find("td", class_="calendar__forecast")
            forecast = forecast_el.get_text(strip=True) if forecast_el else ""

            # 组合 UTC 时间
            dt = None
            if current_date_str:
                try:
                    base_dt = datetime.strptime(
                        f"{current_date_str} {year}", "%b %d %Y"
                    )
                    dt = base_dt.replace(tzinfo=ZoneInfo("UTC"))
                    if current_time_str:
                        if "am" in current_time_str or "pm" in current_time_str:
                            t_match = re.search(
                                r"(\d{1,2}):(\d{2})(am|pm)", current_time_str
                            )
                            if t_match:
                                hh, mm, ampm = (
                                    int(t_match.group(1)),
                                    int(t_match.group(2)),
                                    t_match.group(3),
                                )
                                if ampm == "pm" and hh < 12:
                                    hh += 12
                                if ampm == "am" and hh == 12:
                                    hh = 0
                                dt = dt.replace(hour=hh, minute=mm)
                        elif (
                            "all day" in current_time_str
                            or "tentative" in current_time_str
                        ):
                            dt = dt.replace(hour=0, minute=0)
                except Exception:
                    pass

            day_data.append(
                {
                    "DateTime": dt,
                    "Currency": currency,
                    "Actual": actual,
                    "Forecast": forecast,
                    "Title": title,
                    "EventID": event_id,
                }
            )
        except Exception:
            continue

    return pd.DataFrame(day_data)
