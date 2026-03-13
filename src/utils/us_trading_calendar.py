import pandas_market_calendars as mcal
import pandas as pd

def get_trading_calendar(start_date='2014-01-01', end_date='2025-12-31'):
    # 获取纽交所日历 (美股通用)
    nyse = mcal.get_calendar('NYSE')
    
    # schedule 会自动返回每一天的 UTC 准确开收盘时间
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    
    # 构造符合你 ClickHouse 表结构的 DataFrame
    df_calendar = pd.DataFrame({
        'trade_date': schedule.index.date,
        'market_open_utc': schedule['market_open'].dt.tz_convert('UTC').dt.tz_localize(None),
        'market_close_utc': schedule['market_close'].dt.tz_convert('UTC').dt.tz_localize(None),
    })
    
    # 判断提前闭市：正常交易时间是 6.5 小时。如果少于 6 小时即为提前闭市 (如感恩节次日)
    duration = schedule['market_close'] - schedule['market_open']
    df_calendar['is_early_close'] = (duration < pd.Timedelta(hours=6)).astype(int)
    
    return df_calendar