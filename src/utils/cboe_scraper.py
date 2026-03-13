import requests
import pandas as pd
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import io
from src.utils import *
# CBOE月份代码映射
MONTH_CODES = {
    1:'F',2:'G',3:'H',4:'J',5:'K',6:'M',
    7:'N',8:'Q',9:'U',10:'V',11:'X',12:'Z'
}


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0'})
    return s
def get_vx_expiry(year:int, month:int)->date:

    if month==12:
        next_month_first=date(year+1,1,1)
    else:
        next_month_first=date(year,month+1,1)

    # 找下月第三个周五
    days_to_friday = (4 - next_month_first.weekday()) % 7
    first_friday = next_month_first + timedelta(days=days_to_friday)
    third_friday = first_friday + timedelta(weeks=2)

    # 往前30天，再找最近的周三（往前找是对的，但要确认方向）
    target = third_friday - timedelta(days=30)
    # 往后找最近的周三
    while target.weekday() != 2:
        target += timedelta(days=1)
    return target


def fetch_cboe_vx_contract(year:int,month:int)->pd.DataFrame:

    month_code=MONTH_CODES[month]
    year2=str(year)[-2:]

    if year<2013:
        url=f"https://cdn.cboe.com/resources/futures/archive/volume-and-price/CFE_{month_code}{year2}_VX.csv"
    else:
        expiry=get_vx_expiry(year,month)
        url=f"https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{year}-{month:02d}-{expiry.day:02d}.csv"

    try:

        r=get_session().get(url,timeout=10)

        if r.status_code!=200:
            return pd.DataFrame()

        df=pd.read_csv(io.StringIO(r.text))

        df.columns=df.columns.str.strip().str.lower()

        if 'futures' in df.columns:
            df=df[df['futures'].str.contains(month_code,na=False)]

        col_map={
            'trade date':'trade_date',
            'date':'trade_date',
            'open':'open',
            'high':'high',
            'low':'low',
            'close':'close',
            'volume':'volume',
            'total volume':'volume',
            'open interest':'open_interest'
        }

        df=df.rename(columns=col_map)

        needed=['trade_date','open','high','low','close','volume','open_interest']

        for c in needed:
            if c not in df:
                df[c]=0

        df=df[needed]

        df['trade_date']=pd.to_datetime(df['trade_date']).dt.date

        df=df[df['close']>0]

        df['expiry']=get_vx_expiry(year,month)
        return df

    except Exception as e:
        app_logger.warning(f"cboe scraper {year}-{month} error {e}")
        return pd.DataFrame()


def build_vx_continuous(start_date:date,end_date:date)->pd.DataFrame:

    contracts=[]

    cur=date(start_date.year,start_date.month,1)
    end=date(end_date.year,end_date.month,1)+relativedelta(months=2)

    while cur<=end:

        df=fetch_cboe_vx_contract(cur.year,cur.month)

        if not df.empty:
            contracts.append(df)

        cur+=relativedelta(months=1)

    if not contracts:
        return pd.DataFrame()

    raw=pd.concat(contracts)

    raw=raw.sort_values(['trade_date','expiry'])

    rows=[]

    for d,g in raw.groupby('trade_date'):

        if d<start_date or d>end_date:
            continue

        active=g[g['expiry']>=d].head(2)

        for i,r in enumerate(active.itertuples(),1):

            rows.append({
                'symbol':f'VX{i}',
                'trade_date':d,
                'expiry':r.expiry,
                'open':float(r.open),
                'high':float(r.high),
                'low':float(r.low),
                'close':float(r.close),
                'volume':int(r.volume),
                'open_interest':int(float(r.open_interest or 0))
            })

    return pd.DataFrame(rows)


def fetch_vx_incremental(last_db_date:date):

    start=last_db_date+timedelta(days=1)
    end=date.today()

    if start>end:
        app_logger.info("cboe scraper  already latest")
        return pd.DataFrame()

    return build_vx_continuous(start,end)


# ── 使用示例 ──────────────────────────────────────────
if __name__ == "__main__":
    # 全量拉取近10年
    df = build_vx_continuous(
        start_date=date(2015, 1, 1),
        end_date=date.today()
    )
    print(df.head(10))
    print(f"\n总行数: {len(df)}")
    print(df[df['symbol'] == 'VX1'].tail())