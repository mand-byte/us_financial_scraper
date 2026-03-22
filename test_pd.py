import pandas as pd
df = pd.DataFrame([{"transaction_date": None}])
df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").fillna(pd.to_datetime("1970-01-01")).dt.date
print(df["transaction_date"].tolist())
