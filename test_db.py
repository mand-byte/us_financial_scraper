from src.dao.clickhouse_manager import get_db_manager

db = get_db_manager()
df = db.query_dataframe("SELECT ticker, primary_exchange, market, locale FROM us_stock_universe WHERE length(primary_exchange) > 0 LIMIT 10")
print(df.to_dict('records'))
