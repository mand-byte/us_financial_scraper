from src.dao.clickhouse_manager import get_db_manager


def main() -> None:
    db = get_db_manager()
    df = db.query_dataframe(
        "SELECT ticker, primary_exchange, market, locale "
        "FROM us_stock_universe "
        "WHERE length(primary_exchange) > 0 LIMIT 10"
    )
    print(df.to_dict("records"))


if __name__ == "__main__":
    main()
