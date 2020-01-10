class BaseConfig:
    class DBConnection:
        db_provider = 'sqlite'  # can be any of 'mysql', 'postgres', 'sqlserver', sqlite
        location = 'database.db'  # can also be an ip for other providers: location = 'localhost:3306' for mysql for instance
        username = None
        password = None
        db_name = None

    class DBNames:
        exchange = 'exchanges'
        ohlc = 'ohlc'  # table name for hourly data
        trades = 'trades'
        currency = 'currency'

    class Exchanges:
        class Kraken:
            url = 'https://www.kraken.com'
            sleep_time_between_requests = 1
            ms_ts = True
            recover_from = 1420081200 * (10 ** 9)  # 01/01/2015 00:00 in nanoseconds

        class Buda:
            url = 'https://www.buda.com/chile'
            ms_ts = False
            recover_from = 123
