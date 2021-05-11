CREATE_TICKER_DATA_TABLE = """
    CREATE TABLE IF NOT EXISTS tickerdata_1m (
        open numeric(10, 9),
        high numeric(10, 9),
        low numeric(10, 9),
        close numeric(10, 9),
        -- date_id bigint,
        -- time_id bigint,
        symbol varchar(50),
        exchange_id int,
        market_id int
        close_timestamp bigint
    )
"""

CREATE_DATE_DIM = """
    CREATE TABLE IF NOT EXISTS date_dim(
        date_id bigint,
        date_str varchar(10),
        weekday varchar(15),
        day_of_week int,
        week_of_year int,
        year int
    )
"""

CREATE_TIME_DIM = """
    CREATE TABLE IF NOT EXISTS time_dim(
        time_id bigint,
        time_str varchar(10),
        hour int,
        minute int
    )
"""

CREATE_EXCHANGE_DIM = """
    CREATE TABLE IF NOT EXISTS exchange_dim (
        exchange_id integer not null,
        exchange_name character varying(50),
        market_id int references market_dim(market_id),
        exchange_created_at timestamp without time zone,
        primary key (exchange_id)
    )
"""

CREATE_MARKET_DIM = """
    CREATE TABLE IF NOT EXISTS market_dim (
        market_id integer not null,
        market_name character varying(50) not null,
        market_created_at timestamp without time zone not null,
        primary key (market_id)
    )
"""

CREATE_SYMBOL_TABLE = """
    CREATE TABLE IF NOT EXISTS symbols (
        symbol_id SERIAL,
        symbol_name varchar(50)  not null,
        symbol_created_at timestamp not null,
        symbol_last_updated_at timestamp not null,
        exchange_id int references exchange_dim(exchange_id),
        primary key (symbol_id)
    )
"""

DELETE_DATA_WHERE = """
    DELETE FROM {:s}
    WHERE {where_clause}
"""

GET_MIN_MAX_TS = """
    SELECT MIN()
"""
