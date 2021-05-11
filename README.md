[![Actions Status](https://github.com/kai490952010/trading_alerts/workflows/Build/badge.svg)](https://github.com/kai490952010/trading_alerts/actions)

Trading alerts
==============

This project intends to combine varying sources of OHLC financial data
to automate trading signals and make it easier to watch and react to
traded financial assets.

Currently, crypto markets are being focused on. Soon, the intention is
to pull fiat markets from different regions as well.



Installation
------------

Run `pipenv install` using existing file.

To setup metadata database:
1. Create Postgres metadata database
```
CREATE DATABASE airflow;
```
2. Create airflow user:
```
CREATE USER <username> WITH PASSWORD '<password>';
GRANT ALL PRIVILEGES ON DATABASE <database> TO <username>;
```
3. `cd trading_alerts/trading_alerts`
4. `export AIRFLOW_HOME=$(pwd)`
5. Run: `airflow db init` to create the base config (which needs sqlite only)
6. Now change sql_alchemy_conn variable to a postgres db connection string, for which
connection parameters were created above.



Features
--------

- Uses Binance API to poll for historical data
- Metadata about coin development and interest pulled from Coingecko
- Idempotent pipeline based on Airflow
- Postgres DB as backend

* TODO


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
