==================
OHLC data pipeline
==================






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
CREATE USER USERNAME WITH PASSWORD '<password>';
GRANT ALL PRIVILEGES ON DATABASE <database> TO USERNAME;
```
3. export AIRFLOW_HOME=$(pwd)
4. Run: `airflow db init` to create the base config (which needs sqlite only)
5. Now change sql_alchemy_conn variable to a postgres db connection string, for which
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
