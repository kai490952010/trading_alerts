Trading alerts
==============

This project intends to combine varying sources of OHLC financial data
to automate trading signals and make it easier to watch and react to
traded financial assets.

Currently, crypto markets are being focused on. Soon, the intention is
to pull fiat markets from different regions as well.

Installation
------------

Clone repo and run ``pipenv install``.
This will install the necessary dependencies.

To setup metadata database:
1. Create Postgres metadata database

::

    CREATE DATABASE airflow;

2. Create airflow user:

   ::

       CREATE USER USERNAME WITH PASSWORD '<password>';
       GRANT ALL PRIVILEGES ON DATABASE <database> TO USERNAME;

3. ``export AIRFLOW\_HOME=$(pwd)``
4. Run: ``airflow db init`` to create the base config (which needs
   sqlite only)
5. Now change sql\_alchemy\_conn variable to a postgres db connection
   string, for which connection parameters were created above.

Features
--------

-  Automates data collection using Airflow
-  Internally, depends on Binance's S3 sources for historical data
-  Pulls incremental data using Binance's APIs.
-  Metadata about coin development and interest pulled from Coingecko
-  Idempotent pipeline based on Airflow
-  Postgres DB as backend

Credits
-------

This package was created with Cookiecutter\_ and the
``audreyr/cookiecutter-pypackage``\ \_ project template.

.. *Cookiecutter: https://github.com/audreyr/cookiecutter ..
*\ ``audreyr/cookiecutter-pypackage``:
https://github.com/audreyr/cookiecutter-pypackage
