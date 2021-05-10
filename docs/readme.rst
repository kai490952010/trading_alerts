Trading alerts
==============

This project intends to combine varying sources of OHLC financial data
to automate trading signals and make it easier to watch and react to
traded financial assets.

Currently, crypto markets are being focused on. Soon, the intention is
to pull fiat markets from different regions as well.


Learnings
---------

- Building dynamic graphs
- Custom postgres data upload hook


Features
--------

-  Automates data collection using Airflow
-  Internally, depends on Binance's S3 sources for historical data
-  Pulls incremental data using Binance's APIs.
-  Metadata about coin development and interest pulled from Coingecko
-  Idempotent pipeline based on Airflow
-  Postgres DB as backend
