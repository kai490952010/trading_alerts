[![Actions Status](https://github.com/kai490952010/trading_alerts/workflows/Build/badge.svg)](https://github.com/kai490952010/trading_alerts/actions)

[![Documentation Status](https://readthedocs.org/projects/trading-alerts/badge/?version=latest)](https://trading-alerts.readthedocs.io/en/latest/?badge=latest)

Trading alerts
==============

This project intends to combine varying sources of OHLC financial data
to provide a data-rich pipeline to automate various trading needs. One
of the intended outcomes is to provide realtime alerts indicating best
time to buy or sell a symbol (stock or crypto coin pair like
bitcoin-usd).

As a minimum viable first version, this project intends to ingest
cryptocurrency data with an Airflow pipeline and make it available via a
basic (most likely Plotly) dashboard and Telegram alerts. The dashboard
will aid exploring the coins and adding alerts to them. In this version,
I intend to make RSI, TD-sequential and resistance/support line meteics
available.

In a future version, I wish to add more indicators as I explore other
trading strategies. This project is intended for self use but will be
made available to small group of friends.

Why this project
----------------

Many such tools exist in the market which provide real time updates for
a select set of traded signals. Majority of them are either too
expensive for an invdividual user (above 100\$ per month) or are limited
to a limited set of free indicators. Both these factors were quite
limiting for my use.

### Learnings

**1. Building dynamic graphs**

Dynamic graphs are useful when your pipeline depends on varying number
of inputs which cannot be fixed while designing the pipeline. For
example - a sensor that monitors for a new key in a S3 bucket and hands
of the processing to a new DAG once it finds that new file has been
created. The use case for this project is discussed in more detail in
the Internals\_ section.

Airflow provides a way to launch dynamic graphs via 3 operators
-TriggerDagRunOperator, SubDagOperator, ExternalSensor operator. I chose
to create dynamic graphs using the SubDagOperator because of the
following reasons:

The SubDagOperator creates a hierarchial relationship between parent and
child-dags and makes them available on UI via a drill down feature. So
this not only allows you to glance at the parent dag status but also
lets you "zoom-in" into the status of children dags if you want to. Not
only this, but to use SubDagOperator you need a dag factory (a function
or a file, which when executed returns a independent DAG) - this sort of
brings it together for me.

TriggerDagRunOperator is ideal when you want to run a DAG independent of
parent DAG once a condition is met and state history should be shared
between the dags. These dags are not grouped under a single view in the
UI.

The other alternative is ExternalSensor. It is a special operator whose
function is to keep polling for a condition till its met or if it times
out. These tend to occupy a worker till the conditoin turns true or
operator times out.

So keeping all these things in mind, it was a better alternative to use
SubDagOperator than the other two.

**2. Historical and incremental DAGs**

Inititally, I had thought of patiently polling the Binance API to
collect the data. But I was skeptical of this approach because I needed
minutely data for at least 200 crypto symbols and I was sure I was going
to be banned within a day or two. (api limits to 1200 reqs per minute,
with varying weightages of each API call)

Then I chanced upon historical archives of crypto data at 1m frequencies
via <https://data.binance.vision>. I was actually hoping that maybe
there would an unauthenticated API, which would let me collect data
(safely). But things turned out better because after struggling through
the HTML code I saw requests to S3 buckets. I was able to then take the
bucket name from the "XHR" request and use the AWS cli commands that we
used in pset-4, 5 to list the bucket contents. Turns out I could access
the bucket without using AWS credentials! (yay.)

This is where I broke down the data collection architecture to use
historical and incremental dags. Historical dags would collect data for
new symbols for which no data wasn't collected earlier and incremental
dags would come back every few minutes to top up the minute-wise data
for coins that already had (using the Binance API). I was able to
collect 1m interval data for about 5 coin pairs this way, which came
around to 7Million rows of data.

**3. Extending data upload in Postgres hooks**

Airflow 2.0 does not have a data write functionality in its Postgres
hooks yet. But, its possible to have this by grabbing the base class's
connection URI and passing it to a pandas write statement. Having
created as custom class around the PostgresHook and placing it in the
"plugins" folder, let's you use it in any dag that may need it.

**4. Postgres as a bookmark and a datastore**

I use Postgres as the backend for Airflow pipeline. In this database, a
symbol table keeps track of all symbols for which data extraction is
going on. The last\_updated\_at columns is updated whenever a historical
or incremental dag pulls data for the symbol. By keeping this column
updated, the DAGs are able to calculate how many iterations of pulling
day-wise data would be required to keep under the API throttle limits.
Once this pipeline matures in usage, it would be easy to migrate it to
Amazon's Redshift database (as it is build on Postgres) with least
changes as compared to other databases like MySQL or MongoDB.

**5. Idempotency**

Airflow is an orchestration framework which does not provide options for
idempotent operations for database or otherwise, out of the box. It is
upto the end developer to do this. I have used delete SQL statements to
remove data from the database before inserting into it. Architecturally,
we only write to one table which holds data at 1m granularity, so the
table would ideally need partitioning based on closing timestamp and
symbol name. This way it would be possible to drop a partition and
recreate it again - which as far as I've read is a better alternative
than deleting select rows.

### Features

-   Automates data collection using Airflow. Internally, depends on
    Binance and Coingecko APIs
-   Idempotent pipeline based on Airflow Postgres DB as backend
-   Can scale by separating historical and incremental DAGs.
-   Metadata about coin development pulled from Coingecko
