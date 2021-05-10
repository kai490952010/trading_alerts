.. highlight:: shell

============
Installation
============


Development release
-------------------

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
   And run: ``airflow db reset``.

   This should create the necessary metadata database for data collection
   to proceed.


From sources
------------

The sources for Trading alerts can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/kai490952010/trading_alerts

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install


.. _Github repo: https://github.com/kai490952010/trading_alerts
.. _tarball: https://github.com/kai490952010/trading_alerts/tarball/master
