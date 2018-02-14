==========
pg2kinesis
==========

.. image:: https://travis-ci.org/handshake/pg2kinesis.svg?branch=master
    :target: https://travis-ci.org/handshake/pg2kinesis/

pg2kinesis uses `logical decoding
<https://www.postgresql.org/docs/9.4/static/logicaldecoding.html>`_
in Postgres 9.4 or later to capture a consistent, continuous stream of events
from the database and publishes them to an `AWS Kinesis <https://aws.amazon.com/kinesis/>`_
stream in a format of your choosing.

It does this without requiring any changes to your schema like triggers or
"shadow" columns or tables, and has a negligible impact on database performance.
This is done while being extremely fault tolerant. No data loss will be incurred
on any type of underlying system failure including process crashes, network
outages, or ec2 instance failures. However, in these situations there will
likely be records that are sent more than once, so your consumer should be
designed with this in mind.

The fault tolerance comes from guarantees provided by the underlying
technologies and from the "2-phase commit" style of publishing inherent to the
design of the program. Changes are first peeked from the replication slot and
published to Kinesis. Once Kinesis successfully recieves a batch of records, we
advance the `xmin <https://www.postgresql.org/docs/9.4/static/catalog-pg-replication-slots.html>`_
of the slot, thereby telling postgres it is safe to reclaim the space taken by
the WAL. As is always the case with logical replication slots, unacknowledged
data on the slot will consume disk on the database until it is read.

There are other utilities that do similar things, often by injecting a C library
into Postgres to do data transformations in place. Unfortunately these
approaches are not suitable for managed databases like AWS' RDS where support
for various plugins is limited and ultimately determined by the hosting provider.
We specifically created pg2kinesis to make use of logical decoding on
`Amazon's RDS for PostgreSQL <https://aws.amazon.com/rds/postgresql/>`_. Amazon
supports logical decoding with either the `test_decoding <https://www.postgresql.org/docs/9.4/static/test-decoding.html>`_
or `wal2json <https://aws.amazon.com/about-aws/whats-new/2017/07/amazon-rds-for-postgresql-supports-new-minor-versions-9-6-3-and-9-5-7-and-9-4-12-and-9-3-17/>`_
output plugins. This utility takes the output of either plugin, transforms it
based on a formatter you can define, guarantees publishing to a Kinesis stream
in *transaction commit time order* and with a guarantee that *no data will be lost*.

Installation
------------

Prerequisites:

 #. Python 2.7.*
 #. AWS-CLI installed and configured
 #. A PostgreSQL 9.4+ server with logical replication enabled
 #. A Kinesis stream

Install:

 ``pip install pg2kinesis``

Tests
-----

To run tests you will need a clone of the repo and have to install some additional requirements:

 #. ``git clone git@github.com:handshake/pg2kinesis.git``
 #. ``cd pg2kinesis``
 #. ``pip install -r requirements.txt``
 #. ``pytest``


Usage
-----

Run ``pg2kinesis --help`` to get a list of the latest command line options.

By default pg2kinesis attempts to connect to a local postgres instance and
publish to a stream named ``pg2kinesis`` using the AWS credentials in the
environment the utility was invoked in.

On successful start it will query your database for the primary key definitions
of every table in ``--pg-dbname``. This is used to identify the correct column
in the test_decoding output to publish. If a table does not have a primary key
its changes will **NOT** be published unless using wal2json and ``--full-change``.

You have the choice for 3 different textual formats that will be sent to the
kinesis stream:

* ``CSV``: outputs stings to Kinesis that look like::

    0,CDC,<transaction_id (xid)>,<table name>,<dml operation:DELETE|INSERT|UPDATE>,<primary key of row>

* ``CSVPayload``: outputs similar to the above except the 3rd column is now a
  json object representing the change.

  .. code-block::

      0,CDC,{
        "xid": <transaction_id>
        "table": <...>
        "operation: <...>
        "pkey": <...>
      }

* If ``wal2json`` is being used, this can either be the primary key as above or
  the full changed row.

  .. code-block::

      0,CDC,{
        "xid": 30355,
        "change": {
          "kind": "insert",
          "columnnames": ["a", "b"],
          "columntypes": ["int4", "int4"],
          "table": "foo",
          "columnvalues": [1, null],
          "schema": "public"
        }
      }


Shout Outs
----------

pg2kinesis is based on the ideas of others including:

* Logical Decoding: a new world of data exchange applications for Postgres SQL
  [(`slides <https://www.slideshare.net/8kdata/postgresql-logical-decoding/>`_)]
* psycopg2 [(`main <http://initd.org/psycopg/>`_]) [(`repo
  <https://github.com/psycopg/psycopg2/>`__)]
* bottledwater-pg [(`blog <https://www.confluent.io/blog/bottled-water-real-time-integration-of-postgresql-and-kafka>`_)] [(`repo <https://github.com/confluentinc/bottledwater-pg/>`__)]
* wal2json [(`repo <https://github.com/eulerto/wal2json/>`__)]


Future Road Map
---------------

* Support full change output from test_decoding plugin
* Allow HUPing to notify utility to regenerate primary key cache
* Support above on a schedule specified via commandline with sensible default of once an hour.
* Python 3 Support