# pg2kinesis

[![Build Status](https://travis-ci.com/handshake/pg2kinesis.svg?token=NJkuMLD39WbGpvfKWtZT&branch=master)](https://travis-ci.com/handshake/pg2kinesis)

pg2kinesis uses [logical decoding](https://www.postgresql.org/docs/9.4/static/logicaldecoding.html) 
in Postgres 9.4 or later to capture a consistent, continuous stream of events from the database
and publishes them to an [AWS Kinesis]() stream in a format of your choosing.

It does this without requiring any changes to your schema like triggers or "shadow" columns or tables,
and has a negligible impact on database performance.
This is done while being extremely fault tolerant. No data loss will be incurred on any type of underlying system
failure including process crashes, network outages, or ec2 instance failures. However, in these situations there will likely
be records that are sent more than once, so your consumer should be designed with this in mind.
 
The fault tolerance comes from guarantees provided underlying technologies and from the "2-phase commit" style of 
publishing inherent to the design of the program. Changes are first peeked from the replication slot and published to Kinesis.
Once Kinesis successfully recieves a batch of records, we advance the [xmin](https://www.postgresql.org/docs/9.4/static/catalog-pg-replication-slots.html) of 
the slot, thereby telling postgres it is safe to reclaim the space taken by the 
wal logs. As is always the case with logical replication slots, unacknowledged data on the slot will consume disk on the database until it is read.

There are other utilities that do similar things, often by injecting a C library into Postgres to do data transformations in place. Unfortunately these approaches are not suitable for managed databases like AWS' RDS where support for various plugins is limited and ultimately determined by the hosting provider. 
We specifically created pg2kinesis to make use of logical decoding on [Amazon's RDS for PostgreSQL](https://aws.amazon.com/rds/postgresql/). 
Amazon only supports logical decoding if you use the built in [test_decoding](https://www.postgresql.org/docs/9.4/static/test-decoding.html)
output plugin. What is special about our utility is it takes the output of the test_decoding plugin, transforms it based 
on a formatter you can define, guarantees publishing to a Kinesis stream 
in *transaction commit time order* and with a guarantees that *no data will be lost*.  

## installation

Currently we only support cloning or downloading a zip for installation.

Perquisites:
 1. Python 2.7
 1. AWS-CLI install edand setup with botofile setup
 1. <more detail for above>

Cloning install:
 1. `git clone git@github.com:handshake/pg2kinesis.git`
 1. `cd pg2kinesis`
 1. `pip install -r pg2kinesis` 


## tests

To run tests simply call `pytest` from installation directory.

## usage

 Run `python -m pg2kinesis --help` to get a list of the latest command line options. By default pg2kinesis attempts to 
 connect to a local postgres instance and publish to a stream named `pg2kinesis` using the AWS credentials of the
 environment the utility was invoked in.
  
 On successful start it will query your database for the primary key definitions of every table in `--pg-dbname`.
 This is used to identify the correct column in the test_decoding output to publish. If a table does not have a primary key 
 its changes will NOT be published.
 
 You have the choice for 2 different textual formats that will be sent to the kinesis stream:
  - `CSVFormatter`: outputs stings to Kinesis that look like:
  ```
  0,CDC,<transaction_id (xid)>,<table name>,<dml operation:DELETE|INSERT|UPDATE>,<primary key of row>
  ```
  - `CSVPayloadFormatter`: outputs similar to the above except the 3rd column is now a json object representing the change.
  ```
  0,CDC,{
            "xid": <transaction_id>
            "table": <...>
            "operation: <...>
            "pkey": <...>    
        }   
 ```
 
 Here is an example of how Handshake runs pg2kinesis in production:
 `<TODO>`

## shoutouts
pg2kinesis is based on the ideas of others including:
- Logical Decoding: a new world of data exchange applications for Postgres SQL [(slides)](https://www.slideshare.net/8kdata/postgresql-logical-decoding)
- psycopg2 [(main)](http://initd.org/psycopg/) [(repo)](https://github.com/psycopg/psycopg2/)
- bottledwater-pg [(blog)](https://www.confluent.io/blog/bottled-water-real-time-integration-of-postgresql-and-kafka/)[(repo)](https://github.com/confluentinc/bottledwater-pg)
- wal2json [(repo)](https://github.com/eulerto/wal2json)


## roadmap
Future Road Map:
 - Package up for cheese shop
 - Allow payload format to be specified on Command Line.
 - Allow HUPing to notify utility to regenerate primary key cache
 - Support above on a schedule specified via commandline with sensible default of once an hour.
 - Full Row information publishing (currently we only emit the DML, primary key and table on any type of change)
 - Python 3 Support
