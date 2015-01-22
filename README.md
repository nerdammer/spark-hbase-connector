# Spark-HBase Connector

This library lets your Apache Spark application interact with Apache HBase using a simple and elegant API.

If you want to read and write data to HBase, you don't need using the Hadoop API anymore, you can just use Spark.

## Including the library

The spark-hbase-connector library is not yet available in Maven repository, so you need to build it from the source code.

Check out this Github repo and execute the following command from the root folder:

    sbt package

SBT will create the library jar under `target/scala-2.10`.

Check also if the current branch is passing all tests in Travis-CI before checking out (See the following icon).

[![Build status](https://travis-ci.org/nerdammer/spark-hbase-connector.svg)](https://travis-ci.org/nerdammer/spark-hbase-connector)

## Writing to HBase (Basic)

Writing to HBase is very easy. Remember to import the implicit conversions:

```scala
import it.nerdammer.spark.hbase._
```

You have just to create a sample RDD, as the following one:

```scala
val rdd = sc.parallelize(1 to 100)
            .map(i => (i.toString, i+1, "Hello"))
```

This *rdd* is made of tuples like `("1", 2, "Hello")` or `("27", 28, "Hello")`. The first element of each tuple is considered the **row id**,
the others will be assigned to columns.

```scala
rdd.toHBaseTable("mytable")
    .toColumns("column1", "column2")
    .inColumnFamily("mycf")
    .save()
```

You are done. HBase now contains *100* rows in table *mytable*, each row containing two values for columns *mycf:column1* and *mycf:column2*.


## Reading from HBase (Basic)

Reading from HBase is easier. Remember to import the implicit conversions:

```scala
import it.nerdammer.spark.hbase._
```

Supposing you want to read the sample data you have written in previous example, you just need to write:

```scala
val hBaseRDD = sc.hbaseTable[(String, Int, String)]("mytable")
    .select("column1", "column2")
    .inColumnFamily("mycf")
```

Now *hBaseRDD* contains all data found in the table. Each object in the RDD is a tuple conaining (in order) the *row id*,
the corresponding value of *column1* (Int) and *column2* (String).

## Other Topics

### Filtering
Supported (Doc to be completed)

### Managing Empty Columns
Supported (Doc to be completed)

### Using different column families
Supported (Doc to be completed)

## Advanced

### Salting Prefixes
Supported (Doc to be completed)

### Custom Mapping
Supported (Doc to be completed)

