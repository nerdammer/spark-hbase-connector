# Spark-HBase Connector

[![Build status](https://travis-ci.org/nerdammer/spark-hbase-connector.svg?branch=master)](https://travis-ci.org/nerdammer/spark-hbase-connector)

This library lets your Apache Spark application interact with Apache HBase using a simple and elegant API.

If you want to read and write data to HBase, you don't need using the Hadoop API anymore, you can just use Spark.

## Including the library

The spark-hbase-connector is available in Sonatype repository. You can just add the following dependency in `sbt`:

```
libraryDependencies += "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3"
```

The Maven style version of the dependency is:

```xml
<dependency>
  <groupId>it.nerdammer.bigdata</groupId>
  <artifactId>spark-hbase-connector_2.10</artifactId>
  <version>1.0.3</version>
</dependency>
```

If you don't like sbt or Maven, you can also check out this Github repo and execute the following command from the root folder:

    sbt package

SBT will create the library jar under `target/scala-2.10`.

Note that the library depends on the following artifacts:

```
libraryDependencies +=  "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided"

libraryDependencies +=  "org.apache.hbase" % "hbase-common" % "1.0.3" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"))

libraryDependencies +=  "org.apache.hbase" % "hbase-client" % "1.0.3" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"))

libraryDependencies +=  "org.apache.hbase" % "hbase-server" % "1.0.3" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"))


libraryDependencies +=  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"

```

Check also if the current branch is passing all tests in Travis-CI before checking out (See "build" icon above).

## Setting the HBase host
The HBase Zookeeper quorum host can be set in multiple ways.

(1) Passing the host to the `spark-submit` command:


    spark-submit --conf spark.hbase.host=thehost ...

(2) Using the hbase-site.xml file (in the root of your jar, i.e. `src/main/resources/hbase-site.xml`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
	<property>
		<name>hbase.zookeeper.quorum</name>
		<value>thehost</value>
	</property>
	
	<!-- Put any other property here, it will be used -->
</configuration>
```

(3) If you have access to the JVM parameters:

    java -Dspark.hbase.host=thehost -jar ....

(4) Using the *scala* code:


```scala
val sparkConf = new SparkConf()
...
sparkConf.set("spark.hbase.host", "thehost")
...
val sc = new SparkContext(sparkConf)
```

or you can directly configure with SparkContext:

```scala
sparkContext.hadoopConfiguration.set("spark.hbase.host", "thehost")
```

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

If you want to read the data written in the previous example, you just need to write:

```scala
val hBaseRDD = sc.hbaseTable[(String, Int, String)]("mytable")
    .select("column1", "column2")
    .inColumnFamily("mycf")
```

Now *hBaseRDD* contains all the data found in the table. Each object in the RDD is a tuple containing (in order) the *row id*,
the corresponding value of *column1* (Int) and *column2* (String).

If you don't want the *row id* but, you only want to see the columns, just remove the first element from the tuple specs:

```scala
val hBaseRDD = sc.hbaseTable[(Int, String)]("mytable")
    .select("column1", "column2")
    .inColumnFamily("mycf")
```

This way, only the columns that you have chosen will be selected.

you don't have to provide column family name as a prefix to column name for the provided column family at inColumnFamily(COLUMN_FAMILY_NAME) but for other columns you need to provide prefix with :(colon).

```scala
val hBaseRDD = sc.hbaseTable[(Int, String, String)]("mytable")
    .select("column1","columnfamily2:column2","columnfamily3:column3")
    .inColumnFamily("columnfamily1")
```

## Other Topics

### Filtering
It is possible to filter the results by prefixes of row keys. Filtering also supports additional salting prefixes
(see the [salting](#salting) section).

```scala
val rdd = sc.hbaseTable[(String, String)]("table")
      .select("col")
      .inColumnFamily(columnFamily)
      .withStartRow("00000")
      .withStopRow("00500")
```

The example above retrieves all rows having a row key *greater or equal* to `00000` and *lower* than `00500`.
The options `withStartRow` and `withStopRow` can also be used separately.

### Managing Empty Columns
Empty columns are managed by using `Option[T]` types:

```scala
val rdd = sc.hbaseTable[(Option[String], String)]("table")
      .select("column1", "column2")
      .inColumnFamily(columnFamily)

rdd.foreach(t => {
    if(t._1.nonEmpty) println(t._1.get)
})
```

You can use the `Option[T]` type every time you are not sure whether a given column is present in your HBase RDD.

### Using different column families
Different column families can be used both when reading or writing an RDD.

```scala
data.toHBaseTable("mytable")
      .toColumns("column1", "cf2:column2")
      .inColumnFamily("cf1")
      .save()
```

In the example above, `cf1` refers only to `column1`, because `cf2:column2` is already *fully qualified*.

```scala
val count = sc.hbaseTable[(String, String)]("mytable")
      .select("cf1:column1", "column2")
      inColumnFamily("cf2")
      .count
```

In the *reading example* above, the default column family `cf2` applies only to `column2`.

### Usage in Spark Streaming
The connector can be used in Spark Streaming applications with the same API.

```scala
// stream is a DStream[(Int, Int)]

stream.foreachRDD(rdd =>
    rdd.toHBaseTable("table")
      .inColumnFamily("cf")
      .toColumns("col1")
      .save()
    )
```
### HBaseRDD as Spark Dataframe
You can convert hBaseRDD to Spark Dataframe for further Spark Transformations and moving to any other NoSQL Databases such as (MongoDB, Hive etc).

``` scala
val hBaseRDD = sparkContext.hbaseTable[(Option[String], Option[String], Option[String], Option[String], Option[String])](HBASE_TABLE_NAME).select("column1", "column2","column3","column4","column5").inColumnFamily(HBASE_COLUMN_FAMILY)
```

Iterating hBaseRDD to create ``` scala org.apache.spark.rdd.RDD [org.apache.spark.sql.Row]``` (i.e RDD[Row]) in our example.

``` scala
val rowRDD = hBaseRDD.map(i => Row(i._1.get,i._2.get,i._3.get,i._4.get,i._5.get))
```
Creating schema structure for above SparkRDD[Row]

``` scala
object myschema {
      val column1 = StructField("column1", StringType)
      val column2 = StructField("column2", StringType)
      val column3 = StructField("column2", StringType)
      val column4 = StructField("column2", StringType)
      val column5 = StructField("column2", StringType)
      val struct = StructType(Array(column1,column2,column3,column4,column5))
    }
```
Create Spark Dataframe with RDD[Row] and Schema Structure

``` scala
val myDf = sqlContext.createDataFrame(rowRDD,myschema.struct)
```

Now you can apply any spark transformations and actions, for example.

``` scala
myDF.show()
```
It will show you Dataframe's Data in tabular structure.

### SparkSQL on HBase

Hence, with previous example. you have converted hBaseRDD to appropriate Spark Dataframe you can apply SparkSQL on Dataframe.

Creating temporary table in spark.

``` scala
myDF.registerTempTable("mytable")
```
Applying SparkSQL on created temporary table.

```
sqlContext.sql("SELECT * FROM mytable").show()
```

## Advanced

### Salting Prefixes<a name="salting"></a>

Salting is supported in reads and writes. Only *string valued row id* are supported at the moment, so salting prefixes
should also be of *String* type.

```scala
sc.parallelize(1 to 1000)
      .map(i => (pad(i.toString, 5), "A value"))
      .toHBaseTable(table)
      .inColumnFamily(columnFamily)
      .toColumns("col")
      .withSalting((0 to 9).map(s => s.toString))
      .save()
```

In the example above, each row id is composed of *5* digits: from `00001` to `01000`.
The *salting* property adds a random digit in front, so you will have records like: `800001`, `600031`, ...

When reading the RDD, you have just to declare the salting type used in the table and
ignore it when using bounds (startRow or stopRow). The library takes care of dealing with salting.

```scala
val rdd = sc.hbaseTable[String](table)
      .select("col")
      .inColumnFamily(columnFamily)
      .withStartRow("00501")
      .withSalting((0 to 9).map(s => s.toString))
```

### Custom Mapping with Case Classes

Custom mapping can be used in place of the default tuple-mapping technique. Just define a case class for your type:

```scala
case class MyData(id: Int, prg: Int, name: String)
```

and define an object that contains *implicit* writer and reader for your type

```scala
implicit def myDataWriter: FieldWriter[MyData] = new FieldWriter[MyData] {
    override def map(data: MyData): HBaseData =
      Seq(
        Some(Bytes.toBytes(data.id)),
        Some(Bytes.toBytes(data.prg)),
        Some(Bytes.toBytes(data.name))
      )

    override def columns = Seq("prg", "name")
}
```

Do not forget to override the *columns* method.

Then, you can define an *implicit* reader:
 
```scala
implicit def myDataReader: FieldReader[MyData] = new FieldReader[MyData] {
    override def map(data: HBaseData): MyData = MyData(
      id = Bytes.toInt(data.head.get),
      prg = Bytes.toInt(data.drop(1).head.get),
      name = Bytes.toString(data.drop(2).head.get)
    )

    override def columns = Seq("prg", "name")
}
```

Once you have done, make sure that the implicits are imported and that it does not produce a non-serializable task (Spark will check it at runtime).

You can now use your converters easily:

```scala
val data = sc.parallelize(1 to 100).map(i => new MyData(i, i, "Name" + i.toString))
// data is an RDD[MyData]

data.toHBaseTable("mytable")
  .inColumnFamily("mycf")
  .save()

val read = sc.hbaseTable[MyData]("mytable")
  .inColumnFamily("mycf")

```

The converters above are low level and use directly the HBase API. Since this connector provides you with many predefined converters for
simple and complex types, probably you would like to reuse them.
The new *FieldReaderProxy* and *FieldWriterProxy* API has been created for this purpose.

### High-level converters using FieldWriterProxy

You can create a new *FieldWriterProxy* by declaring a conversion from your custom type to a predefined type.
In this case, the predefined type it is a tuple composed of three basic fields:

```scala
// MySimpleData is a case class

implicit def myDataWriter: FieldWriter[MySimpleData] = new FieldWriterProxy[MySimpleData, (Int, Int, String)] {

  override def convert(data: MySimpleData) = (data.id, data.prg, data.name) // the first element is the row id

  override def columns = Seq("prg", "name")
}
```

The corresponding *FieldReaderProxy* converts back a tuple of three basic fields into objects of class *MySimpleData*:

```scala
implicit def myDataReader: FieldReader[MySimpleData] = new FieldReaderProxy[(Int, Int, String), MySimpleData] {

  override def columns = Seq("prg", "name")

  override def convert(data: (Int, Int, String)) = MySimpleData(data._1, data._2, data._3)
}

```

Note that we have not used the HBase API. Currently, *FieldWriterProxy* can read and write tuples up to 22 fields (including the row id).
