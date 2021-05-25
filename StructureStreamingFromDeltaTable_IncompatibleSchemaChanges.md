# Streaming from Delta Table - dealing with offsets and breaking schema changes

<img src=https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png alt="spark" width="200"/>
<img src=https://camo.githubusercontent.com/5535944a613e60c9be4d3a96e3d9bd34e5aba5cddc1aa6c6153123a958698289/68747470733a2f2f646f63732e64656c74612e696f2f6c61746573742f5f7374617469632f64656c74612d6c616b652d77686974652e706e67 alt="delta" width="200" />

## Links

* [link to databricks delta streaming page](https://docs.databricks.com/delta/delta-streaming.html)

## Use case 
Structure Streaming.

Source: Delta table.

Destination: Console Sink with checkpoint (Possibly any kind of sinks, delta table).
 
Handling breaking changes in the delta table schema and downstream processing.

Lets assume that data is already in the table with schema (Int, Int).
Upstream job which appends data to delta has now different schema, which is not compatible
with current one: (Int, String). We need to overwrite the data and schema in source table, 
that new records can be inserted and rerun downstream job to get only records that comes after the change. 
(If the destination(sink) is delta table you will need to apply changes accordingly to handle new schema etc.)
 
And start processing data from some point in time to avoid processing from the beginning.

## Steps

### 1. Run spark shell

```
spark-shell --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

### 2. Create DataFrame and save test data

```scala
val spark = SparkSession().builder().master("local[2]").getOrCreate()
import spark.implicits._
val data = Seq((1, 1), (2, 2), (3, 3)).toDF("key", "value")
data.write.format("delta").mode("append").save("/tmp/delta-source-table")
// check whether data has been inserted
val source = spark.read.format("delta").load("/tmp/delta-source-table")
source.schema
source.show
```

### 3. specify checkpoint location and start stream
Run first line, wait until it's finished then stop the query
```scala
val query = spark.readStream.format("delta").load("/tmp/delta-source-table").writeStream.format("console").outputMode("append").option("checkpointLocation", "/tmp/delta-out-table/checkpoint").start("/tmp/delta-out-table/data")
if (query.isActive) query.stop()
```

### 4. overwrite the data
Source schema at the beginning was:
```scala
StructType(
  Array(
    StructField("key", LongType),
    StructField("value", LongType)
  )
)
```

And lets assume, there is a requirement to change the type of "value" field from IntegerType to StringType.
This changes is known as incompatible schema change. Delta Table will reject this kind of transaction. 

```scala
import org.apache.spark.sql.types.StringType
val in = spark.read.format("delta").load("/tmp/delta-source-table").select($"key", $"value".cast(StringType))
in.write.format("delta").mode("overwrite").option("overwriteSchema", true).save("/tmp/delta-source-table")
// should return  org.apache.spark.sql.types.StructType = StructType(StructField(key,IntegerType,true), StructField(value,StringType,true))
spark.read.format("delta").load("/tmp/delta-source-table").schema

// add new rows with new schema 
val newData = Seq((10, "10")).toDF("key", "value")
newData.write.format("delta").mode("append").save("/tmp/delta-source-table")
```

### 5 re-run the stream



Follow lines from step 3.

You should receive this error:

```text
21/05/24 16:37:04 ERROR MicroBatchExecution: Query [id = 3a4cb5b6-fcba-4b06-bf2f-9a43b94913eb, runId = 59e62da8-4c17-4718-acd7-049d2b7270f9] terminated with error
java.lang.UnsupportedOperationException: Detected a data update (for example part-00000-52d29896-7731-4ee6-af2c-ed8b10422ae2-c000.snappy.parquet) in the source table at version 3.
This is currently not supported. If you'd like to ignore updates, set the option 'ignoreChanges' to 'true'. 
If you would like the data update to be reflected, please restart this query with a fresh checkpoint directory.
```

### Solution for the issue from step 5 

To get rid of this exception you could set 'ignoreUpdates' (and potentially 'ignoreDeletes') to true, 
This command will start                                                                                                                  but when using this option, all records from the file where the change was applied will be propagated downstream.

```scala
val query = spark.readStream.format("delta").option("ignoreChanges", true).load("/tmp/delta-source-table").writeStream.format("console").outputMode("append").option("checkpointLocation", "/tmp/delta-out-table/checkpoint2").start("/tmp/delta-out-table/data")
```

What if we don't want to process data again? 

There is an option to specify initial position when starting the query. 
You can achieve this by specifying "startingTimestamp" option.

```scala
// "2019-01-01T00:00:00.000Z" | "2019-01-01"
spark.readStream.format("delta").option("startingTimestamp", <timestamp_string | date_string >)
```

How to check when the last item was processed? 

Find last commit time and use it. 
Delta table has history property that contains info about all of changes that has been done.

```scala
import io.delta.tables.DeltaTable
val table = DeltaTable.forPath(spark, "/tmp/delta-source-table")

// last overwrite commit
val lastOverwriteCommit = table.history.filter($"operationParameters.mode" === "Overwrite").select(max("timestamp")).collect()(0).getTimestamp(0)
val lastOverwriteTimeStr = lastOverwriteCommit.toString
```

If you use lastOverwriteTimeStr as a point to start, streaming query will fail.

```text
Detected a data update (for example part-00000-20cb0b04-02a6-4244-a9a1-c530e209b54f-c000.snappy.parquet) in the source table at version 1. 
This is currently not supported. If you'd like to ignore updates, set the option 'ignoreChanges' to 'true'. 
If you would like the data update to be reflected, please restart this query with a fresh checkpoint directory
```

Again ? Yes, we already saw this error.
Even worse...
if the 'overwrite' was the last operation done, you will not be able to re start query.
Wait until new data comes to the table or (add dummy record)?
Find the oldest commit after overwrite commit. 

```scala
val timeToStartFrom = table.history.filter($"timestamp" > lastOverwriteCommit).select(min("timestamp")).collect()(0).getTimestamp(0)
```

```scala
val stream2 = spark.readStream.format("delta").option("startingTimestamp", timeToStartFrom).load("/tmp/delta-source-table").writeStream.format("console").option("checkpointLocation", "/tmp/delta-out-table/offset3").outputMode("append").start()
```

As you might notice I added checkpointLocation option as well.
The startingTimestamp will be only used when the checkpoint is empty.
With next rerun job will start from saved checkpoint. 