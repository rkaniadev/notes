# DeltaMerge

## Releases
* Delta table 0.8
* Spark 3.0.2
* OS: Windows 10

For spark 3.1.1 an issue occurs for merge method [spark3.1.1 delta merge issue](https://github.com/delta-io/delta/issues/594)

Make sure you use appropriate version of Spark.

```text
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.0.2
      /_/
```

## steps

### Run spark shell with Delta

```scala
spark-shell --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

### Create empty table

```scala
val spark = SparkSession().builder().master("local[2]").getOrCreate()
import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val schema = StructType(
  Array(
    StructField("key", LongType),
    StructField("value", LongType),
    StructField("partitionKey", LongType),
    StructField("updateTime", TimestampType)
  )
)

val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
```

### Create table definition (save as delta table, use partitionBy)

```scala
emptyDF.write.format("delta").mode("overwrite").partitionBy("partitionKey").save("/tmp/partitioned/data")
```

### Create DeltaTable object for `tmp/partitioned/data`

```scala
import io.delta.tables.DeltaTable
val table = DeltaTable.forPath(spark, "/tmp/partitioned/data")
// to see history of the table use
table.history.show
```

### Merge data 
Field `key` is our unique key which we will use for merging.
Let's assume we need to keep only the latest data per key.
`updateTime` indicates whether incoming record is newer than existing.
Let's create two datasets and perform two merge statements in a row, one using dataDF1 and second one using dataDF2.
Here you can find databricks notebook with example usage of merge statement 
[merge example notebook](https://docs.azuredatabricks.net/_static/notebooks/merge-in-streaming.html).

```scala
// all records should be inserted
val time = System.currentTimeMillis()
val timestamp0 = new java.sql.Timestamp(time - 1)
val timestamp1 = new java.sql.Timestamp(time)
val timestamp2 = new java.sql.Timestamp(time + 1)

val dataDF1 = Seq((1, 1, 1, timestamp1), (2, 1, 2, timestamp1), (3, 1, 3, timestamp1)).toDF("key", "value", "partitionKey", "updateTime")

// First element has update time lower than existing record
// 3rd records has different partitionValue
val dataDF2 = Seq((1, 1, 1, timestamp0), (3, 1, 1, timestamp2), (4, 1, 2, timestamp2)).toDF("key", "value", "partitionKey", "updateTime")

import org.apache.spark.sql.DataFrame

def mergeVol1(batchDF: DataFrame): Unit = {
  // table already created
  table.as("t")
    .merge(batchDF.as("s"), "s.key = t.key")
    .whenMatched("s.updateTime > t.updateTime").updateAll()
    .whenNotMatched().insertAll()
    .execute()
}

def checkData() = {
  spark.read.format("delta").load("/tmp/partitioned/data").show(false)
}

def mergeVol2(): Unit = {
  // find latest documents per micro batch and keep only these for merging
}

mergeVol1(dataDF1)
```

Run `checkData` method.

```scala
scala> checkData
+---+-----+------------+-----------------------+
|key|value|partitionKey|updateTime             |
+---+-----+------------+-----------------------+
|2  |1    |2           |2021-06-08 16:57:45.502|
|3  |1    |3           |2021-06-08 16:57:45.502|
|1  |1    |1           |2021-06-08 16:57:45.502|
+---+-----+------------+-----------------------+
```

Merge second DF.

```scala
scala> dataDF2.show(false)
+---+-----+------------+-----------------------+
|key|value|partitionKey|updateTime             |
+---+-----+------------+-----------------------+
|1  |1    |1           |2021-06-08 16:57:45.501|
|3  |1    |1           |2021-06-08 16:57:45.503|
|4  |1    |2           |2021-06-08 16:57:45.503|
+---+-----+------------+-----------------------+

scala> mergeVol1(dataDF2)

scala> checkData
+---+-----+------------+-----------------------+
|key|value|partitionKey|updateTime             |
+---+-----+------------+-----------------------+
|4  |1    |2           |2021-06-08 16:57:45.503|
|2  |1    |2           |2021-06-08 16:57:45.502|
|3  |1    |1           |2021-06-08 16:57:45.503|
|1  |1    |1           |2021-06-08 16:57:45.502|
+---+-----+------------+-----------------------+
```

First record from `dataDF2` is omitted due tu the fact it's past data.
Record with key == 1, now has different partition key (was 3, now is 1).
There is a one new record (key == 4), this record didn't exist before we run the merge. 

### Performance cons of the solution

Merge operation is costly.
The requirement was to partition by `partitionKey`. 
Let's assume we would have the partition field `updateDate` which is `YYYY-MM-DD` of updateTime and
we can't have info about last update in source data.
How to limit the data that is involved in merge for this scenario?

## Merge with SparkStreaming

It's possible to run merge using `foreachBatch`