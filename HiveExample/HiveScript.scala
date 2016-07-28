// Script works on the assumption that Hive is running locally.


// Start Spark in local mode and take 2 cores.
// $ cd $SPARK_HOME
// $ ./bin/spark-shell --master local[2]

// create the Hive table, we need this to exist before we write to hive.
sqlContext.sql("CREATE TABLE IF NOT EXISTS test (key int, value String ) STORED AS TEXTFILE")

// create an RDD from a in-memory collection of pair data.
// Notice the pairs here are Int:String, like the table schema we devined in Hive.
val kvRDD = sc.parallelize(List((1, "data"), (2, "more data")))

// convert RDD to DF, we need the DF for the saveAsTable function to persist to hive.
// The paramets here are column names.
val kvDF = kvRDD.toDF("key", "value")
kvDF.select("*").show
// +---+---------+
// |key|    value|
// +---+---------+
// |  1|     data|
// |  2|more data|
// +---+---------+

// import for the save mode to allow us to append to an existing table.
import org.apache.spark.sql.SaveMode
// This step does the saving to hive. 
// Some assumptions are made. We assume Hive is running on default ports.
kvDF.saveAsTable("test", SaveMode.Append)
// exit spark gracefully
sys.exit

// Verify the DF was saved correctly in Hive
// $ hive
// hive> select * from test;
// OK
// 1	data
// 3	Some data
// 4	data
// 2	more data
// Time taken: 0.055 seconds, Fetched: 4 row(s)
// hive> 