// import types
import org.apache.spark.sql.types._
// in spark-shell Spark session is readily available as spark
val spark = SparkSession.builder.master("local").appName("test session").getOrCreate()
// set a smaller number of executors because this is running locally
spark.conf.set("spark.sql.shuffle.partitions", "4")
// read csv
val df = spark.read
			.option("inferSchema", "true")
			.option("header", "true")
			.option("sep", "|")
			.csv("data/customer_data.csv")

