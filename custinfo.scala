// import types
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
// in spark-shell Spark session is readily available as spark
val spark = SparkSession.builder.master("local").appName("test session").getOrCreate()
// set a smaller number of executors because this is running locally
spark.conf.set("spark.sql.shuffle.partitions", "4")
// read csv
val df = spark.read.option("inferSchema", "true").option("header", "true").csv("data/sample_LotusCustomer.csv.gz")
/* filter out customers who aren't on the Ticketek customer list (list number 2) and 
those who have no customer id (we expect that all customer do)
*/
df.filter(df("CustomerID").isNotNull)
	.filter(df("CustomerListId") === 2)
	.filter(df("EmailAddress").contains("@"))
	.filter(!df("EmailAddress").contains("ticketek")).count()
// filter out invalid postcodes
//df.withColumn("cleanPostcode", regexp_extract(df("Postcode"),"\b(([2-8]\\d{3})|([8-9]\\d{2}))\b",0)).collect()