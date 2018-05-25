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
df.filter($"CustomerID".isNotNull)
	.filter($"CustomerListId" === 2)
//	.select(df.columns.map(c => lower(col(c)).alias(c)): _*)
	.filter(!df("EmailAddress").contains("ticketek"))
	.withColumn("EmailAddress", when($"EmailAddress".contains("@"), $"EmailAddress").otherwise(""))
	.withColumn("Postcode", regexp_extract($"Postcode","\b(([2-8]\\d{3})|([8-9]\\d{2}))\b",0))
	.withColumn("Salutation", lower(regexp_replace($"Salutation","[^A-Za-z]","")))
	.withColumn("Salutation", when($"Salutation".isin(List("mr","mrs","miss","mister","dr"):_*), $"Salutation").otherwise(""))
	.withColumn("DateOfBirth", when(year($"DateOfBirth") < 1918, lit(null)).otherwise($"DateOfBirth"))
	.select("CustomerID", "Salutation", "FirstName", "LastName", "DateOfBirth", 
				"CreatedDate", "ModifiedDate", "EmailAddress","City","Postcode",
					"CountryName","MobilePhone", "HomePhone", "WorkPhone")
	.repartition(1).write.option("header","true").mode("overwrite").option("compression", "gzip").csv("out")

