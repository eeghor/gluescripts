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
//	.select(df.columns.map(c => lower(col(c)).alias(c)): _*)
	.filter(!df("EmailAddress").contains("ticketek"))
	.withColumn("EmailAddress", when(df("EmailAddress").contains("@"), lower(df("EmailAddress"))).otherwise(""))
	.withColumn("Postcode", regexp_extract(df("Postcode"),"\\b(([2-8]\\d{3})|([8-9]\\d{2}))\\b",0))
	.withColumn("Salutation", when(lower(regexp_replace(df("Salutation"),"[^A-Za-z]",""))
		.isin(List("mr","ms","mrs","dr","mister","miss"):_*), lower(df("Salutation")))
		.otherwise(""))
	.withColumn("DateOfBirth", when(year(df("DateOfBirth")) < 1918, lit(null)).otherwise(df("DateOfBirth")))
	.withColumn("FirstName", ltrim(lower(regexp_replace(df("FirstName"), "[-]"," "))))
	.withColumn("LastName", ltrim(lower(regexp_replace(regexp_replace(df("LastName"),"['`]",""),"[-]"," "))))
	.withColumn("MobilePhone", regexp_extract(regexp_replace(df("MobilePhone"),"\\s",""),"(\\+*(?:61)*|0*)(4\\d{8})",2))
	.withColumn("State", lower(df("State")))
	.withColumn("City", lower(df("City")))
	.withColumn("CountryName", lower(df("CountryName")))
	.select("CustomerID", "Salutation", "FirstName", "LastName", "DateOfBirth", 
				"CreatedDate", "ModifiedDate", "EmailAddress","State", "City","Postcode",
					"CountryName","MobilePhone", "HomePhone", "WorkPhone")
	.repartition(1)
	.write.option("header","true").mode("overwrite").option("compression", "gzip").csv("out")

