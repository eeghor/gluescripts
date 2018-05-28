// import types
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


// method to figure out university email address types
def isUni(s: String): String = {

	println("received " + s)
	val p = s.toString.split("@")(1)

	println("p=" + p)

	val uni = p match {
		case "adelaide.edu.au" => "university of adelaide"
		case "acu.edu.au" => "australian catholic university"
		case "anu.edu.au" => "australian national university"
		case "bond.edu.au" => "bond university"
		case "canberra.edu.au" => "university of canberra"
		case "cqu.edu.au" => "cquniversity"
		case "cdu.edu.au" => "charles darwin university"
		case "csu.edu.au" => "charles sturt university"
		case "curtin.edu.au" => "curtin university"
		case "deakin.edu.au" => "deakin university"
		case "ecu.edu.au" => "edith cowan university"
		case "federation.edu.au" => "federation university"
		case "flinders.edu.au" => "flinders university"
		case "griffith.edu.au" => "griffith university"
		case "jcu.edu.au" => "james cook university"
		case "latrobe.edu.au" => "la trobe university"
		case "mq.edu.au" => "macquarie university"
		case "unimelb.edu.au" => "university of melbourne"
		case "monash.edu" => "monash university"
		case "murdoch.edu.au" => "murdoch university"
		case "une.edu.au" => "university of new england"
		case "unsw.edu.au" => "university of new south wales"
		case "newcastle.edu.au" => "university of newcastle"
		case "nd.edu.au" => "university of notre dame"
		case "uq.edu.au" => "university of queensland"
		case "qut.edu.au" => "qut"
		case "rmit.edu.au" => "rmit"
		case "unisa.edu.au" => "university of south australia"
		case "scu.edu.au" => "southern cross university"
		case "usq.edu.au" => "university of southern queensland"
		case "usc.edu.au" => "usc"
		case "swin.edu.au" => "swinburne university"
		case "sydney.edu.au" => "university of sydney"
		case "utas.edu.au" => "university of tasmania"
		case "uts.edu.au" => "university of technology sydney"
		case "vu.edu.au" => "victoria university"
		case "uwa.edu.au" => "university of western australia"
		case "westernsydney.edu.au" => "western sydney university"
		case "uow.edu.au" => "university of wollongong"
		case _ => "not a uni"}

		println("matched: " + uni)
		return uni
	}

val isUniUDF = udf[String, String](isUni)

// in spark-shell Spark session is readily available as spark
val spark = SparkSession.builder.master("local").appName("test session").getOrCreate()
// set a smaller number of executors because this is running locally
spark.conf.set("spark.sql.shuffle.partitions", "4")
// read csv
val df = spark.read.option("inferSchema", "true").option("header", "true").csv("data/sample_LotusCustomer.csv.gz")
/* filter out customers who aren't on the Ticketek customer list (list number 2) and 
those who have no customer id (we expect that all customer do)
*/

df.select(isUniUDF(lit("qwdwjdnj@uts.edu.au")).as("is_Uni")).show()

df.filter(df("CustomerID").isNotNull)
	.filter(df("CustomerListId") === 2)
//	.select(df.columns.map(c => lower(col(c)).alias(c)): _*)
	// .filter(!df("EmailAddress").contains("ticketek"))
	.withColumn("EmailAddress", when(df("EmailAddress").contains("@"), lower(df("EmailAddress"))).otherwise(""))
	.withColumn("EmailAddressType", isUniUDF(df("EmailAddress")))
	// .withColumn("Postcode", regexp_extract(df("Postcode"),"\\b(([2-8]\\d{3})|([8-9]\\d{2}))\\b",0))
	// .withColumn("Salutation", when(lower(regexp_replace(df("Salutation"),"[^A-Za-z]",""))
	// 	.isin(List("mr","ms","mrs","dr","mister","miss"):_*), lower(df("Salutation")))
	// 	.otherwise(""))
	// .withColumn("DateOfBirth", when(year(df("DateOfBirth")) < 1918, lit(null)).otherwise(df("DateOfBirth")))
	// .withColumn("FirstName", ltrim(lower(regexp_replace(df("FirstName"), "[-]"," "))))
	// .withColumn("LastName", ltrim(lower(regexp_replace(regexp_replace(df("LastName"),"['`]",""),"[-]"," "))))
	// .withColumn("MobilePhone", regexp_extract(regexp_replace(df("MobilePhone"),"\\s",""),"(\\+*(?:61)*|0*)(4\\d{8})",2))
	// .withColumn("State", lower(df("State")))
	// .withColumn("City", lower(df("City")))
	// .withColumn("EmailAddressType", isUniUDF(df("EmailAddress")))
	// .withColumn("CountryName", lower(df("CountryName")))
	// .select("CustomerID", "Salutation", "FirstName", "LastName", "DateOfBirth", 
	// 			"CreatedDate", "ModifiedDate", "EmailAddress","State", "City","Postcode",
	// 				"CountryName","MobilePhone", "HomePhone", "WorkPhone")
	.repartition(1)
	.write.option("header","true").mode("overwrite").option("compression", "gzip").csv("out")

