import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// need to read JSON; using the standard parser but it's deprecated!
import scala.util.parsing.json.JSON._
// read JSON as a string
val names_json_as_string = scala.io.Source.fromFile("data/data_names_.json").mkString
// convert this string to a map
val name_db:Map[String,String] = parseFull(names_json_as_string).get.asInstanceOf[Map[String, String]]

val titles_json_as_string = scala.io.Source.fromFile("data/data_salutations_.json").mkString
val title_db:Map[String,Map[String, List[String]]] = parseFull(titles_json_as_string).get.asInstanceOf[Map[String,Map[String, List[String]]]]

// hypocs json is like {"al": ["alison", "alexandria", ...

val hypocs_json_as_string = scala.io.Source.fromFile("data/data_hypocorisms_.json").mkString
val hypoc_db:Map[String,Map[String, List[String]]] = parseFull(hypocs_json_as_string).get.asInstanceOf[Map[String,Map[String, List[String]]]]

val grammg_json_as_string = scala.io.Source.fromFile("data/data_grammgender_.json").mkString
val grammg_db:Map[String,String] = parseFull(grammg_json_as_string).get.asInstanceOf[Map[String, String]]


def getGenderTitle(s: String): String = {

//	string s is presumably a title

	for (g <- List("m","f")){

		val all_salutations = title_db("common")(g) ++ title_db("uncommon")(g)

		if (all_salutations.contains(s)){return g}
			}
	return ""
		}

def getGenderName(s: String): Option[String] = {

//	s is a string possibly containing name

	val nameparts_incommon: Option[Set[String]] = Option(s.split("[-_\\s]").toSet.intersect(name_db.keySet | hypoc_db.keySet))

	nameparts_incommon match {

		case Some(Set[String]) => {val name_cand = nameparts_incommon.maxBy(_.length)
			if (name_db.contains(name_cand)){Some(name_db(name_cand))}
				else {Some("noname")}
			}
		case None => Some("result")
	}


	// if (nameparts_incommon.nonEmpty){
	
	// 		val name_cand = nameparts_incommon.maxBy(_.length)

	// 		println(name_cand)
	
	// 		// if candidate is name (and not hypoc)
	// 		if (name_db.contains(name_cand)){return name_db(name_cand)}
	// 		else {
	// 			// find what names corresp.to hypocorism and are in the name database
	// 			val unfolded_hypoc = hypoc_db(name_cand).toSet.intersect(name_db.keySet)

	// 			println("unfolded_hypoc=" + unfolded_hypoc)
				
	// 			if (unfolded_hypoc.nonEmpty){
	// 								val hyp_cand = unfolded_hypoc.maxBy(_.length)
	// 									return name_db(hyp_cand)}
	// 							}
	// 				}
				}

def getDomain(s: String): Option[String] = {

	// given a string s, extract the domain part if this string happens to be an email

	try {
		Some(s.split("@")(1))
	}
	catch {
		case e: Exception => None
	}
}

def isStudentOrStaff(s: String): String = {

	// given a string s and if it's an email address, is it a university student or staff?

	val staffPattern = "\\b[a-z-]+\\.*[a-z-]+\\@[a-z]+\\.?edu\\.*(au)*\\b".r
	val studentPattern = "\\b\\d*\\w+\\d+\\w*\\@[a-z]+\\.?edu\\.*(au)*\\b".r

	val staff_match = staffPattern.findFirstIn(s)
	val student_match = studentPattern.findFirstIn(s)

	var sos = "no"

	if (staff_match.getOrElse("no") != "no") {sos = "staff"}

	if (student_match.getOrElse("no") != "no") {sos = "student"}

	return sos


}


def isUni(s: String): String = {

	// given a string s, figure out which university exactly it's from (if any)

	val p = getDomain(s).getOrElse(" ")

	p match {
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
			case _ => "none"
			}
		}

val getGenderTitleUDF = udf[String, String](getGenderTitle)

val getGenderNameUDF = udf[Option[String], String](getGenderName)

val isUniUDF = udf[String, String](isUni)

val isStudentOrStaffUDF = udf[String, String](isStudentOrStaff)

// in spark-shell Spark session is readily available as spark
val spark = SparkSession.builder.master("local").appName("test session").getOrCreate()

// set a smaller number of executors because this is running locally
spark.conf.set("spark.sql.shuffle.partitions", "4")

val df = spark.read.option("inferSchema", "true").option("header", "true").csv("data/sample_LotusCustomer.csv.gz")
			
val df1 = df.filter(df("CustomerID").isNotNull)
			.filter(df("CustomerListId") === 2)
			.filter(!df("EmailAddress").contains("ticketek"))

val df2 = df1.withColumn("EmailAddress_", when(df1("EmailAddress").contains("@"), lower(df1("EmailAddress"))).otherwise(""))

val df3 = df2.withColumn("University", isUniUDF(df2("EmailAddress_")))
			.withColumn("isStudentOrStaff", isStudentOrStaffUDF(df2("EmailAddress_")))
			.withColumn("Postcode", regexp_extract(df2("Postcode"),"\\b(([2-8]\\d{3})|([8-9]\\d{2}))\\b",0))
			.withColumn("Salutation", when(lower(regexp_replace(df2("Salutation"),"[^A-Za-z]",""))
											.isin(List("mr","ms","mrs","dr","mister","miss"):_*), lower(df("Salutation")))
											.otherwise(""))
val df4 = df3.withColumn("DateOfBirth", when(year(df3("DateOfBirth")) < 1918, lit(null)).otherwise(df3("DateOfBirth")))
			.withColumn("FirstName", ltrim(lower(regexp_replace(df3("FirstName"), "[-]"," "))))
			.withColumn("LastName", ltrim(lower(regexp_replace(regexp_replace(df3("LastName"),"['`]",""),"[-]"," "))))
			.withColumn("MobilePhone", regexp_extract(regexp_replace(df3("MobilePhone"),"\\s",""),"(\\+*(?:61)*|0*)(4\\d{8})",2))
			.withColumn("State", lower(df3("State")))
			.withColumn("City", lower(df3("City")))
			.withColumn("CountryName", lower(df3("CountryName")))
			.withColumn("Gender_Title", getGenderTitleUDF(df3("Salutation")))
			.withColumn("Gender_Name", getGenderNameUDF(df3("FirstName")))
			.select("CustomerID", "Salutation", "Gender_Title", "FirstName", "LastName", "DateOfBirth", 
						"CreatedDate", "ModifiedDate", "EmailAddress","University", "isStudentOrStaff", "State", "City","Postcode",
								"CountryName","MobilePhone", "HomePhone", "WorkPhone")
			.repartition(1)
			.write.option("header","true").mode("overwrite").option("compression", "gzip").csv("out")