from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re
import os
import json
import builtins

# --- load data files

# business emails: exact email to business type
bemails_db = json.load(open('data/business_email_dict.json'))
# business phone: phone number to business type
bephone_db = json.load(open('data/business_phone_dict.json'))
# education: domain to institution name
education_db = json.load(open('data/education_dict.json'))
# get useful files for gender detection
name_db, title_db, hypoc_db, grammg_db = [json.load(open(f,'r')) for f in [os.path.join(os.path.dirname(__file__),'data/data_names_.json'), 
																	 		os.path.join(os.path.dirname(__file__),'data/data_salutations_.json'), 
																	 		os.path.join(os.path.dirname(__file__),'data/data_hypocorisms_.json'),
																	 		os.path.join(os.path.dirname(__file__),'data/data_grammgender_.json')]]

# email domain parts likely favored by testers
testers_db = [line.strip() for line in open('data/tester_domains.txt').readlines() if line.strip()]

# role-based email prefixes
rolebased_db = [line.strip() for line in open('data/rolebased_email_prefixes.txt').readlines() if line.strip()]

# hotel domains
hotel_domains_db = [line.strip() for line in open('data/hotel_domains.txt').readlines() if line.strip()]

# hotel emails
hotel_emails_db = [line.strip() for line in open('data/hotel_emails.txt').readlines() if line.strip()]

# travel agent domains
ta_domains_db = [line.strip() for line in open('data/ta_domains.txt').readlines() if line.strip()]

# travel agent emails
ta_emails_db = [line.strip() for line in open('data/ta_emails.txt').readlines() if line.strip()]

def isTester(name, email_domain):

	if (not name) or (not email_domain):
		return None
		
	has_tester_domain = 0

	for t in testers_db:
		if t in str(email_domain):
			has_tester_domain += 1
			break

	has_tester_name = 1 if set(name.lower().split()) & set('test testing'.split()) else 0

	if has_tester_name and has_tester_domain:
		return 'yes'
	else:
		return 'no'


def getGenderTitle(s):
	"""
	string s is presumably a title
	"""
	for g in 'm f'.split():
		if s and (s.lower() in title_db['common'][g] + title_db['uncommon'][g]):
				return g

def getGenderName(s):
	"""
	s is a string possibly containing name
	"""
	if not s:
		return None

	# any name found straight away or hypocorisms? hypoc_db has hypocs as keys
	nameparts_incommon = {_ for w in s.split() for _ in w.split('-')} & (set(name_db) | set(hypoc_db))

	if nameparts_incommon:

		name_cand = builtins.max(nameparts_incommon, key=len)

		# if candidate is name (and not hypoc)
		if name_cand in name_db:
			return name_db[name_cand]
		else:
			# find what names corresp.to hypocorism and are in the name database
			unfolded_hypoc = set(hypoc_db[name_cand]) & set(name_db)

			if unfolded_hypoc:
				hyp_cand = builtins.max(unfolded_hypoc, key=len)
				return name_db[hyp_cand]

def getGenderEmail(s):

	if not s:
		return None
		
	# find any names in the email prefix; ONLY those separated by _, - or . count
	emailparts = set(q.strip() for w in s.split('_') 
										for v in w.split('.') 
											for q in v.split('-') if q.strip())

	names_in_email_ = emailparts & set(name_db)
	hypocs_in_email_ = emailparts & set(hypoc_db)
	grammgend_in_email_ = emailparts & set(grammg_db)

	if names_in_email_:

		_ = builtins.max(names_in_email_, key=len)

		if _ and (name_db[_] != 'u'):
			return name_db[_]
	
	if hypocs_in_email_:

		_ = builtins.max(hypocs_in_email_, key=len)
	
		if _ and (_ in name_db) and (name_db[_]!= 'u'):
			return name_db[longest_hyp]
	
	# last resort: grammatical gender
	# if grammgend_in_email_:

	# 	_ = builtins.max(grammgend_in_email_, key=len)
	
	# 	if _:
	# 		return grammg_db[_]


def isStudentOrStaff(s):

	# given a string s and if it's an email address, is it a university student or staff?

	staffPattern = re.compile(r"\b^[a-z]+[a-z-_\.]+\@[a-z\.]+\.?edu\.*(au)*\b")
	studentPattern = re.compile(r"\b^[a-z]+[a-z\.]*\d+.*\@[a-z\.]+\.?edu\.*(au)*\b")

	staff_match = re.match(staffPattern, s)
	student_match = re.match(studentPattern, s)

	if staff_match:
		return "staff"
	elif student_match:
		return "student"

def getBusiness(s):
	"""
	get business type by exact email address; to be applied on the column where the EMAIL ADDRESSES are 
	"""
	return bemails_db.get(s, None)

def getBusinessPhone(s):

	return bephone_db.get(s, None)

def isEducation(s):

	return education_db.get(s, None)


getGenderTitleUDF = udf(getGenderTitle, StringType())

getBusinessUDF = udf(getBusiness, StringType())

getBusinessPhoneUDF = udf(getBusinessPhone, StringType())

getGenderNameUDF = udf(getGenderName, StringType())

getGenderEmailUDF = udf(getGenderEmail, StringType())

isEducationUDF = udf(isEducation, StringType())

isStudentOrStaffUDF = udf(isStudentOrStaff, StringType())

isTesterUDF = udf(isTester, StringType())

# in pyspark Spark session is readily available as spark
spark = SparkSession.builder.master("local").appName("test session").getOrCreate()

# set a smaller number of executors because this is running locally
spark.conf.set("spark.sql.shuffle.partitions", "4")

df = spark.read.option("inferSchema", "true").option("header", "true").csv("data/sample_LotusCustomer.csv.gz")

# filter useless stuff out first
df1 = df.select("CustomerID", "Salutation", "FirstName", "LastName", "DateOfBirth", "EmailAddress", 
				"Postcode", "MobilePhone", "HomePhone", "WorkPhone", "CustomerListID") \
		.filter(df.CustomerID.isNotNull()) \
		.filter(df.CustomerListID == 2) 
		# .filter(~(df.EmailAddress.contains("@ticketek") | df.EmailAddress.contains("@teg-")))

# create a lower case trimmed email address 
df2 = df1.withColumn("EmailAddress", \
			when(df1.EmailAddress.contains("@"), rtrim(ltrim(lower(df1.EmailAddress)))).otherwise("")) \
		 .withColumn("Postcode", regexp_extract(df1.Postcode,"\\b(([2-8]\\d{3})|([8-9]\\d{2}))\\b",0)) \
		 .withColumn("Salutation", when(lower(regexp_replace(df1.Salutation,"[^A-Za-z]","")) \
		 								.isin(["mr","ms","mrs","dr","mister","miss"]), lower(df1.Salutation)) \
		 								.otherwise("")) \
		 .withColumn("DateOfBirth", when(year(df1.DateOfBirth) < 1918, lit(None)).otherwise(df1.DateOfBirth)) \
		 .withColumn("FirstName", rtrim(ltrim(lower(regexp_replace(df1.FirstName, "[-]"," "))))) \
		 .withColumn("LastName", rtrim(ltrim(lower(regexp_replace(regexp_replace(df1.LastName,"['`]",""),"[-]"," "))))) \
		 .withColumn("MobilePhone", regexp_extract(regexp_replace(df1.MobilePhone,"\\s",""),"(\\+*(?:61)*|0*)(4\\d{8})",2))

df2 = df2.withColumn("Age", floor(datediff(current_date(), "DateOfBirth")/365))

df3  = df2.withColumn("Gender_Title", getGenderTitleUDF(df2.Salutation)) \
		 	.withColumn("Gender_Name", getGenderNameUDF(df2.FirstName)) \
		 	.withColumn("Gender_Email", getGenderEmailUDF(df2.EmailAddress))

df3 = df3.withColumn("Gender", when(df3.Gender_Title.isNotNull(), 
										df3.Gender_Title).otherwise(when(df3.Gender_Name.isNotNull(), 
											df3.Gender_Name).otherwise(df3.Gender_Email))).drop("Gender_Title", "Gender_Name", "Gender_Email")


df3 = df3.withColumn("_tmp", \
					split(df3.EmailAddress, '@'))
df3 = df3.withColumn("EmailDomain", df3._tmp.getItem(1)).withColumn("EmailPrefix", df3._tmp.getItem(0)).drop("_tmp")
df3 = df3.withColumn("EmailType", when(df3.EmailPrefix.isin(rolebased_db), "role based"))

df3 = df3.withColumn("isTester", isTesterUDF(concat(df3.FirstName, lit(' '), df3.LastName), df3.EmailDomain))

df3 = df3.withColumn("Education", isEducationUDF(df3.EmailDomain)) \
			.withColumn("StudentOrStaff", isStudentOrStaffUDF(df3.EmailAddress)) \
			.withColumn('Business1', getBusinessUDF(df3.EmailAddress)) \
			 .withColumn('Business2', getBusinessPhoneUDF(df3.MobilePhone))

df3 = df3.withColumn("Business", when(df3.Business1.isNotNull(), df3.Business1).otherwise(df3.Business2)).drop("Business1", "Business2")

# travel agents; it's important that business type is 'travel agents' because that's what it is on YellowPages
df3 = df3.withColumn("isTA1", when(df3.EmailAddress.isin(ta_emails_db), "travel agents"))
df3 = df3.withColumn("isTA2", when(df3.EmailDomain.isin(ta_domains_db), "travel agents"))

df3 = df3.withColumn("Business", when(df3.isTA1.isNotNull(), 
										df3.isTA1).otherwise(when(df3.isTA2.isNotNull(), 
											df3.isTA2).otherwise(df3.Business))).drop("isTA1", "isTA2")

df3 = df3.withColumn("isHotel1", when(df3.EmailAddress.isin(hotel_emails_db), "hotels & accommodation"))
df3 = df3.withColumn("isHotel2", when(df3.EmailDomain.isin(hotel_domains_db), "hotels & accommodation"))

df3 = df3.withColumn("Business", when(df3.isHotel1.isNotNull(), 
										df3.isHotel1).otherwise(when(df3.isHotel2.isNotNull(), 
											df3.isHotel2).otherwise(df3.Business))).drop("isHotel1", "isHotel2")


df3.filter(df3.isTester.isNotNull()).select("CustomerID", "Salutation", "FirstName", "LastName", "Gender", "DateOfBirth", 
	"Age", "EmailAddress", "Business", "EmailType", "Education", "StudentOrStaff", "isTester").show()

df3.repartition(1).write.mode("overwrite").parquet("out")