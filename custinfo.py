from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re
import os
import json
import builtins

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
	if grammgend_in_email_:

		_ = builtins.max(grammgend_in_email_, key=len)
	
		if _:
			return grammg_db[_]
	

# def getDomain(s):

# 	# given a string s, extract the domain part if this string happens to be an email

# 	if not isinstance(s, str):
# 		return ''

# 	return s.strip().split('@')[-1]


def isStudentOrStaff(s):

	# given a string s and if it's an email address, is it a university student or staff?

	staffPattern = re.compile(r"\b^[a-z]+[a-z-\.]+\@[a-z]+\.?edu\.*(au)*\b")
	studentPattern = re.compile(r"\b\d*\w+\d+\w*\@[a-z]+\.?edu\.*(au)*\b")

	staff_match = re.match(staffPattern, s)
	student_match = re.match(studentPattern, s)

	sos = "no"

	if staff_match:
		sos = "staff"

	if student_match:
		sos = "student"

	return sos

def getBusiness(s):
	"""
	get business type by exact email address; to be applied on the column where the EMAIL ADDRESSES are 
	"""
	return bemails_db.get(s, None)

def getBusinessPhone(s):

	return bephone_db.get(s, None)

def isEducation(s):

	return education_db.get(s, None)

# def isUni(s):

# 	# given a string s, figure out which university exactly it's from (if any)

# 	p = getDomain(s)

# 	unis = {
# 		"adelaide.edu.au" : "university of adelaide",
# 		"acu.edu.au" : "australian catholic university",
# 		"anu.edu.au" : "australian national university",
# 		"bond.edu.au" : "bond university",
# 		"canberra.edu.au" : "university of canberra",
# 		"cqu.edu.au" : "cquniversity",
# 		"cdu.edu.au" : "charles darwin university",
# 		"csu.edu.au" : "charles sturt university",
# 		"curtin.edu.au" : "curtin university",
# 		"deakin.edu.au" : "deakin university",
# 		"ecu.edu.au" : "edith cowan university",
# 		"federation.edu.au" : "federation university",
# 		"flinders.edu.au" : "flinders university",
# 		"griffith.edu.au" : "griffith university",
# 		"jcu.edu.au" : "james cook university",
# 		"latrobe.edu.au" : "la trobe university",
# 		"mq.edu.au" : "macquarie university",
# 		"unimelb.edu.au" : "university of melbourne",
# 		"monash.edu" : "monash university",
# 		"murdoch.edu.au" : "murdoch university",
# 		"une.edu.au" : "university of new england",
# 		"unsw.edu.au" : "university of new south wales",
# 		"newcastle.edu.au" : "university of newcastle",
# 		"nd.edu.au" : "university of notre dame",
# 		"uq.edu.au" : "university of queensland",
# 		"qut.edu.au" : "qut",
# 		"rmit.edu.au" : "rmit",
# 		"unisa.edu.au" : "university of south australia",
# 		"scu.edu.au" : "southern cross university",
# 		"usq.edu.au" : "university of southern queensland",
# 		"usc.edu.au" : "usc",
# 		"swin.edu.au" : "swinburne university",
# 		"sydney.edu.au" : "university of sydney",
# 		"utas.edu.au" : "university of tasmania",
# 		"uts.edu.au" : "university of technology sydney",
# 		"vu.edu.au" : "victoria university",
# 		"uwa.edu.au" : "university of western australia",
# 		"westernsydney.edu.au" : "western sydney university",
# 		"uow.edu.au" : "university of wollongong"
# 		}

# 	tafes = {
# 	"hunter.tafensw.edu.au": "hunter institute of tafe",
# 	"illawarra.tafensw.edu.au": "illawarra institute of tafe",
# 	"newengland.tafensw.edu.au": "new england institute of tafe",
# 	"northcoasttafe.edu.au": "north coast institute of tafe",
# 	"nsi.tafensw.edu.au": "northern sydney institute of tafe",
# 	"rit.tafensw.edu.au": "riverina institute of tafe",
# 	"swsi.tafensw.edu.au": "south western sydney institute of tafe",
# 	"sydneytafe.edu.au": "sydney institute of tafe",
# 	"tafewestern.edu.au": "western institute of tafe",
# 	"wsi.tafensw.edu.au": "western sydney institute of tafe",
# 	"bendigotafe.edu.au": "bendigo regional institute of tafe",
# 	"boxhill.edu.au": "box hill institute",
# 	'bhtafe.edu.au': "box hill institute",
# 	"chisholm.edu.au": "chisholm institute of tafe",
# 	"federationtraining.edu.au": "federation training institute",
# 	"federation.edu.au": "federation university australia",
# 	"thegordon.edu.au": "gordon institute of tafe",
# 	"gotafe.vic.edu.au": "goulburn ovens institute of tafe",
# 	"holmesglen.edu.au": "holmesglen institute of tafe",
# 	"kangan.edu.au": "kangan institute of tafe",
# 	"melbournepolytechnic.edu.au": "melbourne polytechnic tafe",
# 	"swsi.tafensw.edu.au": "south west institute of tafe",
# 	"sunitafe.edu.au": "sunraysia institute of tafe",
# 	"angliss.edu.au": "william angliss institute of tafe",
# 	"wodongatafe.edu.au": "wodonga institute of tafe",
# 	"tafeqld.edu.au": "tafe queensland",
# 	"bn.tafe.qld.gov.au": "tafe queensland",
# 	"tafeeastcoast.edu.au": "tafe queensland",
# 	"goldcoast.tafe.qld.gov.au": "tafe queensland",
# 	"tafenorth.edu.au": "tafe queensland",
# 	"tafeskillstech.edu.au": "tafe queensland",
# 	"swtafe.edu.au": "south west tafe",
# 	"cit.edu.au": "canberra institute of technology",
# 	"cyoctafe.wa.edu.au": "c y o’connor college",
# 	"central.wa.edu.au": "central institute of tafe of technology",
# 	"southmetrotafe.wa.edu.au": "south metropolitan tafe",
# 	"challenger.wa.edu.au": "south metropolitan tafe",
# 	"durack.edu.au": "durack institute of technology tafe",
# 	"pilbara.wa.edu.au": "pilbara insitute of tafe",
# 	"goldfields.wa.edu.au": "goldfields institute of technology tafe",
# 	"gsit.wa.edu.au": "great southern insitute of technology",
# 	"kti.wa.edu.au": "kimberley training institute",
# 	"polytechnic.wa.edu.au": "polytechnic west tafe",
# 	"swit.wa.edu.au": "south west institute of technology",
# 	"wcit.wa.edu.au": "west coast college of tafe",
# 	"esc.sa.edu.au": "eynesbury senior college",
# 	"ichm.edu.au": "international college of hotel management",
# 	"saibt.sa.edu.au": "south australian institute of tafe of business and technology",
# 	"tastafe.tas.edu.au": "the institute of tafe tasmania"
# 	}

# 	other_collegest = {
# 	"csf.edu.au": "college of sports and fitness",
# 	"aie.edu.au": "academy of interactive entertainment",
# 	"aba.edu.au": "australian business academy",
# 	"aif.edu.au": "australian institute of fitness",
# 	"aihs.edu.au": "australian international hotel school",
# 	"amc.edu.au": "australian maritime college",
# 	"chart.edu.au": "capital hairdressing academy & regional training",
# 	"ctiaustralia.edu.au": "capital training institute", 
# 	"clja.edu.au": "college for law and justice administration",
# 	"medentry.edu.au": "medentry umat preparation",
# 	"unity.edu.au": "unity college",
# 	"ability.edu.au": "ability education",
# 	"aca.nsw.edu.au": "academies australasia group of colleges",
# 	"aah.edu.au": "academy of applied hypnosis",
# 	"access.nsw.edu.au": "access language centre",
# 	"actt.edu.au": "actors college of theatre & television",
# 	"schoolofbeauty.nsw.edu.au": "advanced school of beauty",
# 	"ahbc.nsw.edu.au": "ah & b college",
# 	"awcc.edu.au": "albury wodonga community college",
# 	"alphacrucis.edu.au": "alphacrucis college",
# 	"apm.edu.au": "apm college of business and communication",
# 	"ssbt.nsw.edu.au": "apple study group",
# 	"apicollege.edu.au": "asia pacific international college",
# 	"aspire.edu.au": "aspire institute",
# 	"accm.edu.au": "australian college of commerce and management",
# 	"acnt.edu.au": "australasian college of natural therapies"
# 	}

# 	uni_or_tafe = unis.get(p, None)
	
# 	if not uni_or_tafe:
# 		uni_or_tafe = tafes.get(p, None)
# 	if uni_or_tafe:
# 		return uni_or_tafe
# 	else:
# 		return 'none'

getGenderTitleUDF = udf(getGenderTitle, StringType())

getBusinessUDF = udf(getBusiness, StringType())

getBusinessPhoneUDF = udf(getBusinessPhone, StringType())

getGenderNameUDF = udf(getGenderName, StringType())

getGenderEmailUDF = udf(getGenderEmail, StringType())

isEducationUDF = udf(isEducation, StringType())

# isUniUDF = udf(isUni, StringType())


isStudentOrStaffUDF = udf(isStudentOrStaff, StringType())

# in pyspark Spark session is readily available as spark
spark = SparkSession.builder.master("local").appName("test session").getOrCreate()

# set a smaller number of executors because this is running locally
spark.conf.set("spark.sql.shuffle.partitions", "4")

df = spark.read.option("inferSchema", "true").option("header", "true").csv("data/sample_LotusCustomer.csv.gz")

df1 = df.filter(df.CustomerID.isNotNull()) \
		.filter(df.CustomerListID == 2) \
		.filter(~(df.EmailAddress.contains("@ticketek") | df.EmailAddress.contains("@teg-")))

# create a lower case trimmed email address 
df2 = df1.withColumn("EmailAddress_", \
					when(df1.EmailAddress.contains("@"), rtrim(ltrim(lower(df1.EmailAddress)))).otherwise(""))

df3 = df2.withColumn("_tmp", \
					split(df2.EmailAddress_, '@'))

df3 = df3.withColumn("EmailDomain", df3._tmp.getItem(1)).drop("_tmp")

df3 = df3.withColumn("Education", isEducationUDF(df3.EmailDomain)).withColumn("StudentOrStaff", isStudentOrStaffUDF(df3.EmailAddress_))

df3.filter(df3.Education.isNotNull()).select("EmailAddress_", "EmailDomain", "Education", "StudentOrStaff").show()

# df3 = df2.withColumn("UniOrTAFE", isUniUDF(df2.EmailAddress_)) \
# 		.withColumn("isStudentOrStaff", isStudentOrStaffUDF(df2.EmailAddress_)) \
# 		.withColumn("Postcode", regexp_extract(df2.Postcode,"\\b(([2-8]\\d{3})|([8-9]\\d{2}))\\b",0)) \
# 		.withColumn("Salutation", when(lower(regexp_replace(df2.Salutation,"[^A-Za-z]","")) \
# 										.isin(["mr","ms","mrs","dr","mister","miss"]), lower(df.Salutation)) \
# 										.otherwise("")) \
# 		.withColumn("DateOfBirth", when(year(df2.DateOfBirth) < 1918, lit(None)).otherwise(df2.DateOfBirth)) \
# 		.withColumn("FirstName", ltrim(lower(regexp_replace(df2.FirstName, "[-]"," ")))) \
# 		.withColumn("LastName", ltrim(lower(regexp_replace(regexp_replace(df2.LastName,"['`]",""),"[-]"," ")))) \
# 		.withColumn("MobilePhone", regexp_extract(regexp_replace(df2.MobilePhone,"\\s",""),"(\\+*(?:61)*|0*)(4\\d{8})",2)) \
# 		.withColumn("State", lower(df2.State)) \
# 		.withColumn("City", lower(df2.City)) \
# 		.withColumn("CountryName", lower(df2.CountryName))

# df4 = df3.withColumn("Gender_Title", getGenderTitleUDF(df3.Salutation)) \
# 		 .withColumn("Gender_Name", getGenderNameUDF(df3.FirstName)) \
# 		 .withColumn("Gender_Email", getGenderEmailUDF(df3.EmailAddress_)) \
# 		 .withColumn('Business1', getBusinessUDF(df3.EmailAddress_)) \
# 		 .withColumn('Business2', getBusinessPhoneUDF(df3.MobilePhone)) \
# 		 .select("CustomerID", "Salutation", "Gender_Title", "Gender_Name", "Gender_Email" , "FirstName", "LastName", "DateOfBirth", 
# 					"CreatedDate", "ModifiedDate", "EmailAddress","UniOrTAFE", "isStudentOrStaff", "Business1", "Business2", "State", "City","Postcode",
# 							"CountryName","MobilePhone", "HomePhone", "WorkPhone") \
# 		.repartition(1) \
# 		.write.option("header","true").mode("overwrite").option("compression", "gzip").csv("out")