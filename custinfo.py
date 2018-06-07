from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re
import json

# s3 = boto3.client('s3')

# for dfl in ['data_names_.json', 'data_salutations_.json', 'data_hypocorisms_.json', 'data_grammgender_.json']:
# 	s3.download_fileobj(Bucket='tega-uploads', Key='Igor/temp/gender_data/{}'.format(dfl), Fileobj=open(dfl,'wb'))

name_db = json.load(open('data_names_.json'))
title_db = json.load(open('data_salutations_.json'))
hypoc_db = json.load(open('data_hypocorisms_.json'))
grammg_db = json.load(open('data_grammgender_.json'))

# name_db, title_db, hypoc_db, grammg_db = [json.load(open(f,'r')) for f in [os.path.join(os.path.dirname(__file__),'data/data_names_.json'), 
# 																 		   os.path.join(os.path.dirname(__file__),'data/data_salutations_.json'), 
# 																 		   os.path.join(os.path.dirname(__file__),'data/data_hypocorisms_.json'),
# 																 		   os.path.join(os.path.dirname(__file__),'data/data_grammgender_.json')]]

# info = f'dictionaries: {len(name_db)} names, {len(hypoc_db)} hypocorisms, {len(grammg_db)} grammatical gender words'
																 
# def __init__(self, priority='name'):

# 	 title_gender =  name_gender =  email_gender = likely_gender = None
# 	 PRIORITY = priority



def _longest_common(set_a, set_b):

	"""
	helper to find the longest common word for sets a and b
	"""
	_ = set_a & set_b
	
	return max(_, key=len) if _ else None


def get_gender(title, name, email):

	"""
	detect gender by first name, title and email and then decide which one to pick as the likeliest gender
	"""

	# normalization
	
	title, name, email = [unidecode(_).strip().lower() if isinstance(_, str) else None 
												for _ in [title, name, email]]  
	
	if title:

		title = ''.join([c for c in title if c in ascii_lowercase])
	
	if name:
		# name: only keep letters, white spaces (reduced to a SINGLE white space) and hyphens
		name = ' '.join(''.join([c for c in name.lower() 
									if (c in ascii_lowercase) or (c in [' ','-'])]).split()).strip()
		if not (set(name) & set(ascii_lowercase)):
			name = None

	if email:
		email = ''.join([c for c in  email.split('@')[0].lower().strip() if (c in ascii_lowercase) or (c in ['.','-','_'])])
		
		if not email:
			 email = None

	# gender from title

	title_gender = None

	for g in 'm f'.split():

		if title and (title in  title_db['common'][g] +  title_db['uncommon'][g]):
			 title_gender = g

	# gender from name

	name_gender = None

	if not name:
		 name_gender = None

	else:

		nameparts = {_ for w in  name.split() for _ in w.split('-')}

		_ =  _longest_commonUDF(nameparts, set(name_db) | set(hypoc_db))

		if not _:
			pass
		else:

			if _ in  name_db:
		 		name_gender =  name_db[_]
			else:
				# find what names corresp.to hypocorism and are in the name database
				_ =  _longest_commonUDF(set( hypoc_db[_]), set( name_db))

				if _ and ( name_db[_] != 'u'):
			 		name_gender =  name_db[_]

	if  name_gender not in 'm f'.split():
		 name_gender = None
	
	# gender from email

	email_gender = None

	if email:
	
		# find any names in the email prefix; ONLY those separated by _, - or . count
		emailparts = set(q.strip() for w in  email.split('_') 
									for v in w.split('.') 
										for q in v.split('-') if q.strip())

		_ =  _longest_commonUDF(emailparts, set( name_db))

		if _ and ( name_db[_] != 'u'):
			email_gender =  name_db[_]
		else:
			_ =  _longest_commonUDF(emailparts, set( hypoc_db))

			if _ and (_ in  name_db) and ( name_db[_]!= 'u'):
		 		email_gender =  name_db[longest_hyp]
		 	else:

				# last resort: grammatical gender

				_ =  _longest_commonUDF(emailparts, set( grammg_db))

				if _:
		 			email_gender =  grammg_db[_]


	likely_gender = None

	# the title based suggestion is available and the same as the name based one
	if all([ title_gender,  name_gender,  title_gender ==  name_gender]):
		likely_gender =  name_gender
	# both are available but NOT the same
	elif all([ name_gender,  title_gender]):
		if  PRIORITY == 'name':
			likely_gender =  name_gender
		else:
			likely_gender =  title_gender
	# what is only one is available
	elif all([ title_gender, not  name_gender]):
		likely_gender =  title_gender
	elif all([ name_gender, not  title_gender]):
		likely_gender =  name_gender
	# finally, if the email based is the only one we have, use it
	elif  email_gender:
		likely_gender =  email_gender

	return likely_gender


def getDomain(s):

	# given a string s, extract the domain part if this string happens to be an email

	if not isinstance(s, str):
		return ''

	return s.strip().split('@')[-1]


def isStudentOrStaff(s):

	# given a string s and if it's an email address, is it a university student or staff?

	staffPattern = re.compile(r"\b[a-z-]+\.*[a-z-]+\@[a-z]+\.?edu\.*(au)*\b")
	studentPattern = re.compile(r"\b\d*\w+\d+\w*\@[a-z]+\.?edu\.*(au)*\b")

	staff_match = re.match(staffPattern, s)
	student_match = re.match(studentPattern, s)

	sos = "no"

	if staff_match:
		sos = "staff"

	if student_match:
		sos = "student"

	return sos


def isUni(s):

	# given a string s, figure out which university exactly it's from (if any)

	p = getDomain(s)

	unis = {
		"adelaide.edu.au" : "university of adelaide",
		"acu.edu.au" : "australian catholic university",
		"anu.edu.au" : "australian national university",
		"bond.edu.au" : "bond university",
		"canberra.edu.au" : "university of canberra",
		"cqu.edu.au" : "cquniversity",
		"cdu.edu.au" : "charles darwin university",
		"csu.edu.au" : "charles sturt university",
		"curtin.edu.au" : "curtin university",
		"deakin.edu.au" : "deakin university",
		"ecu.edu.au" : "edith cowan university",
		"federation.edu.au" : "federation university",
		"flinders.edu.au" : "flinders university",
		"griffith.edu.au" : "griffith university",
		"jcu.edu.au" : "james cook university",
		"latrobe.edu.au" : "la trobe university",
		"mq.edu.au" : "macquarie university",
		"unimelb.edu.au" : "university of melbourne",
		"monash.edu" : "monash university",
		"murdoch.edu.au" : "murdoch university",
		"une.edu.au" : "university of new england",
		"unsw.edu.au" : "university of new south wales",
		"newcastle.edu.au" : "university of newcastle",
		"nd.edu.au" : "university of notre dame",
		"uq.edu.au" : "university of queensland",
		"qut.edu.au" : "qut",
		"rmit.edu.au" : "rmit",
		"unisa.edu.au" : "university of south australia",
		"scu.edu.au" : "southern cross university",
		"usq.edu.au" : "university of southern queensland",
		"usc.edu.au" : "usc",
		"swin.edu.au" : "swinburne university",
		"sydney.edu.au" : "university of sydney",
		"utas.edu.au" : "university of tasmania",
		"uts.edu.au" : "university of technology sydney",
		"vu.edu.au" : "victoria university",
		"uwa.edu.au" : "university of western australia",
		"westernsydney.edu.au" : "western sydney university",
		"uow.edu.au" : "university of wollongong"
		}

	return unis.get(p, "none")

_longest_commonUDF = udf(_longest_common)

get_genderUDF = udf(get_gender)

isUniUDF = udf(isUni, StringType())

isStudentOrStaffUDF = udf(isStudentOrStaff, StringType())

# in pyspark Spark session is readily available as spark
spark = SparkSession.builder.master("local").appName("test session").getOrCreate()

# set a smaller number of executors because this is running locally
spark.conf.set("spark.sql.shuffle.partitions", "4")

df = spark.read.option("inferSchema", "true").option("header", "true").csv("data/sample_LotusCustomer.csv.gz")

df1 = df.filter(df.CustomerID.isNotNull()) \
		.filter(df.CustomerListID == 2) \
		.filter(~df.EmailAddress.contains("ticketek"))

df2 = df1.withcolumn("emailaddress_", \
					when(df1.emailaddress.contains("@"), lower(df1.emailaddress)).otherwise(""))

df3 = df2.withcolumn("university", isuniudf(df2.emailaddress_)) \
		.withcolumn("isstudentorstaff", isstudentorstaffudf(df2.emailaddress_)) \
		.withcolumn("postcode", regexp_extract(df2.postcode,"\\b(([2-8]\\d{3})|([8-9]\\d{2}))\\b",0)) \
		.withColumn("salutation", when(lower(regexp_replace(df2.salutation,"[^A-Za-z]","")) \
										.isin(["mr","ms","mrs","dr","mister","miss"]), lower(df.salutation)) \
										.otherwise("")) \
		.withColumn("dateofbirth", when(year(df2.dateofbirth) < 1918, lit(None)).otherwise(df2.dateofbirth)) \
		.withColumn("firstname", ltrim(lower(regexp_replace(df2.firstname, "[-]"," ")))) \
		.withColumn("lastname", ltrim(lower(regexp_replace(regexp_replace(df2.lastname,"['`]",""),"[-]"," ")))) \
		.withColumn("mobilephone", regexp_extract(regexp_replace(df2.mobilephone,"\\s",""),"(\\+*(?:61)*|0*)(4\\d{8})",2)) \
		.withColumn("state", lower(df2.state)) \
		.withColumn("city", lower(df2.city)) \
		.withColumn("countryname", lower(df2.countryname)) \
		.withColumn("gender_", get_genderUDF(col("salutation"), col("firstname") + lit(" ") + col("lastname"), col("emailaddress"))) \
		.select("customerid", "salutation", "gender_", "firstname", "lastname", "dateofbirth", 
					"createddate", "modifieddate", "emailaddress","university", "isstudentorstaff", "state", "city","postcode",
							"countryname","mobilephone", "homephone", "workphone") \
		.repartition(1) \
		.write.option("header","true").mode("overwrite").option("compression", "gzip").csv("out")