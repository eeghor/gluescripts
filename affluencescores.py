from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re
import os
import json
import builtins

"""

Affluence Scores
----------------

required data sources:

	- ABS weekly personal income ranges by age, gender and postcode (Census 2016)

FYI:
	- ABS age groups are the following:
					'15_19', '20_24', '25_34', '35_44', '45_54', '55_64', '65_74', '75_84', '85+'

"""

# in pyspark Spark session is readily available as spark
spark = SparkSession.builder.master("local").appName("test session").getOrCreate()

# set a smaller number of executors because this is running locally
spark.conf.set("spark.sql.shuffle.partitions", "4")

df = spark.read.option("inferSchema", "true").option("header", "true").csv("data/sample_sales_fact.csv.gz") \
			.select("LotusCustomerID", date_format("transaction_datetime", "yyy").alias("year"), "sales_amt") 

df1 = df.filter(df.LotusCustomerID.isNotNull() & (df.sales_amt > 0)).groupBy("LotusCustomerID", "year") \
		.agg(sum("sales_amt").alias("spend_per_year")) \
		.orderBy("LotusCustomerID")

upper_threshold = df1.filter(col("year") == 2017).approxQuantile("spend_per_year", [0.1,0.8], 0.1)[-1]

max_spends = df1.groupBy("LotusCustomerID").agg(max("spend_per_year").alias("max_spend"))

max_spends = max_spends.withColumn("max_spend", bround(max_spends.max_spend, 2))

spend_scores = max_spends.withColumn("purch_score", when(col("max_spend") >= upper_threshold, 1.0).otherwise(bround(col("max_spend")/upper_threshold, 2)))

cust_info = spark.read.option("inferSchema", "true").option("header", "true").csv("data/sample_LotusCustomer.csv.gz") \
				.select("CustomerID", "Gender", "DateOfBirth", floor(datediff(current_date(), "DateOfBirth")/365).alias('age_now'), "Postcode") \
				.filter(col("CustomerID").isNotNull()) \
				.filter(col("CustomerListID") == 2)

# add age groups
cust_info_ag = cust_info.withColumn("age_group", when(col("age_now") >= 85, '85+') \
										.otherwise(when(col("age_now") >= 75, '75_84') \
											.otherwise(when(col("age_now") >= 65, '65_74') \
												.otherwise(when(col("age_now") >= 55, '55_64') \
													.otherwise(when(col("age_now") >= 45, '45_54') \
														.otherwise(when(col("age_now") >= 35, '35_44') \
															.otherwise(when(col("age_now") >= 25, '25_34') \
																.otherwise(when(col("age_now") >= 20, '20_24') \
																	.otherwise(when(col("age_now") >= 15, '15_19'))))))))))

# create new column to match to the ABS incomes
d = cust_info_ag.withColumn('abs_col', 
							concat_ws('/',lower(cust_info_ag.Gender), cust_info_ag.age_group, cust_info_ag.Postcode))

print('loading abs data...')

abs_db = json.load(open('data/abs_census_weekly_income.json'))
abs_df = spark.sparkContext.parallelize(abs_db.items()).toDF(['gap','income'])

d1 = d.join(abs_df, d.abs_col == abs_df.gap).drop('DateOfBirth', 'Postcode', 'abs_col', 'gap')

d2 = d1.withColumn("bias_score", when(col("income").isin(['$0', '$1_149', '$150_299']), 0) \
									.otherwise(when(col("income") == '$300_399', 0.10) \
										.otherwise(when(col("income") == '$400_499', 0.16) \
											.otherwise(when(col("income") == '$500_649', 0.21) \
												.otherwise(when(col("income") == '$650_799', 0.27) \
													.otherwise(when(col("income") == '$800_999', 0.33) \
														.otherwise(when(col("income") == '$1000_1249', 0.39) \
															.otherwise(when(col("income") == '$1250_1499', 0.44) \
																.otherwise(when(col("income") == '$1500_1749', 0.50) \
																	.otherwise(when(col("income") == '$1750_1999', 0.67) \
																		.otherwise(when(col("income") == '$2000_2999', 0.83) \
																			.otherwise(when(col("income") == '$3000+', 1.00)))))))))))))


d3 = spend_scores.join(d2, spend_scores.LotusCustomerID == d2.CustomerID).drop('LotusCustomerID', 'Gender', 'age_group', 'age_now')

# calculate the final purch_score for the case when 0.5 is the affluence threshold for both ABS and purchase scores
d4 = d3.withColumn('affl_score', bround(greatest(d3.purch_score, d3.bias_score), 2))

d5 = d4.withColumn('affl_score', when(d4.affl_score < 1, d4.affl_score).otherwise(1)).select('CustomerID', 'affl_score', 'income', 'bias_score', 'max_spend', 'purch_score')

d5.show()




