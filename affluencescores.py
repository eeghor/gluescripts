from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re
import os
import json
import builtins

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

max_spends.withColumn("score", when(col("max_spend") >= upper_threshold, 1.0).otherwise(format_number(col("max_spend")/upper_threshold, 2))).show()

cust_info = spark.read.option("inferSchema", "true").option("header", "true").csv("data/sample_LotusCustomer.csv.gz") \
				.select("CustomerID", "Gender", "DateOfBirth", "Postcode") \
				.filter(col("CustomerID").isNotNull()) \
				.filter(col("CustomerListID") == 2)