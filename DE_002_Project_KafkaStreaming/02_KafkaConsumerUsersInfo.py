# Databricks notebook source
# MAGIC %md
# MAGIC # Creating a Kafka Consumer with Python

# COMMAND ----------

# importing libraries
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.types import *
import pyspark.sql.functions as f

# COMMAND ----------

dfStream = spark.readStream.format("kafka").\
option("kafka.bootstrap.servers", "localhost:9092").\
option("subscribe", "usersinfotopic").\
option("startingOffsets", "latest").\
load()

# COMMAND ----------

display(dfStream)
