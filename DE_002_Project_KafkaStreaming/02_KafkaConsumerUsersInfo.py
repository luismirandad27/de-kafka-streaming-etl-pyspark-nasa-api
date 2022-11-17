# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install --upgrade pip
# MAGIC pip install kafka-python

# COMMAND ----------

# importing libraries
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.types import *
import pyspark.sql.functions as f

# COMMAND ----------

dfStream = spark.readStream.format("kafka").\
option("kafka.bootstrap.servers", "localhost:9092").\
option("subscribe", "users_info").\
option("startingOffsets", "latest").\
load()

# COMMAND ----------

display(dfStream)

# COMMAND ----------


