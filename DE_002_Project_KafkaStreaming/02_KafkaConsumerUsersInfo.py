# Databricks notebook source
# MAGIC %md
# MAGIC # Creating a Kafka Consumer with Python

# COMMAND ----------

# importing libraries
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, LongType
from pyspark.sql.types import *
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Structure

# COMMAND ----------

data_structure = StructType([
        StructField("links",StructType([
                                StructField("self",StringType(),True)
                            ])),
        StructField("id", StringType(), True),
        StructField("neo_reference_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("nasa_jpl_url", StringType(), True),
        StructField("absolute_magnitude_h", DoubleType(), True),
        StructField("estimated_diameter", StructType([
                                            StructField("kilometers", StructType([
                                                                        StructField("estimated_diameter_min", DoubleType(), True),
                                                                        StructField("estimated_diameter_max", DoubleType(), True)
                                                                      ])),
                                            StructField("meters", StructType([
                                                                        StructField("estimated_diameter_min", DoubleType(), True),
                                                                        StructField("estimated_diameter_max", DoubleType(), True)
                                                                      ])),
                                            StructField("miles", StructType([
                                                                        StructField("estimated_diameter_min", DoubleType(), True),
                                                                        StructField("estimated_diameter_max", DoubleType(), True)
                                                                      ])),
                                            StructField("feet", StructType([
                                                                        StructField("estimated_diameter_min", DoubleType(), True),
                                                                        StructField("estimated_diameter_max", DoubleType(), True)
                                                                      ]))
                                        ])),
        StructField("is_potentially_hazardous_asteroid", BooleanType(), True),
        StructField("close_approach_data", ArrayType(
                                                StructType([
                                                    StructField("close_approach_date", StringType(), True),
                                                    StructField("close_approach_date_full", StringType(), True),
                                                    StructField("epoch_date_close_approach", LongType(), True),
                                                    StructField("relative_velocity", StructType([
                                                                                            StructField("kilometers_per_second",StringType(),True),
                                                                                            StructField("kilometers_per_hour",StringType(),True),
                                                                                            StructField("miles_per_hour",StringType(),True)
                                                                                        ])),
                                                    StructField("miss_distance", StructType([
                                                                                            StructField("astronomical",StringType(),True),
                                                                                            StructField("lunar", StringType(), True),
                                                                                            StructField("kilometers", StringType(), True),
                                                                                            StructField("miles", StringType(), True)
                                                                                        ]))
                                                ])
                                            )),
        StructField("is_sentry_object", BooleanType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Array with the final columns

# COMMAND ----------

final_columns = [
                f.col("F.json_data.id").cast("int").alias("object_id"),
                f.col("F.json_data.neo_reference_id").cast("int").alias("object_neo_reference_id"),
                f.col("F.json_data.name").alias("object_name"),
                f.col("F.json_data.absolute_magnitude_h").alias("absolute_magnitude_h"),
                f.col("F.json_data.estimated_diameter").alias("estimated_diameter"),
                f.col("F.json_data.is_potentially_hazardous_asteroid").alias("is_potentially_hazardous_asteroid"),
                f.col("F.json_data.close_approach_data").alias("close_approach_data"),
                f.col("F.json_data.is_sentry_object").alias("is_sentry_object")
]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading from Kafka

# COMMAND ----------

dfStream = spark.readStream.format("kafka")\
                            .option("kafka.bootstrap.servers","localhost:9092")\
                            .option("subscribe","neowstopic")\
                            .option("startingOffsets","latest")\
                            .load().alias("S")\
                            .select(f.col("S.value").cast("string")).alias("J")\
                            .withColumn("json_data",f.from_json(f.col("J.value"),data_structure))\
                            .alias("F")\
                            .select(final_columns)

# COMMAND ----------

display(dfStream)

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:///FileStore/output/dfRealTimeNeoWs

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing from Stream

# COMMAND ----------

dfStream.\
    writeStream.\
    format("parquet").\
    outputMode("append").\
    option("checkpointLocation", "dbfs:///FileStore/output/dfRealTimeNeoWs/_checkpoints/dfRealTimeNeoWs").\
    trigger(processingTime = "5 seconds").\
    start("dbfs:///FileStore/output/dfRealTimeNeoWs").\
    awaitTermination()
