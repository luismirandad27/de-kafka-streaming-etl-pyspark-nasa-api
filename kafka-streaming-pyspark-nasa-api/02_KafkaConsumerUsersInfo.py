# Databricks notebook source
# MAGIC %md
# MAGIC # Creating a Kafka Consumer with Python

# COMMAND ----------

# importing libraries
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, LongType
from pyspark.sql.types import *
import pyspark.sql.functions as f
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setting Spark Configurations with Amazon S3 Bucket

# COMMAND ----------

spark.conf.set("fs.s3a.access.key", str(os.environ['AWS_ACCESS_KEY']))
spark.conf.set("fs.s3a.secret.key", str(os.environ['AWS_SECRET_ACCESS_KEY']))
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

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
        StructField("is_sentry_object", BooleanType(), True),
        StructField("current_date",StringType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Array with the final columns

# COMMAND ----------

final_columns = [
                f.col("F.json_data.current_date").alias("date"),
                f.col("F.json_data.id").cast("int").alias("object_id"),
                f.col("F.json_data.neo_reference_id").cast("int").alias("object_neo_reference_id"),
                f.col("F.json_data.name").alias("object_name"),
                f.col("F.json_data.absolute_magnitude_h").alias("absolute_magnitude_h"),
                f.col("F.json_data.estimated_diameter.kilometers.estimated_diameter_min").alias("estimated_diameter_min_km"),
                f.col("F.json_data.estimated_diameter.kilometers.estimated_diameter_max").alias("estimated_diameter_max_km"),
                f.col("F.json_data.is_potentially_hazardous_asteroid").alias("is_potentially_hazardous_asteroid"),
                f.col("F.json_data.close_approach_data.close_approach_date_full")[0].alias("close_approach_date_full"),
                f.col("F.json_data.close_approach_data.relative_velocity.kilometers_per_second")[0].cast('double').alias("relative_velocity_km_per_sec"),
                f.col("F.json_data.close_approach_data.relative_velocity.kilometers_per_hour")[0].cast('double').alias("relative_velocity_km_per_hour"),
                f.col("F.json_data.close_approach_data.miss_distance.astronomical")[0].cast('double').alias("miss_distance_astronomical"),
                f.col("F.json_data.close_approach_data.miss_distance.lunar")[0].cast('double').alias("miss_distance_lunar"),
                f.col("F.json_data.close_approach_data.miss_distance.kilometers")[0].cast('double').alias("miss_distance_kilometers"),
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
                            .option("failOnDataLoss","false")\
                            .load().alias("S")\
                            .select(f.col("S.value").cast("string")).alias("J")\
                            .withColumn("json_data",f.from_json(f.col("J.value"),data_structure))\
                            .alias("F")\
                            .select(final_columns)

# COMMAND ----------

display(dfStream)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing from Stream to AWS S3 Bucket

# COMMAND ----------

dfStream.\
    writeStream.\
    format("parquet").\
    partitionBy("date").\
    outputMode("append").\
    option("checkpointLocation", "s3://kafkaneowsbucket/kafka_rt_neows/_checkpoints/kafka_rt_neows").\
    trigger(processingTime = "5 seconds").\
    start("s3://kafkaneowsbucket/kafka_rt_neows/").\
    awaitTermination()
