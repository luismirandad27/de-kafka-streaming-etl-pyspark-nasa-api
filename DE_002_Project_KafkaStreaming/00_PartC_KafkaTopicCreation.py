# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka Topic Creation
# MAGIC 
# MAGIC Because our Zookeper server and Kafka server are running on another notebooks, we have to use another notebook to create the Kafka Topic.

# COMMAND ----------

# MAGIC %sh
# MAGIC ./kafka_2.12-3.2.3/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic neowstopic --partitions 1 --replication-factor 1
