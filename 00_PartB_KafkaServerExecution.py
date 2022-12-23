# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka Server Execution
# MAGIC 
# MAGIC Because the zookeper is running on one notebook, I'm creating a new one to run the kafka server

# COMMAND ----------

# MAGIC %sh 
# MAGIC ./kafka_2.12-3.2.3/bin/kafka-server-start.sh ./kafka_2.12-3.2.3/config/server.properties
