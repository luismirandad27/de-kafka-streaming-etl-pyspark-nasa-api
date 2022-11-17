# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ### Kafka Installation

# COMMAND ----------

# MAGIC %sh sudo wget https://downloads.apache.org/kafka/3.3.1/kafka_2.12-3.3.1.tgz

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uncompressing Kafka tgz file

# COMMAND ----------

# MAGIC %sh 
# MAGIC tar -xvf kafka_2.12-3.3.1.tgz

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initializing Zookeper
# MAGIC 
# MAGIC Leave some definition of this

# COMMAND ----------

# MAGIC %sh 
# MAGIC ./kafka_2.12-3.3.1/bin/zookeeper-server-start.sh ./kafka_2.12-3.3.1/config/zookeeper.properties

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Creating Kafka Topic

# COMMAND ----------

# MAGIC %sh 
# MAGIC ./kafka_2.12-3.3.1/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic users_info --partitions 1 --replication-factor 1
