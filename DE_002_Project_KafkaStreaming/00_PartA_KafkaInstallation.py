# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka Installation

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Kafka Installation

# COMMAND ----------

# MAGIC %sh 
# MAGIC sudo wget https://downloads.apache.org/kafka/3.2.3/kafka_2.12-3.2.3.tgz

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Uncompressing Kafka tgz file

# COMMAND ----------

# MAGIC %sh 
# MAGIC tar -xvf kafka_2.12-3.2.3.tgz

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Initializing Zookeper
# MAGIC 
# MAGIC **Zookeper** will be out centralized manager that helps us to store the metadata information of the consumers, producers, brokers and so on. Also it is used to make the *leadership election* between brokers.

# COMMAND ----------

# MAGIC %sh 
# MAGIC ./kafka_2.12-3.2.3/bin/zookeeper-server-start.sh ./kafka_2.12-3.2.3/config/zookeeper.properties
