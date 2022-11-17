# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Creating a Kafka Producer with Python

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install --upgrade pip
# MAGIC pip install kafka-python

# COMMAND ----------

# importing libraries
import requests
import json

# importing KafkaProducer
from kafka import KafkaProducer

# COMMAND ----------

response = requests.get("https://randomuser.me/api/")
json_record = json.loads(response.content)
data = json.dumps(json_record)
print(data)

# COMMAND ----------

#Creating a new Kafka server
producer = KafkaProducer(bootstrap_servers="localhost:9092",api_version=(0,11,5))

#Encoding the record and send it to the topic
producer.send("users_info", data.encode())

#Sending data
producer.flush()
