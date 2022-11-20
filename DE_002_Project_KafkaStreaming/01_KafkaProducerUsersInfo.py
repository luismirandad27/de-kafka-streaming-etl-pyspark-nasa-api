# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Creating a Kafka Producer with Python

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Installing `kafka-python` library

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install --upgrade pip
# MAGIC pip install kafka-python

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Importing needed libraries

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
from time import sleep

# importing KafkaProducer
from kafka import KafkaProducer

# COMMAND ----------

# MAGIC %md
# MAGIC #### About this Producer
# MAGIC 
# MAGIC I'm using the **NeoWS (Near Earth Object Web Service)** API to get the near earth Asteroids information.<br>
# MAGIC **Link**: https://api.nasa.gov/
# MAGIC 
# MAGIC For this case, the producer will call the API once (from a range of N days of information) and every 2 seconds the process will send each item (asteriod info - info date) to the Kafka server.
# MAGIC 
# MAGIC This will try to simulate a real time process for now :)

# COMMAND ----------

#Making the API Call in a user function
def calling_neows_api(start_date_str, end_date_str):
    
    #API url
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date_str}&end_date={end_date_str}&api_key=DEMO_KEY"
    
    #making API GET call
    response = requests.request("GET",url)
    
    return response.text

# COMMAND ----------

#Sending Responses to Kafka
def send_near_objects(objects_json, start_date_str, end_date_str, producer):
    
    start_date_dt = datetime.strptime(start_date_str,"%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date_str,"%Y-%m-%d")
    
    delta = (end_date_dt - start_date_dt).days
    
    index = 1
    
    for i in range(0,delta+1):
        
        current_date_dt = start_date_dt + timedelta(days=i)
        current_date_str = current_date_dt.strftime("%Y-%m-%d")
        
        asteroids = objects_json[current_date_str]
        
        for asteroid in asteroids:
            
            asteroid['current_date'] = current_date_str
            
            print(f"Sending element {index} to Kafka")
            
            data_asteroid = json.dumps(asteroid)
            
            #sending element to Kafka
            producer.send("neowstopic",data_asteroid.encode())
            
            sleep(10)
            
            index += 1

# COMMAND ----------

def init(start_date_str, end_date_str):
    
    #Creating a new Kafka server
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    
    response = calling_neows_api(start_date_str,end_date_str)
    response_json = json.loads(response)
    objects_elements = response_json["near_earth_objects"]
    
    send_near_objects(objects_elements, start_date_str, end_date_str, producer)
    
    producer.flush()

# COMMAND ----------

start_date_str = "2022-11-11"
end_date_str = "2022-11-17"

#Calling main method
init(start_date_str, end_date_str)
