# Databricks notebook source
# MAGIC %md
# MAGIC pip install kafka-python

# COMMAND ----------

# MAGIC %md 
# MAGIC pip install Faker

# COMMAND ----------

from kafka import KafkaProducer

# COMMAND ----------

producer = KafkaProducer(bootstrap_servers=['18.188.27.254:9092'])

# COMMAND ----------

from faker import Faker

# COMMAND ----------

fake= Faker()

# COMMAND ----------

def get_registered_user():
    return {
        "name" : fake.name(),
        "address" : fake.address(),
        "created_at" : fake.year()
    }

# COMMAND ----------

if __name__ == '__main__':
    print(get_registered_user())

# COMMAND ----------

for _ in range(1,10):
    print(get_registered_user())

# COMMAND ----------

import json

# COMMAND ----------

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# COMMAND ----------

producer = KafkaProducer(bootstrap_servers=['3.18.82.133:9092'], \
                         value_serializer = json_serializer )

# COMMAND ----------

import time

# COMMAND ----------

if __name__ == '__main__':
    while 1==1:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send("registered_user",registered_user)
      #  time.sleep(4)

# COMMAND ----------

def get_partition(key,all,available):
    return 0


# COMMAND ----------

producer = KafkaProducer(bootstrap_servers=['3.18.82.133:9092'], \
                         value_serializer = json_serializer, partitioner = get_partition )

# COMMAND ----------

if __name__ == '__main__':
    while 1==1:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send("registered_user",registered_user)
        time.sleep(4)

# COMMAND ----------


