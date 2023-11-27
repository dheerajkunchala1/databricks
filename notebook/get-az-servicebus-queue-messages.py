# Databricks notebook source
pip install azure-servicebus

# COMMAND ----------

from azure.servicebus import ServiceBusClient,ServiceBusMessage,ServiceBusSender,ServiceBusReceiver

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Send Messages to a queue

# COMMAND ----------

CONNECTION_STR = "Endpoint=sb://sbdheeraj911.servicebus.windows.net/;SharedAccessKeyName=default1;SharedAccessKey=4T/3nU9AEwgesQmKfBBeNrhSQ5wd6rIi2+ASbIcxqgo=;EntityPath=vehicletolldata"

# COMMAND ----------

QUEUE_NAME = "vehicletolldata"

# COMMAND ----------

def send_single_message(sender):
    message = ServiceBusMessage("Single Message")
    sender.send_messages(message)
    print("Send a single message")

# COMMAND ----------

def send_a_list_of_messages(sender):
    messages = [ServiceBusMessage("Message in list" + str(i)) for i in range(5)]
    sender.send_messages(messages)
    print("Send a list of messages")

# COMMAND ----------

def send_batch_of_messages(sender):
    batch_message = sender.create_message_batch()
    for i in range(10):
        try:
            batch_message.addmessage(ServiceBusMessage("Messages inside a ServiceBusMessage"))
        except ValueError:
            break;
    sender.send_messages(batch_message)
    print("Sent a batch of 10 messages")

# COMMAND ----------

servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR,logging_enable=True)
with servicebus_client:
    sender = servicebus_client.get_queue_sender(queue_name = QUEUE_NAME)

# COMMAND ----------

servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR,logging_enable=True)

# COMMAND ----------

sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)

# COMMAND ----------

print(sender)

# COMMAND ----------

with sender:
    send_single_message(sender)
    send_a_list_of_messages(sender)
print('done sending messages')

# COMMAND ----------

# MAGIC %md
# MAGIC # Recieve Messages from a queue

# COMMAND ----------

servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR,logging_enable=True)

# COMMAND ----------

sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME,max_wait_time = 5)

# COMMAND ----------

receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME)
with receiver:
    for msg in receiver:
        print("Received" + str(msg))
        receiver.complete_message(msg)

# COMMAND ----------

# MAGIC %md
# MAGIC # Send messages in a topic

# COMMAND ----------

from azure.servicebus import ServiceBusClient,ServiceBusMessage,ServiceBusSender,ServiceBusReceiver,ServiceBusMessageBatch

# COMMAND ----------

connection_string = "Endpoint=sb://servicebus-demo707.servicebus.windows.net/;SharedAccessKeyName=policy11;SharedAccessKey=p781qHG+0n5FZPuBb+0AH2q9Z07VM2811+ASbNZK438=;EntityPath=servicebusdemotopic11"

# COMMAND ----------

topic_name  = 'servicebusdemotopic11'

# COMMAND ----------

sub_name = 'servicebussub'

# COMMAND ----------

def send_single_message(sender):
    message = ServiceBusMessage("Single Message")
    sender.send_messages(message)
    print("Send a single message")

# COMMAND ----------

def send_a_list_of_messages(sender):
    messages = [ServiceBusMessage("Message in list" + str(i)) for i in range(5)]
    sender.send_messages(messages)
    print("Send a list of messages")

# COMMAND ----------

def send_batch_of_messages(sender):
    batch_message = sender.create_message_batch()
    for i in range(10):
        try:
            batch_message(ServiceBusMessage("Messages inside a ServiceBusMessage"))
        except ValueError:
                break;
    sender.send_messages(batch_message)
    print("Sent a batch of 10 messages")

# COMMAND ----------

servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_string,logging_enable=True)

# COMMAND ----------

sender = servicebus_client.get_topic_sender(topic_name=topic_name)

# COMMAND ----------

with sender:
    send_single_message(sender)
    send_a_list_of_messages(sender)
    send_batch_of_messages(sender)
print('done sending messages')

# COMMAND ----------

# MAGIC %md
# MAGIC #Receive message from a subscription

# COMMAND ----------

connection_string = "Endpoint=sb://sbdheeraj911.servicebus.windows.net/;SharedAccessKeyName=default1;SharedAccessKey=4T/3nU9AEwgesQmKfBBeNrhSQ5wd6rIi2+ASbIcxqgo=;EntityPath=vehicletolldata"

# COMMAND ----------

servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_string,logging_enable=True)

# COMMAND ----------

topic_name  = 'vehicletolldata'

# COMMAND ----------

sub_name = 'vehicle_subscriber1'

# COMMAND ----------

receiver = servicebus_client.get_subscription_receiver(topic_name=topic_name,subscription_name=sub_name)

# COMMAND ----------

with receiver:
    for msg in receiver:
        print("Received" + str(msg))
        receiver.complete_message(msg)

# COMMAND ----------

with receiver:
    for message in receiver.fetch_all():
            print(message.body)

# COMMAND ----------


