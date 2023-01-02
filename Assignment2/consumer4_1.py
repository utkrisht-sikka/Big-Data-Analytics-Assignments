from json import loads  
from kafka import KafkaConsumer  
from pymongo import MongoClient  
import datetime
from kafka.structs import TopicPartition
from math import radians, cos, sin, asin, sqrt
#Reference https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html

# generating the Kafka Consumer  
my_consumer = KafkaConsumer(  
      
     bootstrap_servers = ['localhost : 9092'],  
     group_id="test-consumer-group",
     value_deserializer = lambda x : loads(x.decode('utf-8'))  ,
     auto_offset_reset = 'earliest',
     max_partition_fetch_bytes = 20971520,
     enable_auto_commit = True
     )  
 
my_consumer.assign([TopicPartition('testnum', 1)])      
 
co = 0
for message in my_consumer:  
    val = message.value
    #query 6 computation
    if(val[3] > 40):
        # q6.append(val)
        # print(val)
        co+=1
        print(val)

 
 
 
 
 
 






