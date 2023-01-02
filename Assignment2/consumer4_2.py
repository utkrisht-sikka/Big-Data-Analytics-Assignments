from json import loads  
from kafka import KafkaConsumer  
from pymongo import MongoClient  
import datetime
from kafka.structs import TopicPartition
from math import radians, cos, sin, asin, sqrt
import signal
# Reference https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
# handler borrowed from https://code-maven.com/catch-control-c-in-python
def handler(signum, frame):
   # Printing result of query 7
    print("The result on for 7th query is : ")
    print(count_route)
    exit(0)
 
signal.signal(signal.SIGINT, handler)

# generating the Kafka Consumer  
my_consumer = KafkaConsumer(  
      
     bootstrap_servers = ['localhost : 9092'],  
     group_id="test-consumer-group",
     value_deserializer = lambda x : loads(x.decode('utf-8'))  ,
     auto_offset_reset = 'earliest',
     max_partition_fetch_bytes = 20971520,
     enable_auto_commit = True
     )  
 
my_consumer.assign([TopicPartition('testnum2', 0)])      
 
#Variables for query 7 
routes_time = {}
count_route={}
 

co = 0
for message in my_consumer:  
    val = message.value
    #query 7 computation
    if val[4] in routes_time:
        if val[0] not in routes_time[val[4]]:
            count_route[val[4]]+=1
            routes_time[val[4]].add(val[0])
            
    else:
        routes_time[val[4]] = set([val[0]])
        count_route[val[4]] = 1
    # here count_route stores the count

 

 
 
 
 






