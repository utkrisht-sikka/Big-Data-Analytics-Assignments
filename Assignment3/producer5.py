from time import sleep  
from json import dumps  
from kafka import KafkaProducer  
import sqlite3
import timeit
# Reference https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html


# Create a SQL connection to our SQLite database
con = sqlite3.connect("bus_movements_2021_08_01-002.db")
cur = con.cursor()

# initializing the Kafka producer  
my_producer = KafkaProducer(  
    bootstrap_servers = ['localhost:9092'],  
    value_serializer = lambda x:dumps(x).encode('utf-8') ,
    acks = 0,
    batch_size = 1e7,
    max_request_size = 2e7,
    compression_type  = 'gzip'
    )  
    
start = timeit.default_timer()
'''    
co = 0
for row in cur.execute('SELECT * FROM vehicle_feed;'):
    my_producer.send('testnum2', value = row)  
    print(row)
    co += 1
    # my_producer.flush()
    # sleep(1e-5) # if <1e-2, then all messages wont appear at consumer
    if ( co >=  1000) :
        break;
'''
co = 0    
rows=[]
given_time7 = "00:00:16";
for row in cur.execute('SELECT vehicle_id, lat,lng,speed,route_id,timestamp,time FROM vehicle_feed'):
    '''
    if(row[3] > 40):
        my_producer.send('testnum', value = row, partition=1) 
        
    if row[6] == given_time7:
        my_producer.send('testnum2', value = row, partition=0)
    '''
    # my_producer.send('testnum', value = row, partition=1) 
    rows.append(row)
    # print(row[0])
    if(len(rows)==1):
        my_producer.send('testnum', value = rows, partition=0) 
        sleep(0.1)
        my_producer.flush()
        rows=[]
        print('co')
        print(co)
        
    co+=1
    if(co == 1000):#36010872):
  
        my_producer.send('testnum', value = rows, partition=0) 
        # sleep(4e-5)
        my_producer.flush()
        rows=[]
        print('co')
        print(co)
        break;
    

con.close()
end = timeit.default_timer()
print(end-start)
  
 
 