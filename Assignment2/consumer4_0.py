from json import loads  
from kafka import KafkaConsumer  
from pymongo import MongoClient  
import datetime
from kafka.structs import TopicPartition
from math import radians, cos, sin, asin, sqrt
# Reference https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
# Reference https://www.geeksforgeeks.org/program-distance-two-points-earth/
# generating the Kafka Consumer  
my_consumer = KafkaConsumer(  
     
     bootstrap_servers = ['localhost : 9092'],  
     group_id="test-consumer-group",
     value_deserializer = lambda x : loads(x.decode('utf-8'))  ,
     auto_offset_reset = 'earliest',
     max_partition_fetch_bytes = 20971520,
     enable_auto_commit = True
     )  
print('a1')
my_consumer.assign([TopicPartition('testnum', 0)])
# my_client = MongoClient('localhost', 27017)  
print('a2')
# my_collection = my_client.my-replicated-topic.my-replicated-topic  
print('a3')
def distance(lat1, lat2, lon1, lon2):
     
    # The math module contains a function named
    # radians which converts from degrees to radians.
    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
      
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
 
    c = 2 * asin(sqrt(a))
    
    # Radius of earth in kilometers. Use 3956 for miles
    r = 6371
      
    # calculate the result
    return(c * r)
rvs = {}#vehicle, speed
useless_counter = 0;
'''
Index information:
0-> vehicle_id, 1->lat,2-> lng,3-> speed,4->route_id,5->timestamp,6->time
'''
#Variables for query 7 
routes_time = {}
count_route={}
given_time7 = "00:00:16";
#variables for query5
min_dist = 10**18
ans=[]
#variables for query 4:
avg_speed_route={}
#variables for query 3:
max_route = [0,0]#here we will have 2 elements first will be max_speed second route number
min_route=[10,0]#here we will have 2 elements first will be min_speed second route number
routes_with_buses={}
routes_dict={}
#common debugging variables
co = 0
deb = 0
ite=0
arr = 0
q6 = []
for message in my_consumer:  
    ite+=1
    # print('a4')
    # print( message.value)
    mval = message.value
    for val in mval:
        arr+=1
        # print('val')
        #query 2 computation
        val[4] = int(val[4])
        if(val[4] in rvs):
            if(rvs[val[4]][1] < val[3]):
                rvs[val[4]] = ( val[0], val[3])
        else:
            rvs[val[4]] = ( val[0], val[3])
        #query 6 computation
        if( val[3] > 40):
            q6.append(val)
        #query 7 computation
        if val[6] == given_time7:
            if val[4] in routes_time:
                if val[0] not in routes_time[val[4]]:
                    count_route[val[4]]+=1
                    routes_time[val[4]].add(val[0])
                    
            else:
                routes_time[val[4]] = set([val[0]])
                count_route[val[4]] = 1
        # here count_route stores the count
        #query 5 computation
        elem = 1613932200.0
        hrs=int(val[6][:2])
        mins = int(val[6][3:5])
        secs = int(val[6][6:8])
    
        elem_timestamp = elem+hrs*3600+mins*60+secs
        if(elem_timestamp>1613986200.0 and elem_timestamp<1613989800.0):
        # if(elem_timestamp>1613932200.0 and elem_timestamp<1613989800.0):
            useless_counter+=1
            lat1 = float(val[1])
            lng1 = float(val[2])
            lat2 = 28.5459
            lng2 = 77.2732
            if distance(lat1,lat2,lng1,lng2)<min_dist:
                min_dist = distance(lat1,lat2,lng1,lng2)
                ans = [val[0]]
            elif distance(lat1,lat2,lng1,lng2)==min_dist:
                ans.append(val[0])
        # here ans stores the final result


        
        #query 4 computation
        
        if(elem_timestamp>1613975400.0 and elem_timestamp<1613979000.0):
        # if(elem_timestamp>1613932200.0 and elem_timestamp<1613989800.0):

            useless_counter+=1
            if val[4] in avg_speed_route:
                avg_speed_route[val[4]][0]+=float(val[3])
                avg_speed_route[val[4]][1]+=1
            else:
                avg_speed_route[val[4]] = [float(val[3]),1]
        # here avg_speed_route stores the final result
        #query3 computation
        if val[5]=="1613984417":
            if val[3]>max_route[0]:
                max_route = [val[3],val[4]]
            if val[3]<min_route[0]:
                min_route = [val[3],val[4]]
        if val[4] in routes_dict:
            if val[0] not in routes_dict[val[4]]:
                routes_dict[val[4]].add(val[0])
                routes_with_buses[val[4]].append([val[0],val[1],val[2]])
        else:
            routes_dict[val[4]] = set([val[0]])
            routes_with_buses[val[4]] = [[val[0],val[1],val[2]]]
    if ite>=361:
        break
    print('arr')
    print(arr)

print('arr')
print(arr)
print()
print()
# Printing result of query 2
print("The result on for 2nd query is : ")
print(rvs)
print()
print()
# Printing result of query 3
print("The result on for 3rd query is : ")
print(max_route,min_route)
print(routes_with_buses[max_route[1]])
print(routes_with_buses[min_route[1]])

print()
print()
# Printing result of query 4
print("The result on for 4th query is : ")
for i in avg_speed_route:
    print("route avgspeed:"+str(i)+ " "+str(avg_speed_route[i][0]/avg_speed_route[i][1]))

print()
print()
# Printing result of query 5
print("The result on for 5th query is : ")
print("Useless_counter",useless_counter)
# print(ans)
print(set(ans))
print()
print()
# Printing result of query 6
print("The result on for 6th query is : ")
print(len(q6))
print(q6)
print()
print()
# Printing result of query 7
print("The result on for 7th query is : ")
print(count_route)
 






