from json import loads  
from kafka import KafkaConsumer  
from pymongo import MongoClient  
import datetime
from kafka.structs import TopicPartition
from math import radians, cos, sin, asin, sqrt
import numpy as np
import signal
import pandas as pd
import os
import pickle
from geopy.distance import geodesic

# handler borrowed from https://code-maven.com/catch-control-c-in-pythonbre
def handler(signum, frame):
    print('arr')
    print(arr)
    print()
    print()
    #query 1 results
    print()
    print()
    print("Query 1 results are given below: ")
    print("Answer of first query is : ")
    print(len(final_set))
    print(final_set)
    #query 2 results

    print()
    print()
    print("Query 2 results are given below: ")
    print(vehicle_arr)
    print(lat_final,lng_final)
    graph_query2.to_csv('/media/sf_sharee/graph_query2.csv')
    #query 3 results:
    print()
    print()
    print("Query 3 results are given below: ")
    new_arr=[]
    for x,y in stop_to_vehicles.items():
        new_arr.append([x,y])
    new_arr = sorted(new_arr,key = lambda x: - len(x[1]))
    print(new_arr[:5])
    print()
    print()
    print("Useless for query 2 and 3 are :",useless_query2,useless_query3)
    #query 5 results
    print()
    print()
    print("Query 5 results are given below: ")
    print("The final fare collected is : ")
    print(final_fare_collected) 
    #query 4 results
    print()
    print()
    print("Query 4 results are : ")
    print("The vehicles within 1km of there destination are :")
    print(one_km)
    print()
    print()
    print("The vehicles within 2km of there destination are :")
    print(two_km)
    print()
    print()
    print("The vehicles within 3km of there destination are :")
    print(three_km)
    print()
    print()
    graph_query4.to_csv('/media/sf_sharee/graph_query4.csv')
    #query 6 results
    print()
    print()
    print("Query 6 results are : ")
    print("The vehicles whose first stop is 23 are :")
    print(set_of_vehicles_for_query6)
    print("minidq1:"+str(minidq1))
    print("minidq3:"+str(minidq3))
    print("minidq4:"+str(minidq4))
    exit(0)
 
 
signal.signal(signal.SIGINT, handler)
#load dataframes and variables
cmd = 'ls df5 > df5.txt'
os.system(cmd)
cmd = 'ls df8 > df8.txt'
os.system(cmd)
cmd = 'ls df9 > df9.txt'
os.system(cmd)

dfs = {'df5' : [], 'df8': [], 'df9': []}

for  i in ['df5','df8','df9']:
    
    file1 = open(i+".txt",  "r")
    for lin in file1:
        if lin[:-1] == '_SUCCESS':
            break
        else:
            dfs[i].append(pd.read_csv('/home/vagrant/'+i+"/"+lin[:-1]))
df5 = pd.concat(dfs['df5'])
df8 = pd.concat(dfs['df8'])
df9 = pd.concat(dfs['df9'])

# df5 = pd.read_csv('/home/vagrant/df5/part-00000-c47c6a4b-fb1f-4bfa-9e03-937b876e4855-c000.csv')
# df8 = pd.read_csv('/home/vagrant/df8/part-00000-9ec48f71-7bd6-4ec6-9c49-8767f7e67d1b-c000.csv')
# df9 = pd.read_csv('/home/vagrant/df9/data.csv')
print(len(df5))
# print(df5.dtypes)  

''' 
map_stop_to_route={}


for i in range(len(df5)):
    if( i%1000 == 0):
        print(i)
    row = df5.iloc[i]
    val = row["route_id"]
    val2 = row["stop_id"]
    if val in map_stop_to_route:
        map_stop_to_route[val].add(val2)
    else:
        map_stop_to_route[val] = set([val2])
print(map_stop_to_route)    
pickle.dump(map_stop_to_route, open('map_stop_to_route.sav', 'wb'))
''' 
 
map_stop_to_route = pickle.load(open('map_stop_to_route.sav', 'rb'))
# print(map_stop_to_route)

 
for route_idd in map_stop_to_route:
    map_stop_to_route[route_idd] = list(map_stop_to_route[route_idd])
inverse_map_stop_to_route={}
for ab in map_stop_to_route:
    for bc in map_stop_to_route[ab]:
        if bc in inverse_map_stop_to_route:
            inverse_map_stop_to_route[bc].add(ab) 
        else:
            inverse_map_stop_to_route[bc] = set([ab])
# print(sorted(inverse_map_stop_to_route.keys()))
stop_for_query1_lat = pickle.load( open("/home/vagrant/stop_for_query1_lat.sav",'rb'))
# print('stop_for_query1_lat')
# print(stop_for_query1_lat)
stop_for_query1_lng = pickle.load(open("/home/vagrant/stop_for_query1_lng.sav",'rb'))
# print('stop_for_query1_lng')
# print(stop_for_query1_lng)
stop_for_query2_lat = pickle.load( open("/home/vagrant/stop_for_query2_lat.sav",'rb'))
# print('stop_for_query2_lat')
# print(stop_for_query2_lat)
stop_for_query2_lng = pickle.load(open("//home/vagrant/stop_for_query2_lng.sav",'rb'))
# print('stop_for_query2_lng')
# print(stop_for_query2_lng)
 
dict_for_stops_lat = pickle.load( open("/home/vagrant/dict_for_stops_lat.sav",'rb'))
# print('dict_for_stops_lat')
# print(dict_for_stops_lat)
routes_for_query6 = pickle.load( open("/home/vagrant/routes_for_query6.sav",'rb'))
print('routes_for_query6')
print(routes_for_query6)
dest_lat_lng = pickle.load(open("/home/vagrant/dest_lat_lng.sav",'rb'))
# print('dest_lat_lng')
# print(dest_lat_lng)
route_to_prices = pickle.load(open("/home/vagrant/route_to_prices.sav",'rb'))
# print('route_to_prices')
# print(route_to_prices)

 
 

 

#Variables for query 1
stop_num = 0;
lat_stop = 0;lng_stop = 0;
inverse_map_stop_to_route=inverse_map_stop_to_route
final_set={}

#variables for query2
given_time7 = "00:00:16";
lat2 = 0
lng2 = 0
vehicle_arr = []
curr_dist = 10**18
lat_final = 0;lng_final = 0;
#variables for query 3:
stop_to_vehicles={}
map_store_to_route=map_stop_to_route
lat_lng=[1.0,1.0]

#variables for query 5:
final_fare_collected = 0;
vehicle_visietd_route={}
# df9 will also be used to find total fare of a particular route


 
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
def distance(lat3,lat1,lng3,lng1):
   
    # The math module contains a function named
    # radians which converts from degrees to radians.
    
    return geodesic((lat1,lng1),(lat3,lng3)).m
 
 
 
#Variables for query 1
stop_num = 0;
lat_stop = stop_for_query1_lat;lng_stop = stop_for_query1_lng;
inverse_map_stop_to_route=inverse_map_stop_to_route
final_set=set()

#variables for query2

graph_query2 = pd.DataFrame(columns = ['vehicle_id', 'latitude', 'longitude', 'distance', 'typ'])
given_time7 = "00:00:05";
lat2 = stop_for_query2_lat
lng2 = stop_for_query2_lng
vehicle_arr = []
curr_dist = 10**18
lat_final = 0;lng_final = 0;
 
#variables for query 3:
stop_to_vehicles={}
map_store_to_route=map_stop_to_route
# lat_lng=[1.0,1.0]
dict_for_stops_lat = dict_for_stops_lat

#variables for query 5:
final_fare_collected = 0;
vehicle_visited_route={}
# df9 will also be used to find total fare of a particular route
# variables for query 4:
dest_lat_lng = dest_lat_lng;
one_km = set()
two_km=set()
three_km=set()
graph_query4 = pd.DataFrame(columns = ['vehicle_id', 'latitude', 'longitude', 'distance','km'])
#variables for query 6:
routes_for_query6 = routes_for_query6
set_of_vehicles_for_query6 = set()

#common debugging variables
co = 0
deb = 0
ite=0
arr = 0
q6 = []
useless_query2 = 0;
useless_query3 = 0;
minid = 1e11
minidq4 = 1e11
minidq1 = 1e11
minidq3 = 1e11
for message in my_consumer:  
    ite+=1
 
    mval = message.value
    cc = 1;
    numm = 0
 
    for val in mval:
        numm += 1
        print(numm)
        if(ite==1 and cc==1):
            cc = 0
            # given_time7 = val[6]
        #query 2 computation
        lat1 = float(val[1])
        lng1 = float(val[2])
        dist_val = distance(lat1,lat2,lng1,lng2)
        if val[6] == given_time7:
            
            if(dist_val<curr_dist):
                curr_dist = dist_val
                lat_final = lat1
                lng_final = lng1
                vehicle_arr = set([val[0]])
                graph_query2 = pd.DataFrame(columns = ['vehicle_id', 'latitude', 'longitude', 'distance', 'typ'])

                graph_query2 = graph_query2.append({'vehicle_id' : val[0] , 'latitude' : lat1, 'longitude' : lng1,'distance':dist_val, 'typ':0}, ignore_index = True)

                 
            elif (dist_val==curr_dist):
                lat_final = lat1
                lng_final = lng1
                vehicle_arr.add(val[0])
                graph_query2 = graph_query2.append({'vehicle_id' : val[0] , 'latitude' : lat1, 'longitude' : lng1,'distance':dist_val,'typ':0}, ignore_index = True)


        #query1 computation
        elem = 1613932200.0
        hrs=int(val[6][:2])
        mins = int(val[6][3:5])
        secs = int(val[6][6:8])
        elem_timestamp = elem+hrs*3600+mins*60+secs
        
        if(elem_timestamp>1613975400.0-3600*12 and elem_timestamp<1613979000.0-3600*12):
            # print("deb")
            # print(inverse_map_stop_to_route[572])
            # print(int(val[4]))
            if int(val[4]) in inverse_map_stop_to_route[572]:
                useless_query2+=1
                dist_val2 = distance(lat1,lat_stop,lng1,lng_stop)
                minidq1 = min(minidq1, dist_val2)
                
                if dist_val2<=7000:
                    final_set.add(val[0])
                    # In last we will print size of final_set to show final ans
        # query3 computation
        route_num = val[4]
        if(elem_timestamp>1613975400.0-3600*12 and elem_timestamp<1613979000.0-3600*12):
            
            
            if route_num in map_store_to_route:
                useless_query3+=1
                for stop_d in map_store_to_route[route_num]:
                    minidq3 = min(minidq3, distance(lat1,dict_for_stops_lat[stop_d][0],lng1,dict_for_stops_lat[stop_d][1]))
                    if distance(lat1,dict_for_stops_lat[stop_d][0],lng1,dict_for_stops_lat[stop_d][1])<=3000:
                        if stop_d in stop_to_vehicles:
                            stop_to_vehicles[stop_d].add(val[0])
                        else:
                            stop_to_vehicles[stop_d] = set([val[0]])
        # at last sort stop_to_vehicles according count of length of set and print top 5 stops from that
        #query 5 and 4computation
        if(elem_timestamp>1613975400.0-3600*12 and elem_timestamp<1613979000.0-3600*12):
            route_num = val[4]
            if (route_num in route_to_prices):
                if val[0] in vehicle_visited_route:
                    if route_num != vehicle_visited_route[val[0]]:
                        final_fare_collected+=route_to_prices[route_num]
                        vehicle_visited_route[val[0]]=route_num
                else:
                    vehicle_visited_route[val[0]] = route_num
                    final_fare_collected+=route_to_prices[route_num]
                graph_query5 = pd.DataFrame(columns = ['fare'])
                graph_query5 = graph_query5.append({'fare': route_to_prices[route_num]}, ignore_index=True)
                graph_query5.to_csv('/media/sf_sharee/graph_query5.csv')
            #query 4 starts here
            if (route_num in dest_lat_lng):
                lat3 = dest_lat_lng[route_num][0]
                lng3 = dest_lat_lng[route_num][1]
                dist_new = distance(lat3,lat1,lng3,lng1)
                minidq4 = min(minidq4, dist_new)
                if (dist_new<=1000):
                    one_km.add(val[0]) 
                    graph_query4 = graph_query4.append({'vehicle_id' : val[0] , 'latitude' : lat1, 'longitude' : lng1,'distance':dist_new, 'km':1}, ignore_index = True)
                elif (dist_new<=2000):
                    two_km.add(val[0])
                    graph_query4 = graph_query4.append({'vehicle_id' : val[0] , 'latitude' : lat1, 'longitude' : lng1,'distance':dist_new, 'km':2}, ignore_index = True)
                elif (dist_new<=3000):
                    three_km.add(val[0])
                    graph_query4 = graph_query4.append({'vehicle_id' : val[0] , 'latitude' : lat1, 'longitude' : lng1,'distance':dist_new, 'km':3}, ignore_index = True)
                graph_query4.to_csv('/media/sf_sharee/graph_query4.csv')
        #query 6
        if(route_num in routes_for_query6):
            set_of_vehicles_for_query6.add(val[0])
        


                
           
        
    if ite>=1e9:
        break
    # print('arr')
    # print(arr)

print('arr')
print(arr)
print()
print()
#query 1 results
print()
print()
print("Query 1 results are given below: ")
print("Answer of first query is : ")
print(len(final_set))
print(final_set)
#query 2 results

print()
print()
print("Query 2 results are given below: ")
print(vehicle_arr)
print(lat_final,lng_final)
graph_query2.to_csv('/media/sf_sharee/graph_query2.csv')
#query 3 results:
print()
print()
print("Query 3 results are given below: ")
new_arr=[]
for x,y in stop_to_vehicles.items():
    new_arr.append([x,y])
new_arr = sorted(new_arr,key = lambda x: - len(x[1]))
print(new_arr[:5])
print()
print()
print("Useless for query 2 and 3 are :",useless_query2,useless_query3)
#query 5 results
print()
print()
print("Query 5 results are given below: ")
print("The final fare collected is : ")
print(final_fare_collected) 
#query 4 results
print()
print()
print("Query 4 results are : ")
print("The vehicles within 1km of there destination are :")
print(one_km)
print()
print()
print("The vehicles within 2km of there destination are :")
print(two_km)
print()
print()
print("The vehicles within 3km of there destination are :")
print(three_km)
print()
print()
graph_query4.to_csv('/media/sf_sharee/graph_query4.csv')
#query 6 results
print()
print()
print("Query 6 results are : ")
print("The vehicles whose first stop is 4054 are :")
print(set_of_vehicles_for_query6)
 