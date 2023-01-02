#!/usr/bin/env python
# coding: utf-8

# In[83]:


# import pandas as pd
# df = pd.read_csv("file:///opt/spark/notebooks/stop_timesold.txt")
# df.to_csv("yo")


# In[ ]:





# In[ ]:





# In[6]:


import kudu
from kudu.client import Partitioning
from datetime import datetime

# Connect to Kudu master server
client = kudu.connect(host='172.18.0.2', port=7051) # in our case kudu-master if you havent changed anything

# Define a schema for a new table
builder = kudu.schema_builder()
builder.add_column('key').type(kudu.int64).nullable(False).primary_key()
builder.add_column('ts_val', type_=kudu.unixtime_micros, nullable=False, compression='lz4')
schema = builder.build()

# Define partitioning schema
partitioning = Partitioning().add_hash_partitions(column_names=['key'], num_buckets=3)

# Create new table
# client.create_table('python-example2', schema, partitioning)

# Open a table
table = client.table('python-example2')

# Create a new session so that we can apply write operations
session = client.new_session()

# Insert a row
op = table.new_insert({'key': 5, 'ts_val': datetime.utcnow()})
print("t1")
print(table.show())
session.apply(op)

# Upsert a row
op = table.new_upsert({'key': 6, 'ts_val': "2016-01-01T00:00:00.000000"})
session.apply(op)

# Updating a row
op = table.new_update({'key': 5, 'ts_val': ("2017-01-01", "%Y-%m-%d")})
session.apply(op)

# Delete a row
op = table.new_delete({'key': 5})
session.apply(op)
# Flush write operations, if failures occur, capture print them.
try:
   session.flush()
except kudu.KuduBadStatus as e:
   print(session.get_pending_errors())

# Create a scanner and add a predicate
scanner = table.scanner()
scanner.add_predicate(table['ts_val'] == datetime(2017, 1, 1))

# Open Scanner and read all tuples
# Note: This doesn't scale for large scans
result = scanner.open().read_all_tuples()
print(result)


# In[ ]:





# In[ ]:





# In[20]:


session = client.new_session()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[63]:


builder = kudu.schema_builder()
builder.add_column('route_id').type(kudu.string).nullable(False).primary_key()
builder.add_column('route_short_name',type_=kudu.string,nullable = True, compression='lz4')
builder.add_column('route_long_name', type_=kudu.string, nullable=True, compression='lz4')
builder.add_column('route_type', type_=kudu.string, nullable=True, compression='lz4')
builder.add_column('agency_id', type_=kudu.string, nullable=False, compression='lz4')


schema = builder.build()
partitioning = Partitioning().add_hash_partitions(column_names=['route_id'], num_buckets=30)
# client.create_table('kudu_routes', schema, partitioning)
table = client.table('kudu_routes');

df1 = spark.read.option("delimiter",",").option("header",True).csv("hdfs://namenode:9000//routes.txt")
# df1 = spark.read.option("delimiter",",").option("header",True).csv("file:///opt/spark/notebooks/routes.txt")
df1 = df1.select(df1.route_id, df1.route_short_name, df1.route_long_name, df1.route_type,df1.agency_id)
n_rows = df1.count()
obj = df1.collect()
for i in range(n_rows):
    if(i<3):
        print(obj[i])
    op = table.new_insert(obj[i])
    session.apply(op)
try:
   session.flush()
except kudu.KuduBadStatus as e:
   print(session.get_pending_errors())

routes = spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"172.18.0.2:7051").option('kudu.table',"kudu_routes").load()
# kuduDF.createOrReplaceTempView("mock_routes")



# Open a table



# In[56]:


pip install pandas


# In[64]:



routes.show()


# In[ ]:





# In[ ]:





# In[46]:


builder = kudu.schema_builder()
builder.add_column('trip_id').type(kudu.string).nullable(False).primary_key()
builder.add_column('route_id',type_=kudu.string,nullable = True, compression='lz4')
builder.add_column('service_id', type_=kudu.string, nullable=True, compression='lz4')
builder.add_column('shape_id', type_=kudu.string, nullable=True, compression='lz4')


schema = builder.build()
partitioning = Partitioning().add_hash_partitions(column_names=['trip_id'], num_buckets=10)
# client.create_table('kudu_trips', schema, partitioning)
table = client.table('kudu_trips');

df1 = spark.read.option("delimiter",",").option("header",True).csv("hdfs://namenode:9000//trips.txt")
# df1 = spark.read.option("delimiter",",").option("header",True).csv("file:///opt/spark/notebooks/trips.txt")

df1 = df1.select(df1.trip_id, df1.route_id, df1.service_id, df1.shape_id)
n_rows = df1.count()
obj = df1.collect()
for i in range(n_rows):
    if(i<3):
        print(obj[i])
    op = table.new_insert(obj[i])
    session.apply(op)
try:
   session.flush()
except kudu.KuduBadStatus as e:
   print(session.get_pending_errors())

trips = spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"172.18.0.2:7051").option('kudu.table',"kudu_trips").load()
# kuduDF.createOrReplaceTempView("mock_trips")


# In[111]:


# trips.show()


# In[74]:


stops.count()


# In[ ]:





# In[48]:


builder = kudu.schema_builder()
builder.add_column('stop_id').type(kudu.int64).nullable(False).primary_key()
builder.add_column('stop_code',type_=kudu.string,nullable = True, compression='lz4')
builder.add_column('stop_name', type_=kudu.string, nullable=True, compression='lz4')
builder.add_column('stop_lat', type_=kudu.float, nullable=True, compression='lz4')
builder.add_column('stop_lon', type_=kudu.float, nullable=True, compression='lz4')
builder.add_column('zone_id', type_=kudu.int64, nullable=True, compression='lz4')



schema = builder.build()
partitioning = Partitioning().add_hash_partitions(column_names=['stop_id'], num_buckets=10)
# client.create_table('kudu_stops', schema, partitioning)
table = client.table('kudu_stops');

df1 = spark.read.option("delimiter",",").option("header",True).csv("hdfs://namenode:9000//stops.txt")
# df1 = spark.read.option("delimiter",",").option("header",True).csv("file:///opt/spark/notebooks/stops.txt")

df1 = df1   .withColumn("stop_id" ,
              df1["stop_id"]
              .cast('int'))   \
  .withColumn("stop_lat",
              df1["stop_lat"]
              .cast('float'))    \
  .withColumn("stop_lon"  ,
              df1["stop_lon"]
              .cast('float')) \
    .withColumn("zone_id"  ,
              df1["zone_id"]
              .cast('int')) \
#df1 = df1.select(df1.trip_id, df1.route_id, df1.service_id, df1.shape_id)
n_rows = df1.count()
obj = df1.collect()
for i in range(n_rows):
    if(i<3):
        print(obj[i])
    op = table.new_insert(obj[i])
    session.apply(op)
try:
   session.flush()
except kudu.KuduBadStatus as e:
   print(session.get_pending_errors())

stops = spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"172.18.0.2:7051").option('kudu.table',"kudu_stops").load()
# kuduDF.createOrReplaceTempView("mock_stops")


# In[49]:



stops.show()


# In[114]:


# client.delete_table("kudu_stop_times")


# In[108]:


builder = kudu.schema_builder()
builder.add_column('key').type(kudu.string).nullable(False).primary_key()
# builder.add_column('stop_sequence').type(kudu.string).nullable(False)
# builder.add_column('stop_id').type(kudu.string).nullable(False).primary_key()
# builder.add_column('trip_id').type(kudu.string).nullable(False).primary_key()
builder.add_column('trip_id', type_=kudu.string, nullable=True, compression='lz4')



builder.add_column('arrival_time', type_=kudu.string, nullable=True, compression='lz4')
builder.add_column('departure_time', type_=kudu.string, nullable=True, compression='lz4')
builder.add_column('stop_id',type_=kudu.string,nullable = True, compression='lz4')
builder.add_column('stop_sequence',type_=kudu.string,nullable = True, compression='lz4')

schema = builder.build()
partitioning = Partitioning().add_hash_partitions(column_names=['key'], num_buckets=10)
client.create_table('kudu_stop_times', schema, partitioning)
table = client.table('kudu_stop_times');

df1 = spark.read.option("delimiter",",").option("header",True).csv("hdfs://namenode:9000//stop_times.txt")
# df1 = spark.read.option("delimiter",",").option("header",True).csv("file:///opt/spark/notebooks/stop_times.txt")
# df1 = df1.select(, df1.trip_id,  df1.arrival_time,df1.departure_time, df1.stop_id, df1.stop_sequence)
df1.write.format('org.apache.kudu.spark.kudu').option('kudu.master',"172.18.0.2:7051").option('kudu.table',"kudu_stop_times").mode('append').save()
print(23)
try:
   session.flush()
except kudu.KuduBadStatus as e:
   print(session.get_pending_errors())
print(33)
stop_times = spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"172.18.0.2:7051").option('kudu.table',"kudu_stop_times").load()
# stop_times.createOrReplaceTempView("mock_stop_times")
print(43)


# In[109]:


stop_times.count()


# In[ ]:





# In[110]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType
# print(stop_times.show(5))
stop_times = stop_times.withColumn("stop_sequence", stop_times["stop_sequence"].cast(IntegerType()))
stop_times = stop_times.withColumn("arrival_time", to_timestamp(stop_times["arrival_time"]))
stop_times = stop_times.withColumn("departure_time", to_timestamp(stop_times["departure_time"]))

 


# print(stop_times.show(5))
#routes.show(5)
#stops.show(5)
#trips.show(5)

 
print("1.")
rt = routes.join(trips, on=['route_id'])
rt = rt.dropDuplicates(["route_id","trip_id"])
rt.groupBy("route_id").count().orderBy('route_id').show(rt.count())
rts = rt.join(stop_times, on=['trip_id'])
rts = rts.dropDuplicates(["route_id","stop_id"])

rts.groupBy("route_id").count().orderBy('route_id').show(rts.count())


print("2.")
print("a)")
v1 = (stop_times.join(routes.join(trips, on='route_id'), on='trip_id')) .select('route_id',  'stop_id', 'stop_sequence')
v2 = v1.groupBy('route_id').min('stop_sequence')
v2 = v2.withColumnRenamed("min(stop_sequence)", "stop_sequence")
v3 = (v1.join(v2, on=['route_id','stop_sequence'])).select('route_id', 'stop_id')
v3 = v3.dropDuplicates(["route_id","stop_id"])
v4 = (v3.join(stops, on=['stop_id'])).select('route_id', 'stop_name', 'stop_lat', 'stop_lon')
v4.show(v4.count(), truncate = False)


print("b)")
v1 = (stop_times.join(routes.join(trips, on='route_id'), on='trip_id')) .select('route_id',  'stop_id', 'stop_sequence')
v2 = v1.groupBy('route_id').max('stop_sequence')
v2 = v2.withColumnRenamed("max(stop_sequence)", "stop_sequence")
v3 = (v1.join(v2, on=['route_id','stop_sequence'])).select('route_id', 'stop_id')
v3 = v3.dropDuplicates(["route_id","stop_id"])
v4 = (v3.join(stops, on=['stop_id'])).select('route_id', 'stop_name', 'stop_lat', 'stop_lon')
v4.show(v4.count(), truncate = False)




print("3.")
v1 = stop_times.filter(stop_times.stop_id == '469').select('trip_id')
# v1.show() 
v2 = (v1.join(trips, on=['trip_id'])).select('route_id').distinct()
# v2.show()
v3 = (v2.join(trips, on=['route_id'])).select('route_id', 'trip_id')
# v3.show()
v4 = (v3.join(stop_times, on = ['trip_id'])).select('route_id', 'stop_sequence')
# v4.show()
v5 = v4.groupBy('route_id').max('stop_sequence')
v5 = v5.withColumnRenamed("max(stop_sequence)", "maxdistance")
# v5.show()
minseq = v5.agg({'maxdistance': 'min'})
minseq = minseq.withColumnRenamed("min(maxdistance)", "shortestroute")
minvalue = minseq.collect()[0][0]
# print("minvalue"+str(minvalue))

v6 = v5.filter( v5.maxdistance == minvalue).select('route_id')
# v6.show()
v7 = (trips.join(v6, on = ['route_id'])).select('trip_id') 
# v7.show()
v8 =  (stop_times.join(v7, on = ['trip_id'] )).select('trip_id', 'arrival_time')
v8.show()




print("4.")
v1 = stop_times.groupBy("trip_id").agg({"arrival_time":'min', "departure_time" : 'max'})
print(v1.dtypes)
v1.show( )
v2 = v1.withColumnRenamed("min(arrival_time)", "mintime")
v2 = v2.withColumnRenamed("max(departure_time)", "maxtime")
v2 = v2.withColumn("mintimesec", v2["mintime"].cast("long"))
v2 = v2.withColumn("maxtimesec", v2["maxtime"].cast("long"))
# v2.show( )
v3 = v2.withColumn("diff", v2["maxtimesec"] - v2["mintimesec"])
# v3.show()
v4 = v3.orderBy('diff', ascending = False).select('trip_id')
v4.show()
 

print("5.")
t1 = stop_times.filter(stop_times.stop_id == '469').select('trip_id')
# t1.show() 
t2 = (t1.join(trips, on=['trip_id'])).select('route_id').distinct()
# t2.show() 
t3 = (t2.join(trips, on = ['route_id'])).select('trip_id').distinct()
t4 = stop_times.join(t3, on = ['trip_id']).select('stop_id').distinct()

t5 = stops.join(t4, stops.stop_id == t4.stop_id,  "left_anti").select('stop_id').distinct()

t6 = t5.join(stops, on = ['stop_id']).select('stop_name')
t6.show(truncate = False)

 
print('6.')
print(stop_times.dtypes)
v1 = stop_times.filter(stop_times.stop_sequence == 0 ).select('stop_id')
v2 = v1.groupBy("stop_id").count()
# v2.show()
v2 = v2.orderBy('count', ascending = False)
# v2.show()
v3 = v2.limit(3)
# v3.show()
v4 =  (v3.join(stops,  on = ['stop_id'] )).select('stop_id', 'stop_name')
v4.show(v4.count(), truncate = False)
 

