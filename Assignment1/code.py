import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import to_timestamp
import kudu
from kudu.client import Partitioning
from datetime import datetime

print(spark)
'''
spark = SparkSession.builder.master("local").appName("hdfs_test").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
routes = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/routes.txt")
stops = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/stops.txt")
trips = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/trips.txt")
stop_times = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/stop_times.txt")
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
v2.show( )
v3 = v2.withColumn("diff", v2["maxtimesec"] - v2["mintimesec"])
v3.show()
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
'''