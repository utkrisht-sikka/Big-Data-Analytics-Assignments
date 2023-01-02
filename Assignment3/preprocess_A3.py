import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import to_timestamp
from datetime import datetime
import pickle
 
   
spark = SparkSession.builder.master("local").appName("hdfs_test").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
routes = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/routes.txt")
stops = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/stops.txt")
trips = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/trips.txt")
stop_times = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/stop_times.txt")
fare_attributes = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/fare_attributes.txt")
fare_rules = spark.read.option("header","true").csv("hdfs:///user/test/GTFS/fare_rules.txt")
from pyspark.sql.types import IntegerType
# print(stop_times.show(5))

trips = trips.withColumn("route_id", trips["route_id"].cast(IntegerType()))
routes = routes.withColumn("route_id", routes["route_id"].cast(IntegerType()))
stops = stops.withColumn("stop_id", stops["stop_id"].cast(IntegerType()))
stops = stops.withColumn("stop_lat", stops["stop_lat"].cast(DoubleType()))
stops = stops.withColumn("stop_lon", stops["stop_lon"].cast(DoubleType()))
fare_rules = fare_rules.withColumn("route_id", fare_rules["route_id"].cast(IntegerType()))
stop_times = stop_times.withColumn("stop_id", stop_times["stop_id"].cast(IntegerType()))
stop_times = stop_times.withColumn("stop_sequence", stop_times["stop_sequence"].cast(IntegerType()))
stop_times = stop_times.withColumn("arrival_time", to_timestamp(stop_times["arrival_time"]))
stop_times = stop_times.withColumn("departure_time", to_timestamp(stop_times["departure_time"]))
fare_attributes = fare_attributes.withColumn("price",  fare_attributes["price"].cast(IntegerType()))

stops.createOrReplaceTempView("df")
trips.createOrReplaceTempView("df2")
routes.createOrReplaceTempView("df3")
stop_times.createOrReplaceTempView("df4")
fare_rules.createOrReplaceTempView("df6")
fare_attributes.createOrReplaceTempView("df7")
print("data frames colummns")
print(routes)
print(stops)
print(trips)
print(stop_times)
print(fare_attributes)
print(fare_rules)

df5 = spark.sql("Select route_id,stop_id from df2,df4 where df2.trip_id = df4.trip_id")
df8 = spark.sql("Select route_id, price from df6,df7 where df6.fare_id=df7.fare_id")
print("AAAA")
print(df8.head(5))
print("AAAA")
df8.createOrReplaceTempView("df8")
df9 = spark.sql("Select route_id,max(price) as prices from df8 group by route_id order by route_id")

varx = spark.sql("Select stop_lat,stop_lon from df where stop_id = 0")
print(varx.head(1)[0].stop_lat)

stop_for_query1_lat = spark.sql("Select stop_lat,stop_lon from df where stop_id = 572").head(1)[0].stop_lat
stop_for_query1_lng = spark.sql("Select stop_lat,stop_lon from df where stop_id = 572").head(1)[0].stop_lon
stop_for_query2_lat = spark.sql("Select stop_lat,stop_lon from df where stop_id = 469").head(1)[0].stop_lat
stop_for_query2_lng =  spark.sql("Select stop_lat,stop_lon from df where stop_id = 469").head(1)[0].stop_lon
dict_for_stops_lat={}
varx = stops.collect()
 
for i in range(len(varx)):
    val = varx[i]
    id_val = val.stop_id
    dict_for_stops_lat[id_val] = [val.stop_lat,val.stop_lon]

  
    
df_new = spark.sql("Select trip_id,stop_id,stop_sequence from df4")
df_new.createOrReplaceTempView("df_new")
print('a2')
df_new2 = spark.sql("Select route_id,trip_id from df2")
df_new2.createOrReplaceTempView("df_new2")
print('a3')
df_new3 = spark.sql("Select route_id,stop_id,stop_sequence from df_new,df_new2 where df_new.trip_id = df_new2.trip_id")
df_new3.createOrReplaceTempView("df_new3")
print('a3')
df_new4 = spark.sql("Select route_id,max(stop_sequence) as mp from df_new3 group by route_id")
df_new4.createOrReplaceTempView("df_new4")
print('a3')
df_new5 = spark.sql("Select df_new4.route_id,stop_id,mp from df_new3,df_new4 where df_new4.mp = df_new3.stop_sequence and df_new4.route_id = df_new3.route_id")
df_new5.createOrReplaceTempView("df_new5")
print('a3')
df_new6 = spark.sql("Select route_id,stop_lat,stop_lon from df_new5,df where df_new5.stop_id = df.stop_id")
df_new7 = spark.sql("Select route_id from df_new3 where stop_id = 4054 and stop_sequence = 0")
df_new7 = df_new7.drop_duplicates()

 


routes_for_query6 = set(list(df_new7.select("route_id").rdd.flatMap(lambda x: x).collect()))
df_new6 = df_new6.drop_duplicates()
dest_lat_lng = {}

varx = df_new6.collect()
for i in range(len(varx)):
    val = varx[i]
    dest_lat_lng[val.route_id] = [val.stop_lat,val.stop_lon]

   
route_to_prices={}
varx = df9.collect()
for i in range(len(varx)):
    val1 = varx[i];
    route_to_prices[val1.route_id] = val1.prices
# print(route_to_prices)



#saving variables
print("saving variables")
df5.write.mode("overwrite").option("header","true").csv("file:///home/vagrant/df5")
df8.write.mode("overwrite").option("header","true").csv("file:///home/vagrant/df8")
df9.write.mode("overwrite").option("header","true").csv("file:///home/vagrant/df9")


# oo = open("/home/vagrant/test.sav",'wb')
# pickle.dump(stop_for_query1_lat, oo)
# print(stop_for_query1_lat)
# oo = open("/home/vagrant/test.sav",'rb')
# m=pickle.load(oo)
# print(m)

pickle.dump(stop_for_query1_lat, open("/home/vagrant/stop_for_query1_lat.sav",'wb'))

pickle.dump(stop_for_query1_lng, open("/home/vagrant/stop_for_query1_lng.sav",'wb'))

pickle.dump(stop_for_query2_lat, open("/home/vagrant/stop_for_query2_lat.sav",'wb'))

pickle.dump(stop_for_query2_lng, open("//home/vagrant/stop_for_query2_lng.sav",'wb'))

pickle.dump(dict_for_stops_lat, open("/home/vagrant/dict_for_stops_lat.sav",'wb'))

pickle.dump(routes_for_query6, open("/home/vagrant/routes_for_query6.sav",'wb'))
pickle.dump(dest_lat_lng, open("/home/vagrant/dest_lat_lng.sav",'wb'))
pickle.dump(route_to_prices, open("/home/vagrant/route_to_prices.sav",'wb'))

 
    
    
