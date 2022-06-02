from pyspark.sql import SparkSession
import csv, time
from io import StringIO

start_time = time.time()

spark=SparkSession.builder.appName("broadcast-join").getOrCreate()
sc=spark.sparkContext

R_PATH = "hdfs://master:9000/files/movie-genres-100.csv"
L_PATH = "hdfs://master:9000/files/ratings.csv"



def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def combines(key, val):
        combined = []
        if BrMap.value.get(key, "-") == "-":
                return combined
        for r in BrMap.value.get(key):
                combined.append((key, (r, val)))
        return combined

broadcast_data = sc.textFile(R_PATH). \
        map(lambda row: split_complex(row)). \
        map(lambda x : (x[0], (x[1]) ) ). \
        groupByKey(). \
        map(lambda x : (x[0], list(x[1]))). \
        collectAsMap()

BrMap = sc.broadcast(broadcast_data)

result = \
        sc.textFile(L_PATH). \
        map(lambda row: split_complex(row)). \
        map(lambda x: ( x[1], (x[0], x[2], x[3]) )). \
        flatMap(lambda x: combines(x[0],x[1]))

print(result.collect())
print("Total time: {} sec".format(time.time()-start_time))