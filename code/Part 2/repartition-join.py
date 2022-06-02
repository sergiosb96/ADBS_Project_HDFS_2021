from pyspark.sql import SparkSession
import csv, time
from io import StringIO

start_time = time.time()

spark=SparkSession.builder.appName("repartition-join").getOrCreate()
sc=spark.sparkContext

R_PATH = "hdfs://master:9000/files/movie-genres-100.csv"
L_PATH = "hdfs://master:9000/files/ratings.csv"

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def combines(key, list_of_values):
        length = int(len(list_of_values))
        b_r = []
        b_l = []
        for i in range(length):
                elem = list_of_values[i]
                if(elem[0] == 'R'):
                        b_r.append(elem[1:])
                elif(elem[0] == 'L'):
                        b_l.append(elem[1:])
        combined=[]
        for r in b_r:
                for l in b_l:
                        combined.append((key,(r+l)))
        return combined

R_data = \
        sc.textFile(R_PATH). \
        map(lambda row: split_complex(row)). \
        map(lambda x : (x[0],[('R', x[1])] ) )

L_data = \
        sc.textFile(L_PATH). \
        map(lambda row: split_complex(row)). \
        map(lambda x: (x[1],[('L', (x[0], x[2], x[3]) )]))

res = L_data.union(R_data). \
        reduceByKey(lambda x,y: x + y ). \
        flatMap(lambda x: combines(x[0], x[1]))


print(res.collect())
print("Total time: {} sec".format(time.time()-start_time))
