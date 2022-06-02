from pyspark.sql import SparkSession
from io import StringIO
import csv, time

spark = SparkSession.builder.appName('q1-rdd').getOrCreate()
sc = spark.sparkContext

def reader(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

start = time.time()

res = \
        sc.textFile("hdfs://master:9000/files/movies.csv"). \
        map(lambda x: (reader(x))). \
        filter(lambda x: (x[3]!="" and int(x[3].split('-')[0]) >= 2000 and x[5]!='0' and x[6]!='0')). \
        map(lambda x: (x[3].split('-')[0], (x[1], (float(x[6])-float(x[5]))*100/float(x[5])))). \
        reduceByKey(lambda x, y: x if x[1]>y[1] else y). \
        sortByKey(ascending=True)



print("Year|Movie Title              |Profit")

for i in res.collect():
        print(i[0] + '|{:<25}|'.format(str(i[1][0])) + str(i[1][1]))

end = time.time()
print('Completed in ' + str(end-start) + ' seconds')