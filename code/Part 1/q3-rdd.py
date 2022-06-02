from pyspark.sql import SparkSession
from io import StringIO
import csv, time

spark = SparkSession.builder.appName('q3-rdd').getOrCreate()
sc = spark.sparkContext

start = time.time()

ratings = \
	sc.textFile("hdfs://master:9000/files/ratings.csv"). \
	map(lambda x: (x.split(',')[1], (float(x.split(',')[2]), 1))). \
	reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
	map(lambda x: (x[0], (x[1][0]/x[1][1], x[1][1])))

movie_genres = \
	sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
	map(lambda x: (x.split(',')[0], x.split(',')[1])).distinct()

res = movie_genres.join(ratings).\
	map(lambda x: (x[1][0], (float(x[1][1][0]), 1))). \
	reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
	map(lambda x: (x[0], (x[1][0]/x[1][1], x[1][1]))). \
	sortByKey(ascending=True)

print("Genre            |Average Rating        |Number of Movies")

for i in res.collect():
	print('{:<17}|'.format(str(i[0])) + '{:<22}'.format(str(i[1][0])) + '|{}'.format(str(i[1][1])))

end = time.time()
print('Completed in ' + str(end-start) + ' seconds')