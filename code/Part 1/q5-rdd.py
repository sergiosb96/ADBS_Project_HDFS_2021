from pyspark.sql import SparkSession
from io import StringIO
import csv, time

spark = SparkSession.builder.appName('q5-rdd').getOrCreate()
sc = spark.sparkContext

def reader(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

start = time.time()

movie_genres = \
	sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
	map(lambda x: (x.split(',')[0], x.split(',')[1])).distinct()


movies = \
	sc.textFile("hdfs://master:9000/files/movies.csv"). \
	map(lambda x: (reader(x))). \
	map(lambda x: (x[0], (x[1], float(x[7]))))


movie_info = movie_genres.join(movies).\
	map(lambda x: (x[0], (x[1][0], x[1][1][0], x[1][1][1])))


ratings = \
	sc.textFile("hdfs://master:9000/files/ratings.csv"). \
	map(lambda x: (x.split(',')[1], (x.split(',')[0], float(x.split(',')[2]))))


user = movie_info.join(ratings).\
	map(lambda x: ((x[1][0][0], x[1][1][0]), (1, x[1][0][1], x[1][1][1], x[1][0][2], x[1][0][1], x[1][1][1], x[1][0][2]))). \
	reduceByKey(lambda x, y: (x[0]+y[0], x[1] if x[2]>y[2] else (y[1] if y[2]>x[2] else (x[1] if x[3]>y[3] else y[1])),
		x[2] if x[2]>y[2] else (y[2] if y[2]>x[2] else (x[2] if x[3]>y[3] else y[2])),
		x[3] if x[2]>y[2] else (y[3] if y[2]>x[2] else (x[3] if x[3]>y[3] else y[3])),
		x[4] if x[5]<y[5] else (y[4] if y[5]<x[5] else (x[4] if x[6]>y[6] else y[4])),
		x[5] if x[5]<y[5] else (y[5] if y[5]<x[5] else (x[5] if x[6]>y[6] else y[5])),
		x[6] if x[5]<y[5] else (y[6] if y[5]<x[5] else (x[6] if x[6]>y[6] else y[6])))). \
	map(lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1], x[1][2], x[1][4], x[1][5]))). \
	reduceByKey(lambda x, y: (x[0], x[1], x[2], x[3], x[4], x[5]) if x[1]>y[1] else (y[0], y[1], y[2], y[3], y[4], y[5])). \
	sortByKey(ascending=True)


print("Genre            |# of Ratings|UserID|Max Rated Movie          |Max Rating|Min Rated Movie                           |Min Rating")

for i in user.collect():
	print('{:<17}|'.format(str(i[0])) + '{:<12}|'.format(str(i[1][0])) + '{:<6}|'.format(str(i[1][1]))+'{:<25}|'.format(str(i[1][2])) + '{:<10}|'.format(str(i[1][3])) + '{:<42}|'.format(str(i[1][4])) +'{:<10}'.format(str(i[1][5])))

end = time.time()
print('Completed in ' + str(end-start) + ' seconds')