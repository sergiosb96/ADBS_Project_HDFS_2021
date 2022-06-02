from pyspark.sql import SparkSession
import sys, time

start = time.time()
spark = SparkSession.builder.appName("q4-sql").getOrCreate()
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )

input_format = sys.argv[1]

if input_format == 'parquet':
        movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
        movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
else:
        movie_genres = spark.read.format('csv').options(header = 'false', inferSchema = 'true'). \
                load("hdfs://master:9000/files/movie_genres.csv")
        movies = spark.read.format('csv').options(header = 'false', inferSchema = 'true'). \
                load("hdfs://master:9000/files/movies.csv")

movies.registerTempTable("movies")

movie_genres.registerTempTable("movie_genres")

sqlString = \
        "select sum(length(movies._c2) - length(replace(movies._c2, ' ', ''))+1)/count(movies._c0) as Length, " + \
        "CASE "+\
        "when year(movies._c3) >= 2000 and year(movies._c3)<=2004 then '2000 - 2004' " +\
        "when year(movies._c3) >= 2005 and year(movies._c3)<=2009 then '2005 - 2009' " +\
        "when year(movies._c3) >= 2010 and year(movies._c3)<=2014 then '2010 - 2014' " +\
        "when year(movies._c3) >= 2015 and year(movies._c3)<=2019 then '2015 - 2019' " +\
        "END as Year " + \
        "from movies, movie_genres " +\
        "where movies._c0 = movie_genres._c0 and movie_genres._c1 = 'Drama' and year(movies._c3)>=2000 " + \
        "group by Year " + \
        "order by Year"

res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')