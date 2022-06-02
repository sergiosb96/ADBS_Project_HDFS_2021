from pyspark.sql import SparkSession
import sys, time

start = time.time()
spark = SparkSession.builder.appName("q3-sql").getOrCreate()
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )

input_format = sys.argv[1]

if input_format == 'parquet':
        movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
        movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
        ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
else:
        movies = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movies.csv")
        movie_genres = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movie_genres.csv")
        ratings = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/ratings.csv")

ratings.registerTempTable("ratings")

movie_genres.registerTempTable("movie_genres")

sqlString = \
        "select genre as Genre, sum(rating_sum/rating_count)/count(distinct id) as Average_Rating, count(distinct id) as Total_Movies " + \
        "from (select RatingTable.id, rating_sum, rating_count, (movie_genres._c1) as genre " + \
        "from (select _c1 as id, sum(_c2) as rating_sum, count(_c2) as rating_count " + \
        "from ratings " + \
        "group by _c1) as RatingTable, movie_genres " + \
        "where movie_genres._c0 = RatingTable.id) as MixedTable " +\
        "group by genre " +\
        "order by Genre asc"



res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')