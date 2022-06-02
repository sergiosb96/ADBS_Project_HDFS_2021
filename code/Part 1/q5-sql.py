from pyspark.sql import SparkSession
import sys, time

start = time.time()
spark = SparkSession.builder.appName("q5-sql").getOrCreate()
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

movies.registerTempTable("movies")

movie_genres.registerTempTable("movie_genres")

ratings.registerTempTable("ratings")

sqlString = \
        "select Table2.Genre as Genre, first(Table2.User) as User, first(Table2.Ratings) as Ratings, first(Table1.movie) as Favorite_Movie, first(max_rating) as Max_Rating, first(Table3.movie) as Least_Favorite_Movie, first(min_rating) as Min_Rating " +\
        "from ( " +\
        "select CountTable2.Genre as Genre, CountTable2.user as User, max_count as Ratings, max_rating, min_rating " +\
        "from (select max(Rating_count) as max_count, Genre " +\
        "from (select ratings._c0 as user,movie_genres._c1 as Genre, count(ratings._c0) as Rating_count " +\
        "from ratings, movie_genres "+\
        "where ratings._c1 = movie_genres._c0 "+\
        "group by movie_genres._c1, ratings._c0) as CountTable "+\
        "group by Genre) as MaxTable, (select ratings._c0 as user, movie_genres._c1 as Genre, count(ratings._c0) as Rating_count, max(ratings._c2) as max_rating, min(ratings._c2) as min_rating " +\
        "from ratings, movie_genres "+\
        "where ratings._c1 = movie_genres._c0 "+\
        "group by movie_genres._c1, ratings._c0) as CountTable2 " +\
        "where CountTable2.Rating_count=MaxTable.max_count and CountTable2.Genre = MaxTable.Genre "+\
        "order by Genre asc) as Table2, "+\
        "(select ratings._c0 as user, ratings._c1 as id, movie_genres._c1 as genre, movies._c1 as movie, ratings._c2 as rating, movies._c7 as popularity " +\
        "from ratings, movies, movie_genres " +\
        "where ratings._c1 = movies._c0 and movies._c0 = movie_genres._c0 " +\
        "order by user desc, rating desc, popularity desc) as Table1, " +\
        "(select ratings._c0 as user, ratings._c1 as id, movie_genres._c1 as genre, movies._c1 as movie, ratings._c2 as rating, movies._c7 as popularity " +\
        "from ratings, movies, movie_genres " +\
        "where ratings._c1 = movies._c0 and movies._c0 = movie_genres._c0 " +\
        "order by user desc, rating desc, popularity desc) as Table3 " +\
        "where Table2.User = Table1.user and Table2.User = Table3.user and Table2.max_rating = Table1.rating and Table2.min_rating = Table3.rating and Table2.Genre = Table1.genre and Table2.Genre = Table3.genre " +\
        "group by Table2.Genre "+\
        "order by Table2.Genre asc"

res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')