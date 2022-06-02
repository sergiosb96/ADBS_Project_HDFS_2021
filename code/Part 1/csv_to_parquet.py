from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CsvToParquet").getOrCreate()


df_movies = spark.read.option("header","false").option("inferSchema","true").csv("hdfs://master:9000/files/movies.csv")
df_genres = spark.read.option("header","false").option("inferSchema","true").csv("hdfs://master:9000/files/movie_genres.csv")
df_ratings = spark.read.option("header","false").option("inferSchema","true").csv("hdfs://master:9000/files/ratings.csv")


df_movies.write.parquet("hdfs://master:9000/files/movies.parquet")
df_genres.write.parquet("hdfs://master:9000/files/movie_genres.parquet")
df_ratings.write.parquet("hdfs://master:9000/files/ratings.parquet")