from pyspark.sql import SparkSession
import sys, time

start = time.time()
spark = SparkSession.builder.appName("q2-sql").getOrCreate()
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )

input_format = sys.argv[1]

if input_format == 'csv':
        df = spark.read.format('csv').options(header = 'false', inferSchema = 'true'). \
                load("hdfs://master:9000/files/ratings.csv")
else:
        df = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

df.registerTempTable("ratings")

sqlString = \
        "select (rating_users.users_r)*100/all_users.users_all as Percentage "+\
        "from (select count(distinct _c0) as users_all " + \
        "from ratings " +\
        ")as all_users, " +\
        "(select count(distinct sum_table.id) as users_r " + \
        "from " + \
        "(select (_c0) as id, count(_c3) as number_reviews, sum(_c2) as sum_reviews " + \
        "from ratings " + \
        "group by _c0 " + \
        ") as sum_table " + \
        "where sum_table.sum_reviews/sum_table.number_reviews > 3) as rating_users"


res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')