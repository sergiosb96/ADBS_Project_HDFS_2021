from pyspark.sql import SparkSession
import datetime, sys, time
from pyspark.sql.functions import year, month, dayofmonth

start = time.time()
spark = SparkSession.builder.appName("q1-sql").getOrCreate()

sc = spark.sparkContext

input_format = sys.argv[1]

if input_format == 'csv':
        movies = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movies.csv")
else:
        movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
movies.registerTempTable("movies")

sqlString = \
        "select (_c1) as Name,year(_c3) as Year, (((movies._c6)-(movies._c5))*100/(movies._c5)) as Profit " + \
        "from movies " + \
        "join "+ \
        "(select year(movies._c3) as Year,  max(((movies._c6)-(movies._c5))*100/(movies._c5)) as Profit " + \
        "from movies "+ \
        "where year(movies._c3)>=2000 and movies._c5>0 and movies._c6>0 "+ \
        "group by year(movies._c3) "+ \
        "order by year(movies._c3) desc " + \
        ") MaxProfit "+ \
        "where MaxProfit.Year = year(movies._c3) and MaxProfit.Profit = ((movies._c6)-(movies._c5))*100/(movies._c5) and movies._c5>0 and movies._c6>0 "+ \
        "order by year(movies._c3) "

res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')