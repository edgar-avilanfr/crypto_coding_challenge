import json
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col, lit, avg
from pyspark.sql import Window
from pyspark.sql import SparkSession

spark= SparkSession.builder \
    .appName('spark_to_bq') \
    .getOrCreate()
sc= spark.sparkContext
sc.setLogLevel("WARN")
#######################################
bucket_name= "training-de-workflows8-bucket"
bigquery_table_read = "local_dev.crypto_lz"
bigquery_table_write = "local_dev.crypto_silver"

# Read data from BigQuery
bronzeDf = spark.read.format("bigquery") \
    .option("table", bigquery_table_read) \
    .load()

# Create moving average using a window function
window_spec= Window.partitionBy('ID').orderBy('date').rowsBetween(-5, Window.currentRow)
window_spec2= Window.partitionBy('ID').orderBy('date').rowsBetween(-20, Window.currentRow)
silverDf= (bronzeDf.withColumn('moving_avg_5', avg(col('price')).over(window_spec))
           .withColumn('moving_avg_20', avg(col('price')).over(window_spec2))
)


print("Writing table...")
my_query_final=(silverDf
                .write
                .format("bigquery")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option('temporaryGcsBucket', bucket_name)
                .option('table', bigquery_table_write)
                .save()
)
print("Writing Done :)")