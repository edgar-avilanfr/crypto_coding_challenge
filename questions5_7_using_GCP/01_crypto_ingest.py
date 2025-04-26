import requests
from datetime import datetime
import json
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession

spark= SparkSession.builder \
    .appName('spark_to_bq') \
    .getOrCreate()
sc= spark.sparkContext
sc.setLogLevel("WARN")
#######################################

bucket_name= "training-de-workflows8-bucket"
bigquery_table= "local_dev.crypto_lz"

def convert_from_time_to_unix(my_date):
    d_format= '%Y-%m-%d'
    dt = datetime.strptime(my_date, d_format)
    unix_timestamp = int(dt.timestamp())
    return unix_timestamp

def get_price_history_range(from_date, to_date ,coin , api_key):
    from_date_unix=convert_from_time_to_unix(from_date)
    to_date_unix=convert_from_time_to_unix(to_date)

    url = f'https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range'
    params = {
            'id': coin,
            'vs_currency': 'USD',
            'from': from_date_unix,
            'to': to_date_unix,
            #'interval': 'daily' #only exclusive for paid subscribers
    }

    headers = { 'x-cg-demo-api-key': api_key }
    response = requests.get(url, params = params)

    json_data = json.dumps(response.json()['prices'], indent=4)
    dataprep = json.loads(json_data)

    sparkdf = spark.createDataFrame(dataprep, ['date', 'price'])
    sparkdf1=(sparkdf.withColumn('date', from_unixtime(col('date')/1000).cast('date'))
              .withColumn('ID', lit(coin)))
    return sparkdf1

coindf=get_price_history_range('2024-06-01', '2024-10-01','bitcoin', 'CG-HwZEyrLTyMJR2hhdmCvxFurs')

print("Writing table...")
my_query_final=(coindf
                .write
                .format("bigquery")
                .mode("append")
                .option("overwriteSchema", "true")
                .option('temporaryGcsBucket', bucket_name)
                .option('table', bigquery_table)
                .save()
)
print("Writing Done :)")