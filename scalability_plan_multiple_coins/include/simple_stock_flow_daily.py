# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess # Install the package 
subprocess.run(['pip', 'install', 'yfinance'])

from pyspark.sql.functions import col, lit, current_timestamp, expr, to_date, lag
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import datetime
import yfinance as yfin
from datetime import datetime, timedelta
from datetime import date

spark = SparkSession.builder \
    .appName('spark_gc_to_gc') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")
###########################
BUCKET_NAME= "advanced-de-flows8-bucket"
mydate = spark.sparkContext.getConf().get('spark.executorEnv.mydate')
days_minus = int(spark.sparkContext.getConf().get('spark.executorEnv.days_minus'))
days_more = int(spark.sparkContext.getConf().get('spark.executorEnv.days_more'))

print(f"My date from params is :) {mydate}")

date_string= "20250120"
my_date11=datetime.strptime(mydate, "%Y%m%d")

date_sub=my_date11+timedelta(days=-days_minus)
date_added=my_date11+timedelta(days=days_more)
date_from= str(date_sub.date())
date_to= str(date_added.date())

ticker= 'MXN=X'
dataMXN= yfin.download(tickers=ticker, start= date_from, end= date_to)
dataMXN["date"]= dataMXN.index
exchange_rate=spark.createDataFrame(dataMXN)
exchange_rate_processed=(exchange_rate.withColumnRenamed(exchange_rate.columns[0],f"CLOSEMXN")
 .withColumnRenamed(exchange_rate.columns[2],f"LOW")
 .withColumnRenamed(exchange_rate.columns[5],f"DATE")
 .select(f"CLOSEMXN",f"LOW",f"DATE")
 .withColumn(f"Ticker", lit(ticker) )
 .withColumn("DATE", to_date(col("DATE")))
 )

ticker_list=["NVDA", "COIN","EA", "TTWO", "HOOD", "NU", "GOOG", "EBAY", "MELI", "PYPL", "RBLX", "SQ", "META", "HCA", "V", "BABA", "NFLX", "MSFT"]
for ticker in ticker_list:
    print(ticker)
    data= yfin.download(tickers=ticker, start= date_from, end= date_to)
    data["date"]= data.index
    stocks_df=spark.createDataFrame(data)

    stocks_df_processed=(stocks_df.withColumnRenamed(stocks_df.columns[0],f"CLOSE")
    .withColumnRenamed(stocks_df.columns[2],f"LOW")
    .withColumnRenamed(stocks_df.columns[5],f"DATE")
    .select(f"CLOSE",f"LOW",f"DATE")
    .withColumn(f"Ticker", lit(ticker) )
    .withColumn("DATE", to_date(col("DATE")))
    )

    stocks_df_processed_exch_rate=(stocks_df_processed.join(
    exchange_rate_processed,
    [stocks_df_processed.DATE==exchange_rate_processed.DATE],
    how= "left"
    )
    .select(stocks_df_processed["*"],exchange_rate_processed.CLOSEMXN)
    .withColumn("CLOSE_MXN", col("CLOSE")*col("CLOSEMXN"))
    .withColumn("LOW_MXN", col("LOW")*col("CLOSEMXN"))
    .drop("CLOSEMXN")
    )

    w = Window.orderBy("DATE")

    final_stock_df=(stocks_df_processed_exch_rate
    .withColumn("CLOSE_last_day",
                lag(col("CLOSE"), 1).over(w)
                )
    .withColumn("CLOSE_MXN_last_day",
                lag(col("CLOSE_MXN"), 1).over(w)
                )
    .withColumn("CLOSE_dif", col("CLOSE")/col("CLOSE_last_day")-1)
    .withColumn("CLOSE_MXN_dif", col("CLOSE_MXN")/col("CLOSE_MXN_last_day")-1)
    .drop("CLOSE_last_day", "CLOSE_MXN_last_day")
    )

    final_stock_df.write.format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('table', f"local_dev_stocks.stocks_lz") \
    .mode('append') \
    .save()

    final_stock_df=None
    stocks_df_processed_exch_rate=None
    stocks_df=None

########################
