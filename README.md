# Crypto pipeline challenge using Databricks and GCP

This is the result of the coding challenge which builds a data pipeline by retrieving prices from CoinGecko API.

## Question from 1 to 7 using Databricks:
In order to be able to use pyspark and save the results in my Unity Catalog, I chose to use it, so the files I attached can be simply uploaded into databicks and be run (we should upload the .dbc files inot your Databricks workspace),  I also included the .ipynb files for you to be able to visualize it.

 * questions1_to_7_using_databricks/questions1_to_4.ipynb
   
1.- I conected to the CoinGecko API by providing a free tier API, in order to get the list of available coins, we can use:     'https://api.coingecko.com/api/v3/coins/list' API

2.- 'requests.get' command will connect to the API, and json.dumps will convert it into json format for later to be converted on a spark dataframe

3.- After displaying the results, we can check out dataframe by applying a where function in order to get the Bitcoin id:
```
  all_coins.where(col('name')=='Bitcoin').select('id').show()
```

 * questions1_to_7_using_databricks/question_5_6_databricks.ipynb
   
1.- We can use the 'https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range' API in order to retrieve a range of values

2.- I wrapped it into a function in order to  later use it for retrieving several values.

Note: I can only retrieve data from the past 360 days, so I chose a time under this range

3.- In this case we just call the function with the params you want and it will take care of the Date encoding:
```python
coindf=get_bitcoin_price_history_range('2024-06-01', '2024-10-01','bitcoin', 'CG-HwZEyrLTyMJR2hhdmCvxFurs')
```

4.- Last step is to save it under you Unity Catalog by running the last piece:
```python
my_query_final=(coindf
                .write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable("my_workspacedb.test_schema2.bitcoin_price_historical")
)
```

 * questions1_to_7_using_databricks/question7_databricks.ipynb
   
1.- This step is to create the moving average on the price by calculating the average over a window of 5 days.

## Question from 5 to 7 using GCP:
 * questions5_7_using_GCP/01_crypto_ingest.py
   
1.- I moved the previous code into a python file in order to later use GCP Dataproc, Bigquery, Airflow and Looker.

2.- I order to run these 2 scripts in Dataproc we need:
- GCP Account
- A Cloud Storage bucket in order to store your .py files
- Dataproc API enabled

3.- Simple run the terminal to commands:
- Create a Dataproc cluster (it contains spark)
```bash
gcloud dataproc clusters create cluster-smallxs2 --enable-component-gateway --region us-central1 --zone us-central1-a --single-node --master-machine-type n2-standard-8 --master-boot-disk-size 37 --image-version 2.2-debian12 --project developer3-457903 --delete-max-idle t6m --public-ip-address
```
- Submit a job
```bash
gcloud dataproc jobs submit pyspark --cluster=cluster-smallxs2 --region=us-central1 gs://training-de-workflows8-bucket/crypto_coins_code/01_crypto_ingest.py
```
- Submit second job to calculate moving average
```bash
gcloud dataproc jobs submit pyspark --cluster=cluster-smallxs2 --region=us-central1 gs://training-de-workflows8-bucket/crypto_coins_code/02_crypto_transform_1.py
```
