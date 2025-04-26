# Crypto pipeline challenge

This is the result of the coding challenge which builds a data pipeline by retrieving prices from CoinGecko API.

# Question from 1 to 7 using Databricks:
In order to be able to use pyspark and save the results in my Unity Catalog, I chose to use it, so the files I attached can be simply uploaded into databicks and be run (We should use the .dbc files), but I also included the .ipynb files for you to be able to visualize it

# * questions1_to_7_using_databricks/questions1_to_4.ipynb
1.- I conected to the CoinGecko API by providing a free tier API, in order to get the list of available coins, we can use:     'https://api.coingecko.com/api/v3/coins/list' API

2.- 'requests.get' command will connect to the API, and json.dumps will convert it into json format for later to be converted on a spark dataframe

3.- After displaying the results, we can check out dataframe by applying a where function in order to get the Bitcoin id:
  all_coins.where(col('name')=='Bitcoin').select('id').show()

# * questions1_to_7_using_databricks/question_5_6_databricks.ipynb
1.- We can use the 'https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range' API in order to retrieve a range of values

2.- I wrapped it into a function in order to  later use it for retrieving several values.

Note: I can only retrieve data from the past 360 days, so I chose a time under this range

