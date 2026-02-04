from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date, lit
import requests

spark = SparkSession.builder \
    .appName("raw") \
    .getOrCreate()

BASE_URL = 'https://rest.coincap.io/v3'
HEADERS = {
    'Authorization': 'Bearer e0bd6941eb5cf15c756be46b494c982248ccbc3ab4ff976d9b905c1067994e63'
}

def get_assets():
    URL=f"{BASE_URL}/assets"
    return requests.get(URL, headers=HEADERS).json() 

assets_json = get_assets()

api_timestamp = assets_json["timestamp"]
data = assets_json["data"]

df = spark.createDataFrame(data)
df = df.withColumn('timestamp', lit(api_timestamp))
df = df.withColumn('load_datetime', from_unixtime(col('timestamp')/1000))
df = df.withColumn('load_date', to_date(col('load_datetime')))

df.write.mode('append').partitionBy('load_date').parquet("s3a://raw/assets/")


spark.stop()