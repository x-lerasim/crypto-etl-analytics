from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
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
timestamp = assets_json["timestamp"]
df = spark.createDataFrame(assets_json["data"])
df = df.withColumn("load_dt", lit(timestamp))

df.write.mode('overwrite').partitionBy('load_dt').parquet("s3a://raw/assets/")

spark.stop()