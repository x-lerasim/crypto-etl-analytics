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

def get_assets(limit=10):
    URL=f"{BASE_URL}/assets"
    params={"limit": limit}
    return requests.get(URL, headers=HEADERS, params=params).json() 

assets_json = get_assets()
timestamp = assets_json["timestamp"]
df = spark.createDataFrame(assets_json["data"])
df = df.withColumn("date", lit(timestamp))

df.write.mode('overwrite').parquet("s3a://raw/assets/")

spark.stop()