from pyspark.sql import SparkSession
import requests

spark = SparkSession.builder \
    .appName("CoinCapAPI") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "admin123")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

BASE_URL = 'https://rest.coincap.io/v3'
HEADERS = {
    'Authorization': 'Bearer e0bd6941eb5cf15c756be46b494c982248ccbc3ab4ff976d9b905c1067994e63'
}

def get_assets(limit=10):
    URL=f"{BASE_URL}/assets"
    params={"limit": limit}
    return requests.get(URL, headers=HEADERS, params=params).json()

assets_json = get_assets()
df = spark.createDataFrame(assets_json["data"])

df.write.mode('overwrite').parquet("s3a://raw/assets/")

spark.stop()