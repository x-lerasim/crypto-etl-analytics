from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("crypto_dim").getOrCreate()

df = spark.read.parquet("s3a://core/dim_assets/")

df.write \
  .format("clickhouse") \
  .option("host", "clickhouse") \
  .option("port", "8123") \
  .option("database", "crypto") \
  .option("table", "dim_assets") \
  .option("user", "admin") \
  .option("password", "admin") \
  .mode("append") \
  .save()



spark.stop()
