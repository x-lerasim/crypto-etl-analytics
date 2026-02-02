from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,to_date, from_unixtime
spark = SparkSession.builder \
    .appName("core") \
    .getOrCreate()

df = spark.read.parquet("s3a://raw/assets/")
dim_df = df.select(
            col('id').cast('string').alias('assets_id'),
            col('symbol').cast('string'),
            col('name').cast('string').alias('asset_name'),
            col('rank').cast('int'),
            col('priceUsd').cast('decimal(18,8)').alias('price_usd'),
            col('changePercent24Hr').cast('decimal(18,8)').alias('change_24h'),
            col('marketCapUsd').cast('decimal(18,8)').alias('market_cap'),
            to_date(from_unixtime(col('date')/1000)).alias('load_dt')
        )
dim_df=dim_df.filter(
                col('price_usd').isNotNull() & (col('price_usd')>0)
                    )
dim_df.write.mode('overwrite').partitionBy('load_dt').parquet("s3a://core/dim_assets/")

spark.stop()