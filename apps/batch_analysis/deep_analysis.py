from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count, max, min

spark = SparkSession.builder \
    .appName("SentinelaDeepAnalysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

PATH = "/opt/spark/data/raw/market_delta"
df = spark.read.format("delta").load(PATH)

stats_df = df.groupBy("category").agg(
    count("transaction_id").alias("volume"),
    avg("price").alias("preco_medio"),
    stddev("price").alias("desvio_padrao"),
    max("price").alias("preco_max"),
    min("price").alias("preco_min")
)

df_with_stats = df.join(stats_df, "category")
anomalias = df_with_stats.filter(
    col("price") > (col("preco_medio") + (col("desvio_padrao") * 2))
).select("transaction_id", "category", "price", "location")

stats_df.orderBy(col("volume").desc()).show()
anomalias.show(10)

# Escrita no Delta Lake
anomalias.write.format("delta").mode("overwrite").save("/opt/spark/data/gold/anomalies")
stats_df.write.format("delta").mode("overwrite").save("/opt/spark/data/gold/market_insights")

# Exportação para PostgreSQL (Metabase)
jdbc_url = "jdbc:postgresql://postgres:5432/market_data"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

stats_df.write.jdbc(
    url=jdbc_url, 
    table="gold_market_insights", 
    mode="overwrite", 
    properties=db_properties
)

spark.stop()
