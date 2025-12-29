from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, round, year, month, day
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import redis

# 1. Schema das Transações
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("location", StringType(), True)
])

# 2. Sessão Spark Tunada (RocksDB + Delta Lake)
spark = SparkSession.builder \
    .appName("SentinelaLakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.stateStore.providerClass", 
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Leitura do Kafka (Interface Interna)
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "market_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transformação Base
transactions_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", day(col("timestamp")))

# --- LÓGICA 1: SPEED LAYER (REDIS) ---

windowed_stats = transactions_df \
    .groupBy(window(col("timestamp"), "30 seconds"), col("category")) \
    .agg(round(sum("price"), 2).alias("total_revenue"))

def write_to_redis(batch_df, batch_id):
    r = redis.Redis(host='redis', port=6379, db=0)
    records = batch_df.collect()
    for row in records:
        key = f"revenue:{row['category']}"
        r.set(key, row['total_revenue'])
    print(f"✅ [Redis] Batch {batch_id} sincronizado.")

query_redis = windowed_stats.writeStream \
    .foreachBatch(write_to_redis) \
    .outputMode("complete") \
    .start()

# --- LÓGICA 2: BATCH LAYER (DELTA LAKE) ---

query_delta = transactions_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/data/checkpoints/market_delta") \
    .partitionBy("year", "month", "day") \
    .start("/opt/spark/data/raw/market_delta")

# Mantém os dois streams ativos
spark.streams.awaitAnyTermination()
