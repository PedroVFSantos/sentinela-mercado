import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, round, year, month, day
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import redis

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DATA_DIR = os.getenv("SENTINELA_DATA_DIR", "/opt/spark/data")

WINDOW_DURATION = "30 seconds"
WATERMARK_DELAY = "1 minute"
# Chaves somem do Redis se a categoria parar de receber eventos
REDIS_TTL_SECONDS = 120

# 1. Schema das Transações
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("location", StringType(), True)
])

# Grava a receita só se a janela for igual ou mais nova que a já salva,
# para que eventos atrasados (janelas antigas) não sobrescrevam a atual.
# KEYS[1] = revenue:<categoria>, KEYS[2] = revenue_window:<categoria>
# ARGV[1] = receita, ARGV[2] = início da janela (epoch), ARGV[3] = TTL
SET_IF_NEWER_WINDOW = """
local current = redis.call('GET', KEYS[2])
if current and tonumber(current) > tonumber(ARGV[2]) then
    return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
return 1
"""

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
set_if_newer_window = redis_client.register_script(SET_IF_NEWER_WINDOW)


def write_to_redis(batch_df, batch_id):
    """Envia ao Redis só a janela mais recente de cada categoria do micro-batch."""
    latest = {}
    for row in batch_df.collect():
        start = row["window"]["start"]
        category = row["category"]
        if category not in latest or start > latest[category][0]:
            latest[category] = (start, row["total_revenue"])

    pipe = redis_client.pipeline(transaction=False)
    for category, (start, revenue) in latest.items():
        set_if_newer_window(
            keys=[f"revenue:{category}", f"revenue_window:{category}"],
            args=[revenue, int(start.timestamp()), REDIS_TTL_SECONDS],
            client=pipe,
        )
    pipe.execute()
    print(f"✅ [Redis] Batch {batch_id} sincronizado ({len(latest)} categorias).")


if __name__ == "__main__":
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
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
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
    # Watermark + modo update: só janelas alteradas saem em cada micro-batch
    # e o estado de janelas antigas é descartado (não cresce para sempre).
    windowed_stats = transactions_df \
        .withWatermark("timestamp", WATERMARK_DELAY) \
        .groupBy(window(col("timestamp"), WINDOW_DURATION), col("category")) \
        .agg(round(sum("price"), 2).alias("total_revenue"))

    query_redis = windowed_stats.writeStream \
        .foreachBatch(write_to_redis) \
        .outputMode("update") \
        .option("checkpointLocation", f"{DATA_DIR}/checkpoints/redis_revenue") \
        .start()

    # --- LÓGICA 2: BATCH LAYER (DELTA LAKE) ---
    query_delta = transactions_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{DATA_DIR}/checkpoints/market_delta") \
        .partitionBy("year", "month", "day") \
        .start(f"{DATA_DIR}/raw/market_delta")

    # Mantém os dois streams ativos
    spark.streams.awaitAnyTermination()
