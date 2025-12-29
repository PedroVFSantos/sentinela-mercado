from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# 1. Sessão Spark com Delta habilitado
spark = SparkSession.builder \
    .appName("SentinelaMaintenance") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Caminhos das tabelas no Delta Lake
TABLES = [
    "/opt/spark/data/raw/market_delta",
    "/opt/spark/data/gold/anomalies"
]

print("Starting Lakehouse Maintenance...")

for path in TABLES:
    print(f"Processing table at: {path}")
    
    # Instancia a tabela Delta
    deltaTable = DeltaTable.forPath(spark, path)
    
    # 1. VACUUM: Remove arquivos com mais de 168 horas (7 dias) por padrão
    # Para testes, podemos forçar a limpeza imediata (dry run = false)
    print("- Running VACUUM (cleaning orphan files)...")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    deltaTable.vacuum(0) # CUIDADO: vacuum(0) remove TUDO que não é da versão atual
    
    # 2. OPTIMIZE: Compacta arquivos pequenos em arquivos maiores (melhora leitura)
    # No Delta OSS, usamos o comando SQL para isso
    print("- Running OPTIMIZE (compacting files)...")
    spark.sql(f"OPTIMIZE delta.`{path}`")

print("✅ Maintenance finished. Your Arch Linux disk thanks you!")
spark.stop()
