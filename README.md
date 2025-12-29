# Sentinela Mercado

Real-time anomaly detection for market transactions using distributed streaming architecture.

---

## Architecture

**Bronze**: Python producer streams transactions to Kafka  
**Silver**: PySpark Streaming processes events and writes to Delta Lake  
**Gold**: Batch jobs calculate statistical metrics and detect anomalies  
**Serving**: PostgreSQL stores refined data, visualized in Metabase

---

## Stack

Python (PySpark) • Kafka • Spark • Delta Lake • PostgreSQL • Redis • Docker • Metabase

---

## Anomaly Detection

Uses Z-score to flag suspicious transactions:

$$Z = \frac{X - \mu}{\sigma}$$

Transactions with $Z > 2.0$ are marked as anomalies.

---

## Running

```bash
# Start infrastructure
docker-compose up -d

# Run producer
python apps/producer/main.py

# Process stream
docker exec -it spark-master spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  apps/spark_streaming/stream_processor.py

# Analyze batch
docker exec -it spark-master spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0 \
  apps/batch_analysis/deep_analysis.py

# Dashboard
http://localhost:3000
```

---

## Results

<img width="811" height="637" alt="image" src="https://github.com/user-attachments/assets/549ed876-7f75-4639-a757-973d9b385493" />


---

## Contributing

Follow Conventional Commits: `feat`, `fix`, `chore`, `docs`
