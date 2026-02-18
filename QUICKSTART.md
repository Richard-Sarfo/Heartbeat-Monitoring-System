# Quick Reference Guide

## 🚀 One-Command Setup

Everything runs automatically with Docker Compose:

```bash
# Start all services (producer, consumer, dashboard, kafka, db)
docker compose up -d --build
```

**That's it!** Access the dashboard at http://localhost:8501

**That's it!** The producer and consumer are already running in Docker.

## ⚠️ Troubleshooting: "heartbeat_records does not exist"
If you see errors in the consumer logs about missing tables, the database volume was initialized before the schema was applied.

**Fix:** Reset the database volume:
```bash
docker-compose down -v
docker-compose up -d --build
```

## 📊 Access Points

| Component | URL/Command | Port |
|-----------|-----------|------|
| Kafka UI | http://localhost:8080 | 8080 |
| pgAdmin | http://localhost:5050 | 5050 |
| Streamlit Dashboard | http://localhost:8501 | 8501 |
| PostgreSQL | localhost:5432 | 5432 |
| Zookeeper | localhost:2181 | 2181 |
| Kafka Broker 1 | localhost:9092 | 9092 |
| Kafka Broker 2 | localhost:9094 | 9094 |
| Kafka Broker 3 | localhost:9096 | 9096 |

## 🔍 Quick Checks

**Verify all services running:**
```bash
docker compose ps
# You should see: producer, consumer, kafka1-3, postgres, pgadmin, kafka-ui, zookeeper
```

**Check producer output (generating data):**
```bash
docker compose logs producer -f
# Should show messages being published to Kafka
```

**Check consumer output (storing data):**
```bash
docker compose logs consumer -f
# Should show messages being consumed and inserted into database
```

**Count records in database:**
```bash
docker compose exec postgres psql -U postgres -d heartbeat_db -c "SELECT COUNT(*) FROM heartbeat_records;"
```

## 🧪 Viewing Data

**View individual heartbeat records:**
```bash
docker compose exec postgres psql -U postgres -d heartbeat_db -c \
  "SELECT customer_id, recorded_at, heart_rate, anomaly_level FROM heartbeat_records LIMIT 10;"
```

**Restart services if needed:**
```bash
docker compose down
docker compose up --build
```

## 📊 Database Queries

**Count records by customer:**
```sql
SELECT customer_id, COUNT(*) as total_readings 
FROM heartbeat_records 
GROUP BY customer_id
ORDER BY total_readings DESC;
```

**Find anomalies in last hour:**
```sql
SELECT * FROM critical_anomalies 
WHERE recorded_at >= NOW() - INTERVAL '1 hour';
```

**Avg heart rate by customer:**
```sql
SELECT customer_id, AVG(heart_rate) as avg_hr 
FROM heartbeat_records 
GROUP BY customer_id;
```

## 🐛 Debugging

**View all logs:**
```bash
docker compose logs -f
```

**View specific service logs:**
```bash
docker compose logs -f producer      # Data generation
docker compose logs -f consumer      # Data processing
docker compose logs -f postgres      # Database
docker compose logs -f kafka1        # Kafka broker
```

**Restart a specific service:**
```bash
docker compose restart producer      # Restart producer
docker compose restart consumer      # Restart consumer
```

**Check service health:**
```bash
docker compose exec postgres pg_isready -U postgres -d heartbeat_db
docker compose exec kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092
```
