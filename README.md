# Real-Time Customer Heartbeat Monitoring System

## 📋 Project Overview

This is a **real-time data engineering system** that simulates and processes customer heart rate data using Apache Kafka and PostgreSQL. The system demonstrates core concepts of streaming data pipelines, message queuing, real-time processing, and database integration.

### Architecture

```
┌─────────────────────┐
│  Data Generator     │ (Synthetic user data)
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Kafka Producer     │ (Publish to kafka)
└──────────┬──────────┘
           │
        Topic: heartbeat-data
           │
┌──────────▼──────────┐
│  Kafka Consumer     │ (Subscribe & Validate)
└──────────┬──────────┘
           │
         ↓ (Validate & analyse)
┌─────────────────────┐
│  PostgreSQL DB      │ (Store processed data)
└─────────────────────┘
           │
           ↓
┌─────────────────────┐
│  Streamlit Dashboard│ (Visualize data)
└─────────────────────┘
```

---

## 🎯 Learning Objectives

By completing this project, you will understand:

- ✅ Simulating realistic real-time sensor data
- ✅ Streaming data with Apache Kafka (producers & consumers)
- ✅ Real-time data validation and anomaly detection
- ✅ PostgreSQL schema design for time-series data
- ✅ Building a complete end-to-end data pipeline
- ✅ Docker containerization of data systems

---

## 📦 Core Components

### 1. **Data Generator** (`scripts/heartbeat_producer.py`)
- Generates synthetic heart rate data for 10+ customers
- Simulates realistic heart rate patterns (normal, exercise, rest, anomalies)
- Uses Kafka producer to stream data continuously

**Data Fields:**
- `customer_id`: Unique customer identifier
- `timestamp`: ISO format timestamp
- `heart_rate`: Heart rate in beats per minute (30-250 bpm)
- `state`: Customer activity state (normal, exercise, rest, anomaly)

### 2. **Kafka Stream Processing** 
- **Broker Setup:** Multiple Kafka brokers (3 replicas) for fault tolerance
- **Topic:** `heartbeat-data` with high replication factor
- **Zookeeper:** Manages Kafka cluster coordination

### 3. **Data Consumer & Validator** (`scripts/heartbeat_consumer.py`)
- Consumes messages from Kafka topic
- Validates data structure and values
- Detects anomalies using thresholds:
  - **Critical Low:** < 40 bpm (Severe Bradycardia)
  - **Low:** 40-60 bpm (Bradycardia)
  - **Elevated:** 100-160 bpm (Exercise/Stress)
  - **High:** 160-200 bpm (Tachycardia)
  - **Critical High:** > 200 bpm (Severe Tachycardia)
  - **Normal:** 60-100 bpm
- Stores validated data with anomaly flags to PostgreSQL

### 4. **PostgreSQL Database** (`sql/schema.sql`)
- **Main Table:** `heartbeat_records` stores all readings
- **Indexes:** Optimized for customer and time-series queries
- **Views:** Pre-built views for recent data and anomalies
- **Constraints:** Data validation at database level

### 5. **Streamlit Dashboard** (`Dashboard.py`)
- Real-time visualization of heartbeat data
- Customer statistics and anomaly alerts
- Historical trend analysis
- Interactive filters and controls

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Git

### Step 1: Clone & Setup

```bash
cd c:\Users\RichardAnaneSarfo\Desktop\Heartbeat

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Start Docker Services

```bash
docker compose up --build
```

This automatically starts all services:
- ✅ Zookeeper (port 2181)
- ✅ Kafka Brokers (3 replicas on ports 9092-9097)
- ✅ PostgreSQL (port 5432)
- ✅ pgAdmin (port 5050)
- ✅ Kafka UI (port 8080)
- ✅ Producer service (auto-generating data)
- ✅ Consumer service (processing and storing data)

### Step 3: Verify Services

**Check service status:**
```bash
docker compose ps
```
You should see producer and consumer running.

**View producer logs:**
```bash
docker compose logs producer -f
```

**View consumer logs:**
```bash
docker compose logs consumer -f
```

**Check PostgreSQL data:**
```bash
docker compose exec postgres psql -U postgres -d heartbeat_db -c "SELECT COUNT(*) FROM heartbeat_records;"
```

**Access Kafka UI:**
Open http://localhost:8080 in your browser to monitor message flow

### Step 4: View Dashboard

**Run Streamlit locally (in a new terminal):**
```bash
streamlit run Dashboard.py
```

Opens at: http://localhost:8501

---

## 📊 Database Schema

### heartbeat_records Table

```sql
CREATE TABLE heartbeat_records (
    id BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    heart_rate INTEGER NOT NULL,
    anomaly_level VARCHAR(20),
    anomaly_description VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_heart_rate_range CHECK (heart_rate >= 30 AND heart_rate <= 250),
    CONSTRAINT chk_anomaly_level CHECK (
        anomaly_level IN ('normal', 'elevated', 'low', 'high', 'critical_low', 'critical_high')
    )
);
```

### Indexes for Performance

- `idx_customer_id` → Query by customer
- `idx_recorded_at` → Time-series queries
- `idx_customer_time` → Customer + time range queries
- `idx_anomaly_level` → Find critical anomalies

### Useful Views

```sql
-- Recent heartbeat for each customer
SELECT * FROM recent_heartbeats;

-- All critical anomalies
SELECT * FROM critical_anomalies;
```

---

## 🔧 Configuration

### Database Connection
Default credentials (in docker-compose.yml):
```
host: localhost
port: 5432
database: heartbeat_db
user: postgres
password: postgres
```

Customize in environment variables if needed.

### Kafka Brokers
```
Broker 1: localhost:9092
Broker 2: localhost:9094
Broker 3: localhost:9096

Internal: kafka1:9093, kafka2:9095, kafka3:9097
```

### Data Generation
Edit `scripts/heartbeat_producer.py` to change:
- Number of customers: `num_customers=10`
- Message frequency: `time.sleep(1)` → adjust interval
- Heart rate ranges

---

## 🧪 Testing & Verification

### Query Sample Data

```bash
# Simple way - check record count
docker compose exec postgres psql -U postgres -d heartbeat_db -c \
  "SELECT COUNT(*) as total_records FROM heartbeat_records;"

# View recent records
docker compose exec postgres psql -U postgres -d heartbeat_db -c \
  "SELECT customer_id, recorded_at, heart_rate, anomaly_level FROM heartbeat_records ORDER BY recorded_at DESC LIMIT 10;"
```

### Monitor Kafka Messages

Using Kafka UI (http://localhost:8080):
1. Navigate to **Topics** → `heartbeat-data`
2. View messages in real-time
3. Monitor consumer groups

### Check Consumer Lag

```bash
docker compose exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --group heartbeat-consumer --describe
```

---

## 📁 Project Structure

```
Heartbeat/
├── scripts/
│   ├── heartbeat_producer.py      # Data generator + Kafka producer
│   └── heartbeat_consumer.py      # Consumer + validation + DB insert
├── sql/
│   └── schema.sql                 # PostgreSQL schema
├── Dashboard.py                   # Streamlit visualization
├── docker-compose.yml             # Docker services definition
├── Dockerfile                     # Container image for Python apps
├── requirements.txt               # Python dependencies
├── test_system.py                 # Testing script
└── README.md                      # This file
```

---

## 🛠️ Troubleshooting

### Docker Issues

**Error: "docker Desktop Linux Engine not found"**
- Solution: Start Docker Desktop from Windows Start Menu

**Error: "Port already in use"**
- Solution: Change port mappings in docker-compose.yml

### Kafka Issues

**Producer can't connect to broker**
```bash
# Check Kafka is running
docker compose ps

# Check broker logs
docker compose logs kafka1
```

**Topic not created**
```bash
# Create topic manually
docker compose exec kafka1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic heartbeat-data \
  --partitions 3 \
  --replication-factor 3
```

### PostgreSQL Issues

**Connection refused**
```bash
# Check PostgreSQL is running
docker compose logs postgres

# Test connection
docker compose exec postgres psql -U postgres -d heartbeat_db -c "SELECT 1;"
```

**Table not created**
```bash
# Check schema loaded
docker compose exec postgres psql -U postgres -d heartbeat_db -c "\dt"

# Manually run schema
docker compose exec postgres psql -U postgres -d heartbeat_db < sql/schema.sql
```

---

## 📈 Performance Metrics

### Expected Throughput
- **Producer:** ~10 messages/second (configurable)
- **Consumer:** Handles ~100+ messages/second
- **Database:** Inserts ~1000 records/second

### Resource Usage
- Kafka brokers: ~200-300 MB RAM each
- PostgreSQL: ~100-200 MB RAM
- Dashboard: ~100 MB

---

## 🔐 Security Considerations

For production deployment:

- ✅ Change default PostgreSQL password
- ✅ Enable SASL/SSL for Kafka
- ✅ Restrict network access with firewalls
- ✅ Use secrets management for credentials
- ✅ Enable audit logging

---

## 📚 Documentation Files

- **docker-compose.yml** → Infrastructure as Code
- **Dockerfile** → Container specifications
- **schema.sql** → Database design
- **scripts/heartbeat_producer.py** → Data generation logic
- **scripts/heartbeat_consumer.py** → Real-time processing
- **Dashboard.py** → Visualization logic

---

## 🎓 Learning Resources

### Kafka Concepts
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- Producers, Consumers, Brokers, Topics, Partitions, Replication

### PostgreSQL
- [Time-Series Functions](https://www.postgresql.org/docs/current/functions-datetime.html)
- Indexing strategies for large datasets

### Streamlit
- [Streamlit Docs](https://docs.streamlit.io/)
- Real-time data visualization

---

## 📝 Project Deliverables Checklist

- ✅ Python scripts (producer & consumer)
- ✅ SQL schema file with indexes
- ✅ Docker Compose file with multiple services
- ✅ README with setup instructions
- ✅ Streamlit dashboard
- ✅ Configuration for fault tolerance (3 Kafka brokers)
- ✅ Data validation & anomaly detection
- ✅ Database views for analysis
- ⏳ Architecture diagram (see above)
- ⏳ Sample screenshot outputs

---

## 🚀 Next Steps (Optional Extensions)

1. **Add real sensor integration** → Replace synthetic data with actual IoT sensors
2. **Machine learning anomaly detection** → Isolation Forest or LSTM models
3. **Alert system** → Email/SMS notifications for critical anomalies
4. **Grafana integration** → Advanced dashboarding with PostgreSQL data source
5. **Multi-region deployment** → Kafka cluster federation
6. **Data archival** → Move old data to cheaper storage (S3/MinIO)
7. **Complete compliance tracking** → HIPAA-compliant audit logs

---

## 📞 Support

For issues or questions:
1. Check logs: `docker compose logs <service-name>`
2. Review error messages carefully
3. Verify all services are running: `docker compose ps`
4. Check network connectivity: `docker network ls`

---

**Happy Data Engineering! 🎉**
