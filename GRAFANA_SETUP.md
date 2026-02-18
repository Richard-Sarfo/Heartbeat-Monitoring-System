# Grafana Setup Guide

## Overview
Grafana is now integrated with your Real-Time Customer Heartbeat Monitoring System to provide real-time visualization and monitoring of heart rate data stored in PostgreSQL.

## Quick Start

### 1. Start the System
```bash
docker compose up -d --build
```

This will start:
- **Zookeeper** (port 2181)
- **Kafka Brokers** (ports 9092, 9094, 9096)
- **PostgreSQL** (port 5432)
- **Grafana** (port 3000)
- **Producer** (generates synthetic heartbeat data)
- **Consumer** (stores data in PostgreSQL)
- **pgAdmin** (port 5050) - optional PostgreSQL UI
- **Kafka UI** (port 8080) - optional Kafka monitoring

### 2. Access Grafana
Open your browser and navigate to:
```
http://localhost:3000
```

**Default Credentials:**
- Username: `admin`
- Password: `admin`

### 3. First Login
When you first log in, Grafana will prompt you to change your password. You may skip this or set a new password.

## Grafana Components

### Datasource Configuration
The PostgreSQL datasource is automatically configured at startup via provisioning:
- **Host:** postgres (Docker internal hostname)
- **Database:** heartbeat_db
- **User:** postgres
- **Password:** postgres
- **Port:** 5432

To manually verify or edit the datasource:
1. Go to **Configuration** → **Data Sources**
2. Select **PostgreSQL Heartbeat DB**
3. Verify connection is successful (green checkmark)

### Dashboard
A pre-built dashboard is automatically provisioned with:

#### Key Metrics (Last 1 Hour)
- **Average Heart Rate** - Current avg BPM
- **Total Records Ingested** - Cumulative count
- **Critical Anomalies** - Count of critical events
- **Unique Customers** - Distinct customers monitored

#### Visualizations
1. **Heart Rate Trend (24h)** - Line chart showing 5-minute averages
2. **Anomaly Distribution (24h)** - Pie chart of anomaly types
3. **Recent Critical Alerts** - Table of last 50 critical events

#### Auto-Refresh
- Dashboard refreshes every 30 seconds
- Time range defaults to last 24 hours (configurable via time picker)

## Key Queries in the Dashboard

### 1. Average Heart Rate
```sql
SELECT ROUND(AVG(heart_rate)) as avg_heart_rate 
FROM heartbeat_records 
WHERE recorded_at > NOW() - INTERVAL '1 hour'
```

### 2. Total Records Ingested
```sql
SELECT COUNT(*) as total_records 
FROM heartbeat_records
```

### 3. Critical Anomalies Count
```sql
SELECT COUNT(*) as critical_count 
FROM heartbeat_records 
WHERE anomaly_level IN ('critical_low', 'critical_high') 
  AND recorded_at > NOW() - INTERVAL '1 hour'
```

### 4. Unique Customers
```sql
SELECT COUNT(DISTINCT customer_id) as unique_customers 
FROM heartbeat_records
```

### 5. Heart Rate Trend
```sql
SELECT recorded_at, ROUND(AVG(heart_rate)::numeric, 2) as avg_heart_rate 
FROM heartbeat_records 
WHERE recorded_at > NOW() - INTERVAL '24 hours' 
GROUP BY DATE_TRUNC('5 minutes', recorded_at) 
ORDER BY recorded_at
```

### 6. Anomaly Distribution
```sql
SELECT anomaly_level, COUNT(*) as count 
FROM heartbeat_records 
WHERE recorded_at > NOW() - INTERVAL '24 hours' 
  AND anomaly_level != 'normal' 
GROUP BY anomaly_level 
ORDER BY count DESC
```

### 7. Recent Critical Alerts
```sql
SELECT customer_id, heart_rate, recorded_at, anomaly_level, anomaly_description 
FROM heartbeat_records 
WHERE anomaly_level IN ('critical_low', 'critical_high') 
ORDER BY recorded_at DESC 
LIMIT 50
```

## Creating Custom Panels

### Step-by-Step
1. **Open Dashboard** → Click **+ Add Panel** (top right)
2. **Select Visualization Type:**
   - Time series (for trends)
   - Stat (for KPIs)
   - Table (for detailed records)
   - Pie chart (for distributions)
   - Bar chart (for comparisons)

3. **Write Your Query** in the SQL editor:
   ```sql
   SELECT <columns> 
   FROM heartbeat_records 
   WHERE <conditions>
   ```

4. **Configure Display:**
   - Set units (bpm for heart rate)
   - Configure thresholds (green/orange/red)
   - Adjust axes and legend

5. **Save Panel** → Give it a name

### Example: Heart Rate by Customer (Top 5)
```sql
SELECT customer_id, ROUND(AVG(heart_rate)::numeric, 2) as avg_hr
FROM heartbeat_records
WHERE recorded_at > NOW() - INTERVAL '1 hour'
GROUP BY customer_id
ORDER BY avg_hr DESC
LIMIT 5
```

## Anomaly Levels

The system classifies heart rate anomalies as:
- **normal** - Healthy range (50-100 bpm)
- **low** - Below 50 bpm
- **elevated** - 100-120 bpm
- **high** - 120-140 bpm
- **critical_low** - Below 30 bpm (critical emergency)
- **critical_high** - Above 140 bpm (critical emergency)

## Troubleshooting

### Grafana Won't Start
```bash
# Check logs
docker logs heartbeat-grafana

# Restart container
docker restart heartbeat-grafana
```

### No Data in Dashboard
1. **Verify PostgreSQL is running:**
   ```bash
   docker compose exec postgres psql -U postgres -d heartbeat_db -c "SELECT COUNT(*) FROM heartbeat_records;"
   ```

2. **Check Producer is running:**
   ```bash
   docker compose logs producer
   ```

3. **Check Consumer is running:**
   ```bash
   docker compose logs consumer
   ```

4. **Verify Datasource Connection:**
   - Go to **Configuration** → **Data Sources** → **PostgreSQL Heartbeat DB**
   - Click **Test Connection**

### Datasource Connection Error
If you see "connection refused" or "no such host":
1. Ensure all Docker containers are healthy: `docker compose ps`
2. Check PostgreSQL is accepting connections: `docker compose logs postgres`
3. Verify environment variables in `docker-compose.yml` (postgres service)

## Performance Tips

### For Large Datasets
1. **Use time-based filters** - Limit queries to recent data
2. **Increase aggregation intervals** - Change `DATE_TRUNC('5 minutes')` to `'15 minutes'` or `'1 hour'`
3. **Index optimization** - Schema already has indexes on:
   - `customer_id`
   - `recorded_at`
   - `anomaly_level`

### For Better Responsiveness
1. **Adjust refresh interval** - Go to dashboard settings → **Refresh interval**
2. **Reduce number of visible rows** - Use `LIMIT` in table queries
3. **Use materialized views** - For complex aggregations (advanced)

## Advanced Features

### Alerts (Optional)
You can set up email/webhook alerts when thresholds are breached:
1. **Create Alert Rule** → **+ New Rule**
2. **Set Condition** (e.g., avg heart rate > 120)
3. **Configure Notification Channel** (email, Slack, PagerDuty, etc.)
4. **Test Alert**

### Dashboard Variables (Filters)
Add dynamic filters to your dashboard:
1. Dashboard settings → **Variables** → **+ Add variable**
2. Create variable for customer_id with query:
   ```sql
   SELECT DISTINCT customer_id FROM heartbeat_records ORDER BY 1
   ```
3. Use in panels as `WHERE customer_id = $customer_id`

### Exporting Dashboards
1. **Dashboard menu** → **Share** → **Export**
2. Saves as JSON (portable across Grafana instances)
3. Import via **+ Create** → **Import** → Paste JSON

## File Structure

```
grafana/
├── provisioning/
│   ├── datasources/
│   │   └── postgres.yml          # Auto-provision PostgreSQL datasource
│   └── dashboards/
│       └── dashboards.yml         # Auto-provision dashboard folder
└── dashboards/
    └── heartbeat-dashboard.json   # Pre-built dashboard JSON
```

## Docker Compose Configuration

Added Grafana service to `docker-compose.yml`:
```yaml
grafana:
  image: grafana/grafana:10.2.2
  container_name: heartbeat-grafana
  environment:
    GF_SECURITY_ADMIN_USER: admin
    GF_SECURITY_ADMIN_PASSWORD: admin
    GF_USERS_ALLOW_SIGN_UP: 'false'
  ports:
    - "3000:3000"
  volumes:
    - grafana-data:/var/lib/grafana
    - ./grafana/provisioning/datasources:/etc/grafana/provisioning/datasources
    - ./grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards
    - ./grafana/dashboards:/var/lib/grafana/dashboards
  depends_on:
    postgres:
      condition: service_healthy
```

## Next Steps

1. **Monitor Real-Time Data** - Open the dashboard and watch heartbeat data stream
2. **Create Custom Dashboards** - Build dashboard specific to your use case
3. **Set Up Alerts** - Get notified of critical events
4. **Export & Share** - Share dashboards with team members
5. **Integrate Other Data** - Add additional PostgreSQL tables/queries

## Resources

- [Grafana Official Docs](https://grafana.com/docs/grafana/latest/)
- [PostgreSQL Data Source Docs](https://grafana.com/docs/grafana/latest/datasources/postgres/)
- [Dashboard Best Practices](https://grafana.com/docs/grafana/latest/dashboards/)
