-- ============================================================
-- Real-Time Customer Heartbeat Monitoring System
-- PostgreSQL Database Schema
-- ============================================================
-- Note: Database 'heartbeat_db' is already created by Docker
-- This script runs automatically in the initialized database

-- ============================================================
-- Table: heartbeat_records
-- Stores individual heartbeat readings from customers
-- ============================================================

CREATE TABLE IF NOT EXISTS heartbeat_records (
    id BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    heart_rate INTEGER NOT NULL,
    anomaly_level VARCHAR(20),
    anomaly_description VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_heart_rate_range CHECK (heart_rate >= 30 AND heart_rate <= 250),
    CONSTRAINT chk_anomaly_level CHECK (
        anomaly_level IN ('normal', 'elevated', 'low', 'high', 'critical_low', 'critical_high')
    )
);

-- ============================================================
-- Indexes for Performance
-- ============================================================

-- Index on customer_id for customer-specific queries
CREATE INDEX IF NOT EXISTS idx_customer_id 
ON heartbeat_records(customer_id);

-- Index on recorded_at for time-series queries
CREATE INDEX IF NOT EXISTS idx_recorded_at 
ON heartbeat_records(recorded_at DESC);

-- Composite index for customer + time queries
CREATE INDEX IF NOT EXISTS idx_customer_time 
ON heartbeat_records(customer_id, recorded_at DESC);

-- Index on anomaly_level for filtering critical events
CREATE INDEX IF NOT EXISTS idx_anomaly_level 
ON heartbeat_records(anomaly_level) 
WHERE anomaly_level IN ('critical_low', 'critical_high');

-- ============================================================
-- Table: customer_info (Optional - for storing customer details)
-- ============================================================

CREATE TABLE IF NOT EXISTS customer_info (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    age INTEGER,
    medical_conditions TEXT,
    emergency_contact VARCHAR(100),
    enrolled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_age CHECK (age >= 0 AND age <= 150)
);

-- ============================================================
-- View: recent_heartbeats
-- Shows the most recent heartbeat for each customer
-- ============================================================

CREATE OR REPLACE VIEW recent_heartbeats AS
SELECT DISTINCT ON (customer_id)
    customer_id,
    recorded_at,
    heart_rate,
    anomaly_level,
    anomaly_description
FROM heartbeat_records
ORDER BY customer_id, recorded_at DESC;

-- ============================================================
-- View: critical_anomalies
-- Shows only critical anomaly events
-- ============================================================

CREATE OR REPLACE VIEW critical_anomalies AS
SELECT 
    id,
    customer_id,
    recorded_at,
    heart_rate,
    anomaly_level,
    anomaly_description
FROM heartbeat_records
WHERE anomaly_level IN ('critical_low', 'critical_high')
ORDER BY recorded_at DESC;

-- ============================================================
-- View: customer_stats
-- Aggregated statistics per customer
-- ============================================================

CREATE OR REPLACE VIEW customer_stats AS
SELECT 
    customer_id,
    COUNT(*) AS total_readings,
    AVG(heart_rate) AS avg_heart_rate,
    MIN(heart_rate) AS min_heart_rate,
    MAX(heart_rate) AS max_heart_rate,
    STDDEV(heart_rate) AS stddev_heart_rate,
    COUNT(CASE WHEN anomaly_level IN ('critical_low', 'critical_high') THEN 1 END) AS critical_count,
    MAX(recorded_at) AS last_reading
FROM heartbeat_records
GROUP BY customer_id;

-- ============================================================
-- Function: get_customer_heartbeat_trend
-- Returns heartbeat trend for a specific customer
-- ============================================================

CREATE OR REPLACE FUNCTION get_customer_heartbeat_trend(
    p_customer_id VARCHAR(50),
    p_hours INTEGER DEFAULT 24
)
RETURNS TABLE (
    recorded_at TIMESTAMP,
    heart_rate INTEGER,
    anomaly_level VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        hr.recorded_at,
        hr.heart_rate,
        hr.anomaly_level
    FROM heartbeat_records hr
    WHERE hr.customer_id = p_customer_id
        AND hr.recorded_at >= NOW() - (p_hours || ' hours')::INTERVAL
    ORDER BY hr.recorded_at DESC;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Function: detect_sustained_anomalies
-- Detects if a customer has sustained anomalies (3+ consecutive)
-- ============================================================

CREATE OR REPLACE FUNCTION detect_sustained_anomalies(
    p_customer_id VARCHAR(50),
    p_threshold INTEGER DEFAULT 3
)
RETURNS TABLE (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    anomaly_count INTEGER,
    avg_heart_rate NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH numbered_anomalies AS (
        SELECT 
            recorded_at,
            heart_rate,
            ROW_NUMBER() OVER (ORDER BY recorded_at) - 
            ROW_NUMBER() OVER (PARTITION BY anomaly_level ORDER BY recorded_at) AS grp
        FROM heartbeat_records
        WHERE customer_id = p_customer_id
            AND anomaly_level IN ('critical_low', 'critical_high', 'high', 'low')
    ),
    grouped_anomalies AS (
        SELECT 
            MIN(recorded_at) AS start_time,
            MAX(recorded_at) AS end_time,
            COUNT(*) AS anomaly_count,
            AVG(heart_rate) AS avg_heart_rate
        FROM numbered_anomalies
        GROUP BY grp
        HAVING COUNT(*) >= p_threshold
    )
    SELECT * FROM grouped_anomalies
    ORDER BY start_time DESC;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Sample Data Insertion (for testing)
-- ============================================================

-- Insert sample customers
INSERT INTO customer_info (customer_id, customer_name, age, medical_conditions)
VALUES 
    ('CUST_0001', 'John Doe', 45, 'Hypertension'),
    ('CUST_0002', 'Jane Smith', 32, 'None'),
    ('CUST_0003', 'Bob Johnson', 58, 'Diabetes'),
    ('CUST_0004', 'Alice Williams', 28, 'None'),
    ('CUST_0005', 'Charlie Brown', 65, 'Heart Disease')
ON CONFLICT (customer_id) DO NOTHING;

-- ============================================================
-- Useful Queries for Analysis
-- ============================================================

-- Query 1: Get latest 100 heartbeat records
-- SELECT * FROM heartbeat_records ORDER BY recorded_at DESC LIMIT 100;

-- Query 2: Get all critical anomalies from last 24 hours
-- SELECT * FROM critical_anomalies WHERE recorded_at >= NOW() - INTERVAL '24 hours';

-- Query 3: Get customer statistics
-- SELECT * FROM customer_stats ORDER BY critical_count DESC;

-- Query 4: Get heartbeat trend for specific customer
-- SELECT * FROM get_customer_heartbeat_trend('CUST_0001', 24);

-- Query 5: Detect sustained anomalies for customer
-- SELECT * FROM detect_sustained_anomalies('CUST_0001', 3);

-- Query 6: Get average heart rate per hour for last 24 hours
-- SELECT 
--     DATE_TRUNC('hour', recorded_at) AS hour,
--     AVG(heart_rate) AS avg_heart_rate,
--     COUNT(*) AS reading_count
-- FROM heartbeat_records
-- WHERE recorded_at >= NOW() - INTERVAL '24 hours'
-- GROUP BY DATE_TRUNC('hour', recorded_at)
-- ORDER BY hour DESC;

-- ============================================================
-- Maintenance and Cleanup
-- ============================================================

-- Function to archive old records (older than 90 days)
CREATE OR REPLACE FUNCTION archive_old_records()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM heartbeat_records
    WHERE recorded_at < NOW() - INTERVAL '90 days';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- To run archival: SELECT archive_old_records();

-- ============================================================
-- Grants (adjust based on your user setup)
-- ============================================================

-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
-- GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO postgres;

COMMIT;