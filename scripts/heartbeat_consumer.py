"""
Real-Time Customer Heartbeat Kafka Consumer

This script consumes heartbeat data from Kafka, validates it,
detects anomalies, and stores it in PostgreSQL database.

Features:
- Event time-based processing with watermarking
- Late data detection and handling
- Window-based anomaly detection
"""

import json
import logging
import os
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import sys
from collections import defaultdict
from heapq import heappush, heappop

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HeartbeatValidator:
    """Validates and analyzes heartbeat data"""
    
    # Heart rate thresholds
    CRITICAL_LOW = 40
    LOW = 60
    NORMAL_HIGH = 100
    HIGH = 160
    CRITICAL_HIGH = 200
    
    @staticmethod
    def validate(heartbeat_data):
        """Validate heartbeat data structure and values"""
        required_fields = ['customer_id', 'timestamp', 'heart_rate']
        
        # Check required fields
        for field in required_fields:
            if field not in heartbeat_data:
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate heart rate range
        heart_rate = heartbeat_data['heart_rate']
        if not isinstance(heart_rate, (int, float)):
            logger.warning(f"Invalid heart_rate type: {type(heart_rate)}")
            return False
        
        if heart_rate < 30 or heart_rate > 250:
            logger.warning(f"Heart rate out of realistic bounds: {heart_rate}")
            return False
        
        return True
    
    @classmethod
    def detect_anomaly(cls, heart_rate):
        """Detect if heart rate is anomalous"""
        if heart_rate < cls.CRITICAL_LOW:
            return 'critical_low', 'Severe Bradycardia'
        elif heart_rate < cls.LOW:
            return 'low', 'Bradycardia'
        elif heart_rate > cls.CRITICAL_HIGH:
            return 'critical_high', 'Severe Tachycardia'
        elif heart_rate > cls.HIGH:
            return 'high', 'Tachycardia'
        elif heart_rate > cls.NORMAL_HIGH:
            return 'elevated', 'Elevated (Exercise/Stress)'
        else:
            return 'normal', 'Normal'


class WatermarkTracker:
    """Tracks event-time watermarks for handling late and out-of-order data."""

    def __init__(self, allowed_lateness_seconds=30):
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.current_watermark = None  # No watermark until first message
        self.late_count = 0
        self.on_time_count = 0

    def update(self, event_time):
        """Advance the watermark if event_time is the new maximum."""
        if self.current_watermark is None or event_time > self.current_watermark:
            self.current_watermark = event_time

    def is_late(self, event_time):
        """Check whether an event is late relative to the watermark."""

        if self.current_watermark is None:
            return 'on_time'

        lag = self.current_watermark - event_time

        if lag <= timedelta(0):
            return 'on_time'
        elif lag <= self.allowed_lateness:
            self.late_count += 1
            return 'late'
        else:
            self.late_count += 1
            return 'dropped'

    def get_stats(self):
        return {
            'watermark': self.current_watermark.isoformat() if self.current_watermark else None,
            'on_time': self.on_time_count,
            'late': self.late_count,
        }


class WindowAggregator:
    """Tumbling-window aggregator for per-customer heart rate statistics."""

    def __init__(self, window_size_seconds=300):
        self.window_size = timedelta(seconds=window_size_seconds)
        # {customer_id: {window_start: [heart_rates]}}
        self.windows = defaultdict(lambda: defaultdict(list))

    def _window_start(self, event_time):
        """Compute the start of the tumbling window for a given event time."""
        epoch = datetime(2000, 1, 1)
        elapsed = (event_time - epoch).total_seconds()
        window_num = int(elapsed // self.window_size.total_seconds())
        return epoch + timedelta(seconds=window_num * self.window_size.total_seconds())

    def add(self, customer_id, event_time, heart_rate):
        """Add a reading to the appropriate window."""
        ws = self._window_start(event_time)
        self.windows[customer_id][ws].append(heart_rate)

    def evaluate_and_close(self, current_watermark):
        """Close and evaluate windows that are fully past the watermark."""
        if current_watermark is None:
            return []

        alerts = []
        cutoff = self._window_start(current_watermark)  # current open window

        for customer_id in list(self.windows.keys()):
            for ws in list(self.windows[customer_id].keys()):
                # Only close windows that ended before the current window
                if ws + self.window_size <= cutoff:
                    readings = self.windows[customer_id].pop(ws)
                    if not readings:
                        continue
                    avg_hr = sum(readings) / len(readings)
                    anomaly_level, desc = HeartbeatValidator.detect_anomaly(int(avg_hr))
                    if anomaly_level not in ('normal', 'elevated'):
                        alerts.append({
                            'customer_id': customer_id,
                            'window_start': ws.isoformat(),
                            'window_end': (ws + self.window_size).isoformat(),
                            'avg_heart_rate': round(avg_hr, 1),
                            'reading_count': len(readings),
                            'anomaly_level': anomaly_level,
                            'description': f"Window alert: {desc}",
                        })
            # Clean up empty customer entries
            if not self.windows[customer_id]:
                del self.windows[customer_id]

        return alerts


class DatabaseManager:
    """Manages PostgreSQL database connections and operations"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection_pool = None
        self._create_connection_pool()
    
    def _create_connection_pool(self):
        """Create a connection pool for efficient database operations"""
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,  # min and max connections
                **self.db_config
            )
            logger.info("Database connection pool created successfully")
        except psycopg2.Error as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool"""
        return self.connection_pool.getconn()
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        self.connection_pool.putconn(conn)
    
    def insert_heartbeat(self, heartbeat_data):
        """Insert a heartbeat record into the database"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Detect anomaly
            anomaly_level, anomaly_description = HeartbeatValidator.detect_anomaly(
                heartbeat_data['heart_rate']
            )
            
            # Insert query
            insert_query = """
                INSERT INTO heartbeat_records 
                (customer_id, recorded_at, heart_rate, anomaly_level, anomaly_description)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                heartbeat_data['customer_id'],
                heartbeat_data['timestamp'],
                heartbeat_data['heart_rate'],
                anomaly_level,
                anomaly_description
            ))
            
            conn.commit()
            cursor.close()
            
            # Log anomalies
            if anomaly_level in ['critical_low', 'critical_high']:
                logger.warning(
                    f"CRITICAL ANOMALY: {heartbeat_data['customer_id']} - "
                    f"{heartbeat_data['heart_rate']} bpm - {anomaly_description}"
                )
            
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            return False
        
        finally:
            if conn:
                self.return_connection(conn)
    
    def insert_batch(self, heartbeat_batch):
        """Insert multiple heartbeat records efficiently"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Prepare data with anomaly detection
            values = []
            for hb in heartbeat_batch:
                anomaly_level, anomaly_description = HeartbeatValidator.detect_anomaly(
                    hb['heart_rate']
                )
                values.append((
                    hb['customer_id'],
                    hb['timestamp'],
                    hb['heart_rate'],
                    anomaly_level,
                    anomaly_description
                ))
            
            # Batch insert
            insert_query = """
                INSERT INTO heartbeat_records 
                (customer_id, recorded_at, heart_rate, anomaly_level, anomaly_description)
                VALUES %s
            """
            
            execute_values(cursor, insert_query, values)
            conn.commit()
            cursor.close()
            
            logger.info(f"Batch inserted {len(values)} records")
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Batch insert error: {e}")
            if conn:
                conn.rollback()
            return False
        
        finally:
            if conn:
                self.return_connection(conn)
    
    def close(self):
        """Close all database connections"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connections closed")


class HeartbeatConsumer:
    """Kafka consumer for heartbeat data with watermarking support."""
    
    def __init__(self, bootstrap_servers, topic, group_id, db_manager,
                 allowed_lateness_seconds=30, window_size_seconds=300):
        self.topic = topic
        self.db_manager = db_manager
        self.consumer = None
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

        # Watermark & windowing
        self.watermark = WatermarkTracker(allowed_lateness_seconds)
        self.window_agg = WindowAggregator(window_size_seconds)

        self._connect()
    
    def _connect(self):
        """Connect to Kafka broker"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Start from beginning if no offset
                enable_auto_commit=True,
                auto_commit_interval_ms=1000
            )
            logger.info(f"Connected to Kafka topic: {self.topic}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def consume(self, batch_size=10):
        """Consume messages from Kafka with event-time watermarking."""
        logger.info("Starting to consume messages (watermark-enabled)...")
        logger.info(f"Allowed lateness: {self.watermark.allowed_lateness.total_seconds()}s")
        logger.info(f"Window size:      {self.window_agg.window_size.total_seconds()}s")
        logger.info("Press Ctrl+C to stop\n")
        
        message_count = 0
        valid_count = 0
        invalid_count = 0
        late_accepted_count = 0
        dropped_count = 0
        batch = []
        
        try:
            for message in self.consumer:
                heartbeat_data = message.value
                message_count += 1
                
                #  Validate schema
                if not HeartbeatValidator.validate(heartbeat_data):
                    invalid_count += 1
                    continue
                # Parse event time & advance watermark
                try:
                    event_time = datetime.fromisoformat(heartbeat_data['timestamp'])
                except (ValueError, TypeError):
                    logger.warning(f"Unparseable timestamp: {heartbeat_data['timestamp']}")
                    invalid_count += 1
                    continue

                self.watermark.update(event_time)

                # Late-data check 
                lateness = self.watermark.is_late(event_time)

                if lateness == 'dropped':
                    dropped_count += 1
                    logger.debug(
                        f"DROPPED (late beyond threshold): "
                        f"{heartbeat_data['customer_id']} at {heartbeat_data['timestamp']}"
                    )
                    continue  # Do NOT insert into DB

                if lateness == 'late':
                    late_accepted_count += 1
                    logger.info(
                        f"LATE (accepted): {heartbeat_data['customer_id']} "
                        f"at {heartbeat_data['timestamp']} "
                        f"(watermark: {self.watermark.current_watermark.isoformat()})"
                    )

                # Feed into window aggregator 
                valid_count += 1
                self.window_agg.add(
                    heartbeat_data['customer_id'],
                    event_time,
                    heartbeat_data['heart_rate']
                )
                batch.append(heartbeat_data)
                
                logger.debug(
                    f"Received [{lateness}]: {heartbeat_data['customer_id']} - "
                    f"{heartbeat_data['heart_rate']} bpm at {heartbeat_data['timestamp']}"
                )
                
                #  Batch insert
                if len(batch) >= batch_size:
                    self.db_manager.insert_batch(batch)
                    batch = []

                    # Evaluate closed windows
                    alerts = self.window_agg.evaluate_and_close(
                        self.watermark.current_watermark
                    )
                    for alert in alerts:
                        logger.warning(
                            f"WINDOW ALERT: {alert['customer_id']} | "
                            f"{alert['window_start']} -> {alert['window_end']} | "
                            f"Avg HR: {alert['avg_heart_rate']} bpm | "
                            f"{alert['description']}"
                        )
                
                # Periodic status update
                if message_count % 50 == 0:
                    wm_stats = self.watermark.get_stats()
                    logger.info(
                        f"Processed: {message_count} | Valid: {valid_count} | "
                        f"Invalid: {invalid_count} | Late(accepted): {late_accepted_count} | "
                        f"Dropped: {dropped_count} | "
                        f"Watermark: {wm_stats['watermark']}"
                    )
        
        except KeyboardInterrupt:
            logger.info("\nShutting down consumer...")
            
            # Insert remaining batch
            if batch:
                self.db_manager.insert_batch(batch)
            
            logger.info(f"Total messages processed: {message_count}")
            logger.info(f"Valid (on-time):  {valid_count - late_accepted_count}")
            logger.info(f"Valid (late):     {late_accepted_count}")
            logger.info(f"Dropped (too late): {dropped_count}")
            logger.info(f"Invalid messages: {invalid_count}")
            logger.info(f"Final watermark:  {self.watermark.current_watermark}")
        
        finally:
            self.close()
    
    def close(self):
        """Close consumer connection"""
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer connection closed")


def main():
    """Main execution function"""
    # Kafka Configuration from environment variables
    kafka_servers_env = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9094,localhost:9096')
    KAFKA_BROKERS = kafka_servers_env.split(',') if isinstance(kafka_servers_env, str) else list(kafka_servers_env)
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'heartbeat-data')
    CONSUMER_GROUP = os.getenv('KAFKA_GROUP_ID', 'heartbeat-consumer-group')
    
    # Database Configuration from environment variables
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'heartbeat_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'postgres')
    }
    
    logger.info("=" * 60)
    logger.info("Real-Time Customer Heartbeat Monitoring System")
    logger.info("Kafka Consumer and Database Writer")
    logger.info("=" * 60)
    
    # Initialize database manager
    try:
        db_manager = DatabaseManager(DB_CONFIG)
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        sys.exit(1)
    
    # Initialize consumer
    try:
        logger.info(f"Connecting to Kafka brokers: {', '.join(KAFKA_BROKERS)}")
        consumer = HeartbeatConsumer(
            bootstrap_servers=KAFKA_BROKERS,
            topic=KAFKA_TOPIC,
            group_id=CONSUMER_GROUP,
            db_manager=db_manager
        )
    except Exception as e:
        logger.error(f"Failed to initialize consumer: {e}")
        db_manager.close()
        sys.exit(1)
    
    # Start consuming
    try:
        consumer.consume(batch_size=10)
    finally:
        db_manager.close()


if __name__ == "__main__":
    main()