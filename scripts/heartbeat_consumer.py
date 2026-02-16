"""
Real-Time Customer Heartbeat Kafka Consumer

This script consumes heartbeat data from Kafka, validates it,
detects anomalies, and stores it in PostgreSQL database.
"""

import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import sys

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
    """Kafka consumer for heartbeat data"""
    
    def __init__(self, bootstrap_servers, topic, group_id, db_manager):
        self.topic = topic
        self.db_manager = db_manager
        self.consumer = None
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
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
        """Consume messages from Kafka and process them"""
        logger.info("Starting to consume messages...")
        logger.info("Press Ctrl+C to stop\n")
        
        message_count = 0
        valid_count = 0
        invalid_count = 0
        batch = []
        
        try:
            for message in self.consumer:
                heartbeat_data = message.value
                message_count += 1
                
                # Validate data
                if HeartbeatValidator.validate(heartbeat_data):
                    valid_count += 1
                    batch.append(heartbeat_data)
                    
                    # Log individual message
                    logger.debug(
                        f"Received: {heartbeat_data['customer_id']} - "
                        f"{heartbeat_data['heart_rate']} bpm at {heartbeat_data['timestamp']}"
                    )
                    
                    # Insert batch when size reached
                    if len(batch) >= batch_size:
                        self.db_manager.insert_batch(batch)
                        batch = []
                else:
                    invalid_count += 1
                
                # Periodic status update
                if message_count % 50 == 0:
                    logger.info(
                        f"Processed: {message_count} | Valid: {valid_count} | "
                        f"Invalid: {invalid_count}"
                    )
        
        except KeyboardInterrupt:
            logger.info("\nShutting down consumer...")
            
            # Insert remaining batch
            if batch:
                self.db_manager.insert_batch(batch)
            
            logger.info(f"Total messages processed: {message_count}")
            logger.info(f"Valid messages: {valid_count}")
            logger.info(f"Invalid messages: {invalid_count}")
        
        finally:
            self.close()
    
    def close(self):
        """Close consumer connection"""
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer connection closed")


def main():
    """Main execution function"""
    # Kafka Configuration
    KAFKA_BROKERS = ['localhost:9092', 'localhost:9094', 'localhost:9096']  # Multiple brokers
    KAFKA_TOPIC = 'heartbeat-data'
    CONSUMER_GROUP = 'heartbeat-consumer-group'
    
    # Database Configuration
    DB_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'heartbeat_db',
        'user': 'postgres',
        'password': 'postgres'
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