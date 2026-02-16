"""
Real-Time Customer Heartbeat Data Generator and Kafka Producer

This script generates synthetic heart rate data for multiple customers
and streams it to a Kafka topic in real-time.
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HeartbeatDataGenerator:
    """Generates realistic synthetic heart rate data for customers"""
    
    def __init__(self, num_customers=10):
        self.num_customers = num_customers
        self.customer_ids = [f"CUST_{str(i).zfill(4)}" for i in range(1, num_customers + 1)]
        
        # Realistic heart rate ranges
        self.normal_range = (60, 100)
        self.exercise_range = (100, 160)
        self.rest_range = (50, 70)
        
        # Customer states (simulating different activities)
        self.customer_states = {cid: 'normal' for cid in self.customer_ids}
    
    def generate_heartbeat(self, customer_id):
        """Generate a single heartbeat reading for a customer"""
        state = self.customer_states[customer_id]
        
        # Randomly change customer state occasionally
        if random.random() < 0.05:  # 5% chance to change state
            self.customer_states[customer_id] = random.choice(['normal', 'exercise', 'rest', 'anomaly'])
            state = self.customer_states[customer_id]
        
        # Generate heart rate based on state
        if state == 'normal':
            heart_rate = random.randint(*self.normal_range)
        elif state == 'exercise':
            heart_rate = random.randint(*self.exercise_range)
        elif state == 'rest':
            heart_rate = random.randint(*self.rest_range)
        else:  # anomaly
            # Simulate potential health issues
            heart_rate = random.choice([
                random.randint(40, 50),   # Bradycardia
                random.randint(170, 200)  # Tachycardia
            ])
        
        # Add small random variation
        heart_rate += random.randint(-3, 3)
        heart_rate = max(40, min(200, heart_rate))  # Clamp to realistic bounds
        
        return {
            'customer_id': customer_id,
            'timestamp': datetime.now().isoformat(),
            'heart_rate': heart_rate,
            'state': state
        }


class HeartbeatProducer:
    """Kafka producer for streaming heartbeat data"""
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='heartbeat-data'):
        self.topic = topic
        self.producer = None
        self.bootstrap_servers = bootstrap_servers
        self._connect()
    
    def _connect(self):
        """Establish connection to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def send_heartbeat(self, heartbeat_data):
        """Send a single heartbeat reading to Kafka"""
        try:
            future = self.producer.send(self.topic, value=heartbeat_data)
            # Wait for confirmation (optional, for reliability)
            record_metadata = future.get(timeout=10)
            logger.debug(f"Sent: {heartbeat_data['customer_id']} - {heartbeat_data['heart_rate']} bpm")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer connection closed")


def main():
    """Main execution function"""
    # Configuration
    NUM_CUSTOMERS = 10
    SEND_INTERVAL = 2  # seconds between batches
    KAFKA_BROKER = 'localhost:9092'
    KAFKA_TOPIC = 'heartbeat-data'
    
    logger.info("=" * 60)
    logger.info("Real-Time Customer Heartbeat Monitoring System")
    logger.info("Data Generator and Kafka Producer")
    logger.info("=" * 60)
    
    # Initialize generator and producer
    generator = HeartbeatDataGenerator(num_customers=NUM_CUSTOMERS)
    producer = HeartbeatProducer(bootstrap_servers=KAFKA_BROKER, topic=KAFKA_TOPIC)
    
    logger.info(f"Generating data for {NUM_CUSTOMERS} customers")
    logger.info(f"Sending to Kafka topic: {KAFKA_TOPIC}")
    logger.info(f"Data interval: {SEND_INTERVAL} seconds")
    logger.info("Press Ctrl+C to stop\n")
    
    try:
        message_count = 0
        while True:
            # Generate and send heartbeat for each customer
            for customer_id in generator.customer_ids:
                heartbeat = generator.generate_heartbeat(customer_id)
                if producer.send_heartbeat(heartbeat):
                    message_count += 1
                    if message_count % 10 == 0:
                        logger.info(f"Messages sent: {message_count}")
            
            time.sleep(SEND_INTERVAL)
    
    except KeyboardInterrupt:
        logger.info("\nShutting down gracefully...")
        logger.info(f"Total messages sent: {message_count}")
    
    finally:
        producer.close()


if __name__ == "__main__":
    main()