"""
Test Suite for Real-Time Customer Heartbeat Monitoring System

This script tests the complete data pipeline from generation to storage.
"""

import json
import time
import psycopg2
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class SystemTester:
    """Test suite for heartbeat monitoring system"""
    
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'heartbeat_db',
            'user': 'postgres',
            'password': 'postgres'
        }
        self.test_results = []
    
    def log_test(self, test_name, passed, message=""):
        """Log test results"""
        status = "✓ PASSED" if passed else "✗ FAILED"
        result = f"{status}: {test_name}"
        if message:
            result += f" - {message}"
        print(result)
        self.test_results.append((test_name, passed, message))
    
    def test_database_connection(self):
        """Test PostgreSQL database connection"""
        print("\n" + "="*60)
        print("TEST 1: Database Connection")
        print("="*60)
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            self.log_test("Database Connection", True, f"PostgreSQL connected")
            return True
        except Exception as e:
            self.log_test("Database Connection", False, str(e))
            return False
    
    def test_table_exists(self):
        """Test if heartbeat_records table exists"""
        print("\n" + "="*60)
        print("TEST 2: Database Schema")
        print("="*60)
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'heartbeat_records'
                );
            """)
            exists = cursor.fetchone()[0]
            
            if exists:
                # Check table structure
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'heartbeat_records'
                    ORDER BY ordinal_position;
                """)
                columns = cursor.fetchall()
                
                print("\nTable structure:")
                for col in columns:
                    print(f"  - {col[0]}: {col[1]}")
                
                self.log_test("Table Schema", True, f"Found {len(columns)} columns")
            else:
                self.log_test("Table Schema", False, "heartbeat_records table not found")
            
            cursor.close()
            conn.close()
            return exists
            
        except Exception as e:
            self.log_test("Table Schema", False, str(e))
            return False
    
    def test_indexes(self):
        """Test if indexes are created"""
        print("\n" + "="*60)
        print("TEST 3: Database Indexes")
        print("="*60)
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'heartbeat_records'
                ORDER BY indexname;
            """)
            indexes = cursor.fetchall()
            
            print("\nIndexes found:")
            for idx in indexes:
                print(f"  - {idx[0]}")
            
            self.log_test("Database Indexes", len(indexes) > 0, f"Found {len(indexes)} indexes")
            
            cursor.close()
            conn.close()
            return len(indexes) > 0
            
        except Exception as e:
            self.log_test("Database Indexes", False, str(e))
            return False
    
    def test_insert_sample_data(self):
        """Test inserting sample heartbeat data"""
        print("\n" + "="*60)
        print("TEST 4: Data Insertion")
        print("="*60)
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Insert test data
            test_data = [
                ('TEST_CUST_001', datetime.now(), 75, 'normal', 'Normal'),
                ('TEST_CUST_002', datetime.now(), 165, 'high', 'Tachycardia'),
                ('TEST_CUST_003', datetime.now(), 45, 'low', 'Bradycardia'),
            ]
            
            insert_query = """
                INSERT INTO heartbeat_records 
                (customer_id, recorded_at, heart_rate, anomaly_level, anomaly_description)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id;
            """
            
            inserted_ids = []
            for data in test_data:
                cursor.execute(insert_query, data)
                inserted_ids.append(cursor.fetchone()[0])
            
            conn.commit()
            
            print(f"\nInserted {len(inserted_ids)} test records")
            print(f"IDs: {inserted_ids}")
            
            self.log_test("Data Insertion", True, f"Inserted {len(inserted_ids)} records")
            
            # Cleanup test data
            cursor.execute(
                "DELETE FROM heartbeat_records WHERE customer_id LIKE 'TEST_CUST_%'"
            )
            conn.commit()
            print(f"Cleaned up test data")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            self.log_test("Data Insertion", False, str(e))
            return False
    
    def test_queries(self):
        """Test various SQL queries"""
        print("\n" + "="*60)
        print("TEST 5: Database Queries")
        print("="*60)
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Test 1: Count total records
            cursor.execute("SELECT COUNT(*) FROM heartbeat_records;")
            total_records = cursor.fetchone()[0]
            print(f"\nTotal records in database: {total_records}")
            
            # Test 2: Check views
            cursor.execute("SELECT COUNT(*) FROM recent_heartbeats;")
            recent_count = cursor.fetchone()[0]
            print(f"Recent heartbeats view: {recent_count} customers")
            
            # Test 3: Check customer stats
            cursor.execute("SELECT COUNT(*) FROM customer_stats;")
            stats_count = cursor.fetchone()[0]
            print(f"Customer stats view: {stats_count} customers")
            
            self.log_test("Database Queries", True, "All queries executed successfully")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            self.log_test("Database Queries", False, str(e))
            return False
    
    def test_data_validation(self):
        """Test data validation logic"""
        print("\n" + "="*60)
        print("TEST 6: Data Validation")
        print("="*60)
        
        from scripts.heartbeat_consumer import HeartbeatValidator
        
        # Test valid data
        valid_data = {
            'customer_id': 'CUST_0001',
            'timestamp': datetime.now().isoformat(),
            'heart_rate': 75
        }
        is_valid = HeartbeatValidator.validate(valid_data)
        self.log_test("Valid Data Test", is_valid, "Valid data accepted")
        
        # Test invalid data - missing field
        invalid_data_1 = {
            'customer_id': 'CUST_0001',
            'heart_rate': 75
        }
        is_valid = HeartbeatValidator.validate(invalid_data_1)
        self.log_test("Invalid Data Test 1", not is_valid, "Missing field rejected")
        
        # Test invalid data - out of range
        invalid_data_2 = {
            'customer_id': 'CUST_0001',
            'timestamp': datetime.now().isoformat(),
            'heart_rate': 300  # Unrealistic
        }
        is_valid = HeartbeatValidator.validate(invalid_data_2)
        self.log_test("Invalid Data Test 2", not is_valid, "Out of range rejected")
        
        # Test anomaly detection
        print("\nAnomaly Detection Tests:")
        test_cases = [
            (35, 'critical_low'),
            (55, 'low'),
            (75, 'normal'),
            (110, 'elevated'),
            (170, 'high'),
            (195, 'critical_high')
        ]
        
        for heart_rate, expected_level in test_cases:
            level, desc = HeartbeatValidator.detect_anomaly(heart_rate)
            passed = level == expected_level
            print(f"  Heart Rate {heart_rate}: {level} - {desc} {'✓' if passed else '✗'}")
        
        return True
    
    def print_summary(self):
        """Print test summary"""
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        
        total = len(self.test_results)
        passed = sum(1 for _, p, _ in self.test_results if p)
        failed = total - passed
        
        print(f"\nTotal Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Success Rate: {(passed/total*100):.1f}%")
        
        if failed > 0:
            print("\nFailed Tests:")
            for name, passed, msg in self.test_results:
                if not passed:
                    print(f"  - {name}: {msg}")
    
    def run_all_tests(self):
        """Run all tests"""
        print("\n" + "="*60)
        print("REAL-TIME CUSTOMER HEARTBEAT MONITORING SYSTEM")
        print("AUTOMATED TEST SUITE")
        print("="*60)
        
        # Run tests in sequence
        tests = [
            self.test_database_connection,
            self.test_table_exists,
            self.test_indexes,
            self.test_insert_sample_data,
            self.test_queries,
            self.test_data_validation
        ]
        
        for test in tests:
            try:
                test()
            except Exception as e:
                print(f"\n✗ Test failed with exception: {e}")
        
        # Print summary
        self.print_summary()


def main():
    """Main test execution"""
    tester = SystemTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()