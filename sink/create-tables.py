#!/usr/bin/env python3
"""
Script to create tables in the eos_sink PostgreSQL database.
Creates sink_events table with unique constraints for EOS guarantees.
"""

import sys
import time
import psycopg2
from psycopg2 import sql, Error


def create_tables():
    """Create the sink_events table in PostgreSQL."""
    
    # Database connection parameters
    db_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'eos_sink',
        'user': 'eos',
        'password': 'eos'
    }
    
    # SQL statements to create tables
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sink_events (
        id SERIAL PRIMARY KEY,
        event_id VARCHAR(64),
        kafka_topic TEXT,
        kafka_partition INT,
        kafka_offset BIGINT,
        payload TEXT,
        created_at TIMESTAMP DEFAULT now()
    );
    """
    
    add_constraint_sql = """
    ALTER TABLE sink_events 
    ADD CONSTRAINT unique_kafka_offset 
    UNIQUE (kafka_topic, kafka_partition, kafka_offset);
    """
    
    try:
        # Connect to PostgreSQL
        print("🔌 Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Create the sink_events table
        print("📋 Creating sink_events table...")
        cursor.execute(create_table_sql)
        conn.commit()
        print("✓ Table 'sink_events' created successfully!")
        
        # Add unique constraint (if it doesn't exist)
        try:
            print("🔐 Adding unique constraint...")
            cursor.execute(add_constraint_sql)
            conn.commit()
            print("✓ Unique constraint added successfully!")
        except Error as e:
            if 'already exists' in str(e):
                print("ℹ️  Unique constraint already exists!")
            else:
                # Try without ON CONFLICT for older PostgreSQL versions
                try:
                    cursor.execute("""
                    ALTER TABLE sink_events 
                    ADD CONSTRAINT unique_kafka_offset 
                    UNIQUE (kafka_topic, kafka_partition, kafka_offset);
                    """)
                    conn.commit()
                    print("✓ Unique constraint added successfully!")
                except Error:
                    print("ℹ️  Unique constraint already exists!")
        
        # Verify table was created
        cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'sink_events'
        """)
        
        if cursor.fetchone():
            print("✓ Table verification: sink_events table exists!")
        else:
            print("✗ Table verification failed!")
            cursor.close()
            conn.close()
            return False
        
        # Display table structure
        cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'sink_events'
        ORDER BY ordinal_position
        """)
        
        print("\n📊 Table structure:")
        print(f"{'Column Name':<20} {'Data Type':<20} {'Nullable'}")
        print("-" * 55)
        for row in cursor.fetchall():
            print(f"{row[0]:<20} {row[1]:<20} {row[2]}")
        
        cursor.close()
        conn.close()
        return True
        
    except (Exception, Error) as error:
        print(f"✗ Database error: {error}")
        print(f"  Make sure PostgreSQL is running on {db_config['host']}:{db_config['port']}")
        print(f"  Database: {db_config['database']}, User: {db_config['user']}")
        return False


if __name__ == '__main__':
    success = create_tables()
    sys.exit(0 if success else 1)
