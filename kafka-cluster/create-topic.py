#!/usr/bin/env python3
"""
Script to create Kafka topic with specific configuration.
Creates 'eos-topic' with 12 partitions and replication factor of 3.
"""

import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def create_eos_topic():
    """Create the eos-topic with configured settings."""
    
    # Configuration
    bootstrap_servers = 'localhost:9092'
    topic_name = 'eos-topic'
    num_partitions = 12
    replication_factor = 3
    
    # Topic configurations
    topic_config = {
        'min.insync.replicas': '2',
        'retention.ms': '3600000'
    }
    
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='topic-creator'
        )
        
        # Create NewTopic object
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=topic_config
        )
        
        # Create the topic
        fs = admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        
        # Wait for the topic to be created
        for topic, future in fs.items():
            try:
                future.result()  # The result itself is None
                print(f"✓ Topic '{topic}' created successfully!")
            except TopicAlreadyExistsError:
                print(f"⚠ Topic '{topic}' already exists!")
            except Exception as e:
                print(f"✗ Error creating topic '{topic}': {e}")
                admin_client.close()
                return False
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"✗ Failed to connect to Kafka broker: {e}")
        print(f"  Make sure Kafka is running on {bootstrap_servers}")
        return False


if __name__ == '__main__':
    success = create_eos_topic()
    sys.exit(0 if success else 1)
