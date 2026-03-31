#!/usr/bin/env python3
"""
Script to create Kafka topics with specific configuration.
Creates 'eos-topic' with 12 partitions and audit topics with 3 partitions.
Both with replication factor of 3 and same EOS guarantees.
"""

import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def create_topics():
    """Create the eos-topic and audit topics with configured settings."""
    
    # Configuration
    bootstrap_servers = 'localhost:9092'
    replication_factor = 3
    
    # Shared topic configuration (EOS guarantees)
    base_topic_config = {
        'min.insync.replicas': '2'
    }
    
    # Define topics
    topics = [
        {
            'name': 'eos-topic',
            'num_partitions': 12,
            'topic_configs': {
                **base_topic_config,
                'retention.ms': '600000'
            }
        },
        {
            'name': 'audit-topic',
            'num_partitions': 3,
            'topic_configs': base_topic_config
        },
        {
            'name': 'audit.outcomes',
            'num_partitions': 3,
            'topic_configs': base_topic_config
        }
    ]
    
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='topic-creator'
        )
        
        # Create NewTopic objects
        new_topics = []
        for topic in topics:
            new_topic = NewTopic(
                name=topic['name'],
                num_partitions=topic['num_partitions'],
                replication_factor=replication_factor,
                topic_configs=topic['topic_configs']
            )
            new_topics.append(new_topic)
        
        # Create the topics
        fs = admin_client.create_topics(new_topics=new_topics, validate_only=False)
        
        # Wait for the topics to be created
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
    success = create_topics()
    sys.exit(0 if success else 1)
