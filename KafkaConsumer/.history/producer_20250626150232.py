#!/usr/bin/env python3
"""
Kafka Producer Script for DisburseCommand messages
Sends test messages to the Microfinance.Commands.DisburseCommand topic
"""

import json
import uuid
from datetime import datetime
from kafka import KafkaProducer
import time

# Kafka configuration
KAFKA_SERVERS = ['10.42.53.125:19092', '10.42.53.125:29092', '10.42.53.125:39092']
TOPIC = 'Microfinance.Commands.DisburseCommand'

def create_test_message():
    """Create a test DisburseCommand message"""
    return {
        "name": f"Loan-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "correlationId": str(uuid.uuid4()),
        "userContext": {
            "userId": str(uuid.uuid4()),
            "applicationId": str(uuid.uuid4()),
            "sessionId": str(uuid.uuid4()),
            "tenantId": str(uuid.uuid4()),
            "verticalId": str(uuid.uuid4()),
            "serviceId": "loan-service",
            "email": "test@example.com",
            "phoneNumber": "+1234567890",
            "userName": "testuser",
            "displayName": "Test User",
            "language": "en",
            "roles": ["LOAN_OFFICER", "USER"]
        }
    }

def main():
    print("Starting Kafka Producer...")
    print(f"Connecting to: {KAFKA_SERVERS}")
    print(f"Topic: {TOPIC}")
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None
    )
    
    try:
        while True:
            # Create test message
            message = create_test_message()
            
            # Send message
            future = producer.send(TOPIC, value=message)
            result = future.get(timeout=10)
            
            print(f"✅ Message sent successfully!")
            print(f"   Topic: {result.topic}")
            print(f"   Partition: {result.partition}")
            print(f"   Offset: {result.offset}")
            print(f"   Loan Name: {message['name']}")
            print(f"   Correlation ID: {message['correlationId']}")
            print("-" * 50)
            
            # Wait before sending next message
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()