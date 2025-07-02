#!/usr/bin/env python3
"""
Kafka Producer Script for DisburseCommand messages
Sends test messages to the Microfinance.Commands.DisburseCommand topic
"""

import json
import uuid
from datetime import datetime
from confluent_kafka import Producer
import time

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': '10.42.53.125:19092,10.42.53.125:29092,10.42.53.125:39092'
}
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

def delivery_callback(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message sent successfully!")
        print(f"   Topic: {msg.topic()}")
        print(f"   Partition: {msg.partition()}")
        print(f"   Offset: {msg.offset()}")

def main():
    print("Starting Kafka Producer...")
    print(f"Connecting to: {KAFKA_CONFIG['bootstrap.servers']}")
    print(f"Topic: {TOPIC}")
    
    # Create Kafka producer
    producer = Producer(KAFKA_CONFIG)
    
    try:
        message_count = 0
        while True:
            # Create test message
            message = create_test_message()
            message_json = json.dumps(message)
            
            # Send message
            producer.produce(
                topic=TOPIC,
                value=message_json.encode('utf-8'),
                callback=delivery_callback
            )
            
            # Wait for message delivery
            producer.poll(0)
            
            message_count += 1
            print(f"   Loan Name: {message['name']}")
            print(f"   Correlation ID: {message['correlationId']}")
            print(f"   Message #{message_count}")
            print("-" * 50)
            
            # Wait before sending next message
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        # Wait for any outstanding messages to be delivered
        producer.flush()
        print("Producer closed.")

if __name__ == "__main__":
    main()