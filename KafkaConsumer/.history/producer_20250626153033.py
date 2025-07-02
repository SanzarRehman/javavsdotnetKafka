#!/usr/bin/env python3
"""
Kafka Producer for testing the loan disbursement consumer
Sends test DisburseCommand messages to the loan-disbursement topic
"""

import json
import time
from datetime import datetime
from kafka import KafkaProducer
import uuid

def create_test_message(loan_id, customer_id, amount, currency="USD"):
    """Create a test DisburseCommand message"""
    return {
        "loanId": loan_id,
        "customerId": customer_id,
        "amount": amount,
        "currency": currency,
        "disbursementDate": datetime.now().isoformat(),
        "userContext": {
            "userId": "test-user-123",
            "sessionId": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "correlationId": str(uuid.uuid4())
        }
    }

def main():
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    print("🚀 Starting Kafka test producer...")
    print("📋 Will send test loan disbursement messages every 3 seconds")
    print("🛑 Press Ctrl+C to stop\n")
    
    loan_counter = 1
    
    try:
        while True:
            # Create test message
            message = create_test_message(
                loan_id=f"LOAN-{loan_counter:04d}",
                customer_id=f"CUST-{loan_counter:04d}",
                amount=round(1000 + (loan_counter * 150.75), 2)
            )
            
            # Send message
            key = f"loan-{loan_counter}"
            future = producer.send('loan-disbursement', key=key, value=message)
            
            # Wait for message to be sent
            record_metadata = future.get(timeout=10)
            
            print(f"✅ Sent message {loan_counter}:")
            print(f"   📧 Topic: {record_metadata.topic}")
            print(f"   🔑 Key: {key}")
            print(f"   💰 Loan ID: {message['loanId']}")
            print(f"   👤 Customer: {message['customerId']}")
            print(f"   💵 Amount: ${message['amount']}")
            print(f"   📍 Partition: {record_metadata.partition}")
            print(f"   📊 Offset: {record_metadata.offset}")
            print()
            
            loan_counter += 1
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.close()
        print("👋 Producer closed. Goodbye!")

if __name__ == "__main__":
    main()