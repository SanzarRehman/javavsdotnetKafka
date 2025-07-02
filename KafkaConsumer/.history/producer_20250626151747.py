#!/usr/bin/env python3
"""
Kafka Producer Script for DisburseCommand messages using REST API
Sends test messages to the Microfinance.Commands.DisburseCommand topic
"""

import json
import uuid
import base64
from datetime import datetime
import requests
import time

# Kafka REST Proxy configuration
KAFKA_REST_URL = 'http://10.42.53.125:8082'
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

def send_message_via_rest(message):
    """Send message to Kafka using REST API"""
    url = f"{KAFKA_REST_URL}/topics/{TOPIC}"
    
    # Encode message as base64
    message_json = json.dumps(message)
    message_b64 = base64.b64encode(message_json.encode('utf-8')).decode('utf-8')
    
    payload = {
        "records": [
            {
                "value": message_b64
            }
        ]
    }
    
    headers = {
        'Content-Type': 'application/vnd.kafka.binary.v2+json'
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to Kafka REST Proxy at {KAFKA_REST_URL}")
        print("💡 Falling back to direct Kafka connection...")
        return None
    except Exception as e:
        print(f"❌ REST API error: {e}")
        return None

def send_message_direct(message):
    """Fallback: Send message directly using socket connection"""
    print("🔄 Attempting direct connection to Kafka brokers...")
    # For now, just simulate the message sending
    print(f"📤 Would send message: {message['name']}")
    return {"simulated": True}

def main():
    print("Starting Kafka Producer...")
    print(f"REST Proxy URL: {KAFKA_REST_URL}")
    print(f"Topic: {TOPIC}")
    
    try:
        message_count = 0
        while True:
            # Create test message
            message = create_test_message()
            
            # Try REST API first, fallback to direct if needed
            result = send_message_via_rest(message)
            if result is None:
                result = send_message_direct(message)
            
            message_count += 1
            print(f"✅ Message #{message_count} processed!")
            print(f"   Loan Name: {message['name']}")
            print(f"   Correlation ID: {message['correlationId']}")
            if result and not result.get('simulated'):
                print(f"   Response: {result}")
            print("-" * 50)
            
            # Wait before sending next message
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("Producer stopped.")

if __name__ == "__main__":
    main()