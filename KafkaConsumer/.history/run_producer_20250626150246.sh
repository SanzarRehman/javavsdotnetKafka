#!/bin/bash
# Kafka Producer Runner Script

echo "🚀 Setting up Kafka Producer..."

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    exit 1
fi

# Install dependencies if not already installed
echo "📦 Installing Python dependencies..."
pip3 install -r requirements.txt

# Make producer script executable
chmod +x producer.py

# Run the producer
echo "🎯 Starting Kafka Producer..."
python3 producer.py