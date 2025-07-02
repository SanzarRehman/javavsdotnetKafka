#!/bin/bash
# Kafka Producer Runner Script

echo "🚀 Setting up Kafka Producer..."

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📦 Installing Python dependencies..."
pip install -r requirements.txt

# Make producer script executable
chmod +x producer.py

# Run the producer
echo "🎯 Starting Kafka Producer..."
python producer.py