#!/bin/bash

# TPS Benchmark Test Script
echo "=== Kafka Consumer TPS Benchmark ==="
echo "Testing both .NET and Spring Boot consumers with identical high-performance configurations"
echo ""

# Function to get message count from database
get_dotnet_count() {
    docker exec postgres psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM dotnet_messages;" 2>/dev/null | tr -d ' ' | tr -d '\n'
}

get_springboot_count() {
    docker exec postgres psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM spring_boot_messages;" 2>/dev/null | tr -d ' ' | tr -d '\n'
}

# Function to check if services are running
check_services() {
    echo "Checking if all services are running..."
    
    # Check if containers are running
    if ! docker ps --filter "name=postgres" --format "{{.Names}}" | grep -q "postgres"; then
        echo "Error: PostgreSQL container is not running"
        echo "Use: docker-compose up -d to start all services"
        exit 1
    fi
    
    if ! docker ps --filter "name=spring-boot-consumer" --format "{{.Names}}" | grep -q "spring-boot-consumer"; then
        echo "Error: Spring Boot consumer is not running"
        echo "Use: docker-compose up -d to start all services"
        exit 1
    fi
    
    if ! docker ps --filter "name=dotnet-consumer" --format "{{.Names}}" | grep -q "dotnet-consumer"; then
        echo "Error: .NET consumer is not running"
        echo "Use: docker-compose up -d to start all services"
        exit 1
    fi
    
    if ! docker ps --filter "name=message-producer" --format "{{.Names}}" | grep -q "message-producer"; then
        echo "Error: Message producer is not running"
        echo "Use: docker-compose up -d to start all services"
        exit 1
    fi
    
    echo "All services are running ✓"
}

# Check if PostgreSQL is accessible
check_database() {
    if ! docker exec postgres psql -U postgres -d microfinance_db -c "\q" 2>/dev/null; then
        echo "Error: Cannot connect to PostgreSQL database"
        echo "Make sure the database is ready and tables are created"
        exit 1
    fi
    echo "Database connection verified ✓"
}

# Clear existing data
clear_data() {
    echo "Clearing existing data..."
    docker exec postgres psql -U postgres -d microfinance_db -c "TRUNCATE TABLE dotnet_messages, spring_boot_messages;" 2>/dev/null
    echo "Data cleared ✓"
}

# Check services and database
check_services
check_database

# Clear data if requested
if [ "$1" == "clear" ] || [ "$1" == "start" ]; then
    clear_data
fi

echo "Starting TPS benchmark test..."
echo "Configuration: 50 concurrent threads, manual commits, identical Kafka settings"
echo ""

# Test duration in seconds
TEST_DURATION=60

echo "Starting $TEST_DURATION second test..."
START_TIME=$(date +%s)

# Record initial counts
DOTNET_START=$(get_dotnet_count)
SPRINGBOOT_START=$(get_springboot_count)

# Ensure we have valid numbers
if ! [[ "$DOTNET_START" =~ ^[0-9]+$ ]]; then DOTNET_START=0; fi
if ! [[ "$SPRINGBOOT_START" =~ ^[0-9]+$ ]]; then SPRINGBOOT_START=0; fi

echo "Initial counts - .NET: $DOTNET_START, Spring Boot: $SPRINGBOOT_START"
echo ""

# Monitor every 10 seconds
for i in $(seq 10 10 $TEST_DURATION); do
    sleep 10
    DOTNET_CURRENT=$(get_dotnet_count)
    SPRINGBOOT_CURRENT=$(get_springboot_count)
    
    # Ensure we have valid numbers
    if ! [[ "$DOTNET_CURRENT" =~ ^[0-9]+$ ]]; then DOTNET_CURRENT=0; fi
    if ! [[ "$SPRINGBOOT_CURRENT" =~ ^[0-9]+$ ]]; then SPRINGBOOT_CURRENT=0; fi
    
    DOTNET_PROCESSED=$((DOTNET_CURRENT - DOTNET_START))
    SPRINGBOOT_PROCESSED=$((SPRINGBOOT_CURRENT - SPRINGBOOT_START))
    
    DOTNET_TPS=$((DOTNET_PROCESSED / i))
    SPRINGBOOT_TPS=$((SPRINGBOOT_PROCESSED / i))
    
    echo "[$i s] .NET: $DOTNET_PROCESSED messages ($DOTNET_TPS TPS) | Spring Boot: $SPRINGBOOT_PROCESSED messages ($SPRINGBOOT_TPS TPS)"
done

# Final results
END_TIME=$(date +%s)
ACTUAL_DURATION=$((END_TIME - START_TIME))

DOTNET_FINAL=$(get_dotnet_count)
SPRINGBOOT_FINAL=$(get_springboot_count)

# Ensure we have valid numbers
if ! [[ "$DOTNET_FINAL" =~ ^[0-9]+$ ]]; then DOTNET_FINAL=0; fi
if ! [[ "$SPRINGBOOT_FINAL" =~ ^[0-9]+$ ]]; then SPRINGBOOT_FINAL=0; fi

DOTNET_TOTAL=$((DOTNET_FINAL - DOTNET_START))
SPRINGBOOT_TOTAL=$((SPRINGBOOT_FINAL - SPRINGBOOT_START))

DOTNET_FINAL_TPS=$((DOTNET_TOTAL / ACTUAL_DURATION))
SPRINGBOOT_FINAL_TPS=$((SPRINGBOOT_TOTAL / ACTUAL_DURATION))

echo ""
echo "=== FINAL RESULTS ($ACTUAL_DURATION seconds) ==="
echo ".NET Consumer:"
echo "  Total Messages: $DOTNET_TOTAL"
echo "  Average TPS: $DOTNET_FINAL_TPS"
echo ""
echo "Spring Boot Consumer:"
echo "  Total Messages: $SPRINGBOOT_TOTAL" 
echo "  Average TPS: $SPRINGBOOT_FINAL_TPS"
echo ""

if [ $DOTNET_FINAL_TPS -gt $SPRINGBOOT_FINAL_TPS ]; then
    if [ $SPRINGBOOT_FINAL_TPS -gt 0 ]; then
        DIFF=$(((DOTNET_FINAL_TPS - SPRINGBOOT_FINAL_TPS) * 100 / SPRINGBOOT_FINAL_TPS))
        echo ".NET is $DIFF% faster than Spring Boot"
    else
        echo ".NET processed messages, Spring Boot processed none"
    fi
elif [ $SPRINGBOOT_FINAL_TPS -gt $DOTNET_FINAL_TPS ]; then
    if [ $DOTNET_FINAL_TPS -gt 0 ]; then
        DIFF=$(((SPRINGBOOT_FINAL_TPS - DOTNET_FINAL_TPS) * 100 / DOTNET_FINAL_TPS))
        echo "Spring Boot is $DIFF% faster than .NET"
    else
        echo "Spring Boot processed messages, .NET processed none"
    fi
else
    echo "Both consumers have identical performance"
fi

echo ""
echo "Benchmark completed!"
echo "Use './scripts/check-performance.sh' to view detailed performance analysis"