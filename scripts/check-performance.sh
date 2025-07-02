#!/bin/bash

echo "==================================="
echo "Spring Boot vs .NET Performance Check"
echo "==================================="

# Function to start all services
start_services() {
    echo "Starting all services with docker-compose..."
    cd "$(dirname "$0")/.." # Go to project root
    docker-compose down  # Stop any existing containers
    docker-compose up -d --build
    echo "Waiting for services to be ready..."
    sleep 45  # Increased wait time for all services to fully start
    
    # Wait for Kafka to be ready
    echo "Waiting for Kafka to be ready..."
    timeout 60 bash -c 'until docker exec spvsdot-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; do sleep 2; done'
    
    # Wait for Postgres to be ready
    echo "Waiting for Postgres to be ready..."
    timeout 60 bash -c 'until docker exec spvsdot-postgres-1 pg_isready -U postgres &>/dev/null; do sleep 2; done'
    
    echo "All services should be ready now!"
}

# Check if services should be started
if [ "$1" == "start" ]; then
    start_services
fi

# Check if containers are running
echo "Container Status:"
echo "================="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" --filter "name=spvsdot"

echo ""
echo "Message Processing Counts:"
echo "========================="

# Count messages processed by each service
SPRING_BOOT_TOTAL=$(docker exec spvsdot-postgres-1 psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM spring_boot_messages;" 2>/dev/null | xargs)
DOTNET_TOTAL=$(docker exec spvsdot-postgres-1 psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM dotnet_messages;" 2>/dev/null | xargs)

echo "Spring Boot total messages: $SPRING_BOOT_TOTAL"
echo ".NET total messages: $DOTNET_TOTAL"

# Get recent activity (last 5 minutes) 
SPRING_BOOT_RECENT=$(docker exec spvsdot-postgres-1 psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM spring_boot_messages WHERE processed_at > NOW() - INTERVAL '5 minutes';" 2>/dev/null | xargs)
DOTNET_RECENT=$(docker exec spvsdot-postgres-1 psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM dotnet_messages WHERE processed_at > NOW() - INTERVAL '5 minutes';" 2>/dev/null | xargs)

echo "Spring Boot recent (last 5 min): $SPRING_BOOT_RECENT"
echo ".NET recent (last 5 min): $DOTNET_RECENT"

# Calculate TPS (transactions per second) over last 5 minutes
echo ""
echo "Performance Metrics (TPS):"
echo "========================="

if [ "$SPRING_BOOT_RECENT" -gt 0 ]; then
    SPRING_BOOT_TPS=$(echo "scale=2; $SPRING_BOOT_RECENT / 300" | bc -l 2>/dev/null || echo "$SPRING_BOOT_RECENT" | awk '{print $1/300}')
    echo "Spring Boot TPS (last 5 min): $SPRING_BOOT_TPS"
else
    echo "Spring Boot TPS (last 5 min): 0.00"
fi

if [ "$DOTNET_RECENT" -gt 0 ]; then
    DOTNET_TPS=$(echo "scale=2; $DOTNET_RECENT / 300" | bc -l 2>/dev/null || echo "$DOTNET_RECENT" | awk '{print $1/300}')
    echo ".NET TPS (last 5 min): $DOTNET_TPS"
else
    echo ".NET TPS (last 5 min): 0.00"
fi

# Calculate combined TPS
TOTAL_RECENT=$((SPRING_BOOT_RECENT + DOTNET_RECENT))
if [ "$TOTAL_RECENT" -gt 0 ]; then
    TOTAL_TPS=$(echo "scale=2; $TOTAL_RECENT / 300" | bc -l 2>/dev/null || echo "$TOTAL_RECENT" | awk '{print $1/300}')
    echo "Combined TPS (last 5 min): $TOTAL_TPS"
else
    echo "Combined TPS (last 5 min): 0.00"
fi

echo ""
echo "Kafka Topic Information:"
echo "======================="
docker exec spvsdot-kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic disburse-commands 2>/dev/null || echo "Topic disburse-commands not found"

echo ""
echo "Consumer Group Status:"
echo "====================="
docker exec spvsdot-kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group simple 2>/dev/null || echo "Consumer group 'simple' not found"

echo ""
echo "Container Resource Usage:"
echo "========================"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" \
    spvsdot-spring-boot-consumer-1 spvsdot-dotnet-consumer-1 spvsdot-message-producer-1 spvsdot-postgres-1 spvsdot-kafka-1 spvsdot-zookeeper-1 2>/dev/null || echo "Docker stats not available"

echo ""
echo "Recent Message Processing Activity (by minute):"
echo "==============================================="
docker exec spvsdot-postgres-1 psql -U postgres -d microfinance_db -c "
WITH spring_boot_activity AS (
    SELECT 
        DATE_TRUNC('minute', processed_at) as minute,
        'Spring Boot' as service,
        COUNT(*) as messages_processed
    FROM spring_boot_messages 
    WHERE processed_at > NOW() - INTERVAL '10 minutes' 
    GROUP BY DATE_TRUNC('minute', processed_at)
),
dotnet_activity AS (
    SELECT 
        DATE_TRUNC('minute', processed_at) as minute,
        '.NET' as service,
        COUNT(*) as messages_processed
    FROM dotnet_messages 
    WHERE processed_at > NOW() - INTERVAL '10 minutes' 
    GROUP BY DATE_TRUNC('minute', processed_at)
)
SELECT * FROM spring_boot_activity
UNION ALL
SELECT * FROM dotnet_activity
ORDER BY minute DESC, service 
LIMIT 20;" 2>/dev/null || echo "Unable to query recent activity"

echo ""
echo "Consumer Logs (last 20 lines):"
echo "=============================="
echo "Spring Boot Consumer:"
docker logs --tail 20 spvsdot-spring-boot-consumer-1 2>/dev/null || echo "No logs available"
echo ""
echo ".NET Consumer:"
docker logs --tail 20 spvsdot-dotnet-consumer-1 2>/dev/null || echo "No logs available"
echo ""
echo "Message Producer:"
docker logs --tail 20 spvsdot-message-producer-1 2>/dev/null || echo "No logs available"

echo ""
echo "Message Tables Structure:"
echo "========================"
echo "Spring Boot Messages:"
docker exec spvsdot-postgres-1 psql -U postgres -d microfinance_db -c "\d spring_boot_messages" 2>/dev/null || echo "Unable to describe table structure"
echo ""
echo ".NET Messages:"
docker exec spvsdot-postgres-1 psql -U postgres -d microfinance_db -c "\d dotnet_messages" 2>/dev/null || echo "Unable to describe table structure"

echo ""
echo "Usage Examples:"
echo "==============="
echo "  Start all services:    $0 start"
echo "  View live logs:"
echo "    docker logs -f spvsdot-spring-boot-consumer-1"
echo "    docker logs -f spvsdot-dotnet-consumer-1" 
echo "    docker logs -f spvsdot-message-producer-1"
echo "  Stop all services:     docker-compose down"
echo "  Restart specific service: docker-compose restart spring-boot-consumer"
echo "  View Kafka messages:   docker exec spvsdot-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic disburse-commands --from-beginning"
echo "  Monitor Kafka lag:     docker exec spvsdot-kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group simple"
echo "  Database shell:        docker exec -it spvsdot-postgres-1 psql -U postgres -d microfinance_db"