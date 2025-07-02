#!/bin/bash

echo "=== Performance Comparison Report ==="
echo "Timestamp: $(date)"
echo ""

# Check if PostgreSQL is accessible
if ! docker exec postgres psql -U postgres -d microfinance_db -c "\q" 2>/dev/null; then
    echo "Error: Cannot connect to PostgreSQL. Make sure services are running."
    echo "Use: ./scripts/start.sh to start all services"
    exit 1
fi

echo "Querying database for performance metrics..."
echo ""

# Function to get message counts
get_dotnet_count() {
    docker exec postgres psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM dotnet_messages;" 2>/dev/null | tr -d ' ' | tr -d '\n'
}

get_springboot_count() {
    docker exec postgres psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM spring_boot_messages;" 2>/dev/null | tr -d ' ' | tr -d '\n'
}

get_recent_dotnet_count() {
    docker exec postgres psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM dotnet_messages WHERE processed_at > NOW() - INTERVAL '5 minutes';" 2>/dev/null | tr -d ' ' | tr -d '\n'
}

get_recent_springboot_count() {
    docker exec postgres psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM spring_boot_messages WHERE processed_at > NOW() - INTERVAL '5 minutes';" 2>/dev/null | tr -d ' ' | tr -d '\n'
}

# Get current message counts
SPRING_BOOT_TOTAL=$(get_springboot_count)
DOTNET_TOTAL=$(get_dotnet_count)
SPRING_BOOT_RECENT=$(get_recent_springboot_count)
DOTNET_RECENT=$(get_recent_dotnet_count)

# Ensure we have valid numbers
if ! [[ "$SPRING_BOOT_TOTAL" =~ ^[0-9]+$ ]]; then SPRING_BOOT_TOTAL=0; fi
if ! [[ "$DOTNET_TOTAL" =~ ^[0-9]+$ ]]; then DOTNET_TOTAL=0; fi
if ! [[ "$SPRING_BOOT_RECENT" =~ ^[0-9]+$ ]]; then SPRING_BOOT_RECENT=0; fi
if ! [[ "$DOTNET_RECENT" =~ ^[0-9]+$ ]]; then DOTNET_RECENT=0; fi

# Calculate TPS (transactions per second) over last 5 minutes
SPRING_BOOT_TPS=0
DOTNET_TPS=0
if [ $SPRING_BOOT_RECENT -gt 0 ]; then
    SPRING_BOOT_TPS=$(echo "scale=2; $SPRING_BOOT_RECENT / 300" | bc -l 2>/dev/null || echo "$SPRING_BOOT_RECENT" | awk '{print $1/300}')
fi
if [ $DOTNET_RECENT -gt 0 ]; then
    DOTNET_TPS=$(echo "scale=2; $DOTNET_RECENT / 300" | bc -l 2>/dev/null || echo "$DOTNET_RECENT" | awk '{print $1/300}')
fi

echo "Performance Results (Last 5 Minutes):"
echo "====================================="
echo "Consumer Type | Total Messages | Recent (5min) | TPS (5min) | Status"
echo "-------------|----------------|---------------|------------|--------"
printf "%-12s | %-14s | %-13s | %-10s | %s\n" \
    "Spring Boot" "$SPRING_BOOT_TOTAL" "$SPRING_BOOT_RECENT" "$SPRING_BOOT_TPS" "$([ $SPRING_BOOT_RECENT -gt 0 ] && echo "ACTIVE" || echo "IDLE")"
printf "%-12s | %-14s | %-13s | %-10s | %s\n" \
    ".NET" "$DOTNET_TOTAL" "$DOTNET_RECENT" "$DOTNET_TPS" "$([ $DOTNET_RECENT -gt 0 ] && echo "ACTIVE" || echo "IDLE")"

echo ""
echo "Performance Comparison:"
echo "======================"
TOTAL_RECENT=$((SPRING_BOOT_RECENT + DOTNET_RECENT))
if [ $TOTAL_RECENT -gt 0 ]; then
    TOTAL_TPS=$(echo "scale=2; $TOTAL_RECENT / 300" | bc -l 2>/dev/null || echo "$TOTAL_RECENT" | awk '{print $1/300}')
    echo "Combined TPS (last 5 min): $TOTAL_TPS"
    
    if [ $SPRING_BOOT_RECENT -gt $DOTNET_RECENT ]; then
        if [ $DOTNET_RECENT -gt 0 ]; then
            DIFF=$(((SPRING_BOOT_RECENT - DOTNET_RECENT) * 100 / DOTNET_RECENT))
            echo "Spring Boot is $DIFF% faster than .NET (in last 5 minutes)"
        else
            echo "Spring Boot is processing messages, .NET is idle"
        fi
    elif [ $DOTNET_RECENT -gt $SPRING_BOOT_RECENT ]; then
        if [ $SPRING_BOOT_RECENT -gt 0 ]; then
            DIFF=$(((DOTNET_RECENT - SPRING_BOOT_RECENT) * 100 / SPRING_BOOT_RECENT))
            echo ".NET is $DIFF% faster than Spring Boot (in last 5 minutes)"
        else
            echo ".NET is processing messages, Spring Boot is idle"
        fi
    else
        echo "Both consumers have identical performance"
    fi
else
    echo "No recent activity detected. Check if producer is running."
fi

echo ""
echo "Live Message Counts:"
echo "===================="
echo "Spring Boot Total Messages: $SPRING_BOOT_TOTAL"
echo ".NET Total Messages: $DOTNET_TOTAL"

echo ""
echo "Container Resource Usage:"
echo "========================"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" \
    spring-boot-consumer dotnet-consumer message-producer postgres kafka zookeeper 2>/dev/null || echo "Docker stats not available"

echo ""
echo "Container Status:"
echo "================="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "To view live logs:"
echo "  docker logs -f spring-boot-consumer"
echo "  docker logs -f dotnet-consumer"
echo "  docker logs -f message-producer"