#!/bin/bash

echo "======================================="
echo "Spring Boot vs .NET TPS Benchmark"
echo "======================================="

# Function to get current timestamp
get_timestamp() {
    date +%s
}

# Function to count loans created by each consumer
count_spring_boot_loans() {
    docker exec postgres psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM loans WHERE service_id = 'spring-boot-service';" 2>/dev/null | xargs || echo "0"
}

count_dotnet_loans() {
    docker exec postgres psql -U postgres -d microfinance_db -t -c "SELECT COUNT(*) FROM loans WHERE service_id = 'dotnet-service';" 2>/dev/null | xargs || echo "0"
}

# Initial counts
echo "Starting TPS benchmark..."
START_TIME=$(get_timestamp)

SPRING_BOOT_START=$(count_spring_boot_loans)
DOTNET_START=$(count_dotnet_loans)

echo "Initial counts:"
echo "  Spring Boot: $SPRING_BOOT_START loans"
echo "  .NET: $DOTNET_START loans"
echo ""

# Wait for benchmark period
BENCHMARK_DURATION=60
echo "Running benchmark for $BENCHMARK_DURATION seconds..."
sleep $BENCHMARK_DURATION

# Final counts
END_TIME=$(get_timestamp)
SPRING_BOOT_END=$(count_spring_boot_loans)
DOTNET_END=$(count_dotnet_loans)

# Calculate differences
SPRING_BOOT_PROCESSED=$((SPRING_BOOT_END - SPRING_BOOT_START))
DOTNET_PROCESSED=$((DOTNET_END - DOTNET_START))

ACTUAL_DURATION=$((END_TIME - START_TIME))

# Calculate TPS
SPRING_BOOT_TPS=0
DOTNET_TPS=0

if [ $ACTUAL_DURATION -gt 0 ]; then
    SPRING_BOOT_TPS=$(echo "scale=2; $SPRING_BOOT_PROCESSED / $ACTUAL_DURATION" | bc)
    DOTNET_TPS=$(echo "scale=2; $DOTNET_PROCESSED / $ACTUAL_DURATION" | bc)
fi

echo ""
echo "======================================="
echo "BENCHMARK RESULTS ($ACTUAL_DURATION seconds)"
echo "======================================="
echo "Spring Boot:"
echo "  Messages processed: $SPRING_BOOT_PROCESSED"
echo "  TPS: $SPRING_BOOT_TPS"
echo ""
echo ".NET:"
echo "  Messages processed: $DOTNET_PROCESSED"
echo "  TPS: $DOTNET_TPS"
echo ""

# Determine winner
SPRING_BOOT_TPS_INT=$(echo "$SPRING_BOOT_TPS" | cut -d. -f1)
DOTNET_TPS_INT=$(echo "$DOTNET_TPS" | cut -d. -f1)

if [ "$DOTNET_TPS_INT" -gt "$SPRING_BOOT_TPS_INT" ]; then
    if [ "$SPRING_BOOT_TPS_INT" -gt 0 ]; then
        DIFF=$(echo "scale=2; ($DOTNET_TPS - $SPRING_BOOT_TPS) * 100 / $SPRING_BOOT_TPS" | bc)
        echo "🏆 .NET WINS! - $DIFF% faster than Spring Boot"
    else
        echo "🏆 .NET WINS! - Processed messages while Spring Boot processed none"
    fi
elif [ "$SPRING_BOOT_TPS_INT" -gt "$DOTNET_TPS_INT" ]; then
    if [ "$DOTNET_TPS_INT" -gt 0 ]; then
        DIFF=$(echo "scale=2; ($SPRING_BOOT_TPS - $DOTNET_TPS) * 100 / $DOTNET_TPS" | bc)
        echo "🏆 SPRING BOOT WINS! - $DIFF% faster than .NET"
    else
        echo "🏆 SPRING BOOT WINS! - Processed messages while .NET processed none"
    fi
else
    echo "🤝 TIE! - Both consumers have identical performance"
fi

echo ""
echo "Total combined TPS: $(echo "scale=2; $SPRING_BOOT_TPS + $DOTNET_TPS" | bc)"