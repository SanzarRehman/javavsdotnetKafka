# Spring Boot vs .NET Kafka Consumer Performance Comparison

This project compares the performance of Spring Boot (Java) and .NET Kafka consumers processing messages and writing to a PostgreSQL database.

## Architecture

- **Producer**: Node.js application that generates DisburseCommand messages
- **Spring Boot Consumer**: Java application using Spring Kafka with virtual threads
- **.NET Consumer**: C# application using Confluent.Kafka with concurrent processing
- **Database**: PostgreSQL for storing processed messages
- **Message Broker**: Apache Kafka

## Prerequisites

- Docker and Docker Compose
- At least 8GB RAM available for containers
- `bc` calculator for benchmark calculations (install with `brew install bc` on macOS)

## Quick Start

1. **Start all services**:
   ```bash
   docker-compose up -d
   ```

2. **Wait for services to be ready** (about 2-3 minutes):
   ```bash
   docker-compose logs -f
   ```

3. **Run the benchmark**:
   ```bash
   ./scripts/benchmark-tps.sh
   ```

4. **Check ongoing performance**:
   ```bash
   ./scripts/check-performance.sh
   ```

## Services

### Spring Boot Consumer
- **Container**: `spring-boot-consumer`
- **Port**: Internal only
- **Technology**: Java 21 with Virtual Threads, Spring Boot, Spring Kafka
- **Database Identifier**: Records have `service_id = 'spring-boot-service'`

### .NET Consumer
- **Container**: `dotnet-consumer`
- **Port**: Internal only
- **Technology**: .NET 9, Confluent.Kafka, Entity Framework Core
- **Concurrency**: 50 concurrent workers
- **Database Identifier**: Records have `service_id = 'dotnet-service'`

### Message Producer
- **Container**: `message-producer`
- **Technology**: Node.js with KafkaJS
- **Default Rate**: 1000 messages/second
- **Topic**: `disburse-commands`

### Database
- **Container**: `postgres`
- **Port**: `5432`
- **Database**: `microfinance_db`
- **Table**: `loans` (shared by both consumers)

## Configuration

### Environment Variables

You can customize the benchmark by setting environment variables in `docker-compose.yml`:

```yaml
# Producer settings
MESSAGES_PER_SECOND: 1000  # Messages per second to generate
TOPIC_NAME: disburse-commands

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS: kafka:29092
```

### Consumer Concurrency

- **Spring Boot**: Uses virtual threads with batch processing
- **.NET**: Uses 50 concurrent workers (configurable in `KafkaMessageConsumer.cs`)

## Benchmark Scripts

### `./scripts/benchmark-tps.sh`
Runs a 60-second benchmark comparing TPS (Transactions Per Second) between both consumers.

### `./scripts/check-performance.sh`
Shows current status including:
- Container health
- Total messages processed
- Recent activity (last 5 minutes)
- Resource usage
- Database activity breakdown

## Database Schema

```sql
-- Main table for both consumers
CREATE TABLE loans (
    id UUID PRIMARY KEY,
    member_id UUID NOT NULL,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    service_id VARCHAR(255) NOT NULL,  -- Identifies which consumer processed it
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- ... other audit fields
);
```

## Monitoring

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f spring-boot-consumer
docker-compose logs -f dotnet-consumer
docker-compose logs -f message-producer
```

### Database Access
```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d microfinance_db

# Query performance
SELECT service_id, COUNT(*) as processed_count 
FROM loans 
GROUP BY service_id;
```

### Kafka Topics
```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Topic details
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic disburse-commands
```

## Performance Tuning

### Spring Boot Consumer
- Uses virtual threads for concurrent processing
- Batch processing with manual acknowledgments
- HikariCP connection pool (max 500 connections)

### .NET Consumer
- 50 concurrent message handlers
- Entity Framework Core with connection pooling
- Structured concurrency for message processing

### Database
- Optimized PostgreSQL configuration
- Proper indexing on frequently queried columns
- Connection pooling for both consumers

## Stopping the Benchmark

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean restart)
docker-compose down -v
```

## Troubleshooting

### Services not starting
```bash
# Check service status
docker-compose ps

# Check specific service logs
docker-compose logs service-name
```

### No messages being processed
```bash
# Check if producer is running
docker-compose logs message-producer

# Check Kafka topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Database connection issues
```bash
# Check database is ready
docker-compose logs postgres

# Test connection
docker exec postgres pg_isready -U postgres
```

## Expected Results

Typical performance characteristics:
- **Combined throughput**: 2000-5000 TPS depending on hardware
- **.NET typically faster**: Due to compiled nature and efficient async I/O
- **Spring Boot competitive**: Virtual threads provide good concurrency with lower memory usage

Results will vary based on:
- Available CPU cores
- Memory allocation
- Disk I/O performance
- Network latency (Docker networking)