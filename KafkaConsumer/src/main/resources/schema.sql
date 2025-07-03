-- Create dotnet_messages table for Java Kafka consumer
CREATE TABLE IF NOT EXISTS dotnet_messages (
    id BIGSERIAL PRIMARY KEY,
    message_id VARCHAR(255) NOT NULL,
    content TEXT,
    timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    UNIQUE(message_id)
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_dotnet_messages_message_id ON dotnet_messages(message_id);
CREATE INDEX IF NOT EXISTS idx_dotnet_messages_processed_at ON dotnet_messages(processed_at);