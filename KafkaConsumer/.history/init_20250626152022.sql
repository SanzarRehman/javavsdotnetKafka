-- Initialize the database schema for Kafka Consumer
CREATE TABLE IF NOT EXISTS loans (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    correlation_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255),
    application_id VARCHAR(255),
    session_id VARCHAR(255),
    tenant_id VARCHAR(255),
    vertical_id VARCHAR(255),
    service_id VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(255),
    user_name VARCHAR(255),
    display_name VARCHAR(255),
    language VARCHAR(10),
    roles TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_loans_correlation_id ON loans(correlation_id);
CREATE INDEX IF NOT EXISTS idx_loans_tenant_id ON loans(tenant_id);

-- Insert a test record to verify the setup
INSERT INTO loans (name, correlation_id, user_id, tenant_id, service_id, email, user_name, display_name)
VALUES ('Test-Setup-Loan', 'setup-test-123', 'setup-user', 'setup-tenant', 'test-service', 'test@setup.com', 'setupuser', 'Setup User')
ON CONFLICT DO NOTHING;