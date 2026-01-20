-- Create a sample table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Set REPLICA IDENTITY to FULL to get old values on UPDATE/DELETE
ALTER TABLE users REPLICA IDENTITY FULL;

-- Create publication for CDC
CREATE PUBLICATION cdc_publication FOR TABLE users;

-- Insert some initial data
INSERT INTO users (name, email, status) VALUES 
    ('Ashish Kumar', 'ashish@example.com', 'active'),
    ('John Doe', 'john@example.com', 'active');