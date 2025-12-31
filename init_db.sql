CREATE TABLE IF NOT EXISTS fraud_scores (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    score DECIMAL(10, 6) NOT NULL,
    fraud_flag INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transaction_id ON fraud_scores(transaction_id);
CREATE INDEX IF NOT EXISTS idx_fraud_flag ON fraud_scores(fraud_flag);
CREATE INDEX IF NOT EXISTS idx_created_at ON fraud_scores(created_at);

