CREATE TABLE IF NOT EXISTS STOCK_DATA_INIT (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    adj_close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    file_name VARCHAR(255) NOT NULL
);
