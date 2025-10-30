-- Create dimension and fact tables for the data warehouse

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    registration_date DATE,
    customer_segment TEXT,
    load_date DATE
);

-- Dimension: Merchants
CREATE TABLE IF NOT EXISTS dim_merchants (
    merchant_key INTEGER PRIMARY KEY,
    merchant_id INTEGER NOT NULL,
    merchant_name TEXT NOT NULL,
    category TEXT,
    city TEXT,
    state TEXT,
    load_date DATE
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name TEXT,
    day INTEGER,
    day_of_week INTEGER,
    day_name TEXT,
    is_weekend INTEGER
);

-- Dimension: Transaction Type
CREATE TABLE IF NOT EXISTS dim_transaction_type (
    transaction_type_key INTEGER PRIMARY KEY,
    transaction_type TEXT NOT NULL,
    description TEXT
);

-- Fact Table: Transactions
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id INTEGER PRIMARY KEY,
    date_key INTEGER,
    customer_key INTEGER,
    merchant_key INTEGER,
    transaction_type_key INTEGER,
    amount REAL,
    status TEXT,
    payment_method TEXT,
    transaction_time TEXT,
    load_timestamp TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
    FOREIGN KEY (merchant_key) REFERENCES dim_merchants(merchant_key),
    FOREIGN KEY (transaction_type_key) REFERENCES dim_transaction_type(transaction_type_key)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_transactions(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_transactions(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_merchant ON fact_transactions(merchant_key);
CREATE INDEX IF NOT EXISTS idx_fact_type ON fact_transactions(transaction_type_key);
