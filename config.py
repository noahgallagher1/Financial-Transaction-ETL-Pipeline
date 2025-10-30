"""
Configuration file for the ETL pipeline
"""
import os
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
SQL_DIR = PROJECT_ROOT / "sql"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

# Source file paths
TRANSACTIONS_FILE = RAW_DATA_DIR / "transactions.csv"
CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
MERCHANTS_FILE = RAW_DATA_DIR / "merchants.csv"

# Warehouse database
WAREHOUSE_DB = WAREHOUSE_DIR / "analytics.db"
WAREHOUSE_URL = f"sqlite:///{WAREHOUSE_DB}"

# ETL Configuration
BATCH_SIZE = 1000
LOG_LEVEL = "INFO"

# Data quality thresholds
MIN_ROWS_THRESHOLD = 0.95  # Alert if loaded rows < 95% of source rows
MAX_NULL_PERCENTAGE = 5  # Alert if any column has >5% nulls

# Date range for sample data generation
START_DATE = "2023-01-01"
END_DATE = "2024-12-31"
NUM_TRANSACTIONS = 10000
NUM_CUSTOMERS = 500
NUM_MERCHANTS = 100
