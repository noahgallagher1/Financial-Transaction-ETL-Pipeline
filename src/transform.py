"""
Transform module - cleans data and creates dimensional model
"""
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import config

logging.basicConfig(level=config.LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_data(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """
    Clean and standardize data
    
    Args:
        df: Input DataFrame
        name: Dataset name for logging
    
    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Cleaning {name}...")
    
    initial_rows = len(df)
    
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Trim whitespace from string columns
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        df[col] = df[col].str.strip()
    
    rows_removed = initial_rows - len(df)
    if rows_removed > 0:
        logger.info(f"  Removed {rows_removed} duplicate rows")
    
    logger.info(f"✓ Cleaned {name} - {len(df):,} rows remaining")
    return df

def build_date_dimension(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build date dimension from transaction dates
    
    Args:
        transactions_df: Transactions DataFrame
    
    Returns:
        Date dimension DataFrame
    """
    logger.info("Building date dimension...")
    
    # Get unique dates from transactions
    dates = pd.to_datetime(transactions_df['transaction_date']).unique()
    dates = pd.to_datetime(dates).sort_values()
    
    # Create date dimension
    dim_date = pd.DataFrame({
        'date': dates
    })
    
    # Add date attributes
    dim_date['date_key'] = range(1, len(dim_date) + 1)
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['quarter'] = dim_date['date'].dt.quarter
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['month_name'] = dim_date['date'].dt.month_name()
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['day_of_week'] = dim_date['date'].dt.dayofweek
    dim_date['day_name'] = dim_date['date'].dt.day_name()
    dim_date['is_weekend'] = dim_date['day_of_week'].isin([5, 6]).astype(int)
    
    logger.info(f"✓ Created date dimension with {len(dim_date):,} dates")
    return dim_date

def build_transaction_type_dimension() -> pd.DataFrame:
    """
    Build transaction type dimension
    
    Returns:
        Transaction type dimension DataFrame
    """
    logger.info("Building transaction type dimension...")
    
    transaction_types = [
        {'transaction_type': 'Purchase', 'description': 'Standard purchase transaction'},
        {'transaction_type': 'Refund', 'description': 'Refund transaction'},
        {'transaction_type': 'Payment', 'description': 'Payment transaction'},
        {'transaction_type': 'Transfer', 'description': 'Transfer transaction'}
    ]
    
    dim_transaction_type = pd.DataFrame(transaction_types)
    dim_transaction_type['transaction_type_key'] = range(1, len(dim_transaction_type) + 1)
    
    logger.info(f"✓ Created transaction type dimension with {len(dim_transaction_type):,} types")
    return dim_transaction_type

def transform_dimensions(data: dict) -> dict:
    """
    Transform source data into dimension tables
    
    Args:
        data: Dictionary with source DataFrames
    
    Returns:
        Dictionary with dimension DataFrames
    """
    logger.info("="*60)
    logger.info("TRANSFORM PHASE: Building dimensional model")
    logger.info("="*60)
    
    dimensions = {}
    
    # Clean source data
    customers = clean_data(data['customers'], 'customers')
    merchants = clean_data(data['merchants'], 'merchants')
    transactions = clean_data(data['transactions'], 'transactions')
    
    # Build dimension: Customers
    logger.info("Building customer dimension...")
    dim_customers = customers.copy()
    dim_customers['customer_key'] = range(1, len(dim_customers) + 1)
    dim_customers['load_date'] = datetime.now().strftime('%Y-%m-%d')
    dimensions['dim_customers'] = dim_customers
    logger.info(f"✓ Created customer dimension - {len(dim_customers):,} rows")
    
    # Build dimension: Merchants
    logger.info("Building merchant dimension...")
    dim_merchants = merchants.copy()
    dim_merchants['merchant_key'] = range(1, len(dim_merchants) + 1)
    dim_merchants['load_date'] = datetime.now().strftime('%Y-%m-%d')
    dimensions['dim_merchants'] = dim_merchants
    logger.info(f"✓ Created merchant dimension - {len(dim_merchants):,} rows")
    
    # Build dimension: Date
    dimensions['dim_date'] = build_date_dimension(transactions)
    
    # Build dimension: Transaction Type
    dimensions['dim_transaction_type'] = build_transaction_type_dimension()
    
    logger.info(f"\n✓ Created {len(dimensions)} dimension tables")
    return dimensions, transactions

def transform_facts(transactions: pd.DataFrame, dimensions: dict) -> pd.DataFrame:
    """
    Transform transactions into fact table with foreign keys
    
    Args:
        transactions: Transactions DataFrame
        dimensions: Dictionary with dimension DataFrames
    
    Returns:
        Fact table DataFrame
    """
    logger.info("Building fact table...")
    
    fact = transactions.copy()
    
    # Add foreign keys
    # Customer key
    customer_lookup = dimensions['dim_customers'][['customer_id', 'customer_key']].set_index('customer_id')['customer_key'].to_dict()
    fact['customer_key'] = fact['customer_id'].map(customer_lookup)
    
    # Merchant key
    merchant_lookup = dimensions['dim_merchants'][['merchant_id', 'merchant_key']].set_index('merchant_id')['merchant_key'].to_dict()
    fact['merchant_key'] = fact['merchant_id'].map(merchant_lookup)
    
    # Date key
    date_lookup = dimensions['dim_date'].copy()
    date_lookup['date'] = pd.to_datetime(date_lookup['date']).dt.strftime('%Y-%m-%d')
    date_lookup = date_lookup.set_index('date')['date_key'].to_dict()
    fact['date_key'] = fact['transaction_date'].map(date_lookup)
    
    # Transaction type key
    type_lookup = dimensions['dim_transaction_type'][['transaction_type', 'transaction_type_key']].set_index('transaction_type')['transaction_type_key'].to_dict()
    fact['transaction_type_key'] = fact['transaction_type'].map(type_lookup)
    
    # Add load timestamp
    fact['load_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Select final columns for fact table
    fact_columns = [
        'transaction_id', 'date_key', 'customer_key', 'merchant_key', 
        'transaction_type_key', 'amount', 'status', 'payment_method',
        'transaction_time', 'load_timestamp'
    ]
    fact = fact[fact_columns]
    
    logger.info(f"✓ Created fact table - {len(fact):,} rows")
    
    return fact

def transform_all(data: dict) -> dict:
    """
    Execute all transformations
    
    Args:
        data: Dictionary with source DataFrames
    
    Returns:
        Dictionary with dimension and fact DataFrames
    """
    # Build dimensions
    dimensions, transactions = transform_dimensions(data)
    
    # Build fact table
    fact_transactions = transform_facts(transactions, dimensions)
    
    # Combine all tables
    result = {**dimensions, 'fact_transactions': fact_transactions}
    
    logger.info("\n✓ Transformation complete")
    return result

if __name__ == "__main__":
    # Test transformation
    from extract import extract_all
    
    data = extract_all()
    transformed = transform_all(data)
    
    print("\nTransformed table shapes:")
    for name, df in transformed.items():
        print(f"  {name}: {df.shape}")
