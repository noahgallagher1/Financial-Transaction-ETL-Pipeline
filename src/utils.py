"""
Utility functions for data quality checks and helpers
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
import config

logging.basicConfig(level=config.LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_row_counts(source_df: pd.DataFrame, target_table: str, engine) -> bool:
    """
    Validate that row counts match between source and target
    
    Args:
        source_df: Source DataFrame
        target_table: Target table name
        engine: SQLAlchemy engine
    
    Returns:
        True if validation passes
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}"))
        target_count = result.fetchone()[0]
    
    source_count = len(source_df)
    match_percentage = (target_count / source_count) * 100 if source_count > 0 else 0
    
    logger.info(f"Row count validation for {target_table}:")
    logger.info(f"  Source: {source_count:,} rows")
    logger.info(f"  Target: {target_count:,} rows")
    logger.info(f"  Match: {match_percentage:.1f}%")
    
    if match_percentage < config.MIN_ROWS_THRESHOLD * 100:
        logger.warning(f"⚠ Row count below threshold for {target_table}")
        return False
    
    logger.info(f"✓ Row count validation passed for {target_table}")
    return True

def check_null_values(df: pd.DataFrame, table_name: str) -> dict:
    """
    Check for null values in DataFrame
    
    Args:
        df: DataFrame to check
        table_name: Table name for logging
    
    Returns:
        Dictionary with null value statistics
    """
    null_counts = df.isnull().sum()
    null_percentages = (null_counts / len(df)) * 100
    
    issues = {}
    for col, pct in null_percentages.items():
        if pct > config.MAX_NULL_PERCENTAGE:
            issues[col] = pct
            logger.warning(f"⚠ {table_name}.{col} has {pct:.1f}% null values")
    
    if not issues:
        logger.info(f"✓ No significant null values in {table_name}")
    
    return issues

def run_data_quality_checks(data: dict):
    """
    Run comprehensive data quality checks
    
    Args:
        data: Dictionary with DataFrames
    """
    logger.info("="*60)
    logger.info("DATA QUALITY CHECKS")
    logger.info("="*60)
    
    engine = create_engine(config.WAREHOUSE_URL)
    
    # Check null values for each table
    for table_name, df in data.items():
        check_null_values(df, table_name)
    
    # Validate row counts
    tables_to_validate = ['dim_customers', 'dim_merchants', 'fact_transactions']
    for table_name in tables_to_validate:
        if table_name in data:
            validate_row_counts(data[table_name], table_name, engine)
    
    logger.info("\n✓ Data quality checks complete")

def run_sample_queries():
    """Run sample analytical queries"""
    logger.info("="*60)
    logger.info("RUNNING SAMPLE ANALYTICS")
    logger.info("="*60)
    
    engine = create_engine(config.WAREHOUSE_URL)
    
    queries = {
        "Total Transactions": """
            SELECT COUNT(*) as total_transactions,
                   SUM(amount) as total_revenue
            FROM fact_transactions
        """,
        
        "Transactions by Status": """
            SELECT status, COUNT(*) as count, SUM(amount) as total_amount
            FROM fact_transactions
            GROUP BY status
            ORDER BY count DESC
        """,
        
        "Top 5 Customers": """
            SELECT c.customer_name, 
                   COUNT(f.transaction_id) as transaction_count,
                   SUM(f.amount) as total_spent
            FROM fact_transactions f
            JOIN dim_customers c ON f.customer_key = c.customer_key
            GROUP BY c.customer_name
            ORDER BY total_spent DESC
            LIMIT 5
        """,
        
        "Monthly Transaction Volume": """
            SELECT d.year, d.month_name,
                   COUNT(f.transaction_id) as transaction_count,
                   ROUND(SUM(f.amount), 2) as total_revenue
            FROM fact_transactions f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month
            LIMIT 12
        """
    }
    
    with engine.connect() as conn:
        for name, query in queries.items():
            print(f"\n{name}:")
            print("-" * 60)
            result = pd.read_sql(query, conn)
            print(result.to_string(index=False))
    
    logger.info("\n✓ Sample analytics complete")

if __name__ == "__main__":
    run_sample_queries()
