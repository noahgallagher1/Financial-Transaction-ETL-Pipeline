"""
Load module - loads data into SQLite warehouse
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
import config

logging.basicConfig(level=config.LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database_engine():
    """Create SQLAlchemy engine for warehouse"""
    return create_engine(config.WAREHOUSE_URL)

def create_tables(engine):
    """
    Create warehouse tables if they don't exist
    
    Args:
        engine: SQLAlchemy engine
    """
    logger.info("Creating warehouse tables...")
    
    # Read SQL from file
    sql_file = config.SQL_DIR / "create_tables.sql"
    
    if sql_file.exists():
        with open(sql_file, 'r') as f:
            sql_script = f.read()
        
        # Execute SQL statements
        with engine.connect() as conn:
            for statement in sql_script.split(';'):
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()
        
        logger.info("✓ Warehouse tables created/verified")
    else:
        logger.warning(f"SQL file not found: {sql_file}")

def load_table(df: pd.DataFrame, table_name: str, engine, if_exists='replace'):
    """
    Load DataFrame into warehouse table
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        engine: SQLAlchemy engine
        if_exists: How to handle existing table ('replace', 'append', 'fail')
    """
    try:
        logger.info(f"Loading {table_name}...")
        
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            chunksize=config.BATCH_SIZE
        )
        
        logger.info(f"✓ Loaded {len(df):,} rows into {table_name}")
        
    except Exception as e:
        logger.error(f"✗ Failed to load {table_name}: {str(e)}")
        raise

def load_all(data: dict):
    """
    Load all tables into warehouse
    
    Args:
        data: Dictionary with DataFrames to load
    """
    logger.info("="*60)
    logger.info("LOAD PHASE: Loading data into warehouse")
    logger.info("="*60)
    
    engine = create_database_engine()
    
    # Create tables
    create_tables(engine)
    
    # Load tables in order (dimensions first, then facts)
    load_order = [
        'dim_customers',
        'dim_merchants',
        'dim_date',
        'dim_transaction_type',
        'fact_transactions'
    ]
    
    for table_name in load_order:
        if table_name in data:
            load_table(data[table_name], table_name, engine)
    
    logger.info(f"\n✓ Load complete - {len(data)} tables loaded")
    logger.info(f"Database location: {config.WAREHOUSE_DB}")

if __name__ == "__main__":
    # Test loading
    from extract import extract_all
    from transform import transform_all
    
    data = extract_all()
    transformed = transform_all(data)
    load_all(transformed)
