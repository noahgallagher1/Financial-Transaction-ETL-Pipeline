"""
Extract module - reads data from source CSV files
"""
import pandas as pd
import logging
from pathlib import Path
import config

logging.basicConfig(level=config.LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_csv(file_path: Path, name: str) -> pd.DataFrame:
    """
    Extract data from a CSV file
    
    Args:
        file_path: Path to CSV file
        name: Name of the dataset (for logging)
    
    Returns:
        pandas DataFrame with the extracted data
    """
    try:
        logger.info(f"Extracting {name} from {file_path}")
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        df = pd.read_csv(file_path)
        logger.info(f"✓ Extracted {len(df):,} rows from {name}")
        
        return df
    
    except Exception as e:
        logger.error(f"✗ Failed to extract {name}: {str(e)}")
        raise

def extract_all() -> dict:
    """
    Extract all source data
    
    Returns:
        Dictionary with DataFrames for each source
    """
    logger.info("="*60)
    logger.info("EXTRACT PHASE: Loading source data")
    logger.info("="*60)
    
    data = {}
    
    try:
        data['transactions'] = extract_csv(config.TRANSACTIONS_FILE, 'transactions')
        data['customers'] = extract_csv(config.CUSTOMERS_FILE, 'customers')
        data['merchants'] = extract_csv(config.MERCHANTS_FILE, 'merchants')
        
        logger.info(f"\n✓ Extraction complete - Total rows: {sum(len(df) for df in data.values()):,}")
        return data
    
    except Exception as e:
        logger.error(f"✗ Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Test extraction
    data = extract_all()
    print("\nExtracted data shapes:")
    for name, df in data.items():
        print(f"  {name}: {df.shape}")
