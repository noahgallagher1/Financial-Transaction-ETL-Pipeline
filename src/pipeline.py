"""
Main ETL pipeline orchestrator
Coordinates extract, transform, and load phases
"""
import logging
from datetime import datetime
import config
from extract import extract_all
from transform import transform_all
from load import load_all
from utils import run_data_quality_checks

logging.basicConfig(
    level=config.LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_pipeline():
    """
    Execute the complete ETL pipeline
    """
    start_time = datetime.now()
    
    logger.info("\n" + "="*60)
    logger.info("STARTING ETL PIPELINE")
    logger.info("="*60)
    logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Extract phase
        source_data = extract_all()
        
        # Transform phase
        transformed_data = transform_all(source_data)
        
        # Load phase
        load_all(transformed_data)
        
        # Data quality checks
        run_data_quality_checks(transformed_data)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "="*60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Database: {config.WAREHOUSE_DB}")
        logger.info("\nRun 'python run_analytics.py' to see sample analytics")
        
        return True
        
    except Exception as e:
        logger.error(f"\n✗ ETL Pipeline failed: {str(e)}")
        logger.exception("Full error traceback:")
        return False

if __name__ == "__main__":
    success = run_pipeline()
    exit(0 if success else 1)
