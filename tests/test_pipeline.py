"""
Unit tests for ETL pipeline
"""
import pytest
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from src.extract import extract_csv
from src.transform import clean_data, build_date_dimension
from src.utils import check_null_values

def test_config_paths_exist():
    """Test that configured directories exist"""
    assert config.RAW_DATA_DIR.exists()
    assert config.WAREHOUSE_DIR.exists()

def test_clean_data_removes_duplicates():
    """Test that clean_data removes duplicate rows"""
    # Create DataFrame with duplicates
    df = pd.DataFrame({
        'id': [1, 2, 2, 3],
        'value': ['a', 'b', 'b', 'c']
    })
    
    cleaned = clean_data(df, 'test')
    
    assert len(cleaned) == 3
    assert not cleaned.duplicated().any()

def test_clean_data_strips_whitespace():
    """Test that clean_data strips whitespace"""
    df = pd.DataFrame({
        'name': ['  Alice  ', 'Bob  ', '  Charlie']
    })
    
    cleaned = clean_data(df, 'test')
    
    assert cleaned['name'].tolist() == ['Alice', 'Bob', 'Charlie']

def test_date_dimension_attributes():
    """Test that date dimension has correct attributes"""
    transactions = pd.DataFrame({
        'transaction_date': ['2024-01-01', '2024-01-02', '2024-01-03']
    })
    
    dim_date = build_date_dimension(transactions)
    
    required_columns = ['date_key', 'year', 'quarter', 'month', 'day', 
                       'day_of_week', 'day_name', 'is_weekend']
    
    for col in required_columns:
        assert col in dim_date.columns

def test_null_value_detection():
    """Test that null values are detected correctly"""
    df = pd.DataFrame({
        'col1': [1, 2, None, 4, 5] * 20,  # 20% nulls
        'col2': [1, 2, 3, 4, 5] * 20      # 0% nulls
    })
    
    issues = check_null_values(df, 'test')
    
    # col1 should be flagged (20% > 5% threshold)
    assert 'col1' in issues
    # col2 should not be flagged
    assert 'col2' not in issues

def test_data_generation_creates_files(tmp_path):
    """Test that data generation creates all required files"""
    # This is a placeholder - you'd need to mock the file paths
    # and run generate_sample_data.py
    pass

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
