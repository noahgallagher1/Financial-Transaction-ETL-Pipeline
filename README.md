# Financial Transaction ETL Pipeline

A simple, production-ready ETL pipeline demonstrating core data engineering principles: extracting data from multiple sources, transforming it using dimensional modeling, and loading it into a data warehouse for analytics.

## 🎯 Project Overview

This project simulates a real-world scenario where transaction data from multiple sources needs to be consolidated into a data warehouse for business intelligence and reporting.

**Key Features:**
- ✅ Extract data from multiple CSV sources
- ✅ Transform using dimensional modeling (star schema)
- ✅ Data quality validation and logging
- ✅ Load into SQLite data warehouse
- ✅ Sample analytical queries
- ✅ Automated testing
- ✅ Clear documentation

## 🏗️ Architecture
```
CSV Sources → Extract → Transform → Validate → Load → SQLite Warehouse
                ↓          ↓           ↓
            Logging    Dimensional   Quality
                       Modeling      Checks
```

## 📊 Data Model

**Star Schema Design:**

**Fact Table:**
- `fact_transactions` - Transaction events with foreign keys to dimensions

**Dimension Tables:**
- `dim_customers` - Customer information
- `dim_merchants` - Merchant/vendor information  
- `dim_date` - Date dimension for time-based analysis
- `dim_transaction_type` - Transaction category lookup

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- pip

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/financial-etl-pipeline.git
cd financial-etl-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Generate sample data:
```bash
python generate_sample_data.py
```

4. Run the ETL pipeline:
```bash
python src/pipeline.py
```

5. Explore the results:
```bash
# Run sample analytics queries
python -c "from src.utils import run_sample_queries; run_sample_queries()"

# Or open the Jupyter notebook
jupyter notebook notebooks/data_exploration.ipynb
```

## 📁 Project Structure
```
financial-etl-pipeline/
├── src/
│   ├── extract.py        # Data extraction from CSV files
│   ├── transform.py      # Data transformations and dimensional modeling
│   ├── load.py          # Loading data into warehouse
│   ├── pipeline.py      # Main ETL orchestrator
│   └── utils.py         # Helper functions and data quality checks
├── data/
│   ├── raw/             # Source CSV files
│   └── warehouse/       # SQLite database
├── sql/
│   ├── create_tables.sql      # DDL for warehouse tables
│   └── analytics_queries.sql  # Sample analytical queries
├── tests/
│   └── test_pipeline.py # Unit tests
└── notebooks/
    └── data_exploration.ipynb # Analysis and validation
```

## 🔄 ETL Process

### 1. Extract
- Reads transaction, customer, and merchant data from CSV files
- Validates file existence and basic structure
- Logs extraction metrics (row counts, file sizes)

### 2. Transform
- Cleans and standardizes data
- Handles missing values and data type conversions
- Creates dimensional model (star schema)
- Generates surrogate keys
- Builds date dimension
- Implements data quality checks

### 3. Load
- Creates warehouse tables if they don't exist
- Inserts data using efficient bulk loading
- Handles duplicates and updates
- Logs load statistics

## 📈 Sample Analytics

The pipeline enables various analytical queries:
```sql
-- Daily transaction volume and revenue
SELECT 
    d.date,
    COUNT(*) as transaction_count,
    SUM(f.amount) as total_revenue
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.date
ORDER BY d.date;

-- Top customers by transaction volume
SELECT 
    c.customer_name,
    COUNT(*) as transaction_count,
    SUM(f.amount) as total_spent
FROM fact_transactions f
JOIN dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_name
ORDER BY total_spent DESC
LIMIT 10;
```

## ✅ Data Quality Checks

The pipeline includes automated data quality validations:
- Row count validation (source vs target)
- Null check for required fields
- Data type validation
- Referential integrity checks
- Date range validation
- Duplicate detection

## 🧪 Testing

Run the test suite:
```bash
pytest tests/
```

## 📊 Key Metrics

The pipeline tracks and logs:
- Extraction: Rows extracted, files processed, extraction time
- Transformation: Rows transformed, quality check results
- Loading: Rows inserted, load time, database size

## 🛠️ Technologies Used

- **Python 3.8+** - Core programming language
- **pandas** - Data manipulation and transformation
- **SQLite** - Data warehouse (easily swappable for PostgreSQL/Snowflake)
- **pytest** - Testing framework
- **Jupyter** - Data exploration and validation

## 📝 Future Enhancements

Potential improvements for v2:
- [ ] Add Apache Airflow for orchestration
- [ ] Implement dbt for transformation layer
- [ ] Add Great Expectations for data quality
- [ ] Dockerize the pipeline
- [ ] Add incremental loading
- [ ] Integrate with cloud data warehouse (Snowflake/BigQuery)
- [ ] Add data lineage tracking
- [ ] Build monitoring dashboard

## 👤 Author

Noah Gallagher
- GitHub: [@noahgallagher1](https://github.com/noahgallagher1)
- LinkedIn: [linkedin.com/in/noahgallagher](https://linkedin.com/in/noahgallagher)

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

This project demonstrates data engineering best practices including dimensional modeling, data quality validation, and clean code architecture.
