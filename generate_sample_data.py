"""
Generate sample data for the ETL pipeline
Creates realistic transaction, customer, and merchant data
"""
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import config

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

def generate_customers(num_customers=500):
    """Generate customer dimension data"""
    print(f"Generating {num_customers} customers...")
    
    customers = []
    for i in range(1, num_customers + 1):
        customers.append({
            'customer_id': i,
            'customer_name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'registration_date': fake.date_between(start_date='-3y', end_date='today'),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic'])
        })
    
    df = pd.DataFrame(customers)
    df.to_csv(config.CUSTOMERS_FILE, index=False)
    print(f"✓ Created {config.CUSTOMERS_FILE}")
    return df

def generate_merchants(num_merchants=100):
    """Generate merchant dimension data"""
    print(f"Generating {num_merchants} merchants...")
    
    categories = ['Grocery', 'Restaurant', 'Gas Station', 'Retail', 'Entertainment', 
                  'Travel', 'Healthcare', 'Utilities', 'Online', 'Other']
    
    merchants = []
    for i in range(1, num_merchants + 1):
        merchants.append({
            'merchant_id': i,
            'merchant_name': fake.company(),
            'category': random.choice(categories),
            'city': fake.city(),
            'state': fake.state_abbr()
        })
    
    df = pd.DataFrame(merchants)
    df.to_csv(config.MERCHANTS_FILE, index=False)
    print(f"✓ Created {config.MERCHANTS_FILE}")
    return df

def generate_transactions(num_transactions=10000, customers_df=None, merchants_df=None):
    """Generate transaction fact data"""
    print(f"Generating {num_transactions} transactions...")
    
    start_date = datetime.strptime(config.START_DATE, '%Y-%m-%d')
    end_date = datetime.strptime(config.END_DATE, '%Y-%m-%d')
    date_range = (end_date - start_date).days
    
    transaction_types = ['Purchase', 'Refund', 'Payment', 'Transfer']
    statuses = ['Completed', 'Pending', 'Failed']
    
    transactions = []
    for i in range(1, num_transactions + 1):
        transaction_date = start_date + timedelta(days=random.randint(0, date_range))
        
        # Weighted transaction amounts (more small transactions, fewer large ones)
        if random.random() < 0.7:
            amount = round(random.uniform(5, 100), 2)
        elif random.random() < 0.9:
            amount = round(random.uniform(100, 500), 2)
        else:
            amount = round(random.uniform(500, 5000), 2)
        
        transactions.append({
            'transaction_id': i,
            'transaction_date': transaction_date.strftime('%Y-%m-%d'),
            'transaction_time': f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            'customer_id': random.choice(customers_df['customer_id'].tolist()),
            'merchant_id': random.choice(merchants_df['merchant_id'].tolist()),
            'amount': amount,
            'transaction_type': random.choice(transaction_types),
            'status': random.choices(statuses, weights=[0.85, 0.10, 0.05])[0],
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'Cash', 'Digital Wallet'])
        })
    
    df = pd.DataFrame(transactions)
    df.to_csv(config.TRANSACTIONS_FILE, index=False)
    print(f"✓ Created {config.TRANSACTIONS_FILE}")
    return df

def main():
    """Generate all sample data"""
    print("="*60)
    print("Generating Sample Data for ETL Pipeline")
    print("="*60)
    
    # Generate dimension data
    customers_df = generate_customers(config.NUM_CUSTOMERS)
    merchants_df = generate_merchants(config.NUM_MERCHANTS)
    
    # Generate fact data
    transactions_df = generate_transactions(
        config.NUM_TRANSACTIONS, 
        customers_df, 
        merchants_df
    )
    
    print("\n" + "="*60)
    print("Sample Data Generation Complete!")
    print("="*60)
    print(f"Customers:    {len(customers_df):,} records")
    print(f"Merchants:    {len(merchants_df):,} records")
    print(f"Transactions: {len(transactions_df):,} records")
    print("\nRun 'python src/pipeline.py' to execute the ETL pipeline")

if __name__ == "__main__":
    main()
