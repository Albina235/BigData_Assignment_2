import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

DATA_DIR = 'data'
OUTPUT_DIR = 'output/lakehouse_model'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def create_lakehouse():
    print("Creating Scalable Lakehouse Model (Parquet)...")
    
    df = pd.read_csv(f"{DATA_DIR}/events_cleaned.csv")
    
    top_categories = df['category_code'].value_counts().nlargest(5).index
    df_filtered = df[df['category_code'].isin(top_categories)]
    
    table = pa.Table.from_pandas(df_filtered)
    
    pq.write_to_dataset(
        table,
        root_path=OUTPUT_DIR,
        partition_cols=['category_code']
    )
    print(f"Model implemented at {OUTPUT_DIR}")

if __name__ == "__main__":
    create_lakehouse()