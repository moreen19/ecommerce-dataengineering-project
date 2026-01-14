import pandas as pd
from pyathena import connect
from sklearn.ensemble import IsolationForest
import numpy as np
import os
import io

# --- Configuration (Matches dbt/Athena setup) ---
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
S3_BUCKET = os.environ.get("S3_BUCKET", "de-ecommerce-lake-2025-moeandy") 
ATHENA_DATABASE = "ecommerce_data_lake"
ATHENA_WORKGROUP = "primary"
ATHENA_S3_STAGING_DIR = f"s3://{S3_BUCKET}/athena_query_results/"
OUTPUT_PATH = f"s3://{S3_BUCKET}/ml_results/anomaly_detections.csv"

def run_anomaly_detection():
    print("Connecting to Athena to fetch transformed data...")
    
    conn = connect(
        s3_staging_dir=ATHENA_S3_STAGING_DIR,
        region_name=AWS_REGION,
        work_group=ATHENA_WORKGROUP,
    )

    SQL_QUERY = f"""
    SELECT
        purchase_date,
        SUM(final_amount) as daily_total_sales,
        COUNT(transaction_id) as daily_transaction_count
    FROM {ATHENA_DATABASE}.fct_purchases
    GROUP BY purchase_date
    ORDER BY purchase_date
    """

    # ... (rest of the run_anomaly_detection function for IsolationForest remains the same) ...
    try:
        df = pd.read_sql(SQL_QUERY, conn)
    except Exception as e:
        print(f"Error fetching data from Athena. Ensure dbt model 'fct_purchases' exists. Error: {e}")
        return

    if df.empty:
        print("DataFrame is empty. Cannot run anomaly detection.")
        return

    X = np.log1p(df[['daily_total_sales', 'daily_transaction_count']].values)
    model = IsolationForest(random_state=42, contamination='auto', n_estimators=100)
    model.fit(X)

    df['anomaly_score'] = model.decision_function(X)
    df['is_anomaly'] = model.predict(X)
    df['is_anomaly'] = df['is_anomaly'].apply(lambda x: True if x == -1 else False)

    print(f"Detected {df['is_anomaly'].sum()} anomalies. Saving results to S3...")
    
    import s3fs
    fs = s3fs.S3FileSystem(key=os.environ.get("AWS_ACCESS_KEY_ID"), secret=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    
    with fs.open(OUTPUT_PATH, 'w') as f:
        df.to_csv(f, index=False)

    print(f"Anomaly results saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    run_anomaly_detection()