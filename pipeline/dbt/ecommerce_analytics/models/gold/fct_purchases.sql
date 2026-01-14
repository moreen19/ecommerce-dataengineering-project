
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='append',
    partitioned_by=['purchase_date'],
    unique_key='transaction_id'
) }}

WITH raw_purchases AS (
    SELECT
        transaction_id,
        product_id,
        price,
        quantity,
        is_member,
        member_discount,
        add_supplement,
        supplement_price,
        transaction_time,
        ingestion_timestamp
    FROM {{ source('ecommerce_data_lake', 'purchases_iceberg') }} 
    
    {% if is_incremental() %}
      -- Only pull new records from the source to save compute
      WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

calculated_fields AS (
    SELECT
        *,
        (price * quantity) AS total_item_price,
        (price * quantity * member_discount) AS total_discount,
        (price * quantity * (1 - member_discount)) + supplement_price AS final_amount,
        DATE(transaction_time) AS purchase_date
    FROM raw_purchases
)

SELECT
    CAST(transaction_id AS STRING) AS transaction_id,
    product_id,
    purchase_date,
    final_amount,
    is_member,
    ingestion_timestamp
FROM calculated_fields