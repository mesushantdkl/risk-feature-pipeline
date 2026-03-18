SELECT
    transaction_id,
    account_id,
    amount,
    LOWER(transaction_type)  AS transaction_type,
    LOWER(merchant_category) AS merchant_category,
    UPPER(country_code)      AS country_code,
    is_flagged,
    created_at,
    DATE_TRUNC('day', created_at) AS transaction_date
FROM {{ source('risk_raw', 'raw_transactions') }}
WHERE transaction_id IS NOT NULL
