{% set risk_level = var('risk_level', 'high') %}
{% set amount_threshold = 10000 if risk_level == 'high' else 5000 %}

WITH account_stats AS (
    SELECT
        account_id,
        COUNT(*)                                        AS total_transactions,
        AVG(amount)                                     AS avg_amount,
        SUM(amount)                                     AS total_amount,
        SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)    AS flagged_count,
        COUNT(DISTINCT country_code)                    AS countries_used,
        COUNT(DISTINCT merchant_category)               AS merchant_diversity
    FROM {{ ref('stg_transactions') }}
    GROUP BY account_id
),

transaction_risk AS (
    SELECT
        t.*,
        s.total_transactions,
        s.avg_amount,
        s.flagged_count,
        s.countries_used,
        CASE
            WHEN t.amount > s.avg_amount * 5   THEN 5
            WHEN t.amount > s.avg_amount * 3   THEN 4
            WHEN t.amount > s.avg_amount * 1.5 THEN 3
            WHEN t.amount > s.avg_amount       THEN 2
            ELSE 1
        END AS velocity_score,
        CASE
            WHEN t.country_code IN ('NG','RU','KP','IR') THEN 3
            WHEN s.countries_used > 3                    THEN 2
            ELSE 1
        END AS geo_risk_score,
        CASE
            WHEN t.merchant_category = 'gambling'            THEN 3
            WHEN t.merchant_category IN ('crypto','unknown') THEN 2
            ELSE 1
        END AS merchant_risk_score
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN account_stats s USING (account_id)
)

SELECT
    *,
    LEAST(5, GREATEST(1, ROUND(
        (velocity_score + geo_risk_score + merchant_risk_score) / 3.0
    ))) AS risk_score,
    CASE
        WHEN amount > {{ amount_threshold }} THEN TRUE
        ELSE FALSE
    END AS is_high_risk_flag
FROM transaction_risk
