SELECT
    transaction_id,
    account_id,
    risk_score,
    created_at,
    CONCAT(
        'Transaction ', transaction_id,
        ' from account ', account_id,
        ' processed a ', transaction_type,
        ' of $', ROUND(amount, 2),
        ' in ', merchant_category, ' (', country_code, '). ',
        'Risk score: ', risk_score, '/5. ',
        'Velocity score: ', velocity_score, '. ',
        'Geographic risk: ', geo_risk_score, '. ',
        'Merchant risk: ', merchant_risk_score, '. ',
        CASE WHEN is_flagged THEN 'FLAGGED for review. ' ELSE '' END,
        'Account avg transaction: $', ROUND(avg_amount, 2), '. ',
        'Total flagged this account: ', flagged_count, '.'
    ) AS feature_text,
    velocity_score,
    geo_risk_score,
    merchant_risk_score,
    flagged_count,
    countries_used,
    is_high_risk_flag
FROM {{ ref('int_risk_scores') }}
