{{ config(
    materialized = 'incremental',
    unique_key = 'day',
    incremental_strategy = 'delete+insert',
    tags = ['atlas']
) }}

WITH date_range AS (
    SELECT
        date_day as day
    FROM {{ ref('silver__dates') }}
    WHERE
        {% if is_incremental() %}
            date_day >= SYSDATE() - INTERVAL '3 DAY'
        {% else %}
            date_day >= '2021-01-01' -- first day of data
        {% endif %}
    AND date_day <= SYSDATE()::DATE 
)


SELECT day,
       COUNT(DISTINCT tx_signer) - COUNT(DISTINCT CASE WHEN first_tx >= day - INTERVAL '30 Days' AND first_tx < day THEN wallet END) AS Returning_MAAs,
       COUNT(DISTINCT CASE WHEN first_tx >= day - INTERVAL '30 Days' AND first_tx < day THEN wallet END) AS New_MAAs,
       COUNT(DISTINCT tx_signer) AS MAAs
FROM date_range a
LEFT OUTER JOIN (SELECT DISTINCT tx_signer, block_timestamp::date AS active_day 
                 FROM {{ ref('silver__streamline_transactions_final') }}) b
             ON active_day >= day - INTERVAL '30 DAYS'
            AND active_day < day
JOIN (SELECT tx_signer AS wallet, MIN(block_timestamp) AS first_tx FROM {{ ref('silver__streamline_transactions_final') }} tr GROUP BY 1) c
  ON b.tx_signer = c.wallet
GROUP BY 1
ORDER BY 1 DESC


-- 4.20 Mins all
-- 2.5 mins incremental 
