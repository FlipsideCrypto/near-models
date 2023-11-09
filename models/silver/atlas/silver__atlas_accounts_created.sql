
{{ config(
    materialized = 'incremental',
    unique_key = 'atlas_account_created_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],    
    tags = ['atlas']
) }}

WITH ACCTS AS (
    SELECT
        *
    FROM {{ ref('silver__streamline_receipts_final') }}
    WHERE
        status_value NOT LIKE '%Failure%'
        AND 
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_last_x_days('_inserted_timestamp', 2) }}
        {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY receiver_id ORDER BY block_timestamp) = 1
),
DAILY_TOTALS AS (
    SELECT
        day,
        COUNT(*) AS wallets_created
    FROM ACCTS
    GROUP BY day
)
FINAL AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
          ['receiver_id']
        ) }} AS atlas_account_created_id,
        block_timestamp::date AS day,
        COUNT(*) AS wallets_created,
        SUM(count(*)) OVER (ORDER BY day) total_wallets,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS invocation_id
    FROM ACCTS
    GROUP BY 1
)

SELECT  
    * 
FROM FINAL
