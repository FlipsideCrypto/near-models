{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['curated','scheduled_non_core']
) }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(DATE_TRUNC('hour', block_timestamp)) block_timestamp_hour
FROM
    {{ ref('silver__streamline_transactions_final') }} -- Streamline Migration TODO - change this to fact transactions once table
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_timestamp_hour = run_query(query).columns [0].values() [0] %}
{% endif %}
{% endif %}
SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS block_timestamp_hour,
    COUNT(
        DISTINCT tx_hash
    ) AS transaction_count,
    COUNT(
        DISTINCT CASE
            WHEN tx_succeeded THEN tx_hash
        END
    ) AS transaction_count_success,
    COUNT(
        DISTINCT CASE
            WHEN NOT tx_succeeded THEN tx_hash
        END
    ) AS transaction_count_failed,
    COUNT(
        DISTINCT tx_signer
    ) AS unique_from_count,
    COUNT(
        DISTINCT tx_receiver
    ) AS unique_to_count,
    SUM(transaction_fee / pow(10, 24)) AS total_fees,
    MAX(_inserted_timestamp) AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__streamline_transactions_final') }}  -- Streamline Migration TODO - change this to fact transactions once table
WHERE
    block_timestamp_hour < DATE_TRUNC(
        'hour',
        CURRENT_TIMESTAMP
    )

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
