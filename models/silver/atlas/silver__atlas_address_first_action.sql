{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['atlas']
) }}

WITH txs AS (

    SELECT
        tx_signer AS address,
        block_id,
        block_timestamp,
        tx_hash,
        COALESCE(
            _load_timestamp,
            _inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
             {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
),
FINAL AS (
    SELECT
        address,
        MIN(block_timestamp) AS first_tx_timestamp,
        MIN(block_id) AS first_tx_block_id,
        MIN(_inserted_timestamp) AS _inserted_timestamp
    FROM
        txs
    GROUP BY
        1
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY address
        ORDER BY
            first_tx_timestamp
    ) = 1