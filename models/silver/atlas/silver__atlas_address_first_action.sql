{{ config(
    materialized = 'incremental',
    incremental_stratege = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
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
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS atlas_address_first_action_id,
    address,
    first_tx_timestamp,
    first_tx_block_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY address
        ORDER BY
            first_tx_timestamp
    ) = 1
