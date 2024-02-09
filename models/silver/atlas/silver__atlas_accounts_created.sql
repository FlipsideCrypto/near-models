{{ config(
    materialized = 'incremental',
    unique_key = 'atlas_account_created_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['atlas']
) }}

WITH accts AS (

    SELECT
        receiver_id,
        block_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        receipt_succeeded
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if is_incremental() %}
                AND _modified_timestamp >= (
                    SELECT
                        MAX(_modified_timestamp) - INTERVAL '2 days'
                    FROM
                        {{ this }}
                )
            {% endif %}
        {% endif %}

        qualify ROW_NUMBER() over (
            PARTITION BY receiver_id
            ORDER BY
                block_timestamp
        ) = 1
),
FINAL AS (
    SELECT
        block_timestamp :: DATE AS "DAY",
        COUNT(*) AS wallets_created,
        MAX(_modified_timestamp) AS _modified_timestamp
    FROM
        accts
    GROUP BY
        1
)
SELECT
    day,
    wallets_created,
    _modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['DAY']
    ) }} AS atlas_account_created_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    DAY IS NOT NULL
