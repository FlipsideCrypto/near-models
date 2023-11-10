{{ config(
    materialized = 'incremental',
    unique_key = 'atlas_account_created_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['atlas']
) }}

WITH accts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        receipt_succeeded = TRUE
        AND {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_last_x_days(
                '_inserted_timestamp',
                2
            ) }}
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
        {{ dbt_utils.generate_surrogate_key(
            ['DAY']
        ) }} AS atlas_account_created_id,
        COUNT(*) AS wallets_created,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS invocation_id
    FROM
        accts
    GROUP BY
        1,
        2
)
SELECT
    *
FROM
    FINAL
WHERE
    DAY IS NOT NULL
