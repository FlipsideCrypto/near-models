{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert',
    tags = ['core', 'transactions'],
    cluster_by = ['block_timestamp']
) }}

WITH FINAL AS (

    SELECT
        *
    FROM
        {{ source(
            "chainwalkers",
            "near_txs"
        ) }}
    WHERE
        {{ incremental_load_filter("ingested_at") }}
        qualify ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                ingested_at DESC
        ) = 1
)
SELECT
    *
FROM
    FINAL
