{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    tags = ['core'],
    cluster_by = ['block_timestamp']
) }}

WITH FINAL AS (

    SELECT
        *
    FROM
        {{ source(
            "chainwalkers",
            "near_blocks"
        ) }}
    WHERE
        {{ incremental_load_filter("ingested_at") }}
        qualify ROW_NUMBER() over (
            PARTITION BY block_id
            ORDER BY
                ingested_at DESC
        ) = 1
)
SELECT
    *
FROM
    FINAL
