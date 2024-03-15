{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, LENDING' }} },
    tags = ['core']
) }}

WITH burrow AS (
    SELECT
        *
    FROM
        {{ ref('silver__burrow_lending') }}
),
FINAL AS (
    SELECT
        platform,
        tx_hash,
        block_id,
        block_timestamp,
        sender_id,
        actions,
        contract_address,
        token_address,
        amount_raw,
        burrow_lending_id AS fact_lending_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        burrow
)
SELECT
    *
FROM
    FINAL