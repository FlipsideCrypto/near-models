{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} },
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
        sender,
        actions,
        contract_address,
        amount,
        burrow_lending_id AS fact_lending_burrow_id,
        token_address,
        inserted_timestamp,
        modified_timestamp
    FROM
        burrow
)
SELECT
    *
FROM
    FINAL