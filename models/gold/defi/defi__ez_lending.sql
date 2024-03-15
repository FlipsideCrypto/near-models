{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} },
    tags = ['core']
) }}

WITH fact AS (

    SELECT
        platform,
        tx_hash,
        block_id,
        block_timestamp,
        sender_id,
        actions,
        contract_address,
        amount,
        fact_lending_burrow_id AS ez_lending_id,
        token_address,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('defi__fact_lending') }}
),
labels AS (
TBD
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
