{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} },
    tags = ['core']
) }}

WITH rainbow AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        destination_address,
        source_address,
        bridge,
        destination_chain,
        source_chain,
        receipt_succeeded,
        bridge_rainbow_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_rainbow') }}
)
SELECT
    *
FROM
    rainbow
